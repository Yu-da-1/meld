//! Executes one assigned command as a native child process.

use std::{
    fs,
    future::pending,
    io,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use meld_core::{CapturedStream, ExecutionAssignment, ExecutionOutput, ExecutionResult};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, ChildStderr, ChildStdout, Command},
    sync::oneshot,
    time::sleep,
};

const MAX_CAPTURED_STREAM_BYTES: usize = 1024 * 1024;
const READ_BUFFER_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone)]
pub struct NativeExecutor {
    executions_directory: PathBuf,
}

impl NativeExecutor {
    pub fn new(state_directory: &Path) -> Self {
        Self {
            executions_directory: state_directory.join("executions"),
        }
    }

    pub fn start(&self, assignment: &ExecutionAssignment) -> io::Result<NativeExecution> {
        fs::create_dir_all(&self.executions_directory)?;
        let workspace = self
            .executions_directory
            .join(assignment.execution_id.to_string());
        fs::create_dir(&workspace)?;

        let mut command = Command::new(&assignment.spec.program);
        command
            .args(&assignment.spec.args)
            .current_dir(&workspace)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.as_std_mut().process_group(0);
        }

        match command.spawn() {
            Ok(mut child) => {
                let stdout = child
                    .stdout
                    .take()
                    .expect("piped stdout should be available");
                let stderr = child
                    .stderr
                    .take()
                    .expect("piped stderr should be available");
                Ok(NativeExecution {
                    child,
                    stdout: Some(stdout),
                    stderr: Some(stderr),
                    workspace,
                })
            }
            Err(error) => {
                if let Err(cleanup_error) = fs::remove_dir_all(&workspace) {
                    tracing::warn!(
                        path = %workspace.display(),
                        %cleanup_error,
                        "failed to clean workspace after process start failure"
                    );
                }
                Err(error)
            }
        }
    }
}

#[derive(Debug)]
pub struct NativeExecution {
    child: Child,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    workspace: PathBuf,
}

impl NativeExecution {
    #[cfg(test)]
    pub async fn wait(self) -> io::Result<CompletedExecution> {
        let (sender, receiver) = oneshot::channel();
        let outcome = self.wait_controlled(receiver, None).await;
        drop(sender);
        match outcome? {
            ControlledExecutionOutcome::Finished(completed) => Ok(completed),
            ControlledExecutionOutcome::Cancelled { .. }
            | ControlledExecutionOutcome::TimedOut { .. } => {
                unreachable!("wait without control cannot be interrupted")
            }
        }
    }

    pub async fn wait_controlled(
        mut self,
        mut cancellation: oneshot::Receiver<()>,
        timeout: Option<Duration>,
    ) -> io::Result<ControlledExecutionOutcome> {
        let stdout = self
            .stdout
            .take()
            .expect("stdout should only be consumed once");
        let stderr = self
            .stderr
            .take()
            .expect("stderr should only be consumed once");
        let stdout_task = tokio::spawn(capture_stream(stdout));
        let stderr_task = tokio::spawn(capture_stream(stderr));
        let timeout_future = async {
            match timeout {
                Some(duration) => sleep(duration).await,
                None => pending::<()>().await,
            }
        };
        tokio::pin!(timeout_future);

        enum Termination {
            Finished(ExecutionResult),
            Cancelled,
            TimedOut,
        }

        let termination = tokio::select! {
            biased;
            status = self.child.wait() => Termination::Finished(ExecutionResult {
                exit_code: status?.code(),
            }),
            _ = &mut cancellation => Termination::Cancelled,
            _ = &mut timeout_future => Termination::TimedOut,
        };
        if !matches!(termination, Termination::Finished(_)) {
            terminate_child(&mut self.child).await?;
        }
        let stdout = stdout_task
            .await
            .map_err(|error| io::Error::other(format!("stdout capture task failed: {error}")))??;
        let stderr = stderr_task
            .await
            .map_err(|error| io::Error::other(format!("stderr capture task failed: {error}")))??;
        let output = ExecutionOutput { stdout, stderr };

        if let Err(error) = fs::remove_dir_all(&self.workspace) {
            tracing::warn!(
                path = %self.workspace.display(),
                %error,
                "failed to clean execution workspace"
            );
        }

        Ok(match termination {
            Termination::Finished(result) => {
                ControlledExecutionOutcome::Finished(CompletedExecution { result, output })
            }
            Termination::Cancelled => ControlledExecutionOutcome::Cancelled { output },
            Termination::TimedOut => ControlledExecutionOutcome::TimedOut { output },
        })
    }

    #[cfg(test)]
    fn workspace(&self) -> &Path {
        &self.workspace
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedExecution {
    pub result: ExecutionResult,
    pub output: ExecutionOutput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlledExecutionOutcome {
    Finished(CompletedExecution),
    Cancelled { output: ExecutionOutput },
    TimedOut { output: ExecutionOutput },
}

async fn capture_stream(mut stream: impl AsyncRead + Unpin) -> io::Result<CapturedStream> {
    let mut captured = Vec::new();
    let mut buffer = vec![0; READ_BUFFER_BYTES];
    let mut truncated = false;

    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }

        let remaining = MAX_CAPTURED_STREAM_BYTES.saturating_sub(captured.len());
        let retained = remaining.min(read);
        captured.extend_from_slice(&buffer[..retained]);
        truncated |= retained < read;
    }

    let (content, lossy) = match String::from_utf8(captured) {
        Ok(content) => (content, false),
        Err(error) => (String::from_utf8_lossy(error.as_bytes()).into_owned(), true),
    };
    Ok(CapturedStream {
        content,
        truncated,
        lossy,
    })
}

impl Drop for NativeExecution {
    fn drop(&mut self) {
        #[cfg(unix)]
        let _ = signal_process_group(&self.child);
        let _ = self.child.start_kill();
        if self.workspace.exists()
            && let Err(error) = fs::remove_dir_all(&self.workspace)
        {
            tracing::warn!(
                path = %self.workspace.display(),
                %error,
                "failed to clean dropped execution workspace"
            );
        }
    }
}

async fn terminate_child(child: &mut Child) -> io::Result<()> {
    #[cfg(unix)]
    signal_process_group(child)?;
    #[cfg(not(unix))]
    child.start_kill()?;

    child.wait().await.map(|_| ())
}

#[cfg(unix)]
fn signal_process_group(child: &Child) -> io::Result<()> {
    let process_id = child
        .id()
        .ok_or_else(|| io::Error::other("child process has no operating system ID"))?;
    let process_group =
        i32::try_from(process_id).map_err(|_| io::Error::other("child process ID exceeds i32"))?;
    // SAFETY: the negative, non-zero PID addresses only the process group
    // created for this execution. SIGKILL requires no borrowed memory.
    let result = unsafe { libc::kill(-process_group, libc::SIGKILL) };
    if result == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use meld_core::{ExecutionId, JobId, JobSpec, NodeId, ResourceRequirements};
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn native_process_runs_in_an_isolated_workspace_and_cleans_it() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let assignment = assignment("rustc", vec!["--version"]);

        let execution = executor
            .start(&assignment)
            .expect("installed Rust compiler should start");
        let workspace = execution.workspace().to_owned();
        assert!(workspace.is_dir());

        let result = execution.wait().await.expect("process wait should succeed");

        assert_eq!(result.result.exit_code, Some(0));
        assert!(result.output.stdout.content.contains("rustc"));
        assert!(!result.output.stdout.truncated);
        assert!(!result.output.stdout.lossy);
        assert!(!workspace.exists());
    }

    #[test]
    fn process_start_failure_removes_the_workspace() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let assignment = assignment("meld-command-that-does-not-exist", Vec::new());
        let workspace = state_directory
            .path()
            .join("executions")
            .join(assignment.execution_id.to_string());

        executor
            .start(&assignment)
            .expect_err("missing executable must fail to start");

        assert!(!workspace.exists());
    }

    #[tokio::test]
    async fn stream_capture_is_bounded_and_drains_remaining_bytes() {
        let input = vec![b'x'; MAX_CAPTURED_STREAM_BYTES + 17];

        let captured = capture_stream(input.as_slice())
            .await
            .expect("in-memory stream should be readable");

        assert_eq!(captured.content.len(), MAX_CAPTURED_STREAM_BYTES);
        assert!(captured.truncated);
        assert!(!captured.lossy);
    }

    #[tokio::test]
    async fn invalid_utf8_is_marked_as_lossy() {
        let input = [b'a', 0xff, b'b'];

        let captured = capture_stream(input.as_slice())
            .await
            .expect("in-memory stream should be readable");

        assert_eq!(captured.content, "a\u{fffd}b");
        assert!(!captured.truncated);
        assert!(captured.lossy);
    }

    #[tokio::test]
    async fn cancellation_stops_process_and_cleans_workspace() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let executable = std::env::current_exe().expect("test executable path should be available");
        let assignment = assignment(
            executable
                .to_str()
                .expect("test executable path should be Unicode"),
            vec![
                "--ignored",
                "--exact",
                "executor::tests::long_running_child",
            ],
        );
        let execution = executor
            .start(&assignment)
            .expect("test child should start");
        let workspace = execution.workspace().to_owned();
        let (cancel, cancellation) = oneshot::channel();
        cancel
            .send(())
            .expect("cancellation receiver should be open");

        let outcome = execution
            .wait_controlled(cancellation, None)
            .await
            .expect("cancelled process should be reaped");

        assert!(matches!(
            outcome,
            ControlledExecutionOutcome::Cancelled { .. }
        ));
        assert!(!workspace.exists());
    }

    #[tokio::test]
    async fn timeout_stops_process_and_cleans_workspace() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let executable = std::env::current_exe().expect("test executable path should be available");
        let assignment = assignment(
            executable
                .to_str()
                .expect("test executable path should be Unicode"),
            vec![
                "--ignored",
                "--exact",
                "executor::tests::long_running_child",
            ],
        );
        let execution = executor
            .start(&assignment)
            .expect("test child should start");
        let workspace = execution.workspace().to_owned();
        let (_cancel, cancellation) = oneshot::channel();

        let outcome = execution
            .wait_controlled(cancellation, Some(Duration::from_millis(10)))
            .await
            .expect("timed out process should be reaped");

        assert!(matches!(
            outcome,
            ControlledExecutionOutcome::TimedOut { .. }
        ));
        assert!(!workspace.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_stops_descendant_processes_without_waiting_for_them() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let assignment = assignment("/bin/sh", vec!["-c", "sleep 10"]);
        let execution = executor.start(&assignment).expect("shell should start");
        let (_cancel, cancellation) = oneshot::channel();
        let started = std::time::Instant::now();

        let outcome = execution
            .wait_controlled(cancellation, Some(Duration::from_millis(10)))
            .await
            .expect("process group should be stopped");

        assert!(matches!(
            outcome,
            ControlledExecutionOutcome::TimedOut { .. }
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    #[ignore = "spawned explicitly by cancellation and timeout tests"]
    fn long_running_child() {
        std::thread::sleep(Duration::from_secs(30));
    }

    fn assignment(program: &str, args: Vec<&str>) -> ExecutionAssignment {
        ExecutionAssignment {
            execution_id: ExecutionId::generate(),
            job_id: JobId::generate(),
            node_id: NodeId::generate(),
            spec: JobSpec {
                program: program.to_owned(),
                args: args.into_iter().map(str::to_owned).collect(),
                requirements: ResourceRequirements {
                    logical_cpus: 1,
                    memory_bytes: 1,
                },
                job_timeout_secs: None,
                execution_timeout_secs: None,
                constraints: meld_core::PlacementConstraints::default(),
                data: meld_core::DataSpec::default(),
            },
        }
    }
}
