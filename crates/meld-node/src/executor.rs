//! Executes one assigned command as a native child process.

use std::{
    fs,
    future::pending,
    io,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use meld_core::{
    CapturedStream, ExecutionAssignment, ExecutionOutput, ExecutionResult, LimitBreach,
    create_private_directory,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, ChildStderr, ChildStdout, Command},
    sync::oneshot,
    time::{MissedTickBehavior, interval, sleep},
};

use crate::limits::{MemoryMeter, MemoryWatch};

const MAX_CAPTURED_STREAM_BYTES: usize = 1024 * 1024;
const READ_BUFFER_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone)]
pub struct NativeExecutor {
    executions_directory: PathBuf,
    /// Whether a job that uses more memory than it reserved is stopped.
    enforce_memory_limit: bool,
}

impl NativeExecutor {
    pub fn new(state_directory: &Path) -> Self {
        Self {
            executions_directory: state_directory.join("executions"),
            enforce_memory_limit: true,
        }
    }

    /// Turns the memory limit on or off for this node.
    ///
    /// It is on by default. An operator turns it off where the measurement is
    /// not trustworthy, such as for processes that use a GPU through a
    /// compatibility layer, which can look many times larger than they are.
    #[must_use]
    pub fn with_memory_limit(mut self, enforced: bool) -> Self {
        self.enforce_memory_limit = enforced;
        self
    }

    /// What to watch for an execution that reserved `reserved_bytes`, or
    /// `None` when this node does not enforce the limit.
    pub fn memory_watch(&self, reserved_bytes: u64) -> Option<MemoryWatch> {
        self.enforce_memory_limit.then(|| {
            MemoryWatch::for_reservation(reserved_bytes, crate::limits::MEMORY_CHECK_INTERVAL)
        })
    }

    /// Removes the workspaces an earlier run of this node left behind, and
    /// returns how many there were.
    ///
    /// A workspace is removed when its execution ends, but not when the node
    /// is killed or loses power while it runs. Nothing in this process can
    /// own one yet, and the controller already regards those executions as
    /// lost, so they are of no use to anyone and may hold large or private
    /// files. A process of such an execution that is still running is not
    /// found or stopped by this.
    pub fn remove_stale_workspaces(&self) -> io::Result<usize> {
        let entries = match fs::read_dir(&self.executions_directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error),
        };
        let mut removed = 0;
        for entry in entries {
            let path = entry?.path();
            // `symlink_metadata`, so a link is removed and never followed.
            let result = if fs::symlink_metadata(&path)?.is_dir() {
                fs::remove_dir_all(&path)
            } else {
                fs::remove_file(&path)
            };
            match result {
                Ok(()) => removed += 1,
                Err(error) => {
                    tracing::warn!(path = %path.display(), %error, "failed to remove a stale workspace");
                }
            }
        }
        Ok(removed)
    }

    /// Creates the empty directory the process will run in.
    pub fn prepare(&self, assignment: &ExecutionAssignment) -> io::Result<Workspace> {
        create_private_directory(&self.executions_directory)?;
        let path = self
            .executions_directory
            .join(assignment.execution_id.to_string());
        fs::create_dir(&path)?;
        Ok(Workspace { path })
    }

    /// Starts the process in `workspace`, which the execution then owns.
    ///
    /// A process that fails to start takes the workspace with it.
    pub fn start(
        &self,
        workspace: Workspace,
        assignment: &ExecutionAssignment,
    ) -> io::Result<NativeExecution> {
        let mut command = Command::new(&assignment.spec.program);
        command
            .args(&assignment.spec.args)
            .current_dir(workspace.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        withhold_meld_environment(&mut command);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.as_std_mut().process_group(0);
        }

        let mut child = command.spawn()?;
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
            workspace: Some(workspace),
        })
    }
}

/// The directory an execution runs in, removed when dropped.
///
/// It outlives the process so that results can be read from it first.
#[derive(Debug)]
pub struct Workspace {
    path: PathBuf,
}

impl Workspace {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if self.path.exists()
            && let Err(error) = fs::remove_dir_all(&self.path)
        {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "failed to clean execution workspace"
            );
        }
    }
}

#[derive(Debug)]
pub struct NativeExecution {
    child: Child,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    workspace: Option<Workspace>,
}

impl NativeExecution {
    #[cfg(test)]
    pub async fn wait(self) -> io::Result<CompletedExecution> {
        let (sender, receiver) = oneshot::channel();
        let outcome = self.wait_controlled(receiver, None, None).await;
        drop(sender);
        match outcome?.0 {
            ControlledExecutionOutcome::Finished(completed) => Ok(completed),
            ControlledExecutionOutcome::Cancelled { .. }
            | ControlledExecutionOutcome::TimedOut { .. }
            | ControlledExecutionOutcome::LimitExceeded { .. } => {
                unreachable!("wait without control cannot be interrupted")
            }
        }
    }

    /// Waits for the process to end, or stops it: when cancelled, when
    /// `timeout` passes, or when `memory` says it has used more than it may.
    pub async fn wait_controlled(
        mut self,
        mut cancellation: oneshot::Receiver<()>,
        timeout: Option<Duration>,
        memory: Option<MemoryWatch>,
    ) -> io::Result<(ControlledExecutionOutcome, Workspace)> {
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
            LimitExceeded(LimitBreach),
        }

        let process_id = self.child.id();
        let mut meter = MemoryMeter::new();
        let mut checks = interval(memory.map_or(Duration::from_secs(1), |watch| watch.interval()));
        checks.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let termination = loop {
            tokio::select! {
                biased;
                status = self.child.wait() => break Termination::Finished(ExecutionResult {
                    exit_code: status?.code(),
                }),
                _ = &mut cancellation => break Termination::Cancelled,
                _ = &mut timeout_future => break Termination::TimedOut,
                _ = checks.tick(), if memory.is_some() => {
                    let breach = memory.zip(process_id).and_then(|(watch, process_id)| {
                        meter
                            .tree_bytes(process_id)
                            .and_then(|used_bytes| watch.breach(used_bytes))
                    });
                    if let Some(breach) = breach {
                        break Termination::LimitExceeded(breach);
                    }
                }
            }
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

        let outcome = match termination {
            Termination::Finished(result) => {
                ControlledExecutionOutcome::Finished(CompletedExecution { result, output })
            }
            Termination::Cancelled => ControlledExecutionOutcome::Cancelled { output },
            Termination::TimedOut => ControlledExecutionOutcome::TimedOut { output },
            Termination::LimitExceeded(breach) => {
                ControlledExecutionOutcome::LimitExceeded { output, breach }
            }
        };
        let workspace = self
            .workspace
            .take()
            .expect("workspace should only be taken once");
        Ok((outcome, workspace))
    }

    #[cfg(test)]
    fn workspace(&self) -> &Path {
        self.workspace
            .as_ref()
            .expect("workspace is held until the execution ends")
            .path()
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
    Cancelled {
        output: ExecutionOutput,
    },
    TimedOut {
        output: ExecutionOutput,
    },
    /// Stopped because it used more memory than it reserved.
    LimitExceeded {
        output: ExecutionOutput,
        breach: LimitBreach,
    },
}

/// Keeps the node's own configuration, tokens included, out of a job's
/// environment.
///
/// A child inherits the node's environment, so without this a job could read
/// `MELD_NODE_TOKEN` with `env`. Everything else is left as it is: a job still
/// needs `PATH`, `HOME` and the like.
fn withhold_meld_environment(command: &mut Command) {
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("MELD_") {
            command.env_remove(name);
        }
    }
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
        // The workspace field is dropped after this, once the process is gone.
        #[cfg(unix)]
        let _ = signal_process_group(&self.child);
        let _ = self.child.start_kill();
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

    use crate::limits::MemoryWatch;

    use super::*;

    #[tokio::test]
    async fn native_process_runs_in_an_isolated_workspace_and_cleans_it() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let assignment = assignment("rustc", vec!["--version"]);

        let execution = start_in_new_workspace(&executor, &assignment)
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

        start_in_new_workspace(&executor, &assignment)
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
        let execution =
            start_in_new_workspace(&executor, &assignment).expect("test child should start");
        let workspace = execution.workspace().to_owned();
        let (cancel, cancellation) = oneshot::channel();
        cancel
            .send(())
            .expect("cancellation receiver should be open");

        let (outcome, held) = execution
            .wait_controlled(cancellation, None, None)
            .await
            .expect("cancelled process should be reaped");

        assert!(matches!(
            outcome,
            ControlledExecutionOutcome::Cancelled { .. }
        ));
        assert!(workspace.exists(), "the caller still owns the workspace");
        drop(held);
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
        let execution =
            start_in_new_workspace(&executor, &assignment).expect("test child should start");
        let workspace = execution.workspace().to_owned();
        let (_cancel, cancellation) = oneshot::channel();

        let (outcome, held) = execution
            .wait_controlled(cancellation, Some(Duration::from_millis(10)), None)
            .await
            .expect("timed out process should be reaped");

        assert!(matches!(
            outcome,
            ControlledExecutionOutcome::TimedOut { .. }
        ));
        drop(held);
        assert!(!workspace.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_stops_descendant_processes_without_waiting_for_them() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let assignment = assignment("/bin/sh", vec!["-c", "sleep 10"]);
        let execution = start_in_new_workspace(&executor, &assignment).expect("shell should start");
        let (_cancel, cancellation) = oneshot::channel();
        let started = std::time::Instant::now();

        let (outcome, _held) = execution
            .wait_controlled(cancellation, Some(Duration::from_millis(10)), None)
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

    fn start_in_new_workspace(
        executor: &NativeExecutor,
        assignment: &ExecutionAssignment,
    ) -> io::Result<NativeExecution> {
        let workspace = executor.prepare(assignment)?;
        executor.start(workspace, assignment)
    }

    #[tokio::test]
    async fn process_runs_with_staged_files_in_its_working_directory() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let assignment = assignment("rustc", vec!["--version"]);
        let workspace = executor.prepare(&assignment).expect("workspace");
        fs::write(workspace.path().join("input.txt"), b"staged").expect("stage file");
        let marker = workspace.path().join("input.txt");

        let execution = executor
            .start(workspace, &assignment)
            .expect("process should start");

        assert_eq!(fs::read(&marker).expect("staged file"), b"staged");
        let (_cancel, cancellation) = oneshot::channel();
        let (_, held) = execution
            .wait_controlled(cancellation, None, None)
            .await
            .expect("wait");
        assert_eq!(fs::read(&marker).expect("kept until dropped"), b"staged");
        drop(held);
        assert!(!marker.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_job_does_not_see_the_nodes_meld_environment() {
        // SAFETY: the name is unique to this test, and nothing else reads it.
        unsafe { std::env::set_var("MELD_TEST_ONLY_SECRET", "hunter2-hunter2") };
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let assignment = assignment("env", Vec::new());

        let execution = start_in_new_workspace(&executor, &assignment).expect("env should start");
        let result = execution.wait().await.expect("process wait should succeed");

        let environment = result.output.stdout.content;
        assert_eq!(result.result.exit_code, Some(0));
        assert!(!environment.contains("MELD_"), "{environment}");
        assert!(!environment.contains("hunter2"), "{environment}");
        assert!(environment.contains("PATH="), "{environment}");
    }

    #[test]
    fn dropping_an_unstarted_workspace_removes_it() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let assignment = assignment("rustc", Vec::new());

        let workspace = executor.prepare(&assignment).expect("workspace");
        let path = workspace.path().to_owned();
        assert!(path.is_dir());
        drop(workspace);

        assert!(!path.exists());
    }

    /// A shell script that holds about 40 MB, then waits.
    #[cfg(unix)]
    const HOLD_MEMORY: &str = "x=$(head -c 40000000 /dev/zero | tr '\\0' x); sleep 30";

    #[cfg(unix)]
    async fn run_watched(script: &str, reserved_bytes: u64) -> ControlledExecutionOutcome {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let assignment = assignment("sh", vec!["-c", script]);
        let execution = start_in_new_workspace(&executor, &assignment).expect("sh should start");
        let (_cancel, cancellation) = oneshot::channel();
        let watch = MemoryWatch::for_reservation(reserved_bytes, Duration::from_millis(50));

        let started = std::time::Instant::now();
        let (outcome, _held) = tokio::time::timeout(
            Duration::from_secs(20),
            execution.wait_controlled(cancellation, None, Some(watch)),
        )
        .await
        .expect("the execution should end by itself")
        .expect("wait should succeed");
        // Stopped promptly, not left to run its 30 seconds.
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "took {:?}",
            started.elapsed()
        );
        outcome
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_process_over_its_reservation_is_stopped() {
        let outcome = run_watched(HOLD_MEMORY, 5 * 1024 * 1024).await;

        let ControlledExecutionOutcome::LimitExceeded { breach, .. } = outcome else {
            panic!("expected the process to be stopped, got {outcome:?}");
        };
        let LimitBreach::Memory {
            reserved_bytes,
            limit_bytes,
            used_bytes,
        } = breach;
        assert_eq!(reserved_bytes, 5 * 1024 * 1024);
        assert!(limit_bytes > reserved_bytes);
        assert!(used_bytes > limit_bytes, "{used_bytes} <= {limit_bytes}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn memory_held_by_a_child_counts_against_the_job() {
        // The parent holds almost nothing; the child holds the memory.
        let script = format!("( {HOLD_MEMORY} ) & wait");

        let outcome = run_watched(&script, 5 * 1024 * 1024).await;

        assert!(
            matches!(outcome, ControlledExecutionOutcome::LimitExceeded { .. }),
            "{outcome:?}"
        );
    }

    #[test]
    fn a_node_that_does_not_enforce_the_limit_watches_nothing() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let enforcing = NativeExecutor::new(state_directory.path());
        let relaxed = NativeExecutor::new(state_directory.path()).with_memory_limit(false);

        assert!(enforcing.memory_watch(1_000).is_some());
        assert!(relaxed.memory_watch(1_000).is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_process_within_its_reservation_runs_to_the_end() {
        let outcome = run_watched("sleep 1; echo done", 256 * 1024 * 1024).await;

        let ControlledExecutionOutcome::Finished(completed) = outcome else {
            panic!("expected the process to finish, got {outcome:?}");
        };
        assert_eq!(completed.result.exit_code, Some(0));
        assert_eq!(completed.output.stdout.content, "done\n");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_stopped_process_leaves_nothing_running() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let marker = state_directory.path().join("alive");
        // Writes a file every moment, for as long as it runs.
        let script = format!(
            "{HOLD_MEMORY} & while true; do date >> '{}'; sleep 0.1; done",
            marker.display()
        );
        let assignment = assignment("sh", vec!["-c", &script]);
        let execution = start_in_new_workspace(&executor, &assignment).expect("sh should start");
        let (_cancel, cancellation) = oneshot::channel();
        let watch = MemoryWatch::for_reservation(5 * 1024 * 1024, Duration::from_millis(50));

        // Bounded, so a watch that never fires fails this test instead of
        // leaving a script running that never ends.
        let (outcome, _held) = tokio::time::timeout(
            Duration::from_secs(20),
            execution.wait_controlled(cancellation, None, Some(watch)),
        )
        .await
        .expect("the execution should be stopped")
        .expect("wait should succeed");
        assert!(matches!(
            outcome,
            ControlledExecutionOutcome::LimitExceeded { .. }
        ));

        let size = fs::metadata(&marker).map(|meta| meta.len()).unwrap_or(0);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let later = fs::metadata(&marker).map(|meta| meta.len()).unwrap_or(0);
        assert_eq!(size, later, "the process group kept running");
    }

    #[test]
    fn workspaces_left_by_an_earlier_run_are_removed_and_nothing_else() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let executions = state_directory.path().join("executions");
        for name in ["one", "two"] {
            fs::create_dir_all(executions.join(name).join("deep")).expect("stale workspace");
            fs::write(executions.join(name).join("deep/file"), b"left behind").expect("file");
        }
        fs::write(executions.join("stray-file"), b"x").expect("stray file");
        // Something that is not the node's to remove stays where it is.
        let elsewhere = state_directory.path().join("cache");
        fs::create_dir_all(&elsewhere).expect("cache directory");
        fs::write(elsewhere.join("kept"), b"x").expect("kept file");

        let removed = executor
            .remove_stale_workspaces()
            .expect("stale workspaces are removed");

        assert_eq!(removed, 3);
        assert_eq!(fs::read_dir(&executions).unwrap().count(), 0);
        assert!(elsewhere.join("kept").exists());
    }

    #[test]
    fn there_is_nothing_to_remove_before_the_first_execution() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());

        assert_eq!(executor.remove_stale_workspaces().expect("no directory"), 0);
    }

    #[cfg(unix)]
    #[test]
    fn a_link_in_the_executions_directory_is_removed_not_followed() {
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());
        let executions = state_directory.path().join("executions");
        fs::create_dir_all(&executions).expect("executions directory");
        let precious = state_directory.path().join("precious");
        fs::create_dir(&precious).expect("precious directory");
        fs::write(precious.join("file"), b"keep me").expect("precious file");
        std::os::unix::fs::symlink(&precious, executions.join("link")).expect("symlink");

        executor.remove_stale_workspaces().expect("cleanup");

        assert!(!executions.join("link").exists());
        assert_eq!(fs::read(precious.join("file")).unwrap(), b"keep me");
    }

    #[cfg(unix)]
    #[test]
    fn the_directory_workspaces_live_in_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let state_directory = tempdir().expect("temporary state directory should be created");
        let executor = NativeExecutor::new(state_directory.path());

        let workspace = executor
            .prepare(&assignment("true", vec![]))
            .expect("workspace");

        // Nobody else can enter it, so what jobs leave in their workspaces
        // cannot be read by other users of the machine.
        let mode = fs::metadata(state_directory.path().join("executions"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0);
        assert!(workspace.path().is_dir());
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
                retry: Default::default(),
            },
        }
    }
}
