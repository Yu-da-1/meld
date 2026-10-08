//! Helpers shared by the end-to-end tests.
//!
//! A [`Cluster`] is a real controller and any number of real nodes, started as
//! separate processes and driven only through the `meld` command, as a user
//! would. Unlike a mock, it can be hurt the way a real deployment is: the
//! controller or a node can be killed, stopped, resumed and started again on
//! the same state directory.
//!
//! These tests need `sh` and `kill`, so they run on Unix only.
#![cfg(unix)]
#![allow(dead_code)] // each test binary uses a different part

use std::{
    fs::{self, File, OpenOptions},
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::OnceLock,
    thread::sleep,
    time::{Duration, Instant},
};

use tempfile::TempDir;

pub const WAIT_LIMIT: Duration = Duration::from_secs(40);
pub const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Directory holding the controller and node binaries, building them first.
///
/// Cargo only builds the binaries of the package under test, so the others
/// are built here, once for the whole test run.
fn binary_directory() -> &'static Path {
    static DIRECTORY: OnceLock<PathBuf> = OnceLock::new();
    DIRECTORY.get_or_init(|| {
        let executable = std::env::current_exe().expect("test executable path");
        // target/<profile>/deps/<test> -> target/<profile>
        let directory = executable
            .parent()
            .and_then(Path::parent)
            .expect("profile directory")
            .to_owned();
        let mut build = Command::new(env!("CARGO"));
        build
            .args(["build", "--quiet", "--workspace", "--bins"])
            .current_dir(env!("CARGO_MANIFEST_DIR"));
        if directory.file_name().is_some_and(|name| name == "release") {
            build.arg("--release");
        }
        let status = build.status().expect("cargo should run");
        assert!(status.success(), "building the Meld binaries failed");
        directory
    })
}

/// Timings that make failures show up in seconds instead of minutes.
///
/// Silence is noticed after the heartbeat timeout, an execution is given up
/// on a grace period later, and an automatic retry waits `retry_backoff`.
pub const FAST_FAILURE_DETECTION: &[(&str, &str)] = &[
    ("MELD_HEARTBEAT_TIMEOUT_SECS", "2"),
    ("MELD_LOST_GRACE_SECS", "1"),
    ("MELD_RETRY_BACKOFF_SECS", "1"),
    ("MELD_RETRY_BACKOFF_MAX_SECS", "2"),
];

/// A controller and any number of nodes, stopped when dropped.
pub struct Cluster {
    directory: TempDir,
    port: u16,
    url: String,
    controller_env: Vec<(String, String)>,
    controller: Option<Child>,
    nodes: Vec<Option<Child>>,
}

impl Cluster {
    /// Starts a controller with no nodes.
    pub fn start() -> Self {
        Self::start_with(&[])
    }

    /// Starts a controller with extra environment, such as [`FAST_FAILURE_DETECTION`].
    pub fn start_with(controller_env: &[(&str, &str)]) -> Self {
        let directory = TempDir::new().expect("cluster directory");
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("a free port")
            .local_addr()
            .expect("local address")
            .port();
        let mut cluster = Self {
            directory,
            port,
            url: format!("http://127.0.0.1:{port}"),
            controller_env: controller_env
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            controller: None,
            nodes: Vec::new(),
        };
        cluster.start_controller();
        cluster
    }

    /// Starts a controller and one node.
    pub fn with_node() -> Self {
        let mut cluster = Self::start();
        cluster.add_node();
        cluster
    }

    /// Starts the controller on the cluster's address and state directory.
    ///
    /// After a [`Self::stop_controller`] this is a restart: it finds the
    /// state the earlier process saved.
    pub fn start_controller(&mut self) {
        assert!(
            self.controller.is_none(),
            "the controller is already running"
        );
        let mut command = Command::new(binary_directory().join("meld-controller"));
        command
            .env("MELD_CONTROLLER_ADDR", format!("127.0.0.1:{}", self.port))
            .env(
                "MELD_CONTROLLER_STATE_DIR",
                self.directory.path().join("controller"),
            )
            .stdout(Stdio::from(self.log("controller.log")))
            .stderr(Stdio::from(self.log("controller.err")));
        for (name, value) in &self.controller_env {
            command.env(name, value);
        }
        self.controller = Some(command.spawn().expect("controller should start"));
        self.wait_until_listening();
    }

    /// Kills the controller without warning, as a crash would.
    pub fn stop_controller(&mut self) {
        let mut controller = self.controller.take().expect("the controller is running");
        let _ = controller.kill();
        let _ = controller.wait();
    }

    /// Starts a node and returns its index.
    pub fn add_node(&mut self) -> usize {
        let index = self.nodes.len();
        self.nodes.push(None);
        self.start_node(index);
        index
    }

    /// Starts (or restarts) the node on its own state directory, so a
    /// restarted node keeps the identity it had.
    pub fn start_node(&mut self, index: usize) {
        assert!(
            self.nodes[index].is_none(),
            "node {index} is already running"
        );
        let node = Command::new(binary_directory().join("meld-node"))
            .env("MELD_CONTROLLER_URL", &self.url)
            .env("MELD_STATE_DIR", self.node_directory(index))
            .env("MELD_HEARTBEAT_INTERVAL_SECS", "1")
            .env("RUST_LOG", "meld_node=debug")
            .stdout(Stdio::from(self.log(&format!("node{index}.log"))))
            .stderr(Stdio::from(self.log(&format!("node{index}.err"))))
            .spawn()
            .expect("node should start");
        self.nodes[index] = Some(node);
    }

    /// Kills a node without warning. Whatever it was running keeps running.
    pub fn kill_node(&mut self, index: usize) {
        let mut node = self.nodes[index].take().expect("the node is running");
        let _ = node.kill();
        let _ = node.wait();
    }

    /// Freezes a node, so it stops answering without having died.
    pub fn pause_node(&self, index: usize) {
        self.signal_node(index, "STOP");
    }

    pub fn resume_node(&self, index: usize) {
        self.signal_node(index, "CONT");
    }

    fn signal_node(&self, index: usize, signal: &str) {
        let node = self.nodes[index].as_ref().expect("the node is running");
        let status = Command::new("kill")
            .args([format!("-{signal}"), node.id().to_string()])
            .status()
            .expect("kill should run");
        assert!(status.success(), "could not send {signal} to node {index}");
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn directory(&self) -> &Path {
        self.directory.path()
    }

    pub fn node_directory(&self, index: usize) -> PathBuf {
        self.directory.path().join(format!("node{index}"))
    }

    /// Files the controller holds as blobs.
    pub fn controller_blobs(&self) -> Vec<PathBuf> {
        let blobs = self.directory.path().join("controller/blobs");
        fs::read_dir(blobs)
            .expect("controller blob directory")
            .map(|entry| entry.expect("directory entry").path())
            .collect()
    }

    fn log(&self, name: &str) -> File {
        // Appended, so a restarted process adds to what its predecessor wrote.
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.path().join(name))
            .expect("log file")
    }

    fn wait_until_listening(&self) {
        let deadline = Instant::now() + WAIT_LIMIT;
        while !self.meld(Path::new("."), &["nodes"]).status.success() {
            assert!(Instant::now() < deadline, "the controller did not start");
            sleep(POLL_INTERVAL);
        }
    }

    pub fn meld(&self, directory: &Path, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_meld"))
            .args(args)
            .current_dir(directory)
            .env("MELD_CONTROLLER_URL", &self.url)
            .output()
            .expect("meld should run")
    }

    /// Runs `meld` expecting success.
    pub fn meld_ok(&self, directory: &Path, args: &[&str]) -> Output {
        let output = self.meld(directory, args);
        assert!(
            output.status.success(),
            "meld {args:?} failed:\n{}",
            describe(&output)
        );
        output
    }

    /// Submits a job and returns its ID.
    pub fn submit(&self, directory: &Path, args: &[&str]) -> String {
        let output = self.meld_ok(directory, args);
        stdout(&output)
            .lines()
            .find_map(|line| line.strip_prefix("job_id: "))
            .expect("meld run should print the job ID")
            .to_owned()
    }

    /// Submits `command` as a shell script and returns the job ID.
    pub fn run_script(&self, flags: &[&str], script: &str) -> String {
        let mut args = vec!["run"];
        args.extend_from_slice(flags);
        args.extend_from_slice(&["--", "sh", "-c", script]);
        self.submit(Path::new("."), &args)
    }

    pub fn status(&self, job: &str) -> Status {
        Status(stdout(&self.meld_ok(Path::new("."), &["status", job])))
    }

    /// Waits until `check` accepts the job's status, and returns it.
    pub fn wait_for(
        &self,
        job: &str,
        what: &str,
        mut check: impl FnMut(&Status) -> bool,
    ) -> Status {
        let deadline = Instant::now() + WAIT_LIMIT;
        loop {
            let status = self.status(job);
            if check(&status) {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "job {job} never {what}:\n{}\n{}",
                status.0,
                self.logs()
            );
            sleep(POLL_INTERVAL);
        }
    }

    /// Waits for the job to reach exactly `state`.
    pub fn wait_for_state(&self, job: &str, state: &str) -> Status {
        self.wait_for(job, &format!("reached {state}"), |status| {
            status.state() == state
        })
    }

    /// Waits for the job to reach a final state and returns its status.
    ///
    /// `lost` counts as final here; a test that expects a lost job to recover
    /// must wait for the state it expects instead.
    pub fn wait_finished(&self, job: &str) -> Status {
        self.wait_for(job, "finished", |status| {
            matches!(
                status.state(),
                "succeeded" | "failed" | "cancelled" | "timed_out" | "lost"
            )
        })
    }

    pub fn fetch(&self, job: &str, out_dir: &Path, extra: &[&str]) -> Output {
        let mut args = vec![
            "fetch",
            job,
            "--out-dir",
            out_dir.to_str().expect("UTF-8 path"),
        ];
        args.extend_from_slice(extra);
        self.meld(Path::new("."), &args)
    }

    /// Everything the cluster processes logged, for failure messages.
    pub fn logs(&self) -> String {
        fs::read_dir(self.directory.path())
            .expect("cluster directory")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "log" || ext == "err")
            })
            .map(|entry| {
                format!(
                    "--- {} ---\n{}",
                    entry.file_name().to_string_lossy(),
                    fs::read_to_string(entry.path()).unwrap_or_default()
                )
            })
            .collect()
    }

    /// Debug log of the first node, with terminal colors removed.
    pub fn node_log(&self) -> String {
        let text = fs::read_to_string(self.directory.path().join("node0.log")).unwrap_or_default();
        let mut plain = String::new();
        let mut in_escape = false;
        for character in text.chars() {
            match (in_escape, character) {
                (false, '\u{1b}') => in_escape = true,
                (true, 'm') => in_escape = false,
                (false, _) => plain.push(character),
                (true, _) => {}
            }
        }
        plain
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // A node that was paused must be resumed to be killed cleanly.
        for node in self.nodes.iter_mut().flatten() {
            let _ = Command::new("kill")
                .args(["-CONT", &node.id().to_string()])
                .status();
        }
        for child in self
            .nodes
            .iter_mut()
            .flatten()
            .chain(self.controller.iter_mut())
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// `meld status` output.
pub struct Status(pub String);

impl Status {
    pub fn state(&self) -> &str {
        self.value("state").unwrap_or("")
    }

    pub fn value(&self, name: &str) -> Option<&str> {
        self.0
            .lines()
            .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
    }

    /// Number of attempts listed, or 1 when the job has had a single one.
    pub fn attempts(&self) -> usize {
        self.0
            .lines()
            .skip_while(|line| *line != "attempts:")
            .skip(1)
            .take_while(|line| line.starts_with("  "))
            .count()
            .max(1)
    }
}

pub fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

pub fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

pub fn describe(output: &Output) -> String {
    format!("stdout:\n{}\nstderr:\n{}", stdout(output), stderr(output))
}

pub fn write(directory: &Path, path: &str, content: &[u8]) {
    let path = directory.join(path);
    fs::create_dir_all(path.parent().expect("a parent directory")).expect("create directories");
    fs::write(path, content).expect("write file");
}

pub fn make_executable(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
}

/// Number of lines in a file that may not exist yet.
pub fn line_count(path: &Path) -> usize {
    fs::read_to_string(path).map_or(0, |text| text.lines().count())
}
