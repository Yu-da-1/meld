//! End-to-end tests of Phase 5 data movement.
//!
//! Each test starts a real controller and node and drives them only through
//! the `meld` command, as a user would. They need `sh`, `tr` and `ln`, so they
//! run on Unix only.
#![cfg(unix)]

use std::{
    fs::{self, File},
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::OnceLock,
    thread::sleep,
    time::{Duration, Instant},
};

use tempfile::TempDir;

const WAIT_LIMIT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

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

/// A controller and any number of nodes, stopped when dropped.
struct Cluster {
    directory: TempDir,
    url: String,
    controller: Child,
    nodes: Vec<Child>,
}

impl Cluster {
    /// Starts a controller with no nodes.
    fn start() -> Self {
        let directory = TempDir::new().expect("cluster directory");
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("a free port")
            .local_addr()
            .expect("local address")
            .port();
        let controller = Command::new(binary_directory().join("meld-controller"))
            .env("MELD_CONTROLLER_ADDR", format!("127.0.0.1:{port}"))
            .env(
                "MELD_CONTROLLER_STATE_DIR",
                directory.path().join("controller"),
            )
            .stdout(Stdio::from(log(&directory, "controller.log")))
            .stderr(Stdio::from(log(&directory, "controller.err")))
            .spawn()
            .expect("controller should start");
        let cluster = Self {
            directory,
            url: format!("http://127.0.0.1:{port}"),
            controller,
            nodes: Vec::new(),
        };
        cluster.wait_until_listening();
        cluster
    }

    /// Starts a controller and one node.
    fn with_node() -> Self {
        let mut cluster = Self::start();
        cluster.add_node();
        cluster
    }

    fn add_node(&mut self) {
        let index = self.nodes.len();
        let node = Command::new(binary_directory().join("meld-node"))
            .env("MELD_CONTROLLER_URL", &self.url)
            .env("MELD_STATE_DIR", self.node_directory(index))
            .env("MELD_HEARTBEAT_INTERVAL_SECS", "1")
            .env("RUST_LOG", "meld_node=debug")
            .stdout(Stdio::from(log(
                &self.directory,
                &format!("node{index}.log"),
            )))
            .stderr(Stdio::from(log(
                &self.directory,
                &format!("node{index}.err"),
            )))
            .spawn()
            .expect("node should start");
        self.nodes.push(node);
    }

    fn node_directory(&self, index: usize) -> PathBuf {
        self.directory.path().join(format!("node{index}"))
    }

    /// Files the controller holds as blobs.
    fn controller_blobs(&self) -> Vec<PathBuf> {
        let blobs = self.directory.path().join("controller/blobs");
        fs::read_dir(blobs)
            .expect("controller blob directory")
            .map(|entry| entry.expect("directory entry").path())
            .collect()
    }

    fn wait_until_listening(&self) {
        let deadline = Instant::now() + WAIT_LIMIT;
        while !self.meld(Path::new("."), &["nodes"]).status.success() {
            assert!(Instant::now() < deadline, "the controller did not start");
            sleep(POLL_INTERVAL);
        }
    }

    fn meld(&self, directory: &Path, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_meld"))
            .args(args)
            .current_dir(directory)
            .env("MELD_CONTROLLER_URL", &self.url)
            .output()
            .expect("meld should run")
    }

    /// Runs `meld` expecting success.
    fn meld_ok(&self, directory: &Path, args: &[&str]) -> Output {
        let output = self.meld(directory, args);
        assert!(
            output.status.success(),
            "meld {args:?} failed:\n{}",
            describe(&output)
        );
        output
    }

    /// Submits a job and returns its ID.
    fn submit(&self, directory: &Path, args: &[&str]) -> String {
        let output = self.meld_ok(directory, args);
        stdout(&output)
            .lines()
            .find_map(|line| line.strip_prefix("job_id: "))
            .expect("meld run should print the job ID")
            .to_owned()
    }

    /// Waits for the job to reach a final state and returns its status.
    fn wait_finished(&self, job: &str) -> Status {
        let deadline = Instant::now() + WAIT_LIMIT;
        loop {
            let status = Status(stdout(&self.meld_ok(Path::new("."), &["status", job])));
            if matches!(
                status.state(),
                "succeeded" | "failed" | "cancelled" | "timed_out" | "lost"
            ) {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "job {job} did not finish:\n{}\n{}",
                status.0,
                self.logs()
            );
            sleep(POLL_INTERVAL);
        }
    }

    fn fetch(&self, job: &str, out_dir: &Path, extra: &[&str]) -> Output {
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
    fn logs(&self) -> String {
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
    fn node_log(&self) -> String {
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
        for child in self.nodes.iter_mut().chain([&mut self.controller]) {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn log(directory: &TempDir, name: &str) -> File {
    File::create(directory.path().join(name)).expect("log file")
}

/// `meld status` output.
struct Status(String);

impl Status {
    fn state(&self) -> &str {
        self.value("state").unwrap_or("")
    }

    fn value(&self, name: &str) -> Option<&str> {
        self.0
            .lines()
            .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn describe(output: &Output) -> String {
    format!("stdout:\n{}\nstderr:\n{}", stdout(output), stderr(output))
}

fn write(directory: &Path, path: &str, content: &[u8]) {
    let path = directory.join(path);
    fs::create_dir_all(path.parent().expect("a parent directory")).expect("create directories");
    fs::write(path, content).expect("write file");
}

fn make_executable(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
}

#[test]
fn inputs_are_processed_remotely_and_outputs_are_fetched() {
    let cluster = Cluster::with_node();
    let work = TempDir::new().expect("work directory");
    write(work.path(), "data.txt", b"alpha\nbeta\ngamma\n");
    write(
        work.path(),
        "scripts/process.sh",
        b"#!/bin/sh\nmkdir -p out\ntr a-z A-Z < \"$1\" > out/result.txt\nwc -l < \"$1\" | tr -d ' ' > out/count.txt\n",
    );
    make_executable(&work.path().join("scripts/process.sh"));

    let job = cluster.submit(
        work.path(),
        &[
            "run",
            "--input",
            "data.txt",
            "--input",
            "scripts",
            "--output",
            "out/result.txt",
            "--output",
            "out/count.txt",
            "--",
            "scripts/process.sh",
            "data.txt",
        ],
    );
    let status = cluster.wait_finished(&job);

    assert_eq!(
        status.state(),
        "succeeded",
        "{}\n{}",
        status.0,
        cluster.logs()
    );
    assert!(status.0.contains("out/result.txt"), "{}", status.0);
    assert!(status.0.contains("inputs: 2 file(s)"), "{}", status.0);

    let fetched = TempDir::new().expect("download directory");
    cluster.meld_ok(
        Path::new("."),
        &["fetch", &job, "--out-dir", fetched.path().to_str().unwrap()],
    );
    assert_eq!(
        fs::read(fetched.path().join("out/result.txt")).expect("result"),
        b"ALPHA\nBETA\nGAMMA\n"
    );
    assert_eq!(
        fs::read(fetched.path().join("out/count.txt")).expect("count"),
        b"3\n"
    );

    // A second download must not clobber what is already there.
    fs::write(fetched.path().join("out/result.txt"), b"mine").expect("local edit");
    let refused = cluster.fetch(&job, fetched.path(), &[]);
    assert!(!refused.status.success());
    assert!(
        stderr(&refused).contains("--force"),
        "{}",
        describe(&refused)
    );
    assert_eq!(
        fs::read(fetched.path().join("out/result.txt")).unwrap(),
        b"mine"
    );
    let forced = cluster.fetch(&job, fetched.path(), &["--force"]);
    assert!(forced.status.success(), "{}", describe(&forced));
    assert_eq!(
        fs::read(fetched.path().join("out/result.txt")).unwrap(),
        b"ALPHA\nBETA\nGAMMA\n"
    );

    // The job's working directory does not outlive it.
    let executions = cluster.node_directory(0).join("executions");
    assert_eq!(
        fs::read_dir(executions)
            .expect("executions directory")
            .count(),
        0
    );
}

#[test]
fn repeated_inputs_are_sent_once_and_served_from_the_nodes_cache() {
    let cluster = Cluster::with_node();
    let work = TempDir::new().expect("work directory");
    write(work.path(), "data.txt", b"reused content\n");

    let first = cluster.submit(
        work.path(),
        &["run", "--input", "data.txt", "--", "cat", "data.txt"],
    );
    assert_eq!(cluster.wait_finished(&first).state(), "succeeded");
    let again = cluster.meld_ok(
        work.path(),
        &["run", "--input", "data.txt", "--", "cat", "data.txt"],
    );
    let second = stdout(&again)
        .lines()
        .find_map(|line| line.strip_prefix("job_id: "))
        .expect("job ID")
        .to_owned();
    assert_eq!(cluster.wait_finished(&second).state(), "succeeded");

    assert!(
        stderr(&again).contains("sent 0 file(s)")
            && stderr(&again).contains("1 already on the controller"),
        "{}",
        describe(&again)
    );
    let log = cluster.node_log();
    assert_eq!(log.matches("input downloaded").count(), 1, "{log}");
    assert_eq!(log.matches("input served from cache").count(), 1, "{log}");
    let logs = cluster.meld_ok(Path::new("."), &["logs", &second]);
    assert_eq!(stdout(&logs), "reused content\n");
}

#[test]
fn large_files_make_the_round_trip_intact() {
    let cluster = Cluster::with_node();
    let work = TempDir::new().expect("work directory");
    let content: Vec<u8> = (0..20 * 1024 * 1024u32)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    write(work.path(), "big.bin", &content);

    let job = cluster.submit(
        work.path(),
        &[
            "run", "--input", "big.bin", "--output", "copy.bin", "--", "cp", "big.bin", "copy.bin",
        ],
    );
    let status = cluster.wait_finished(&job);
    assert_eq!(
        status.state(),
        "succeeded",
        "{}\n{}",
        status.0,
        cluster.logs()
    );

    let fetched = TempDir::new().expect("download directory");
    let output = cluster.fetch(&job, fetched.path(), &[]);
    assert!(output.status.success(), "{}", describe(&output));
    assert!(
        fs::read(fetched.path().join("copy.bin")).expect("copy") == content,
        "the fetched file differs from the original"
    );
}

#[test]
fn an_output_the_job_never_created_fails_it_with_a_reason() {
    let cluster = Cluster::with_node();
    let work = TempDir::new().expect("work directory");

    let job = cluster.submit(
        work.path(),
        &["run", "--output", "result.txt", "--", "true"],
    );
    let status = cluster.wait_finished(&job);

    assert_eq!(status.state(), "failed", "{}", status.0);
    assert_eq!(
        status.value("data_failure"),
        Some("declared output `result.txt` was not produced")
    );
    let fetched = cluster.fetch(&job, work.path(), &[]);
    assert!(!fetched.status.success());
    assert!(
        stderr(&fetched).contains("failed"),
        "{}",
        describe(&fetched)
    );
}

#[test]
fn a_failing_process_is_not_reported_as_a_data_failure() {
    let cluster = Cluster::with_node();
    let work = TempDir::new().expect("work directory");

    let job = cluster.submit(
        work.path(),
        &["run", "--output", "result.txt", "--", "sh", "-c", "exit 3"],
    );
    let status = cluster.wait_finished(&job);

    assert_eq!(status.state(), "failed");
    assert_eq!(status.value("exit_code"), Some("3"));
    assert_eq!(status.value("data_failure"), None, "{}", status.0);
}

#[test]
fn an_output_that_links_outside_the_workspace_is_refused_and_never_uploaded() {
    let cluster = Cluster::with_node();
    let work = TempDir::new().expect("work directory");
    let secret = TempDir::new().expect("secret directory");
    write(secret.path(), "secret.txt", b"private");
    let target = secret.path().join("secret.txt");

    let job = cluster.submit(
        work.path(),
        &[
            "run",
            "--output",
            "result.txt",
            "--",
            "ln",
            "-s",
            target.to_str().expect("UTF-8 path"),
            "result.txt",
        ],
    );
    let status = cluster.wait_finished(&job);

    assert_eq!(status.state(), "failed", "{}", status.0);
    assert_eq!(
        status.value("data_failure"),
        Some("path `result.txt` is not allowed")
    );
    assert!(
        cluster.controller_blobs().is_empty(),
        "nothing may be stored: {:?}",
        cluster.controller_blobs()
    );
}

#[test]
fn input_that_vanished_from_the_controller_fails_the_job_with_a_reason() {
    let mut cluster = Cluster::start();
    let work = TempDir::new().expect("work directory");
    write(work.path(), "data.txt", b"soon to be lost");
    // No node yet, so the job waits in the queue.
    let job = cluster.submit(
        work.path(),
        &["run", "--input", "data.txt", "--", "cat", "data.txt"],
    );
    for blob in cluster.controller_blobs() {
        fs::remove_file(blob).expect("remove blob");
    }

    cluster.add_node();
    let status = cluster.wait_finished(&job);

    assert_eq!(status.state(), "failed", "{}\n{}", status.0, cluster.logs());
    assert_eq!(
        status.value("data_failure"),
        Some("input `data.txt` could not be fetched")
    );
}

#[test]
fn unusable_inputs_are_refused_before_anything_is_uploaded() {
    let cluster = Cluster::start();
    let work = TempDir::new().expect("work directory");
    write(work.path(), "data.txt", b"content");

    let missing = cluster.meld(work.path(), &["run", "--input", "absent.txt", "--", "true"]);
    let unsafe_destination = cluster.meld(
        work.path(),
        &["run", "--input", "data.txt=../escape.txt", "--", "true"],
    );

    assert!(!missing.status.success());
    assert!(
        stderr(&missing).contains("absent.txt"),
        "{}",
        describe(&missing)
    );
    assert!(!unsafe_destination.status.success());
    assert!(
        stderr(&unsafe_destination).contains("components"),
        "{}",
        describe(&unsafe_destination)
    );
    assert!(cluster.controller_blobs().is_empty());
}
