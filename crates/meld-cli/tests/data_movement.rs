//! End-to-end tests of data movement.
//!
//! Each test starts a real controller and node and drives them only through
//! the `meld` command, as a user would. They need `sh`, `tr` and `ln`, so they
//! run on Unix only.
#![cfg(unix)]

mod common;

use std::{fs, path::Path};

use common::*;
use tempfile::TempDir;

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
