//! End-to-end tests that hurt a running cluster.
//!
//! Each test starts a real controller and real nodes, then kills, freezes,
//! restarts or replays something while a job is in flight, and checks what a
//! user sees afterwards. The unit tests cover each rule alone; these check
//! that the rules still hold when real processes and real time are involved.
//!
//! Failure detection is sped up (see [`FAST_FAILURE_DETECTION`]), but every
//! test still waits for real seconds to pass, so they are slow by nature.
#![cfg(unix)]

mod common;

use std::{
    io::{Read, Write},
    net::TcpStream,
    path::PathBuf,
    thread::sleep,
    time::Duration,
};

use common::*;
use meld_core::{
    ExecutionEvent, ExecutionId, ExecutionOutput, ExecutionResult, NodeId,
    ReportExecutionEventRequest, RequestMetadata,
};

/// A script that records each time it starts, then runs for a while.
fn counted_script(cluster: &Cluster, seconds: u32, then: &str) -> (String, PathBuf) {
    let runs = cluster.directory().join("runs.txt");
    let script = format!("echo run >> '{}'; sleep {seconds}; {then}", runs.display());
    (script, runs)
}

fn wait_running(cluster: &Cluster, job: &str) {
    cluster.wait_for_state(job, "running");
}

#[test]
fn a_controller_restart_does_not_interrupt_a_running_job() {
    let mut cluster = Cluster::start_with(FAST_FAILURE_DETECTION);
    cluster.add_node();
    let job = cluster.run_script(&[], "sleep 4; echo survived");
    wait_running(&cluster, &job);

    cluster.stop_controller();
    sleep(Duration::from_secs(1));
    cluster.start_controller();

    // The job is still known, and the node carries on and reports to the new process.
    let status = cluster.wait_for_state(&job, "succeeded");
    assert_eq!(status.value("exit_code"), Some("0"), "{}", cluster.logs());
    let logs = cluster.meld_ok(std::path::Path::new("."), &["logs", &job]);
    assert_eq!(stdout(&logs), "survived\n");
    assert_eq!(status.attempts(), 1, "nothing was run twice");
}

#[test]
fn a_plain_job_whose_node_is_killed_becomes_lost_and_is_not_run_again() {
    let mut cluster = Cluster::start_with(FAST_FAILURE_DETECTION);
    cluster.add_node();
    let (script, runs) = counted_script(&cluster, 6, "echo finished");
    let job = cluster.run_script(&[], &script);
    wait_running(&cluster, &job);

    cluster.kill_node(0);
    let lost = cluster.wait_for_state(&job, "lost");
    assert_eq!(lost.value("exit_code"), None, "no result was ever reported");

    // A healthy node is available now, and the retry delay is long past.
    cluster.add_node();
    sleep(Duration::from_secs(5));
    let status = cluster.status(&job);
    assert_eq!(status.state(), "lost", "{}", cluster.logs());
    assert_eq!(
        line_count(&runs),
        1,
        "a job that never asked for retry ran twice"
    );
}

#[test]
fn an_idempotent_job_runs_again_on_another_node_when_its_node_is_killed() {
    let mut cluster = Cluster::start_with(FAST_FAILURE_DETECTION);
    cluster.add_node();
    let (script, runs) = counted_script(&cluster, 3, "echo finished-on-retry");
    let job = cluster.run_script(&["--max-attempts", "3", "--idempotent"], &script);
    wait_running(&cluster, &job);

    cluster.kill_node(0);
    cluster.add_node();

    let status = cluster.wait_for_state(&job, "succeeded");
    assert_eq!(status.attempts(), 2, "{}", status.0);
    assert_eq!(line_count(&runs), 2, "one run per attempt");
    let logs = cluster.meld_ok(std::path::Path::new("."), &["logs", &job]);
    assert_eq!(stdout(&logs), "finished-on-retry\n");
}

#[test]
fn an_idempotent_job_that_exits_with_an_error_is_not_run_again() {
    let mut cluster = Cluster::start_with(FAST_FAILURE_DETECTION);
    cluster.add_node();
    let (script, runs) = counted_script(&cluster, 0, "exit 3");
    let job = cluster.run_script(&["--max-attempts", "3", "--idempotent"], &script);

    let status = cluster.wait_for_state(&job, "failed");
    sleep(Duration::from_secs(4));

    assert_eq!(status.value("exit_code"), Some("3"));
    assert_eq!(cluster.status(&job).state(), "failed", "{}", cluster.logs());
    assert_eq!(
        line_count(&runs),
        1,
        "an error exit is the job's own answer"
    );
}

#[test]
fn a_process_that_outlives_its_timeout_is_stopped_and_not_run_again() {
    let mut cluster = Cluster::start_with(FAST_FAILURE_DETECTION);
    cluster.add_node();
    let (script, runs) = counted_script(&cluster, 30, "echo never");
    let job = cluster.run_script(
        &["--timeout", "2", "--max-attempts", "3", "--idempotent"],
        &script,
    );

    let status = cluster.wait_for_state(&job, "failed");
    sleep(Duration::from_secs(4));

    // Running out of time says something about the job, not the node.
    assert_eq!(
        status.value("execution_state"),
        Some("timed_out"),
        "{}",
        status.0
    );
    assert_eq!(cluster.status(&job).state(), "failed", "{}", cluster.logs());
    assert_eq!(line_count(&runs), 1);
}

#[test]
fn the_job_timeout_ends_a_running_job_even_when_retries_are_allowed() {
    let mut cluster = Cluster::start_with(FAST_FAILURE_DETECTION);
    cluster.add_node();
    let (script, runs) = counted_script(&cluster, 30, "echo never");
    let job = cluster.run_script(
        &["--job-timeout", "3", "--max-attempts", "3", "--idempotent"],
        &script,
    );

    cluster.wait_for_state(&job, "timed_out");
    sleep(Duration::from_secs(4));

    assert_eq!(
        cluster.status(&job).state(),
        "timed_out",
        "{}",
        cluster.logs()
    );
    assert_eq!(line_count(&runs), 1);
}

#[test]
fn a_job_is_lost_when_its_node_never_returns_after_a_controller_restart() {
    let mut cluster = Cluster::start_with(FAST_FAILURE_DETECTION);
    cluster.add_node();
    let job = cluster.run_script(&[], "sleep 30");
    wait_running(&cluster, &job);

    // The node dies, and the controller goes down before it notices.
    cluster.kill_node(0);
    cluster.stop_controller();
    cluster.start_controller();

    // The restarted controller has never heard from the node, and says so
    // after the usual silence rather than waiting forever.
    cluster.wait_for_state(&job, "lost");
}

#[test]
fn a_waiting_retry_survives_a_controller_restart() {
    // A delay long enough to restart the controller inside it.
    let mut cluster = Cluster::start_with(&[
        ("MELD_HEARTBEAT_TIMEOUT_SECS", "2"),
        ("MELD_LOST_GRACE_SECS", "1"),
        ("MELD_RETRY_BACKOFF_SECS", "8"),
        ("MELD_RETRY_BACKOFF_MAX_SECS", "8"),
    ]);
    cluster.add_node();
    let (script, runs) = counted_script(&cluster, 2, "echo second-run");
    let job = cluster.run_script(&["--max-attempts", "2", "--idempotent"], &script);
    wait_running(&cluster, &job);

    cluster.kill_node(0);
    let waiting = cluster.wait_for(&job, "waited to retry", |status| {
        status.value("queue_reason") == Some("waiting_to_retry")
    });
    assert!(waiting.0.contains("retry: attempt 2 of 2"), "{}", waiting.0);

    cluster.stop_controller();
    cluster.start_controller();

    // Still waiting out the same delay. Had the delay been forgotten, the job
    // would be queued with no node to take it and would say so instead.
    let restored = cluster.status(&job);
    assert_eq!(
        restored.value("queue_reason"),
        Some("waiting_to_retry"),
        "{}",
        restored.0
    );
    assert!(
        restored.0.contains("retry: attempt 2 of 2"),
        "{}",
        restored.0
    );

    cluster.add_node();
    let status = cluster.wait_for_state(&job, "succeeded");
    assert_eq!(status.attempts(), 2, "{}", status.0);
    assert_eq!(line_count(&runs), 2);
}

#[test]
fn a_node_that_was_only_frozen_reports_the_real_result_over_lost() {
    let mut cluster = Cluster::start_with(FAST_FAILURE_DETECTION);
    cluster.add_node();
    let job = cluster.run_script(&[], "sleep 5; echo finished-late");
    wait_running(&cluster, &job);

    // The node stops answering but the process it started keeps running.
    cluster.pause_node(0);
    cluster.wait_for_state(&job, "lost");
    cluster.resume_node(0);

    // Losing contact never proved the process had died, so its outcome wins.
    let status = cluster.wait_for_state(&job, "succeeded");
    assert_eq!(status.value("exit_code"), Some("0"), "{}", cluster.logs());
    let logs = cluster.meld_ok(std::path::Path::new("."), &["logs", &job]);
    assert_eq!(stdout(&logs), "finished-late\n");
}

#[test]
fn repeated_and_conflicting_reports_do_not_change_a_finished_job() {
    let mut cluster = Cluster::start_with(FAST_FAILURE_DETECTION);
    cluster.add_node();
    let job = cluster.submit(std::path::Path::new("."), &["run", "--", "true"]);
    let status = cluster.wait_for_state(&job, "succeeded");
    let node_id: NodeId = status
        .value("node_id")
        .expect("node ID")
        .parse()
        .expect("valid node ID");
    let execution_id: ExecutionId = status
        .value("execution_id")
        .expect("execution ID")
        .parse()
        .expect("valid execution ID");
    let report = |event| post_event(cluster.url(), node_id, execution_id, event);
    let finished = |exit_code| ExecutionEvent::Finished {
        result: ExecutionResult {
            exit_code: Some(exit_code),
        },
        output: ExecutionOutput::default(),
        outputs: vec![],
    };

    // The same report arriving again, as when an acknowledgement was lost.
    assert_eq!(report(finished(0)), 200, "a duplicate must be accepted");
    assert_eq!(report(finished(0)), 200);
    // A different outcome for the same execution is a contradiction.
    assert_eq!(report(finished(1)), 409);
    // Earlier stages cannot be replayed over a finished execution.
    assert_eq!(report(ExecutionEvent::Running), 409);

    let after = cluster.status(&job);
    assert_eq!(after.state(), "succeeded", "{}", cluster.logs());
    assert_eq!(after.value("exit_code"), Some("0"));
    assert_eq!(after.attempts(), 1);
}

/// Sends one execution event to the controller and returns the HTTP status.
fn post_event(url: &str, node_id: NodeId, execution_id: ExecutionId, event: ExecutionEvent) -> u16 {
    let body = serde_json::to_string(&ReportExecutionEventRequest {
        metadata: RequestMetadata::new(),
        node_id,
        execution_id,
        event,
    })
    .expect("event serializes");
    let address = url.strip_prefix("http://").expect("an http URL");
    let mut stream = TcpStream::connect(address).expect("controller is listening");
    write!(
        stream,
        "POST /v1/nodes/executions/events HTTP/1.1\r\nHost: {address}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("request is sent");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("response is read");
    response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status in response: {response}"))
}
