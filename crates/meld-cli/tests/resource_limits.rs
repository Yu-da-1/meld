//! End-to-end tests of what a job may use.
//!
//! A real controller, node and `meld`. A job that holds far more memory than
//! it reserved must be stopped on its own, with the reason shown, while the
//! jobs beside it and the node itself carry on.
#![cfg(unix)]

mod common;

use common::*;

/// Holds about 60 MB, then waits for a long time.
const HOG: &str = "x=$(head -c 60000000 /dev/zero | tr '\\0' x); sleep 60";

#[test]
fn a_job_that_uses_more_memory_than_it_reserved_is_stopped_and_says_so() {
    let cluster = Cluster::with_node();

    let job = cluster.run_script(&["--memory", "16MiB"], HOG);
    let status = cluster.wait_finished(&job);

    assert_eq!(status.state(), "failed", "{}\n{}", status.0, cluster.logs());
    assert!(
        status.0.contains("limit: memory limit exceeded"),
        "the reason should be shown:\n{}",
        status.0
    );
    assert!(status.0.contains("--memory"), "{}", status.0);
}

#[test]
fn a_job_beside_it_and_the_node_are_not_affected() {
    let cluster = Cluster::with_node();
    let hog = cluster.run_script(&["--memory", "16MiB"], HOG);
    let neighbour = cluster.run_script(&["--memory", "64MiB"], "sleep 4; echo fine");

    let hog = cluster.wait_finished(&hog);
    let neighbour = cluster.wait_finished(&neighbour);

    assert_eq!(hog.state(), "failed", "{}", hog.0);
    assert_eq!(neighbour.state(), "succeeded", "{}", neighbour.0);
    // The node still takes work after having stopped one.
    let again = cluster.run_script(&[], "echo again");
    assert_eq!(cluster.wait_finished(&again).state(), "succeeded");
}

#[test]
fn a_job_that_keeps_to_its_reservation_is_left_alone() {
    let cluster = Cluster::with_node();

    let job = cluster.run_script(&["--memory", "64MiB"], "sleep 3; echo fine");
    let status = cluster.wait_finished(&job);

    assert_eq!(
        status.state(),
        "succeeded",
        "{}\n{}",
        status.0,
        cluster.logs()
    );
    assert!(!status.0.contains("limit:"), "{}", status.0);
}

#[test]
fn a_stopped_job_is_not_started_again_by_a_retry_policy() {
    let cluster = Cluster::with_node();

    let job = cluster.run_script(
        &["--memory", "16MiB", "--max-attempts", "3", "--idempotent"],
        HOG,
    );
    let status = cluster.wait_finished(&job);

    assert_eq!(status.state(), "failed", "{}", status.0);
    // Wait out the first automatic retry's delay: nothing may start.
    std::thread::sleep(std::time::Duration::from_secs(7));
    let status = cluster.status(&job);
    assert_eq!(status.state(), "failed", "{}", status.0);
    assert_eq!(status.attempts(), 1, "{}", status.0);
}
