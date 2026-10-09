//! End-to-end tests of authentication.
//!
//! A real controller that requires tokens, real nodes and the real `meld`
//! command. The route-by-route rules are in the controller's own tests; these
//! check that the processes are wired to use them.
#![cfg(unix)]

mod common;

use std::{fs, path::Path};

use common::*;
use tempfile::TempDir;

const CLIENT_TOKEN: &str = "client-secret-0123456789";
const NODE_TOKEN: &str = "node-secret-0123456789ab";

fn tokens() -> Tokens {
    Tokens {
        client: CLIENT_TOKEN.to_owned(),
        node: NODE_TOKEN.to_owned(),
    }
}

#[test]
fn a_job_runs_when_everyone_presents_the_right_token() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    cluster.add_node();
    let work = TempDir::new().expect("work directory");
    fs::write(work.path().join("in.txt"), "hello\n").unwrap();

    let job = cluster.submit(
        work.path(),
        &[
            "run",
            "--input",
            "in.txt",
            "--output",
            "out.txt",
            "--",
            "sh",
            "-c",
            "tr a-z A-Z < in.txt > out.txt; echo done",
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

    let out_dir = work.path().join("results");
    let fetched = cluster.fetch(&job, &out_dir, &[]);
    assert!(fetched.status.success(), "{}", describe(&fetched));
    assert_eq!(
        fs::read_to_string(out_dir.join("out.txt")).unwrap(),
        "HELLO\n"
    );
}

#[test]
fn meld_without_a_valid_client_token_is_refused_and_says_why() {
    let cluster = Cluster::start_with_tokens(tokens());
    for (token, expected) in [
        (None, "401"),
        (Some("wrong-secret-0123456789"), "401"),
        // A real token, but the node's: valid, yet not for this role.
        (Some(NODE_TOKEN), "403"),
    ] {
        let output = cluster.meld_with_token(Path::new("."), &["nodes"], token);
        assert!(!output.status.success(), "{token:?}: {}", describe(&output));
        let message = stderr(&output);
        assert!(message.contains(expected), "{token:?}: {message}");
        assert!(
            message.contains("MELD_CLIENT_TOKEN"),
            "{token:?}: {message}"
        );
        for secret in [CLIENT_TOKEN, NODE_TOKEN, "wrong-secret-0123456789"] {
            assert!(!message.contains(secret), "the error repeats a token");
        }
    }
}

#[test]
fn a_job_cannot_be_submitted_without_a_token() {
    let cluster = Cluster::start_with_tokens(tokens());
    let output = cluster.meld_with_token(
        Path::new("."),
        &["run", "--", "touch", "should-not-exist"],
        None,
    );
    assert!(!output.status.success(), "{}", describe(&output));
    // Nothing was accepted.
    let status = cluster.meld_ok(Path::new("."), &["nodes"]);
    assert!(status.status.success(), "an authorized client still works");
}

#[test]
fn a_node_with_the_wrong_token_never_joins() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    for (what, token) in [
        ("no token", None),
        ("a wrong token", Some("wrong-secret-0123456789")),
        // The client token must not let a node in either.
        ("the client token", Some(CLIENT_TOKEN)),
    ] {
        let index = cluster.add_node_with_token(token);
        assert!(cluster.node_exits(index), "a node with {what} kept running");
    }

    let nodes = stdout(&cluster.meld_ok(Path::new("."), &["nodes"]));
    assert!(
        !nodes.contains("ready") && !nodes.contains("Ready"),
        "an unauthenticated node joined:\n{nodes}"
    );
}

#[test]
fn a_node_that_only_has_the_client_token_cannot_run_jobs() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    let node = cluster.add_node_with_token(Some(CLIENT_TOKEN));
    assert!(cluster.node_exits(node), "the node should give up");
    let job = cluster.run_script(&[], "touch ran.marker");
    // No node can take it, so it waits for one.
    let status = cluster.status(&job);
    assert_eq!(status.state(), "queued", "{}", status.0);
}

#[test]
fn tokens_are_not_written_to_any_log() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    cluster.add_node();
    let job = cluster.run_script(&[], "echo hi");
    cluster.wait_finished(&job);
    // A refused request is the likeliest thing to log a token by mistake.
    cluster.meld_with_token(Path::new("."), &["nodes"], Some("wrong-secret-0123456789"));

    let logs = cluster.logs();
    for secret in [CLIENT_TOKEN, NODE_TOKEN, "wrong-secret-0123456789"] {
        assert!(!logs.contains(secret), "a token appears in the logs");
    }
}

#[test]
fn the_controller_refuses_to_listen_openly_without_tokens() {
    for env in [
        // Reachable from the network, and open.
        vec![("MELD_CONTROLLER_ADDR", "0.0.0.0:0")],
        // One token alone would leave the other role unprotected.
        vec![
            ("MELD_CONTROLLER_ADDR", "127.0.0.1:0"),
            ("MELD_CLIENT_TOKEN", CLIENT_TOKEN),
        ],
        vec![
            ("MELD_CONTROLLER_ADDR", "127.0.0.1:0"),
            ("MELD_NODE_TOKEN", NODE_TOKEN),
        ],
        // The roles would be indistinguishable.
        vec![
            ("MELD_CONTROLLER_ADDR", "127.0.0.1:0"),
            ("MELD_CLIENT_TOKEN", CLIENT_TOKEN),
            ("MELD_NODE_TOKEN", CLIENT_TOKEN),
        ],
        // Too easy to guess.
        vec![
            ("MELD_CONTROLLER_ADDR", "127.0.0.1:0"),
            ("MELD_CLIENT_TOKEN", "short"),
            ("MELD_NODE_TOKEN", NODE_TOKEN),
        ],
    ] {
        let output = Cluster::controller_start_attempt(&env);
        assert_eq!(
            output.status.code(),
            Some(1),
            "{env:?} should be refused:\n{}",
            describe(&output)
        );
    }
}

#[test]
fn the_controller_starts_on_loopback_without_tokens_and_anywhere_with_them() {
    // Loopback without tokens is the development default and still starts.
    let output = Cluster::controller_start_attempt(&[("MELD_CONTROLLER_ADDR", "127.0.0.1:0")]);
    assert_ne!(output.status.code(), Some(1), "{}", describe(&output));

    // A wide listen address is fine once both tokens are set.
    let output = Cluster::controller_start_attempt(&[
        ("MELD_CONTROLLER_ADDR", "0.0.0.0:0"),
        ("MELD_CLIENT_TOKEN", CLIENT_TOKEN),
        ("MELD_NODE_TOKEN", NODE_TOKEN),
    ]);
    assert_ne!(output.status.code(), Some(1), "{}", describe(&output));
}
