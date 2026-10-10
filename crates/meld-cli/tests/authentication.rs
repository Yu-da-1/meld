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

#[test]
fn a_new_node_waits_for_approval_and_then_runs_jobs() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    let node = cluster.add_node_awaiting_approval();
    cluster.wait_for_node_state(node, "pending");

    // Waiting is not a failure: the node keeps asking, and gets no work.
    let job = cluster.run_script(&[], "echo ran");
    let status = cluster.status(&job);
    assert_eq!(status.state(), "queued", "{}", status.0);
    assert_eq!(cluster.node_state(node), "pending");

    cluster.approve_node(node);
    let status = cluster.wait_finished(&job);
    assert_eq!(
        status.state(),
        "succeeded",
        "{}\n{}",
        status.0,
        cluster.logs()
    );
}

#[test]
fn a_revoked_node_stops_and_its_work_is_given_up_on() {
    let mut cluster = Cluster::start_with_tokens_and_env(tokens(), FAST_FAILURE_DETECTION);
    let node = cluster.add_node();
    let job = cluster.run_script(&[], "sleep 30");
    cluster.wait_for_state(&job, "running");

    let id = cluster.node_id(node);
    cluster.meld_ok(Path::new("."), &["revoke", &id]);

    // The node is turned away and ends, and what it ran is given up on.
    assert!(cluster.node_exits(node), "a revoked node kept running");
    cluster.wait_for_state(&job, "lost");
    assert_eq!(cluster.node_state(node), "revoked");
    assert!(
        cluster.logs().contains("revoked"),
        "the node should say why it stopped:\n{}",
        cluster.logs()
    );
}

#[test]
fn a_revoked_node_cannot_come_back_until_it_is_approved_again() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    let node = cluster.add_node();
    cluster.meld_ok(Path::new("."), &["revoke", &cluster.node_id(node)]);
    assert!(cluster.node_exits(node));

    // Starting it again changes nothing: it is refused and stops again.
    cluster.kill_node(node);
    cluster.start_node(node);
    assert!(cluster.node_exits(node), "a revoked node was let back in");
    assert_eq!(cluster.node_state(node), "revoked");

    cluster.kill_node(node);
    cluster.meld_ok(Path::new("."), &["approve", &cluster.node_id(node)]);
    cluster.start_node(node);
    cluster.wait_for_node_state(node, "ready");
}

#[test]
fn approval_survives_a_controller_restart() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    let approved = cluster.add_node();
    let waiting = cluster.add_node_awaiting_approval();
    cluster.wait_for_node_state(waiting, "pending");

    cluster.stop_controller();
    cluster.start_controller();

    // The approved node reconnects by itself; the other is still waiting.
    cluster.wait_for_node_state(approved, "ready");
    assert_eq!(cluster.node_state(waiting), "pending");
}

#[test]
fn without_authentication_nodes_are_taken_in_without_approval() {
    let cluster = Cluster::with_node();
    cluster.wait_for_node_state(0, "ready");
}

#[test]
fn a_stranger_with_the_enrollment_token_cannot_take_over_a_node() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    let real = cluster.add_node();

    // It holds the enrollment token and names the real node's identity, but
    // has a secret of its own.
    let stranger = cluster.add_node_posing_as(real);
    assert!(cluster.node_exits(stranger), "the impostor kept running");
    assert!(
        cluster.logs().contains("under a different secret"),
        "the refusal should say what is wrong:\n{}",
        cluster.logs()
    );

    // The real node is untouched, and still gets and runs work.
    cluster.wait_for_node_state(real, "ready");
    let job = cluster.run_script(&[], "echo mine");
    let status = cluster.wait_finished(&job);
    assert_eq!(
        status.state(),
        "succeeded",
        "{}\n{}",
        status.0,
        cluster.logs()
    );
}

#[test]
fn a_node_that_lost_its_secret_is_recovered_by_revoking_and_approving_it() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    let node = cluster.add_node();
    let id = cluster.node_id(node);
    cluster.kill_node(node);
    fs::remove_file(cluster.node_secret_path(node)).expect("the secret existed");

    // It makes a new secret, which the controller does not accept.
    cluster.start_node(node);
    assert!(
        cluster.node_exits(node),
        "a node with a new secret was accepted"
    );

    cluster.meld_ok(Path::new("."), &["revoke", &id]);
    cluster.meld_ok(Path::new("."), &["approve", &id]);
    cluster.kill_node(node);
    cluster.start_node(node);
    cluster.wait_for_node_state(node, "ready");
}

#[test]
fn a_node_secret_is_private_and_never_logged_or_listed() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    let node = cluster.add_node();
    let job = cluster.run_script(&[], "echo hi");
    cluster.wait_finished(&job);

    let secret = fs::read_to_string(cluster.node_secret_path(node))
        .expect("the node keeps its secret")
        .trim()
        .to_owned();
    assert_eq!(secret.len(), 64);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(cluster.node_secret_path(node))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "the secret file is readable by others");
    }

    assert!(!cluster.logs().contains(&secret), "the secret is in a log");
    let nodes = stdout(&cluster.meld_ok(Path::new("."), &["nodes"]));
    assert!(!nodes.contains(&secret), "the secret is listed");
    // The controller keeps only a digest, and shows neither.
    let digest = {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(secret.as_bytes()))
    };
    assert!(!nodes.contains(&digest), "the digest is listed");
}

#[test]
fn the_controller_is_trusted_only_by_its_pinned_fingerprint() {
    let cluster = Cluster::start_with_tokens(tokens());
    let real = cluster.fingerprint().expect("a fingerprint").to_owned();

    let ok = cluster.meld_with_fingerprint(&["nodes"], Some(&real));
    assert!(ok.status.success(), "{}", describe(&ok));
    // The colon-separated, upper-case form that other tools print.
    let spelled = real
        .as_bytes()
        .chunks(2)
        .map(|pair| std::str::from_utf8(pair).unwrap().to_uppercase())
        .collect::<Vec<_>>()
        .join(":");
    let ok = cluster.meld_with_fingerprint(&["nodes"], Some(&spelled));
    assert!(ok.status.success(), "{}", describe(&ok));

    // A self-signed certificate is not trusted on its own...
    let output = cluster.meld_with_fingerprint(&["nodes"], None);
    assert!(!output.status.success(), "{}", describe(&output));
    // ...nor is another fingerprint, and nothing is sent to the peer first.
    let output = cluster.meld_with_fingerprint(&["nodes"], Some(&"0".repeat(64)));
    assert!(!output.status.success(), "{}", describe(&output));
    assert!(
        !stderr(&output).contains(CLIENT_TOKEN),
        "the token is in the error"
    );

    // A value that is not a fingerprint is an error, not "no pinning".
    let output = cluster.meld_with_fingerprint(&["nodes"], Some("not-a-fingerprint"));
    assert!(!output.status.success(), "{}", describe(&output));
    assert!(stderr(&output).contains("MELD_CONTROLLER_FINGERPRINT"));
}

#[test]
fn the_controller_does_not_answer_in_the_clear() {
    use std::{
        io::{Read, Write},
        net::TcpStream,
        time::Duration,
    };

    let cluster = Cluster::start_with_tokens(tokens());
    let port = cluster.url().rsplit(':').next().unwrap().to_owned();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).expect("connects");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        stream,
        "GET /v1/nodes HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {CLIENT_TOKEN}\r\n\r\n"
    )
    .unwrap();

    let mut answer = Vec::new();
    let _ = stream.read_to_end(&mut answer);

    // A TLS alert or nothing at all; never an HTTP response or the listing.
    let text = String::from_utf8_lossy(&answer);
    assert!(!text.starts_with("HTTP/"), "answered in the clear: {text}");
    assert!(!text.contains("nodes"), "answered in the clear: {text}");
}

#[test]
fn a_node_with_the_wrong_fingerprint_does_not_join_and_says_why() {
    let mut cluster = Cluster::start_with_tokens(tokens());
    let node = cluster.add_node_awaiting_approval_with_fingerprint(&"0".repeat(64));

    // It keeps trying, as it would for a controller that is down.
    std::thread::sleep(std::time::Duration::from_secs(3));
    let nodes = stdout(&cluster.meld_ok(Path::new("."), &["nodes"]));
    assert!(nodes.contains("no nodes registered"), "{nodes}");
    assert!(
        cluster.logs().contains("pinned fingerprint"),
        "the node should say what is wrong:\n{}",
        cluster.logs()
    );
    cluster.kill_node(node);
}
