//! Who may use which route.
//!
//! Each route is requested by every kind of caller. Only whether the request
//! gets past authentication matters here: a request that does may still be
//! rejected for its (empty) body, but never with 401 or 403. What a handler
//! does with a caller it admitted is tested next to the handlers.

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use meld_controller::{
    api::{
        APPROVE_NODE_PATH, BLOB_PATH, CANCEL_JOB_PATH, ControllerState, DRAIN_NODE_PATH,
        HEARTBEAT_PATH, JOB_LOGS_PATH, JOB_STATUS_PATH, LIST_NODES_PATH, POLL_NODE_COMMAND_PATH,
        REGISTER_NODE_PATH, REPORT_EXECUTION_EVENT_PATH, RESUME_NODE_PATH, RETRY_JOB_PATH,
        REVOKE_NODE_PATH, SUBMIT_JOB_PATH, router_with_auth,
    },
    auth::Authenticator,
};
use meld_core::{
    NodeDescriptor, NodeId, NodeSecret, RegisterNodeRequest, RequestMetadata, ResourceCapacity,
    SecretToken,
};
use tower::ServiceExt;

const CLIENT_TOKEN: &str = "client-token-0123456789";
const ENROLLMENT_TOKEN: &str = "enroll-token-0123456789";

/// The routes, grouped by who is meant to call them.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    /// Operator actions.
    Client,
    /// Joining the cluster.
    Join,
    /// What a node does as itself.
    Node,
    /// Files.
    Data,
}

/// Every route of the controller. A new route has to be added here, and
/// that is where its audience is decided.
fn routes() -> Vec<(Method, String, Kind)> {
    let id = "00000000-0000-0000-0000-000000000000";
    let digest = "0".repeat(64);
    let path = |template: &str| {
        template
            .replace("{node_id}", id)
            .replace("{job_id}", id)
            .replace("{sha256}", &digest)
    };
    vec![
        (Method::GET, path(LIST_NODES_PATH), Kind::Client),
        (Method::POST, path(DRAIN_NODE_PATH), Kind::Client),
        (Method::POST, path(RESUME_NODE_PATH), Kind::Client),
        (Method::POST, path(APPROVE_NODE_PATH), Kind::Client),
        (Method::POST, path(REVOKE_NODE_PATH), Kind::Client),
        (Method::POST, path(SUBMIT_JOB_PATH), Kind::Client),
        (Method::GET, path(JOB_STATUS_PATH), Kind::Client),
        (Method::GET, path(JOB_LOGS_PATH), Kind::Client),
        (Method::POST, path(CANCEL_JOB_PATH), Kind::Client),
        (Method::POST, path(RETRY_JOB_PATH), Kind::Client),
        (Method::POST, path(REGISTER_NODE_PATH), Kind::Join),
        (Method::POST, path(HEARTBEAT_PATH), Kind::Node),
        (Method::POST, path(POLL_NODE_COMMAND_PATH), Kind::Node),
        (Method::POST, path(REPORT_EXECUTION_EVENT_PATH), Kind::Node),
        (Method::GET, path(BLOB_PATH), Kind::Data),
        (Method::PUT, path(BLOB_PATH), Kind::Data),
    ]
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Caller {
    Nobody,
    WrongToken,
    Client,
    Enrollment,
    ApprovedNode,
    PendingNode,
    RevokedNode,
}

const CALLERS: [Caller; 7] = [
    Caller::Nobody,
    Caller::WrongToken,
    Caller::Client,
    Caller::Enrollment,
    Caller::ApprovedNode,
    Caller::PendingNode,
    Caller::RevokedNode,
];

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// Past authentication.
    Admitted,
    /// Known, but not for this route.
    Forbidden,
    /// Not known.
    Unauthorized,
}

fn expected(kind: Kind, caller: Caller) -> Outcome {
    use Caller::*;
    use Outcome::*;
    match (kind, caller) {
        (_, Nobody | WrongToken) => Unauthorized,
        (Kind::Client, Client) => Admitted,
        (Kind::Join, Enrollment | ApprovedNode | PendingNode | RevokedNode) => Admitted,
        // A waiting or revoked node gets in, so that it is told why it is refused.
        (Kind::Node, ApprovedNode | PendingNode | RevokedNode) => Admitted,
        (Kind::Data, Client | ApprovedNode) => Admitted,
        _ => Forbidden,
    }
}

struct Cluster {
    app: Router,
    approved: NodeSecret,
    pending: NodeSecret,
    revoked: NodeSecret,
}

fn descriptor(id: NodeId) -> NodeDescriptor {
    NodeDescriptor {
        id,
        hostname: "worker".to_owned(),
        operating_system: "linux".to_owned(),
        architecture: "x86_64".to_owned(),
        capacity: ResourceCapacity {
            logical_cpus: 2,
            memory_bytes: 1_000,
            max_concurrent_executions: 1,
        },
        capabilities: vec![],
    }
}

async fn send(app: &Router, method: &Method, path: &str, bearer: Option<&str>) -> StatusCode {
    send_body(app, method, path, bearer, "{}").await
}

async fn send_body(
    app: &Router,
    method: &Method,
    path: &str,
    bearer: Option<&str>,
    body: &str,
) -> StatusCode {
    let mut request = Request::builder()
        .method(method.clone())
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(bearer) = bearer {
        request = request.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    }
    app.clone()
        .oneshot(request.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap()
        .status()
}

async fn join(app: &Router, secret: &NodeSecret) -> NodeId {
    let id = NodeId::generate();
    let request = RegisterNodeRequest {
        metadata: RequestMetadata::new(),
        node: descriptor(id),
        credential: secret.digest(),
    };
    let status = send_body(
        app,
        &Method::POST,
        REGISTER_NODE_PATH,
        Some(ENROLLMENT_TOKEN),
        &serde_json::to_string(&request).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    id
}

async fn act(app: &Router, template: &str, id: NodeId) {
    let status = send(
        app,
        &Method::POST,
        &template.replace("{node_id}", &id.to_string()),
        Some(CLIENT_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

async fn cluster() -> Cluster {
    let authenticator = Authenticator::enabled(
        &SecretToken::new(CLIENT_TOKEN).unwrap(),
        &SecretToken::new(ENROLLMENT_TOKEN).unwrap(),
    )
    .unwrap();
    let app = router_with_auth(ControllerState::new(), authenticator);
    let (approved, pending, revoked) = (
        NodeSecret::generate(),
        NodeSecret::generate(),
        NodeSecret::generate(),
    );

    let id = join(&app, &approved).await;
    act(&app, APPROVE_NODE_PATH, id).await;
    join(&app, &pending).await;
    let id = join(&app, &revoked).await;
    act(&app, APPROVE_NODE_PATH, id).await;
    act(&app, REVOKE_NODE_PATH, id).await;

    Cluster {
        app,
        approved,
        pending,
        revoked,
    }
}

impl Cluster {
    fn bearer(&self, caller: Caller) -> Option<String> {
        match caller {
            Caller::Nobody => None,
            Caller::WrongToken => Some("not-a-token-anyone-has-0000".to_owned()),
            Caller::Client => Some(CLIENT_TOKEN.to_owned()),
            Caller::Enrollment => Some(ENROLLMENT_TOKEN.to_owned()),
            Caller::ApprovedNode => Some(self.approved.expose().to_owned()),
            Caller::PendingNode => Some(self.pending.expose().to_owned()),
            Caller::RevokedNode => Some(self.revoked.expose().to_owned()),
        }
    }
}

fn outcome(status: StatusCode) -> Outcome {
    match status {
        StatusCode::UNAUTHORIZED => Outcome::Unauthorized,
        StatusCode::FORBIDDEN => Outcome::Forbidden,
        _ => Outcome::Admitted,
    }
}

#[tokio::test]
async fn every_route_admits_only_the_callers_meant_for_it() {
    let cluster = cluster().await;
    for (method, path, kind) in routes() {
        for caller in CALLERS {
            let bearer = cluster.bearer(caller);
            let status = send(&cluster.app, &method, &path, bearer.as_deref()).await;
            // A client's approve/revoke would act on the all-zero node id,
            // which nobody has, and is answered 404: still "admitted".
            assert_eq!(
                outcome(status),
                expected(kind, caller),
                "{caller:?} on {method} {path} ({kind:?}): {status}"
            );
        }
    }
}

#[tokio::test]
async fn a_malformed_authorization_header_is_unauthorized_everywhere() {
    let cluster = cluster().await;
    for (method, path, _) in routes() {
        for value in [
            "Bearer ",
            "Bearer",
            "Basic client-token-0123456789",
            CLIENT_TOKEN, // the right secret, but no scheme
        ] {
            let request = Request::builder()
                .method(method.clone())
                .uri(&path)
                .header(header::AUTHORIZATION, value)
                .body(Body::empty())
                .unwrap();
            let status = cluster.app.clone().oneshot(request).await.unwrap().status();
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {path} {value:?}"
            );
        }
    }
}

#[tokio::test]
async fn the_scheme_name_is_case_insensitive() {
    let cluster = cluster().await;
    let request = Request::builder()
        .uri(LIST_NODES_PATH)
        .header(header::AUTHORIZATION, format!("bearer {CLIENT_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let status = cluster.app.clone().oneshot(request).await.unwrap().status();
    assert_eq!(outcome(status), Outcome::Admitted, "{status}");
}

#[tokio::test]
async fn a_rejection_asks_for_a_bearer_token_and_does_not_echo_the_secret() {
    let cluster = cluster().await;
    let request = Request::builder()
        .uri(LIST_NODES_PATH)
        .header(header::AUTHORIZATION, "Bearer wrong-secret-value-000")
        .body(Body::empty())
        .unwrap();
    let response = cluster.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[header::WWW_AUTHENTICATE], "Bearer");
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(!body.contains("wrong-secret-value-000"), "{body}");
}

#[tokio::test]
async fn without_authentication_every_route_is_open() {
    for (method, path, _) in routes() {
        let request = Request::builder()
            .method(method.clone())
            .uri(&path)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let app = meld_controller::api::router(ControllerState::new());
        let status = app.oneshot(request).await.unwrap().status();
        assert_eq!(
            outcome(status),
            Outcome::Admitted,
            "{method} {path}: {status}"
        );
    }
}
