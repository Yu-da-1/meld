//! Which caller may use which route.
//!
//! Each route is requested with no token, a wrong token, and each role's
//! token. Only the status class matters here: a request that gets past
//! authentication may still be rejected for its (empty) body, but never with
//! 401 or 403.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use meld_controller::{
    api::{
        BLOB_PATH, CANCEL_JOB_PATH, ControllerState, DRAIN_NODE_PATH, HEARTBEAT_PATH,
        JOB_LOGS_PATH, JOB_STATUS_PATH, LIST_NODES_PATH, POLL_NODE_COMMAND_PATH,
        REGISTER_NODE_PATH, REPORT_EXECUTION_EVENT_PATH, RESUME_NODE_PATH, RETRY_JOB_PATH,
        SUBMIT_JOB_PATH, router_with_auth,
    },
    auth::Authenticator,
};
use meld_core::SecretToken;
use tower::ServiceExt;

const CLIENT_TOKEN: &str = "client-token-0123456789";
const NODE_TOKEN: &str = "node-token-0123456789ab";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Who {
    Client,
    Node,
    Either,
}

/// Every route of the controller. A new route has to be added here, and
/// that is where its audience is decided.
fn routes() -> Vec<(Method, String, Who)> {
    let id = "00000000-0000-0000-0000-000000000000";
    let digest = "0".repeat(64);
    let path = |template: &str| {
        template
            .replace("{node_id}", id)
            .replace("{job_id}", id)
            .replace("{sha256}", &digest)
    };
    vec![
        (Method::GET, path(LIST_NODES_PATH), Who::Client),
        (Method::POST, path(DRAIN_NODE_PATH), Who::Client),
        (Method::POST, path(RESUME_NODE_PATH), Who::Client),
        (Method::POST, path(SUBMIT_JOB_PATH), Who::Client),
        (Method::GET, path(JOB_STATUS_PATH), Who::Client),
        (Method::GET, path(JOB_LOGS_PATH), Who::Client),
        (Method::POST, path(CANCEL_JOB_PATH), Who::Client),
        (Method::POST, path(RETRY_JOB_PATH), Who::Client),
        (Method::POST, path(REGISTER_NODE_PATH), Who::Node),
        (Method::POST, path(HEARTBEAT_PATH), Who::Node),
        (Method::POST, path(POLL_NODE_COMMAND_PATH), Who::Node),
        (Method::POST, path(REPORT_EXECUTION_EVENT_PATH), Who::Node),
        (Method::GET, path(BLOB_PATH), Who::Either),
        (Method::PUT, path(BLOB_PATH), Who::Either),
    ]
}

fn app() -> axum::Router {
    let authenticator = Authenticator::enabled(
        &SecretToken::new(CLIENT_TOKEN).unwrap(),
        &SecretToken::new(NODE_TOKEN).unwrap(),
    )
    .unwrap();
    router_with_auth(ControllerState::new(), authenticator)
}

async fn status_of(method: &Method, path: &str, authorization: Option<&str>) -> StatusCode {
    let mut request = Request::builder().method(method.clone()).uri(path);
    if let Some(value) = authorization {
        request = request.header(header::AUTHORIZATION, value);
    }
    let request = request
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{}"))
        .unwrap();
    app().oneshot(request).await.unwrap().status()
}

fn admitted(status: StatusCode) -> bool {
    status != StatusCode::UNAUTHORIZED && status != StatusCode::FORBIDDEN
}

#[tokio::test]
async fn no_route_answers_without_a_token() {
    for (method, path, _) in routes() {
        assert_eq!(
            status_of(&method, &path, None).await,
            StatusCode::UNAUTHORIZED,
            "{method} {path}"
        );
    }
}

#[tokio::test]
async fn a_wrong_or_malformed_token_is_unauthorized() {
    for (method, path, _) in routes() {
        for header in [
            "Bearer not-the-token-at-all",
            "Bearer ",
            "Basic client-token-0123456789",
            CLIENT_TOKEN, // right secret, but no scheme
        ] {
            assert_eq!(
                status_of(&method, &path, Some(header)).await,
                StatusCode::UNAUTHORIZED,
                "{method} {path} with {header:?}"
            );
        }
    }
}

#[tokio::test]
async fn each_token_works_only_where_its_role_is_allowed() {
    let client = format!("Bearer {CLIENT_TOKEN}");
    let node = format!("Bearer {NODE_TOKEN}");
    for (method, path, who) in routes() {
        let with_client = status_of(&method, &path, Some(&client)).await;
        let with_node = status_of(&method, &path, Some(&node)).await;
        assert_eq!(
            admitted(with_client),
            matches!(who, Who::Client | Who::Either),
            "client token on {method} {path}: {with_client}"
        );
        assert_eq!(
            admitted(with_node),
            matches!(who, Who::Node | Who::Either),
            "node token on {method} {path}: {with_node}"
        );
        if !admitted(with_client) {
            assert_eq!(with_client, StatusCode::FORBIDDEN, "{method} {path}");
        }
        if !admitted(with_node) {
            assert_eq!(with_node, StatusCode::FORBIDDEN, "{method} {path}");
        }
    }
}

#[tokio::test]
async fn the_scheme_name_is_case_insensitive() {
    let (method, path, _) = routes().remove(0);
    let status = status_of(&method, &path, Some(&format!("bearer {CLIENT_TOKEN}"))).await;
    assert!(admitted(status), "{status}");
}

#[tokio::test]
async fn a_rejection_asks_for_a_bearer_token_and_does_not_echo_the_secret() {
    let request = Request::builder()
        .uri(LIST_NODES_PATH)
        .header(header::AUTHORIZATION, "Bearer wrong-secret-value-000")
        .body(Body::empty())
        .unwrap();
    let response = app().oneshot(request).await.unwrap();
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
        assert!(admitted(status), "{method} {path}: {status}");
    }
}
