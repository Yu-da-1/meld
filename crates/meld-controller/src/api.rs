//! HTTP boundary for controller-node protocol messages.

use std::{
    error::Error,
    fmt,
    sync::{Arc, RwLock},
    time::Instant,
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use meld_core::{
    Acknowledgement, CURRENT_PROTOCOL_VERSION, HeartbeatRequest, NodeDescriptor, NodeId, NodeState,
    ProtocolError, ProtocolErrorResponse, RegisterNodeRequest, RegisterNodeResponse,
    ResourceSnapshot, ResponseMetadata,
};
use serde::{Deserialize, Serialize};

use crate::{
    failure_detector::FailureDetector,
    node_registry::{NodeRegistry, NodeRegistryError},
};

pub const REGISTER_NODE_PATH: &str = "/v1/nodes/register";
pub const HEARTBEAT_PATH: &str = "/v1/nodes/heartbeat";
pub const LIST_NODES_PATH: &str = "/v1/nodes";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListNodesResponse {
    pub nodes: Vec<NodeView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeView {
    pub descriptor: NodeDescriptor,
    pub state: NodeState,
    pub snapshot: Option<ResourceSnapshot>,
    pub last_heartbeat_age_ms: Option<u64>,
}

/// Shared controller state exposed to HTTP handlers.
#[derive(Debug, Clone, Default)]
pub struct ControllerState {
    registry: Arc<RwLock<NodeRegistry>>,
}

impl ControllerState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn detect_unreachable_nodes(
        &self,
        detector: &FailureDetector,
        now: Instant,
    ) -> Result<Vec<NodeId>, ControllerStateError> {
        let mut registry = self.registry.write().map_err(|_| ControllerStateError)?;
        Ok(detector.detect(&mut registry, now))
    }

    fn node_views_at(&self, now: Instant) -> Result<Vec<NodeView>, ControllerStateError> {
        let registry = self.registry.read().map_err(|_| ControllerStateError)?;
        Ok(registry
            .nodes()
            .map(|node| NodeView {
                descriptor: node.descriptor().clone(),
                state: node.state(),
                snapshot: node.snapshot(),
                last_heartbeat_age_ms: node.last_heartbeat_at().map(|last_heartbeat| {
                    let age = now.saturating_duration_since(last_heartbeat).as_millis();
                    age.min(u128::from(u64::MAX)) as u64
                }),
            })
            .collect())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerStateError;

impl fmt::Display for ControllerStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("controller state lock is poisoned")
    }
}

impl Error for ControllerStateError {}

pub fn router(state: ControllerState) -> Router {
    Router::new()
        .route(LIST_NODES_PATH, get(list_nodes))
        .route(REGISTER_NODE_PATH, post(register_node))
        .route(HEARTBEAT_PATH, post(record_heartbeat))
        .with_state(state)
}

async fn list_nodes(State(state): State<ControllerState>) -> Response {
    match state.node_views_at(Instant::now()) {
        Ok(nodes) => Json(ListNodesResponse { nodes }).into_response(),
        Err(error) => {
            tracing::error!(%error, "node registry lock is poisoned");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            )
                .into_response()
        }
    }
}

async fn register_node(
    State(state): State<ControllerState>,
    Json(request): Json<RegisterNodeRequest>,
) -> Response {
    let request_metadata = request.metadata;
    if let Some(response) = protocol_version_error(request_metadata) {
        return response;
    }

    let node_id = request.node.id;
    let mut registry = match state.registry.write() {
        Ok(registry) => registry,
        Err(error) => {
            tracing::error!(%node_id, %error, "node registry lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            )
                .into_response();
        }
    };
    registry.register(request.node);
    drop(registry);

    tracing::info!(%node_id, "node registered");

    Json(RegisterNodeResponse {
        metadata: ResponseMetadata::for_request(request_metadata),
        node_id,
    })
    .into_response()
}

async fn record_heartbeat(
    State(state): State<ControllerState>,
    Json(request): Json<HeartbeatRequest>,
) -> Response {
    let request_metadata = request.metadata;
    if let Some(response) = protocol_version_error(request_metadata) {
        return response;
    }

    let mut registry = match state.registry.write() {
        Ok(registry) => registry,
        Err(error) => {
            tracing::error!(node_id = %request.node_id, %error, "node registry lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            )
                .into_response();
        }
    };

    if let Err(NodeRegistryError::NodeNotFound(node_id)) =
        registry.record_heartbeat(request.node_id, request.snapshot)
    {
        return (
            StatusCode::NOT_FOUND,
            Json(ProtocolErrorResponse {
                metadata: ResponseMetadata::for_request(request_metadata),
                error: ProtocolError::NodeNotRegistered { node_id },
            }),
        )
            .into_response();
    }
    drop(registry);

    tracing::debug!(
        node_id = %request.node_id,
        cpu_usage_percent = request.snapshot.cpu_usage_percent,
        available_memory_bytes = request.snapshot.available_memory_bytes,
        running_executions = request.snapshot.running_executions,
        "heartbeat recorded"
    );

    Json(Acknowledgement {
        metadata: ResponseMetadata::for_request(request_metadata),
    })
    .into_response()
}

fn protocol_version_error(metadata: meld_core::RequestMetadata) -> Option<Response> {
    (metadata.protocol_version != CURRENT_PROTOCOL_VERSION).then(|| {
        (
            StatusCode::UPGRADE_REQUIRED,
            Json(ProtocolErrorResponse {
                metadata: ResponseMetadata::for_request(metadata),
                error: ProtocolError::ProtocolVersionMismatch {
                    expected: CURRENT_PROTOCOL_VERSION,
                    received: metadata.protocol_version,
                },
            }),
        )
            .into_response()
    })
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, header},
    };
    use meld_core::{
        MessageId, NodeDescriptor, NodeId, NodeState, ProtocolVersion, RequestMetadata,
        ResourceCapacity, ResourceSnapshot,
    };
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn node_list_is_empty_before_registration() {
        let response = router(ControllerState::new())
            .oneshot(list_nodes_request())
            .await
            .expect("node list request should be handled");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: ListNodesResponse =
            serde_json::from_slice(&body).expect("response should contain node list JSON");
        assert!(response.nodes.is_empty());
    }

    #[tokio::test]
    async fn node_list_returns_observed_state_in_node_id_order() {
        let state = ControllerState::new();
        let lower_id = "00000000-0000-0000-0000-000000000001"
            .parse()
            .expect("fixed Node ID should be valid");
        let higher_id = "00000000-0000-0000-0000-000000000002"
            .parse()
            .expect("fixed Node ID should be valid");
        let heartbeat_at = Instant::now();
        {
            let mut registry = state
                .registry
                .write()
                .expect("registry lock should be available");
            registry.register(descriptor(higher_id));
            registry.register(descriptor(lower_id));
            registry
                .record_heartbeat_at(lower_id, snapshot(), heartbeat_at)
                .expect("registered node should accept heartbeat");
        }

        let response = router(state)
            .oneshot(list_nodes_request())
            .await
            .expect("node list request should be handled");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: ListNodesResponse =
            serde_json::from_slice(&body).expect("response should contain node list JSON");

        assert_eq!(response.nodes.len(), 2);
        assert_eq!(response.nodes[0].descriptor.id, lower_id);
        assert_eq!(response.nodes[0].state, NodeState::Ready);
        assert_eq!(response.nodes[0].snapshot, Some(snapshot()));
        assert!(response.nodes[0].last_heartbeat_age_ms.is_some());
        assert_eq!(response.nodes[1].descriptor.id, higher_id);
        assert_eq!(response.nodes[1].state, NodeState::Joining);
        assert_eq!(response.nodes[1].snapshot, None);
        assert_eq!(response.nodes[1].last_heartbeat_age_ms, None);
    }

    #[tokio::test]
    async fn registration_stores_node_and_correlates_response() {
        let state = ControllerState::new();
        let request = registration_request(CURRENT_PROTOCOL_VERSION);
        let request_metadata = request.metadata;
        let node_id = request.node.id;

        let response = router(state.clone())
            .oneshot(registration_json_request(request))
            .await
            .expect("registration request should be handled");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: RegisterNodeResponse =
            serde_json::from_slice(&body).expect("response should contain registration JSON");
        assert_eq!(response.node_id, node_id);
        assert_eq!(response.metadata.in_reply_to, request_metadata.message_id);

        let registry = state
            .registry
            .read()
            .expect("registry lock should be available");
        assert_eq!(
            registry.get(node_id).map(|node| node.state()),
            Some(NodeState::Joining)
        );
    }

    #[tokio::test]
    async fn version_mismatch_is_rejected_without_registering_node() {
        let state = ControllerState::new();
        let request =
            registration_request(ProtocolVersion::new(CURRENT_PROTOCOL_VERSION.value() + 1));
        let request_metadata = request.metadata;
        let node_id = request.node.id;

        let response = router(state.clone())
            .oneshot(registration_json_request(request))
            .await
            .expect("registration request should be handled");

        assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: ProtocolErrorResponse =
            serde_json::from_slice(&body).expect("response should contain protocol error JSON");
        assert_eq!(response.metadata.in_reply_to, request_metadata.message_id);
        assert_eq!(
            response.error,
            ProtocolError::ProtocolVersionMismatch {
                expected: CURRENT_PROTOCOL_VERSION,
                received: request_metadata.protocol_version,
            }
        );

        let registry = state
            .registry
            .read()
            .expect("registry lock should be available");
        assert!(registry.get(node_id).is_none());
    }

    #[tokio::test]
    async fn heartbeat_makes_registered_node_ready_and_updates_snapshot() {
        let state = ControllerState::new();
        let registration = registration_request(CURRENT_PROTOCOL_VERSION);
        let node_id = registration.node.id;
        state
            .registry
            .write()
            .expect("registry lock should be available")
            .register(registration.node);
        let snapshot = snapshot();
        let request = HeartbeatRequest {
            metadata: RequestMetadata::new(),
            node_id,
            snapshot,
        };
        let request_metadata = request.metadata;

        let response = router(state.clone())
            .oneshot(heartbeat_json_request(request))
            .await
            .expect("heartbeat request should be handled");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: Acknowledgement =
            serde_json::from_slice(&body).expect("response should contain acknowledgement JSON");
        assert_eq!(response.metadata.in_reply_to, request_metadata.message_id);

        let registry = state
            .registry
            .read()
            .expect("registry lock should be available");
        let node = registry
            .get(node_id)
            .expect("node should remain registered");
        assert_eq!(node.state(), NodeState::Ready);
        assert_eq!(node.snapshot(), Some(snapshot));
    }

    #[tokio::test]
    async fn heartbeat_from_unregistered_node_is_rejected() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let request = HeartbeatRequest {
            metadata: RequestMetadata::new(),
            node_id,
            snapshot: snapshot(),
        };

        let response = router(state.clone())
            .oneshot(heartbeat_json_request(request))
            .await
            .expect("heartbeat request should be handled");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: ProtocolErrorResponse =
            serde_json::from_slice(&body).expect("response should contain protocol error JSON");
        assert_eq!(response.error, ProtocolError::NodeNotRegistered { node_id });
    }

    fn list_nodes_request() -> Request<Body> {
        Request::builder()
            .uri(LIST_NODES_PATH)
            .body(Body::empty())
            .expect("HTTP request should be valid")
    }

    fn registration_json_request(request: RegisterNodeRequest) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(REGISTER_NODE_PATH)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&request).expect("request should serialize"),
            ))
            .expect("HTTP request should be valid")
    }

    fn heartbeat_json_request(request: HeartbeatRequest) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(HEARTBEAT_PATH)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&request).expect("request should serialize"),
            ))
            .expect("HTTP request should be valid")
    }

    fn registration_request(protocol_version: ProtocolVersion) -> RegisterNodeRequest {
        RegisterNodeRequest {
            metadata: RequestMetadata {
                message_id: MessageId::generate(),
                protocol_version,
            },
            node: descriptor(NodeId::generate()),
        }
    }

    fn descriptor(node_id: NodeId) -> NodeDescriptor {
        NodeDescriptor {
            id: node_id,
            hostname: "worker-1".to_owned(),
            operating_system: "linux".to_owned(),
            architecture: "x86_64".to_owned(),
            capacity: ResourceCapacity {
                logical_cpus: 8,
                memory_bytes: 16_000,
            },
        }
    }

    fn snapshot() -> ResourceSnapshot {
        ResourceSnapshot {
            cpu_usage_percent: 12,
            available_memory_bytes: 8_000,
            running_executions: 0,
        }
    }
}
