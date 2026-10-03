//! Transport-independent messages exchanged by user clients and the controller.

use serde::{Deserialize, Serialize};

use crate::{
    ExecutionId, ExecutionOutput, ExecutionResult, ExecutionState, JobId, JobSpec, JobState,
    NodeDescriptor, NodeId, NodeState, ResourceSnapshot,
};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitJobResponse {
    pub job_id: JobId,
    pub state: JobState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelJobResponse {
    pub job_id: JobId,
    pub state: JobState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStatusResponse {
    pub job_id: JobId,
    pub spec: JobSpec,
    pub state: JobState,
    pub queue_reason: Option<QueueReason>,
    pub execution: Option<ExecutionView>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueReason {
    NoReadyNodes,
    InsufficientResources,
    NoAvailableNodes,
    WaitingForEarlierJob,
    AwaitingAssignment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionView {
    pub execution_id: ExecutionId,
    pub node_id: NodeId,
    pub state: ExecutionState,
    pub result: Option<ExecutionResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobLogsResponse {
    pub job_id: JobId,
    pub execution_id: ExecutionId,
    pub output: ExecutionOutput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiErrorResponse {
    pub error: String,
}
