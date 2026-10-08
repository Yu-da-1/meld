//! Transport-independent messages exchanged by user clients and the controller.

use serde::{Deserialize, Serialize};

use crate::{
    DataFailure, ExecutionId, ExecutionOutput, ExecutionResult, ExecutionState, JobId, JobSpec,
    JobState, NodeDescriptor, NodeId, NodeState, OutputFile, ResourceSnapshot, Sha256Digest,
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

/// Effective state of a node after an operator action such as drain or resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStateResponse {
    pub node_id: NodeId,
    pub state: NodeState,
}

/// How the scheduler judged one node for one job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAssessment {
    pub node_id: NodeId,
    pub hostname: String,
    pub verdict: NodeVerdict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeVerdict {
    /// Chosen: lowest load once the job is added (lowest Node ID on a tie).
    Selected {
        load_permille: u32,
    },
    /// Could run the job, but the chosen node had a lower load or, on equal
    /// load, a lower Node ID.
    Eligible {
        load_permille: u32,
    },
    /// Not accepting work: joining, draining, or unreachable.
    NotReady {
        state: NodeState,
    },
    ConstraintsNotSatisfied,
    /// Total capacity is smaller than the request, so waiting cannot help.
    InsufficientCapacity,
    /// Large enough in total, but this resource is currently reserved by other jobs.
    NoFreeCapacity {
        resource: LimitedResource,
    },
}

/// The first resource found to be exhausted on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitedResource {
    ConcurrentExecutions,
    Cpu,
    Memory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitJobResponse {
    pub job_id: JobId,
    pub state: JobState,
}

/// Asks for a new attempt of a job that ended unsuccessfully.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryJobRequest {
    /// Retry a lost job even though its first process may still be running.
    #[serde(default)]
    pub allow_duplicate_run: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryJobResponse {
    pub job_id: JobId,
    pub state: JobState,
    /// Attempts made before this retry.
    pub previous_attempts: u32,
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
    /// Every attempt so far, oldest first. `execution` is the last of them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<ExecutionView>,
    /// Why the latest execution failed to move the job's data, if it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_failure: Option<DataFailure>,
    /// Files the latest execution left for collection.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<OutputFile>,
    /// Per-node reasoning: live while the job is queued, as recorded at
    /// placement time once it has been assigned.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub placement: Vec<NodeAssessment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueReason {
    NoReadyNodes,
    ConstraintsNotSatisfied,
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

/// Describes a blob held by the controller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobResponse {
    pub sha256: Sha256Digest,
    pub size_bytes: u64,
}

/// Rejects a submission whose input content has not been uploaded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingInputsResponse {
    pub error: String,
    /// Digests the client must upload before submitting again.
    pub missing: Vec<Sha256Digest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiErrorResponse {
    pub error: String,
}
