//! Shared domain model and protocol contracts for Meld.

mod client;
mod execution;
mod id;
mod job;
mod node;
mod protocol;
mod resource;
mod transition;

pub use client::{
    ApiErrorResponse, CancelJobResponse, ExecutionView, JobLogsResponse, JobStatusResponse,
    LimitedResource, ListNodesResponse, NodeAssessment, NodeStateResponse, NodeVerdict, NodeView,
    QueueReason, SubmitJobResponse,
};
pub use execution::{
    CapturedStream, Execution, ExecutionCompletionError, ExecutionOutput, ExecutionResult,
    ExecutionState,
};
pub use id::{ExecutionId, JobId, MessageId, NodeId};
pub use job::{Job, JobSpec, JobSpecValidationError, JobState, PlacementConstraints};
pub use node::{NodeDescriptor, NodeState};
pub use protocol::{
    Acknowledgement, CURRENT_PROTOCOL_VERSION, ExecutionAssignment, ExecutionEvent,
    HeartbeatRequest, NodeCommand, PollNodeCommandRequest, PollNodeCommandResponse, ProtocolError,
    ProtocolErrorResponse, ProtocolVersion, RegisterNodeRequest, RegisterNodeResponse,
    ReportExecutionEventRequest, RequestMetadata, ResponseMetadata,
};
pub use resource::{ResourceCapacity, ResourceRequirements, ResourceSnapshot};
pub use transition::InvalidStateTransition;
