//! Shared domain model and protocol contracts for Meld.

mod execution;
mod id;
mod job;
mod node;
mod protocol;
mod resource;
mod transition;

pub use execution::{Execution, ExecutionCompletionError, ExecutionResult, ExecutionState};
pub use id::{ExecutionId, JobId, MessageId, NodeId};
pub use job::{Job, JobSpec, JobSpecValidationError, JobState};
pub use node::{NodeDescriptor, NodeState};
pub use protocol::{
    Acknowledgement, CURRENT_PROTOCOL_VERSION, ExecutionAssignment, ExecutionEvent,
    HeartbeatRequest, PollAssignmentRequest, PollAssignmentResponse, ProtocolVersion,
    RegisterNodeRequest, RegisterNodeResponse, ReportExecutionEventRequest, RequestMetadata,
    ResponseMetadata,
};
pub use resource::{ResourceCapacity, ResourceRequirements, ResourceSnapshot};
pub use transition::InvalidStateTransition;
