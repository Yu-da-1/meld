//! Shared domain model and protocol contracts for Meld.

mod client;
mod data;
mod execution;
mod id;
mod job;
mod node;
mod protocol;
mod resource;
mod transition;

pub use client::{
    ApiErrorResponse, BlobResponse, CancelJobResponse, ExecutionView, JobLogsResponse,
    JobStatusResponse, LimitedResource, ListNodesResponse, MissingInputsResponse, NodeAssessment,
    NodeStateResponse, NodeVerdict, NodeView, QueueReason, SubmitJobResponse,
};
pub use data::{
    DataFailure, DataSpec, DataSpecError, InputFile, InvalidDigest, InvalidPath, MAX_FILE_BYTES,
    MAX_FILES_PER_JOB, MAX_JOB_INPUT_BYTES, MAX_PATH_BYTES, OutputFile, OutputSpec, Sha256Digest,
    validate_relative_path,
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
