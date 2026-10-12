//! Shared domain model and protocol contracts for Meld.

mod auth;
mod client;
mod data;
mod execution;
mod id;
mod job;
mod node;
mod private_dir;
mod protocol;
mod resource;
mod transition;

pub use auth::{
    CLIENT_TOKEN_ENV, InvalidToken, NODE_TOKEN_ENV, NodeSecret, SecretToken, credential_digest,
};
pub use client::{
    ApiErrorResponse, BlobResponse, CancelJobResponse, ExecutionView, JobLogsResponse,
    JobStatusResponse, LimitedResource, ListNodesResponse, MissingInputsResponse, NodeAssessment,
    NodeStateResponse, NodeVerdict, NodeView, QueueReason, RetryCause, RetryJobRequest,
    RetryJobResponse, RetryView, SubmitJobResponse,
};
pub use data::{
    DataFailure, DataSpec, DataSpecError, InputFile, InvalidDigest, InvalidPath, MAX_FILE_BYTES,
    MAX_FILES_PER_JOB, MAX_JOB_INPUT_BYTES, MAX_PATH_BYTES, OutputFile, OutputSpec, Sha256Digest,
    validate_relative_path,
};
pub use execution::{
    CapturedStream, Execution, ExecutionCompletionError, ExecutionOutput, ExecutionResult,
    ExecutionState, LimitBreach, MEMORY_LIMIT_MARGIN_PERCENT, memory_limit_bytes,
};
pub use id::{ExecutionId, JobId, MessageId, NodeId};
pub use job::{Job, JobSpec, JobSpecValidationError, JobState, PlacementConstraints, RetryPolicy};
pub use node::{Admission, NodeDescriptor, NodeState};
pub use private_dir::create_private_directory;
pub use protocol::{
    Acknowledgement, CURRENT_PROTOCOL_VERSION, ExecutionAssignment, ExecutionEvent,
    HeartbeatRequest, NodeCommand, PollNodeCommandRequest, PollNodeCommandResponse, ProtocolError,
    ProtocolErrorResponse, ProtocolVersion, RegisterNodeRequest, RegisterNodeResponse,
    ReportExecutionEventRequest, RequestMetadata, ResponseMetadata,
};
pub use resource::{ResourceCapacity, ResourceRequirements, ResourceSnapshot};
pub use transition::InvalidStateTransition;

/// An error and the causes beneath it, as `outer: inner: innermost`.
///
/// Libraries often report only the outermost step ("error sending request"),
/// which hides what an operator needs to see.
pub fn error_chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        // Some errors repeat their cause in their own text.
        if !text.contains(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fmt};

    use super::error_chain;

    #[derive(Debug)]
    struct Layer(&'static str, Option<Box<Layer>>);

    impl fmt::Display for Layer {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl Error for Layer {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.1
                .as_deref()
                .map(|inner| inner as &(dyn Error + 'static))
        }
    }

    #[test]
    fn every_cause_is_named_once() {
        let error = Layer(
            "sending the request failed",
            Some(Box::new(Layer(
                "handshake failed",
                Some(Box::new(Layer("fingerprint does not match", None))),
            ))),
        );

        assert_eq!(
            error_chain(&error),
            "sending the request failed: handshake failed: fingerprint does not match"
        );
    }

    #[test]
    fn a_cause_the_outer_text_already_repeats_is_not_said_twice() {
        let error = Layer(
            "failed: disk full",
            Some(Box::new(Layer("disk full", None))),
        );

        assert_eq!(error_chain(&error), "failed: disk full");
    }
}
