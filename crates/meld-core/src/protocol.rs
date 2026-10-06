//! Transport-independent messages exchanged by controllers and nodes.

use serde::{Deserialize, Serialize};

use crate::{
    DataFailure, ExecutionId, ExecutionOutput, ExecutionResult, JobId, JobSpec, MessageId,
    NodeDescriptor, NodeId, OutputFile, ResourceSnapshot,
};

/// Protocol version implemented by this build.
pub const CURRENT_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::new(7);

/// Version of the controller-node message contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProtocolVersion(u16);

impl ProtocolVersion {
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u16 {
        self.0
    }
}

/// Metadata included in every request from a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestMetadata {
    pub message_id: MessageId,
    pub protocol_version: ProtocolVersion,
}

impl RequestMetadata {
    pub fn new() -> Self {
        Self {
            message_id: MessageId::generate(),
            protocol_version: CURRENT_PROTOCOL_VERSION,
        }
    }
}

impl Default for RequestMetadata {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata included in a response correlated to one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseMetadata {
    pub message_id: MessageId,
    pub protocol_version: ProtocolVersion,
    pub in_reply_to: MessageId,
}

impl ResponseMetadata {
    pub fn for_request(request: RequestMetadata) -> Self {
        Self {
            message_id: MessageId::generate(),
            protocol_version: CURRENT_PROTOCOL_VERSION,
            in_reply_to: request.message_id,
        }
    }
}

/// First request sent by a node when joining or reconnecting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterNodeRequest {
    pub metadata: RequestMetadata,
    pub node: NodeDescriptor,
}

/// Confirms that the controller accepted a node identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterNodeResponse {
    pub metadata: ResponseMetadata,
    pub node_id: NodeId,
}

/// Describes a protocol-level request failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProtocolError {
    ProtocolVersionMismatch {
        expected: ProtocolVersion,
        received: ProtocolVersion,
    },
    NodeNotRegistered {
        node_id: NodeId,
    },
}

/// Returns a structured protocol failure correlated to one request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolErrorResponse {
    pub metadata: ResponseMetadata,
    pub error: ProtocolError,
}

/// Reports current resource usage and proves node liveness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub metadata: RequestMetadata,
    pub node_id: NodeId,
    pub snapshot: ResourceSnapshot,
}

/// Acknowledges a request that has no response payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acknowledgement {
    pub metadata: ResponseMetadata,
}

/// Polls for the next control command for a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollNodeCommandRequest {
    pub metadata: RequestMetadata,
    pub node_id: NodeId,
    /// Executions the node currently manages. The controller does not
    /// redeliver these and may request cancellation of any of them.
    pub active_execution_ids: Vec<ExecutionId>,
}

/// Returns a control command or no value when the poll expires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollNodeCommandResponse {
    pub metadata: ResponseMetadata,
    pub command: Option<NodeCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
// One command is built per poll and never stored in bulk, so the size gap
// costs nothing; boxing would only add indirection at every call site.
#[allow(clippy::large_enum_variant)]
pub enum NodeCommand {
    Start { assignment: ExecutionAssignment },
    Cancel { execution_id: ExecutionId },
}

/// Tells one node to execute one attempt of a logical job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionAssignment {
    pub execution_id: ExecutionId,
    pub job_id: JobId,
    pub node_id: NodeId,
    pub spec: JobSpec,
}

/// Reports one observed execution lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportExecutionEventRequest {
    pub metadata: RequestMetadata,
    pub node_id: NodeId,
    pub execution_id: ExecutionId,
    pub event: ExecutionEvent,
}

/// Facts a node may report about an assigned execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionEvent {
    Accepted,
    Running,
    Finished {
        result: ExecutionResult,
        output: ExecutionOutput,
        /// Declared outputs, already uploaded to the controller. Empty unless
        /// the process succeeded.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        outputs: Vec<OutputFile>,
    },
    StartFailed {
        reason: String,
    },
    /// The node could not move the job's data, so the process never started.
    DataFailed {
        failure: DataFailure,
    },
    Rejected {
        reason: String,
    },
    Cancelled {
        output: ExecutionOutput,
    },
    TimedOut {
        output: ExecutionOutput,
    },
}

#[cfg(test)]
mod tests {
    use crate::{CapturedStream, ResourceCapacity, ResourceRequirements};

    use super::*;

    #[test]
    fn registration_round_trips_through_json() {
        let request = RegisterNodeRequest {
            metadata: RequestMetadata::new(),
            node: descriptor(),
        };

        assert_json_round_trip(&request);
    }

    #[test]
    fn protocol_error_round_trips_through_json() {
        let request = RequestMetadata {
            message_id: MessageId::generate(),
            protocol_version: ProtocolVersion::new(2),
        };
        let response = ProtocolErrorResponse {
            metadata: ResponseMetadata::for_request(request),
            error: ProtocolError::ProtocolVersionMismatch {
                expected: CURRENT_PROTOCOL_VERSION,
                received: request.protocol_version,
            },
        };

        assert_eq!(response.metadata.in_reply_to, request.message_id);
        assert_json_round_trip(&response);
    }

    #[test]
    fn assignment_round_trips_through_json() {
        let request = RequestMetadata::new();
        let response = PollNodeCommandResponse {
            metadata: ResponseMetadata::for_request(request),
            command: Some(NodeCommand::Start {
                assignment: ExecutionAssignment {
                    execution_id: ExecutionId::generate(),
                    job_id: JobId::generate(),
                    node_id: NodeId::generate(),
                    spec: JobSpec {
                        program: "rustc".to_owned(),
                        args: vec!["--version".to_owned()],
                        requirements: ResourceRequirements {
                            logical_cpus: 1,
                            memory_bytes: 256_000_000,
                        },
                        job_timeout_secs: None,
                        execution_timeout_secs: None,
                        constraints: crate::PlacementConstraints::default(),
                        data: crate::DataSpec::default(),
                    },
                },
            }),
        };

        assert_eq!(response.metadata.in_reply_to, request.message_id);
        assert_json_round_trip(&response);
    }

    #[test]
    fn data_failure_event_round_trips_through_json() {
        let request = ReportExecutionEventRequest {
            metadata: RequestMetadata::new(),
            node_id: NodeId::generate(),
            execution_id: ExecutionId::generate(),
            event: ExecutionEvent::DataFailed {
                failure: DataFailure::ChecksumMismatch {
                    path: "data.csv".to_owned(),
                },
            },
        };

        let json = serde_json::to_string(&request).expect("event should serialize");

        assert!(json.contains(r#""type":"data_failed""#));
        assert_eq!(
            serde_json::from_str::<ReportExecutionEventRequest>(&json)
                .expect("event should deserialize"),
            request
        );
    }

    #[test]
    fn tagged_execution_event_round_trips_through_json() {
        let request = ReportExecutionEventRequest {
            metadata: RequestMetadata::new(),
            node_id: NodeId::generate(),
            execution_id: ExecutionId::generate(),
            event: ExecutionEvent::Finished {
                result: ExecutionResult { exit_code: Some(0) },
                outputs: vec![],
                output: ExecutionOutput {
                    stdout: CapturedStream {
                        content: "done\n".to_owned(),
                        truncated: false,
                        lossy: false,
                    },
                    stderr: CapturedStream {
                        content: String::new(),
                        truncated: false,
                        lossy: false,
                    },
                },
            },
        };

        let json = serde_json::to_string(&request).expect("event should serialize");
        assert!(json.contains(r#""type":"finished""#));
        let deserialized = serde_json::from_str::<ReportExecutionEventRequest>(&json)
            .expect("event should deserialize");
        assert_eq!(deserialized, request);
    }

    fn assert_json_round_trip<T>(value: &T)
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug,
    {
        let json = serde_json::to_string(value).expect("message should serialize");
        let deserialized: T = serde_json::from_str(&json).expect("message should deserialize");
        assert_eq!(&deserialized, value);
    }

    fn descriptor() -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId::generate(),
            hostname: "worker".to_owned(),
            operating_system: "windows".to_owned(),
            architecture: "x86_64".to_owned(),
            capacity: ResourceCapacity {
                logical_cpus: 8,
                memory_bytes: 16_000_000_000,
                max_concurrent_executions: 8,
            },
            capabilities: vec![],
        }
    }
}
