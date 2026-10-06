//! HTTP client boundary for controller-node protocol requests.

use std::{error::Error, fmt, io, time::Duration};

use futures_util::StreamExt;
use meld_core::Sha256Digest;
use meld_core::{
    Acknowledgement, CURRENT_PROTOCOL_VERSION, ExecutionEvent, ExecutionId, HeartbeatRequest,
    NodeCommand, NodeDescriptor, NodeId, PollNodeCommandRequest, PollNodeCommandResponse,
    ProtocolError, ProtocolErrorResponse, RegisterNodeRequest, RegisterNodeResponse,
    ReportExecutionEventRequest, RequestMetadata, ResourceSnapshot, ResponseMetadata,
};
use reqwest::{Body, Client, Response, StatusCode, header::CONTENT_LENGTH};
use tokio_util::io::ReaderStream;

use crate::{
    collection::{BlobSink, UploadError},
    staging::{BlobSource, BlobStream, DownloadError},
};

const REGISTER_NODE_PATH: &str = "/v1/nodes/register";
const HEARTBEAT_PATH: &str = "/v1/nodes/heartbeat";
const POLL_NODE_COMMAND_PATH: &str = "/v1/nodes/commands/poll";
const REPORT_EXECUTION_EVENT_PATH: &str = "/v1/nodes/executions/events";
const BLOBS_PATH: &str = "/v1/blobs";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const BLOB_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest a transfer may stall; its total duration is deliberately unbounded.
const BLOB_READ_TIMEOUT: Duration = Duration::from_secs(30);
const COMMAND_POLL_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ControllerClient {
    http: Client,
    /// Without a total timeout, so large files can transfer.
    blob_http: Client,
    controller_url: String,
}

impl ControllerClient {
    pub fn new(controller_url: &str) -> Result<Self, reqwest::Error> {
        Ok(Self {
            http: Client::builder().timeout(REQUEST_TIMEOUT).build()?,
            blob_http: Client::builder()
                .connect_timeout(BLOB_CONNECT_TIMEOUT)
                .read_timeout(BLOB_READ_TIMEOUT)
                .build()?,
            controller_url: controller_url.trim_end_matches('/').to_owned(),
        })
    }

    pub async fn register(&self, node: &NodeDescriptor) -> Result<(), ControllerClientError> {
        let request = RegisterNodeRequest {
            metadata: RequestMetadata::new(),
            node: node.clone(),
        };
        let response = self
            .http
            .post(format!("{}{REGISTER_NODE_PATH}", self.controller_url))
            .json(&request)
            .send()
            .await
            .map_err(ControllerClientError::Unavailable)?;

        if is_protocol_error(response.status()) {
            return Err(decode_protocol_error(response).await);
        }
        if !response.status().is_success() {
            return Err(ControllerClientError::Rejected(response.status()));
        }

        let response = response
            .json::<RegisterNodeResponse>()
            .await
            .map_err(|error| ControllerClientError::InvalidResponse(error.to_string()))?;
        validate_response_metadata(request.metadata, response.metadata)?;
        if response.node_id != request.node.id {
            return Err(ControllerClientError::InvalidResponse(
                "controller response contains a different node identity".to_owned(),
            ));
        }
        Ok(())
    }

    pub async fn heartbeat(
        &self,
        node_id: NodeId,
        snapshot: ResourceSnapshot,
    ) -> Result<(), ControllerClientError> {
        let request = HeartbeatRequest {
            metadata: RequestMetadata::new(),
            node_id,
            snapshot,
        };
        let response = self
            .http
            .post(format!("{}{HEARTBEAT_PATH}", self.controller_url))
            .json(&request)
            .send()
            .await
            .map_err(ControllerClientError::Unavailable)?;

        if is_protocol_error(response.status()) {
            return Err(decode_protocol_error(response).await);
        }
        if !response.status().is_success() {
            return Err(ControllerClientError::Rejected(response.status()));
        }

        let response = response
            .json::<Acknowledgement>()
            .await
            .map_err(|error| ControllerClientError::InvalidResponse(error.to_string()))?;
        validate_response_metadata(request.metadata, response.metadata)
    }

    pub async fn poll_node_command(
        &self,
        node_id: NodeId,
        active_execution_ids: &[ExecutionId],
    ) -> Result<Option<NodeCommand>, ControllerClientError> {
        let request = PollNodeCommandRequest {
            metadata: RequestMetadata::new(),
            node_id,
            active_execution_ids: active_execution_ids.to_vec(),
        };
        let response = self
            .http
            .post(format!("{}{POLL_NODE_COMMAND_PATH}", self.controller_url))
            .timeout(COMMAND_POLL_REQUEST_TIMEOUT)
            .json(&request)
            .send()
            .await
            .map_err(ControllerClientError::Unavailable)?;

        if is_protocol_error(response.status()) {
            return Err(decode_protocol_error(response).await);
        }
        if !response.status().is_success() {
            return Err(ControllerClientError::Rejected(response.status()));
        }

        let response = response
            .json::<PollNodeCommandResponse>()
            .await
            .map_err(|error| ControllerClientError::InvalidResponse(error.to_string()))?;
        validate_response_metadata(request.metadata, response.metadata)?;
        validate_node_command(node_id, active_execution_ids, response.command.as_ref())?;
        Ok(response.command)
    }

    pub async fn report_execution_event(
        &self,
        node_id: NodeId,
        execution_id: ExecutionId,
        event: ExecutionEvent,
    ) -> Result<(), ControllerClientError> {
        let request = ReportExecutionEventRequest {
            metadata: RequestMetadata::new(),
            node_id,
            execution_id,
            event,
        };
        let response = self
            .http
            .post(format!(
                "{}{REPORT_EXECUTION_EVENT_PATH}",
                self.controller_url
            ))
            .json(&request)
            .send()
            .await
            .map_err(ControllerClientError::Unavailable)?;

        if is_protocol_error(response.status()) {
            return Err(decode_protocol_error(response).await);
        }
        if !response.status().is_success() {
            return Err(ControllerClientError::Rejected(response.status()));
        }

        let response = response
            .json::<Acknowledgement>()
            .await
            .map_err(|error| ControllerClientError::InvalidResponse(error.to_string()))?;
        validate_response_metadata(request.metadata, response.metadata)
    }
}

impl BlobSource for ControllerClient {
    async fn open(&self, digest: &Sha256Digest) -> Result<BlobStream, DownloadError> {
        let response = self
            .blob_http
            .get(format!("{}{BLOBS_PATH}/{digest}", self.controller_url))
            .send()
            .await
            .map_err(|error| DownloadError::Unavailable(error.to_string()))?;

        match response.status() {
            StatusCode::NOT_FOUND => Err(DownloadError::NotFound),
            status if status.is_success() => Ok(Box::pin(
                response
                    .bytes_stream()
                    .map(|chunk| chunk.map_err(io::Error::other)),
            )),
            status => Err(DownloadError::Unavailable(format!("HTTP {status}"))),
        }
    }
}

impl BlobSink for ControllerClient {
    async fn upload(
        &self,
        digest: &Sha256Digest,
        file: &std::path::Path,
        size_bytes: u64,
    ) -> Result<(), UploadError> {
        let url = format!("{}{BLOBS_PATH}/{digest}", self.controller_url);
        // Content is named by its digest, so an existing blob needs no transfer.
        if let Ok(existing) = self.blob_http.head(&url).send().await
            && existing.status().is_success()
        {
            return Ok(());
        }

        let file = tokio::fs::File::open(file)
            .await
            .map_err(|error| UploadError::Rejected(error.to_string()))?;
        let response = self
            .blob_http
            .put(&url)
            .header(CONTENT_LENGTH, size_bytes)
            .body(Body::wrap_stream(ReaderStream::new(file)))
            .send()
            .await
            .map_err(|error| UploadError::Unavailable(error.to_string()))?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else if status == StatusCode::INSUFFICIENT_STORAGE {
            // Full storage is a verdict, not a hiccup.
            Err(UploadError::Rejected(format!("HTTP {status}")))
        } else if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
            Err(UploadError::Unavailable(format!("HTTP {status}")))
        } else {
            Err(UploadError::Rejected(format!("HTTP {status}")))
        }
    }
}

#[derive(Debug)]
pub enum ControllerClientError {
    Unavailable(reqwest::Error),
    Protocol(ProtocolError),
    Rejected(StatusCode),
    InvalidResponse(String),
}

impl ControllerClientError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_))
            || matches!(self, Self::Rejected(status) if status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS)
    }

    pub fn requires_registration(&self) -> bool {
        matches!(
            self,
            Self::Protocol(ProtocolError::NodeNotRegistered { .. })
        )
    }
}

impl fmt::Display for ControllerClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(error) => write!(formatter, "controller is unavailable: {error}"),
            Self::Protocol(error) => write!(formatter, "controller rejected protocol: {error:?}"),
            Self::Rejected(status) => {
                write!(formatter, "controller rejected request with HTTP {status}")
            }
            Self::InvalidResponse(message) => {
                write!(
                    formatter,
                    "controller returned an invalid response: {message}"
                )
            }
        }
    }
}

impl Error for ControllerClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Unavailable(error) => Some(error),
            Self::Protocol(_) | Self::Rejected(_) | Self::InvalidResponse(_) => None,
        }
    }
}

fn is_protocol_error(status: StatusCode) -> bool {
    matches!(status, StatusCode::UPGRADE_REQUIRED | StatusCode::NOT_FOUND)
}

async fn decode_protocol_error(response: Response) -> ControllerClientError {
    match response.json::<ProtocolErrorResponse>().await {
        Ok(response) => ControllerClientError::Protocol(response.error),
        Err(error) => ControllerClientError::InvalidResponse(error.to_string()),
    }
}

fn validate_response_metadata(
    request: RequestMetadata,
    response: ResponseMetadata,
) -> Result<(), ControllerClientError> {
    if response.protocol_version != CURRENT_PROTOCOL_VERSION {
        return Err(ControllerClientError::InvalidResponse(format!(
            "protocol version is {}, expected {}",
            response.protocol_version.value(),
            CURRENT_PROTOCOL_VERSION.value()
        )));
    }
    if response.in_reply_to != request.message_id {
        return Err(ControllerClientError::InvalidResponse(
            "response does not correlate to the request".to_owned(),
        ));
    }

    Ok(())
}

fn validate_node_command(
    expected_node_id: NodeId,
    active_execution_ids: &[ExecutionId],
    command: Option<&NodeCommand>,
) -> Result<(), ControllerClientError> {
    match command {
        Some(NodeCommand::Start { assignment }) if assignment.node_id != expected_node_id => {
            Err(ControllerClientError::InvalidResponse(
                "controller returned an assignment for a different node".to_owned(),
            ))
        }
        Some(NodeCommand::Start { assignment })
            if active_execution_ids.contains(&assignment.execution_id) =>
        {
            Err(ControllerClientError::InvalidResponse(
                "controller redelivered an execution that is already active".to_owned(),
            ))
        }
        Some(NodeCommand::Cancel { execution_id })
            if !active_execution_ids.contains(execution_id) =>
        {
            Err(ControllerClientError::InvalidResponse(
                "controller returned cancellation for a different execution".to_owned(),
            ))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use meld_core::{
        ExecutionAssignment, JobId, JobSpec, MessageId, ProtocolVersion, ResourceRequirements,
    };

    use super::*;

    /// Starts a real controller with file storage on an ephemeral port.
    async fn spawn_controller(
        limits: meld_controller::blob_store::BlobLimits,
    ) -> (std::net::SocketAddr, tempfile::TempDir) {
        use std::sync::Arc;

        use meld_controller::{
            api::{ControllerState, router},
            blob_store::BlobStore,
        };

        let directory = tempfile::tempdir().expect("temp dir");
        let store = BlobStore::new(directory.path(), limits).expect("blob store");
        let state = ControllerState::new().with_blob_store(Arc::new(store));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral port");
        let address = listener.local_addr().expect("local address");
        tokio::spawn(async move { axum::serve(listener, router(state)).await });
        (address, directory)
    }

    fn digest_of(content: &[u8]) -> Sha256Digest {
        use sha2::{Digest, Sha256};

        Sha256Digest::from_bytes(Sha256::digest(content).into())
    }

    #[tokio::test]
    async fn blob_source_streams_content_from_a_real_controller() {
        let (address, _directory) =
            spawn_controller(meld_controller::blob_store::BlobLimits::default()).await;
        let content = vec![42u8; 300_000];
        let digest = digest_of(&content);
        reqwest::Client::new()
            .put(format!("http://{address}/v1/blobs/{digest}"))
            .body(content.clone())
            .send()
            .await
            .expect("upload should be sent")
            .error_for_status()
            .expect("upload should be accepted");
        let client = ControllerClient::new(&format!("http://{address}")).expect("client");

        let mut stream = client.open(&digest).await.expect("blob should open");
        let mut received = Vec::new();
        while let Some(chunk) = stream.next().await {
            received.extend_from_slice(&chunk.expect("chunk should arrive"));
        }

        assert_eq!(received, content);
        assert!(matches!(
            client.open(&digest_of(b"absent")).await,
            Err(DownloadError::NotFound)
        ));
    }

    #[tokio::test]
    async fn blob_sink_uploads_a_file_a_controller_can_serve_back() {
        let (address, _directory) =
            spawn_controller(meld_controller::blob_store::BlobLimits::default()).await;
        let client = ControllerClient::new(&format!("http://{address}")).expect("client");
        let workspace = tempfile::tempdir().expect("workspace");
        let content = vec![7u8; 300_000];
        let file = workspace.path().join("result.bin");
        std::fs::write(&file, &content).expect("write output");
        let digest = digest_of(&content);

        client
            .upload(&digest, &file, content.len() as u64)
            .await
            .expect("upload should succeed");
        // Uploading again finds the content stored and sends nothing.
        client
            .upload(&digest, &file, content.len() as u64)
            .await
            .expect("repeat upload should succeed");

        let mut stream = client.open(&digest).await.expect("blob should open");
        let mut received = Vec::new();
        while let Some(chunk) = stream.next().await {
            received.extend_from_slice(&chunk.expect("chunk should arrive"));
        }
        assert_eq!(received, content);
    }

    #[tokio::test]
    async fn blob_sink_treats_a_full_store_as_a_refusal() {
        let (address, _directory) = spawn_controller(meld_controller::blob_store::BlobLimits {
            max_blob_bytes: 100,
            quota_bytes: 100,
            ..meld_controller::blob_store::BlobLimits::default()
        })
        .await;
        let client = ControllerClient::new(&format!("http://{address}")).expect("client");
        let workspace = tempfile::tempdir().expect("workspace");
        let file = workspace.path().join("big.bin");
        std::fs::write(&file, vec![1u8; 101]).expect("write output");

        let error = client
            .upload(&digest_of(&[1u8; 101]), &file, 101)
            .await
            .expect_err("over the limit");

        assert!(matches!(error, UploadError::Rejected(_)), "{error:?}");
    }

    #[tokio::test]
    async fn blob_sink_reports_an_unreachable_controller_as_retryable() {
        let client = ControllerClient::new("http://127.0.0.1:1").expect("client");
        let workspace = tempfile::tempdir().expect("workspace");
        let file = workspace.path().join("f");
        std::fs::write(&file, b"x").expect("write output");

        let error = client
            .upload(&digest_of(b"x"), &file, 1)
            .await
            .expect_err("nothing is listening");

        assert!(matches!(error, UploadError::Unavailable(_)), "{error:?}");
    }

    #[test]
    fn unavailable_and_server_errors_are_retryable() {
        assert!(ControllerClientError::Rejected(StatusCode::SERVICE_UNAVAILABLE).is_retryable());
        assert!(ControllerClientError::Rejected(StatusCode::TOO_MANY_REQUESTS).is_retryable());
        assert!(!ControllerClientError::Rejected(StatusCode::BAD_REQUEST).is_retryable());
    }

    #[test]
    fn only_unregistered_node_error_requires_registration() {
        let node_id = NodeId::generate();

        assert!(
            ControllerClientError::Protocol(ProtocolError::NodeNotRegistered { node_id })
                .requires_registration()
        );
        assert!(
            !ControllerClientError::Protocol(ProtocolError::ProtocolVersionMismatch {
                expected: CURRENT_PROTOCOL_VERSION,
                received: ProtocolVersion::new(CURRENT_PROTOCOL_VERSION.value() + 1),
            })
            .requires_registration()
        );
    }

    #[test]
    fn response_metadata_must_correlate_to_request() {
        let request = RequestMetadata::new();
        let response = ResponseMetadata {
            message_id: MessageId::generate(),
            protocol_version: CURRENT_PROTOCOL_VERSION,
            in_reply_to: MessageId::generate(),
        };

        let error = validate_response_metadata(request, response)
            .expect_err("uncorrelated response must be rejected");

        assert!(matches!(error, ControllerClientError::InvalidResponse(_)));
    }

    #[test]
    fn assignment_must_belong_to_polling_node() {
        let polling_node_id = NodeId::generate();
        let assignment = ExecutionAssignment {
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
                constraints: meld_core::PlacementConstraints::default(),
                data: meld_core::DataSpec::default(),
            },
        };

        let command = NodeCommand::Start { assignment };
        let error = validate_node_command(polling_node_id, &[], Some(&command))
            .expect_err("foreign assignment must be rejected");

        assert!(matches!(error, ControllerClientError::InvalidResponse(_)));
        assert!(validate_node_command(polling_node_id, &[], None).is_ok());
    }
}
