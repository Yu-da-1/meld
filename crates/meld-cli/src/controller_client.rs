use std::{error::Error, fmt, path::Path, time::Duration};

use meld_core::{
    ApiErrorResponse, BlobResponse, CancelJobResponse, JobId, JobLogsResponse, JobSpec,
    JobStatusResponse, ListNodesResponse, MissingInputsResponse, NodeId, NodeStateResponse,
    RetryJobRequest, RetryJobResponse, Sha256Digest, SubmitJobResponse,
};
use reqwest::{Body, Client, Response, StatusCode, Url, header::CONTENT_LENGTH};
use serde::de::DeserializeOwned;
use tokio_util::io::ReaderStream;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const BLOB_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest a transfer may stall; its total duration is deliberately unbounded.
const BLOB_READ_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ControllerClient {
    http: Client,
    /// Without a total timeout, so large files can transfer.
    blob_http: Client,
    controller_url: Url,
}

impl ControllerClient {
    pub fn new(controller_url: &str) -> Result<Self, ControllerClientError> {
        let normalized_url = format!("{}/", controller_url.trim_end_matches('/'));
        let controller_url = Url::parse(&normalized_url)
            .map_err(|error| ControllerClientError::InvalidUrl(error.to_string()))?;
        let http = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(ControllerClientError::Request)?;

        let blob_http = Client::builder()
            .connect_timeout(BLOB_CONNECT_TIMEOUT)
            .read_timeout(BLOB_READ_TIMEOUT)
            .build()
            .map_err(ControllerClientError::Request)?;

        Ok(Self {
            http,
            blob_http,
            controller_url,
        })
    }

    /// Submits a job. Input files must already be uploaded; if some are not,
    /// the error names them so the caller can upload and submit again.
    pub async fn submit(&self, spec: &JobSpec) -> Result<SubmitJobResponse, ControllerClientError> {
        let response = self
            .http
            .post(self.endpoint("v1/jobs"))
            .json(spec)
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        if response.status() == StatusCode::UNPROCESSABLE_ENTITY {
            let missing = response
                .json::<MissingInputsResponse>()
                .await
                .map_err(ControllerClientError::InvalidResponse)?;
            return Err(ControllerClientError::MissingInputs(missing.missing));
        }
        decode_response(response).await
    }

    /// Returns whether the controller already holds this content.
    pub async fn has_blob(&self, digest: &Sha256Digest) -> Result<bool, ControllerClientError> {
        let response = self
            .blob_http
            .head(self.endpoint(&format!("v1/blobs/{digest}")))
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        match response.status() {
            StatusCode::NOT_FOUND => Ok(false),
            status if status.is_success() => Ok(true),
            status => Err(ControllerClientError::Rejected {
                status,
                message: format!("controller returned HTTP {status}"),
            }),
        }
    }

    /// Streams a local file to the controller as the content named by `digest`.
    pub async fn upload_blob(
        &self,
        digest: &Sha256Digest,
        file: &Path,
        size_bytes: u64,
    ) -> Result<BlobResponse, ControllerClientError> {
        let file = tokio::fs::File::open(file)
            .await
            .map_err(ControllerClientError::Io)?;
        let response = self
            .blob_http
            .put(self.endpoint(&format!("v1/blobs/{digest}")))
            .header(CONTENT_LENGTH, size_bytes)
            .body(Body::wrap_stream(ReaderStream::new(file)))
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        decode_response(response).await
    }

    /// Opens the content named by `digest`, or `None` if it is not stored.
    pub async fn open_blob(
        &self,
        digest: &Sha256Digest,
    ) -> Result<Option<Response>, ControllerClientError> {
        let response = self
            .blob_http
            .get(self.endpoint(&format!("v1/blobs/{digest}")))
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        match response.status() {
            StatusCode::NOT_FOUND => Ok(None),
            status if status.is_success() => Ok(Some(response)),
            status => Err(ControllerClientError::Rejected {
                status,
                message: format!("controller returned HTTP {status}"),
            }),
        }
    }

    pub async fn status(&self, job_id: JobId) -> Result<JobStatusResponse, ControllerClientError> {
        let response = self
            .http
            .get(self.endpoint(&format!("v1/jobs/{job_id}")))
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        decode_response(response).await
    }

    pub async fn logs(&self, job_id: JobId) -> Result<JobLogsResponse, ControllerClientError> {
        let response = self
            .http
            .get(self.endpoint(&format!("v1/jobs/{job_id}/logs")))
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        decode_response(response).await
    }

    pub async fn cancel(&self, job_id: JobId) -> Result<CancelJobResponse, ControllerClientError> {
        let response = self
            .http
            .post(self.endpoint(&format!("v1/jobs/{job_id}/cancel")))
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        decode_response(response).await
    }

    /// Queues a new attempt of a job that ended unsuccessfully.
    pub async fn retry(
        &self,
        job_id: JobId,
        allow_duplicate_run: bool,
    ) -> Result<RetryJobResponse, ControllerClientError> {
        let response = self
            .http
            .post(self.endpoint(&format!("v1/jobs/{job_id}/retry")))
            .json(&RetryJobRequest {
                allow_duplicate_run,
            })
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        decode_response(response).await
    }

    pub async fn nodes(&self) -> Result<ListNodesResponse, ControllerClientError> {
        let response = self
            .http
            .get(self.endpoint("v1/nodes"))
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        decode_response(response).await
    }

    pub async fn drain(&self, node_id: NodeId) -> Result<NodeStateResponse, ControllerClientError> {
        self.post_node_action(node_id, "drain").await
    }

    pub async fn resume(
        &self,
        node_id: NodeId,
    ) -> Result<NodeStateResponse, ControllerClientError> {
        self.post_node_action(node_id, "resume").await
    }

    async fn post_node_action(
        &self,
        node_id: NodeId,
        action: &str,
    ) -> Result<NodeStateResponse, ControllerClientError> {
        let response = self
            .http
            .post(self.endpoint(&format!("v1/nodes/{node_id}/{action}")))
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        decode_response(response).await
    }

    fn endpoint(&self, path: &str) -> Url {
        self.controller_url
            .join(path)
            .expect("static API paths must be valid URL paths")
    }
}

async fn decode_response<T>(response: Response) -> Result<T, ControllerClientError>
where
    T: DeserializeOwned,
{
    let status = response.status();
    if !status.is_success() {
        let message = response
            .json::<ApiErrorResponse>()
            .await
            .map(|response| response.error)
            .unwrap_or_else(|_| format!("controller returned HTTP {status}"));
        return Err(ControllerClientError::Rejected { status, message });
    }

    response
        .json::<T>()
        .await
        .map_err(ControllerClientError::InvalidResponse)
}

#[derive(Debug)]
pub enum ControllerClientError {
    InvalidUrl(String),
    Request(reqwest::Error),
    Rejected {
        status: StatusCode,
        message: String,
    },
    InvalidResponse(reqwest::Error),
    /// The job's input files are not all stored on the controller.
    MissingInputs(Vec<Sha256Digest>),
    Io(std::io::Error),
}

impl fmt::Display for ControllerClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl(error) => write!(formatter, "invalid controller URL: {error}"),
            Self::Request(error) => write!(formatter, "controller request failed: {error}"),
            Self::Rejected { status, message } => {
                write!(
                    formatter,
                    "controller rejected the request ({status}): {message}"
                )
            }
            Self::InvalidResponse(error) => {
                write!(
                    formatter,
                    "controller returned an invalid response: {error}"
                )
            }
            Self::MissingInputs(missing) => write!(
                formatter,
                "controller is missing {} input file(s) that were uploaded earlier",
                missing.len()
            ),
            Self::Io(error) => write!(formatter, "failed to read a local file: {error}"),
        }
    }
}

impl Error for ControllerClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(error) | Self::InvalidResponse(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::InvalidUrl(_) | Self::Rejected { .. } | Self::MissingInputs(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::serve;
    use meld_controller::api::{ControllerState, router};
    use meld_core::{JobState, QueueReason, ResourceRequirements};
    use tokio::{net::TcpListener, task::JoinHandle};

    use super::*;

    #[tokio::test]
    async fn client_submits_queries_and_cancels_a_job() {
        let (controller_url, server) = start_controller().await;
        let client =
            ControllerClient::new(&controller_url).expect("controller URL should be valid");
        let spec = JobSpec {
            program: "rustc".to_owned(),
            args: vec!["--version".to_owned()],
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 256 * 1024 * 1024,
            },
            job_timeout_secs: None,
            execution_timeout_secs: Some(30),
            constraints: meld_core::PlacementConstraints::default(),
            data: meld_core::DataSpec::default(),
            retry: Default::default(),
        };

        let submitted = client.submit(&spec).await.expect("job should be submitted");
        assert_eq!(submitted.state, JobState::Queued);

        let status = client
            .status(submitted.job_id)
            .await
            .expect("submitted job should be queryable");
        assert_eq!(status.spec, spec);
        assert_eq!(status.state, JobState::Queued);
        assert_eq!(status.queue_reason, Some(QueueReason::NoReadyNodes));

        let cancelled = client
            .cancel(submitted.job_id)
            .await
            .expect("queued job should be cancellable");
        assert_eq!(cancelled.state, JobState::Cancelled);

        server.abort();
    }

    #[tokio::test]
    async fn client_rejects_draining_an_unknown_node_and_lists_no_nodes() {
        let (controller_url, server) = start_controller().await;
        let client =
            ControllerClient::new(&controller_url).expect("controller URL should be valid");
        let node_id = NodeId::generate();

        let error = client
            .drain(node_id)
            .await
            .expect_err("unknown node cannot be drained");
        assert!(matches!(
            error,
            ControllerClientError::Rejected {
                status: StatusCode::NOT_FOUND,
                ..
            }
        ));
        assert!(
            client
                .nodes()
                .await
                .expect("node list should be readable")
                .nodes
                .is_empty()
        );

        server.abort();
    }

    #[tokio::test]
    async fn controller_error_message_is_preserved() {
        let (controller_url, server) = start_controller().await;
        let client =
            ControllerClient::new(&controller_url).expect("controller URL should be valid");
        let job_id = JobId::generate();

        let error = client
            .status(job_id)
            .await
            .expect_err("unknown job should be rejected");

        assert!(matches!(
            error,
            ControllerClientError::Rejected {
                status: StatusCode::NOT_FOUND,
                ref message,
            } if message.contains(&job_id.to_string())
        ));

        server.abort();
    }

    async fn start_controller() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        let server = tokio::spawn(async move {
            serve(listener, router(ControllerState::new()))
                .await
                .expect("test controller should serve");
        });
        (format!("http://{address}"), server)
    }
}
