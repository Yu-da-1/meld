use std::{error::Error, fmt, time::Duration};

use meld_core::{
    ApiErrorResponse, CancelJobResponse, JobId, JobLogsResponse, JobSpec, JobStatusResponse,
    SubmitJobResponse,
};
use reqwest::{Client, Response, StatusCode, Url};
use serde::de::DeserializeOwned;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct ControllerClient {
    http: Client,
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

        Ok(Self {
            http,
            controller_url,
        })
    }

    pub async fn submit(&self, spec: &JobSpec) -> Result<SubmitJobResponse, ControllerClientError> {
        let response = self
            .http
            .post(self.endpoint("v1/jobs"))
            .json(spec)
            .send()
            .await
            .map_err(ControllerClientError::Request)?;
        decode_response(response).await
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
    Rejected { status: StatusCode, message: String },
    InvalidResponse(reqwest::Error),
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
        }
    }
}

impl Error for ControllerClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(error) | Self::InvalidResponse(error) => Some(error),
            Self::InvalidUrl(_) | Self::Rejected { .. } => None,
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
