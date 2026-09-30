//! HTTP client boundary for controller-node protocol requests.

use std::{error::Error, fmt, time::Duration};

use meld_core::{
    Acknowledgement, CURRENT_PROTOCOL_VERSION, HeartbeatRequest, NodeDescriptor, NodeId,
    ProtocolError, ProtocolErrorResponse, RegisterNodeRequest, RegisterNodeResponse,
    RequestMetadata, ResourceSnapshot, ResponseMetadata,
};
use reqwest::{Client, Response, StatusCode};

const REGISTER_NODE_PATH: &str = "/v1/nodes/register";
const HEARTBEAT_PATH: &str = "/v1/nodes/heartbeat";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub struct ControllerClient {
    http: Client,
    controller_url: String,
}

impl ControllerClient {
    pub fn new(controller_url: &str) -> Result<Self, reqwest::Error> {
        Ok(Self {
            http: Client::builder().timeout(REQUEST_TIMEOUT).build()?,
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

#[cfg(test)]
mod tests {
    use meld_core::{MessageId, ProtocolVersion};

    use super::*;

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
}
