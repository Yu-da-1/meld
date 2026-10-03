//! HTTP boundary for controller-node protocol messages.

use std::{
    error::Error,
    fmt,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use meld_core::{
    Acknowledgement, ApiErrorResponse, CURRENT_PROTOCOL_VERSION, CancelJobResponse, ExecutionEvent,
    ExecutionOutput, ExecutionView, HeartbeatRequest, JobId, JobLogsResponse, JobSpec, JobState,
    JobStatusResponse, ListNodesResponse, NodeCommand, NodeId, NodeState, NodeView,
    PollNodeCommandRequest, PollNodeCommandResponse, ProtocolError, ProtocolErrorResponse,
    QueueReason, RegisterNodeRequest, RegisterNodeResponse, ReportExecutionEventRequest,
    ResponseMetadata, SubmitJobResponse,
};
use tokio::{sync::watch, time::timeout};

use crate::{
    failure_detector::FailureDetector,
    job_manager::{JobManager, JobManagerError},
    node_registry::{NodeRegistry, NodeRegistryError},
    scheduler::{Scheduler, SchedulingFailure},
};

pub const REGISTER_NODE_PATH: &str = "/v1/nodes/register";
pub const HEARTBEAT_PATH: &str = "/v1/nodes/heartbeat";
pub const LIST_NODES_PATH: &str = "/v1/nodes";
pub const SUBMIT_JOB_PATH: &str = "/v1/jobs";
pub const JOB_STATUS_PATH: &str = "/v1/jobs/{job_id}";
pub const JOB_LOGS_PATH: &str = "/v1/jobs/{job_id}/logs";
pub const CANCEL_JOB_PATH: &str = "/v1/jobs/{job_id}/cancel";
pub const POLL_NODE_COMMAND_PATH: &str = "/v1/nodes/commands/poll";
pub const REPORT_EXECUTION_EVENT_PATH: &str = "/v1/nodes/executions/events";
const COMMAND_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(25);

/// Shared controller state exposed to HTTP handlers.
#[derive(Debug, Clone)]
pub struct ControllerState {
    registry: Arc<RwLock<NodeRegistry>>,
    jobs: Arc<RwLock<JobManager>>,
    command_updates: watch::Sender<u64>,
}

impl Default for ControllerState {
    fn default() -> Self {
        let (command_updates, _) = watch::channel(0);
        Self {
            registry: Arc::default(),
            jobs: Arc::default(),
            command_updates,
        }
    }
}

impl ControllerState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn detect_unreachable_nodes(
        &self,
        detector: &FailureDetector,
        now: Instant,
    ) -> Result<Vec<NodeId>, ControllerStateError> {
        let mut registry = self.registry.write().map_err(|_| ControllerStateError)?;
        Ok(detector.detect(&mut registry, now))
    }

    pub fn expire_jobs(&self, now: Instant) -> Result<Vec<JobId>, ControllerStateError> {
        let mut jobs = self.jobs.write().map_err(|_| ControllerStateError)?;
        let timed_out = jobs.expire_jobs_at(now).map_err(|error| {
            tracing::error!(%error, "job timeout processing failed");
            ControllerStateError
        })?;
        drop(jobs);
        if !timed_out.is_empty() {
            self.notify_command_update();
        }
        Ok(timed_out)
    }

    fn notify_command_update(&self) {
        self.command_updates
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    fn node_views_at(&self, now: Instant) -> Result<Vec<NodeView>, ControllerStateError> {
        let registry = self.registry.read().map_err(|_| ControllerStateError)?;
        Ok(registry
            .nodes()
            .map(|node| NodeView {
                descriptor: node.descriptor().clone(),
                state: node.state(),
                snapshot: node.snapshot(),
                last_heartbeat_age_ms: node.last_heartbeat_at().map(|last_heartbeat| {
                    let age = now.saturating_duration_since(last_heartbeat).as_millis();
                    age.min(u128::from(u64::MAX)) as u64
                }),
            })
            .collect())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerStateError;

impl fmt::Display for ControllerStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("controller state lock is poisoned")
    }
}

impl Error for ControllerStateError {}

pub fn router(state: ControllerState) -> Router {
    Router::new()
        .route(LIST_NODES_PATH, get(list_nodes))
        .route(REGISTER_NODE_PATH, post(register_node))
        .route(HEARTBEAT_PATH, post(record_heartbeat))
        .route(SUBMIT_JOB_PATH, post(submit_job))
        .route(JOB_STATUS_PATH, get(job_status))
        .route(JOB_LOGS_PATH, get(job_logs))
        .route(CANCEL_JOB_PATH, post(cancel_job))
        .route(POLL_NODE_COMMAND_PATH, post(poll_node_command))
        .route(REPORT_EXECUTION_EVENT_PATH, post(report_execution_event))
        .with_state(state)
}

async fn report_execution_event(
    State(state): State<ControllerState>,
    Json(request): Json<ReportExecutionEventRequest>,
) -> Response {
    let request_metadata = request.metadata;
    if let Some(response) = protocol_version_error(request_metadata) {
        return response;
    }

    let registry = match state.registry.read() {
        Ok(registry) => registry,
        Err(error) => {
            tracing::error!(node_id = %request.node_id, %error, "node registry lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            )
                .into_response();
        }
    };
    if registry.get(request.node_id).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(ProtocolErrorResponse {
                metadata: ResponseMetadata::for_request(request_metadata),
                error: ProtocolError::NodeNotRegistered {
                    node_id: request.node_id,
                },
            }),
        )
            .into_response();
    }
    drop(registry);

    let mut jobs = match state.jobs.write() {
        Ok(jobs) => jobs,
        Err(error) => {
            tracing::error!(node_id = %request.node_id, %error, "job manager lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            )
                .into_response();
        }
    };
    let Some(execution) = jobs.execution(request.execution_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: format!("execution {} was not found", request.execution_id),
            }),
        )
            .into_response();
    };
    if execution.node_id() != request.node_id {
        return (
            StatusCode::CONFLICT,
            Json(ApiErrorResponse {
                error: "execution is assigned to a different node".to_owned(),
            }),
        )
            .into_response();
    }

    let job_timeout_requested = jobs.job_timeout_applies(request.execution_id);
    let transition = match request.event {
        ExecutionEvent::Accepted => jobs.accept_execution(request.execution_id),
        ExecutionEvent::Running => jobs.start_execution(request.execution_id),
        ExecutionEvent::Finished { output, .. } if job_timeout_requested => {
            jobs.confirm_job_timeout_with_output(request.execution_id, output)
        }
        ExecutionEvent::Finished { result, output } => {
            jobs.finish_execution_with_output(request.execution_id, result, output)
        }
        ExecutionEvent::StartFailed { .. } if job_timeout_requested => {
            jobs.confirm_job_timeout_with_output(request.execution_id, ExecutionOutput::default())
        }
        ExecutionEvent::StartFailed { reason } => {
            tracing::warn!(
                node_id = %request.node_id,
                execution_id = %request.execution_id,
                %reason,
                "execution process failed to start"
            );
            jobs.fail_execution_start(request.execution_id)
        }
        ExecutionEvent::Rejected { reason } => {
            tracing::warn!(
                node_id = %request.node_id,
                execution_id = %request.execution_id,
                %reason,
                "node rejected execution assignment"
            );
            jobs.reject_execution(request.execution_id)
        }
        ExecutionEvent::Cancelled { output } if job_timeout_requested => {
            jobs.confirm_job_timeout_with_output(request.execution_id, output)
        }
        ExecutionEvent::Cancelled { output } => {
            jobs.confirm_execution_cancellation_with_output(request.execution_id, output)
        }
        ExecutionEvent::TimedOut { output } if job_timeout_requested => {
            jobs.confirm_job_timeout_with_output(request.execution_id, output)
        }
        ExecutionEvent::TimedOut { output } => {
            jobs.mark_execution_timed_out(request.execution_id, output)
        }
    };

    if let Err(error) = transition {
        return (
            StatusCode::CONFLICT,
            Json(ApiErrorResponse {
                error: error.to_string(),
            }),
        )
            .into_response();
    }

    Json(Acknowledgement {
        metadata: ResponseMetadata::for_request(request_metadata),
    })
    .into_response()
}

async fn poll_node_command(
    State(state): State<ControllerState>,
    Json(request): Json<PollNodeCommandRequest>,
) -> Response {
    poll_node_command_with_timeout(state, request, COMMAND_LONG_POLL_TIMEOUT).await
}

async fn poll_node_command_with_timeout(
    state: ControllerState,
    request: PollNodeCommandRequest,
    wait_timeout: Duration,
) -> Response {
    let request_metadata = request.metadata;
    if let Some(response) = protocol_version_error(request_metadata) {
        return response;
    }

    let mut updates = state.command_updates.subscribe();
    let command = match command_for_node(&state, &request) {
        Ok(Some(command)) => Some(command),
        Ok(None) => {
            let wait_for_command = async {
                loop {
                    if updates.changed().await.is_err() {
                        return Ok(None);
                    }
                    match command_for_node(&state, &request) {
                        Ok(Some(command)) => return Ok(Some(command)),
                        Ok(None) => {}
                        Err(response) => return Err(response),
                    }
                }
            };

            match timeout(wait_timeout, wait_for_command).await {
                Ok(Ok(command)) => command,
                Ok(Err(response)) => return *response,
                Err(_) => None,
            }
        }
        Err(response) => return *response,
    };

    Json(PollNodeCommandResponse {
        metadata: ResponseMetadata::for_request(request_metadata),
        command,
    })
    .into_response()
}

fn command_for_node(
    state: &ControllerState,
    request: &PollNodeCommandRequest,
) -> Result<Option<NodeCommand>, Box<Response>> {
    let registry = match state.registry.read() {
        Ok(registry) => registry,
        Err(error) => {
            tracing::error!(node_id = %request.node_id, %error, "node registry lock is poisoned");
            return Err(Box::new(
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "controller state is unavailable",
                )
                    .into_response(),
            ));
        }
    };
    let Some(node) = registry.get(request.node_id) else {
        return Err(Box::new(
            (
                StatusCode::NOT_FOUND,
                Json(ProtocolErrorResponse {
                    metadata: ResponseMetadata::for_request(request.metadata),
                    error: ProtocolError::NodeNotRegistered {
                        node_id: request.node_id,
                    },
                }),
            )
                .into_response(),
        ));
    };

    let mut jobs = match state.jobs.write() {
        Ok(jobs) => jobs,
        Err(error) => {
            tracing::error!(node_id = %request.node_id, %error, "job manager lock is poisoned");
            return Err(Box::new(
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "controller state is unavailable",
                )
                    .into_response(),
            ));
        }
    };

    let command = if let Some(execution_id) = request.active_execution_id {
        let Some(execution) = jobs.execution(execution_id) else {
            return Err(Box::new(
                (
                    StatusCode::NOT_FOUND,
                    Json(ApiErrorResponse {
                        error: format!("execution {execution_id} was not found"),
                    }),
                )
                    .into_response(),
            ));
        };
        if execution.node_id() != request.node_id {
            return Err(Box::new(
                (
                    StatusCode::CONFLICT,
                    Json(ApiErrorResponse {
                        error: "execution is assigned to a different node".to_owned(),
                    }),
                )
                    .into_response(),
            ));
        }
        jobs.cancellation_requested(execution_id, request.node_id)
            .then_some(NodeCommand::Cancel { execution_id })
    } else {
        let assignment = match jobs.pending_assignment_for(request.node_id) {
            Ok(Some(assignment)) => Some(assignment),
            Ok(None) if node.state() != NodeState::Ready => None,
            Ok(None) => {
                match jobs.schedule_next(&Scheduler::new(), &registry) {
                    Ok(_)
                    | Err(JobManagerError::Scheduling(SchedulingFailure::NoReadyNodes))
                    | Err(JobManagerError::Scheduling(
                        SchedulingFailure::InsufficientResources
                        | SchedulingFailure::NoAvailableNodes,
                    )) => {}
                    Err(error) => {
                        tracing::error!(node_id = %request.node_id, %error, "job scheduling failed");
                        return Err(Box::new(
                            (StatusCode::INTERNAL_SERVER_ERROR, "job scheduling failed")
                                .into_response(),
                        ));
                    }
                }

                match jobs.pending_assignment_for(request.node_id) {
                    Ok(assignment) => assignment,
                    Err(error) => {
                        tracing::error!(node_id = %request.node_id, %error, "assignment lookup failed");
                        return Err(Box::new(
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "assignment lookup failed",
                            )
                                .into_response(),
                        ));
                    }
                }
            }
            Err(error) => {
                tracing::error!(node_id = %request.node_id, %error, "assignment lookup failed");
                return Err(Box::new(
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "assignment lookup failed",
                    )
                        .into_response(),
                ));
            }
        };
        assignment.map(|assignment| NodeCommand::Start { assignment })
    };

    Ok(command)
}

async fn cancel_job(State(state): State<ControllerState>, Path(job_id): Path<JobId>) -> Response {
    let mut jobs = match state.jobs.write() {
        Ok(jobs) => jobs,
        Err(error) => {
            tracing::error!(%job_id, %error, "job manager lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiErrorResponse {
                    error: "controller state is unavailable".to_owned(),
                }),
            )
                .into_response();
        }
    };
    if let Err(error) = jobs.request_job_cancellation(job_id) {
        let status = if matches!(error, JobManagerError::JobNotFound(_)) {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::CONFLICT
        };
        return (
            status,
            Json(ApiErrorResponse {
                error: error.to_string(),
            }),
        )
            .into_response();
    }
    let job_state = jobs
        .job(job_id)
        .expect("cancelled job should remain stored")
        .state();
    drop(jobs);
    state.notify_command_update();

    (
        StatusCode::ACCEPTED,
        Json(CancelJobResponse {
            job_id,
            state: job_state,
        }),
    )
        .into_response()
}

async fn job_status(State(state): State<ControllerState>, Path(job_id): Path<JobId>) -> Response {
    let registry = match state.registry.read() {
        Ok(registry) => registry,
        Err(error) => {
            tracing::error!(%job_id, %error, "node registry lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiErrorResponse {
                    error: "controller state is unavailable".to_owned(),
                }),
            )
                .into_response();
        }
    };
    let jobs = match state.jobs.read() {
        Ok(jobs) => jobs,
        Err(error) => {
            tracing::error!(%job_id, %error, "job manager lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiErrorResponse {
                    error: "controller state is unavailable".to_owned(),
                }),
            )
                .into_response();
        }
    };
    let Some(job) = jobs.job(job_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: format!("job {job_id} was not found"),
            }),
        )
            .into_response();
    };
    let execution = jobs
        .latest_execution_for_job(job_id)
        .map(|execution| ExecutionView {
            execution_id: execution.id(),
            node_id: execution.node_id(),
            state: execution.state(),
            result: execution.result(),
        });
    let queue_reason = if job.state() == JobState::Queued {
        match jobs.pending_position(job_id) {
            Some(position) if position > 0 => Some(QueueReason::WaitingForEarlierJob),
            Some(_) => Some(
                match jobs
                    .scheduling_failure_for(job_id, &Scheduler::new(), &registry)
                    .expect("queued job should support scheduling diagnosis")
                {
                    None => QueueReason::AwaitingAssignment,
                    Some(SchedulingFailure::NoReadyNodes) => QueueReason::NoReadyNodes,
                    Some(SchedulingFailure::InsufficientResources) => {
                        QueueReason::InsufficientResources
                    }
                    Some(SchedulingFailure::NoAvailableNodes) => QueueReason::NoAvailableNodes,
                },
            ),
            None => Some(QueueReason::AwaitingAssignment),
        }
    } else {
        None
    };

    Json(JobStatusResponse {
        job_id,
        spec: job.spec().clone(),
        state: job.state(),
        queue_reason,
        execution,
    })
    .into_response()
}

async fn job_logs(State(state): State<ControllerState>, Path(job_id): Path<JobId>) -> Response {
    let jobs = match state.jobs.read() {
        Ok(jobs) => jobs,
        Err(error) => {
            tracing::error!(%job_id, %error, "job manager lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiErrorResponse {
                    error: "controller state is unavailable".to_owned(),
                }),
            )
                .into_response();
        }
    };
    if jobs.job(job_id).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: format!("job {job_id} was not found"),
            }),
        )
            .into_response();
    }
    let Some(execution) = jobs.latest_execution_for_job(job_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: "job has no execution logs".to_owned(),
            }),
        )
            .into_response();
    };
    let Some(output) = jobs.execution_output(execution.id()) else {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: "execution logs are not available yet".to_owned(),
            }),
        )
            .into_response();
    };

    Json(JobLogsResponse {
        job_id,
        execution_id: execution.id(),
        output: output.clone(),
    })
    .into_response()
}

async fn submit_job(State(state): State<ControllerState>, Json(spec): Json<JobSpec>) -> Response {
    let mut jobs = match state.jobs.write() {
        Ok(jobs) => jobs,
        Err(error) => {
            tracing::error!(%error, "job manager lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiErrorResponse {
                    error: "controller state is unavailable".to_owned(),
                }),
            )
                .into_response();
        }
    };

    match jobs.submit(spec) {
        Ok(job_id) => {
            drop(jobs);
            state.notify_command_update();
            (
                StatusCode::ACCEPTED,
                Json(SubmitJobResponse {
                    job_id,
                    state: JobState::Queued,
                }),
            )
                .into_response()
        }
        Err(JobManagerError::InvalidSpec(error)) => (
            StatusCode::BAD_REQUEST,
            Json(ApiErrorResponse {
                error: error.to_string(),
            }),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, "job submission failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiErrorResponse {
                    error: "job submission failed".to_owned(),
                }),
            )
                .into_response()
        }
    }
}

async fn list_nodes(State(state): State<ControllerState>) -> Response {
    match state.node_views_at(Instant::now()) {
        Ok(nodes) => Json(ListNodesResponse { nodes }).into_response(),
        Err(error) => {
            tracing::error!(%error, "node registry lock is poisoned");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            )
                .into_response()
        }
    }
}

async fn register_node(
    State(state): State<ControllerState>,
    Json(request): Json<RegisterNodeRequest>,
) -> Response {
    let request_metadata = request.metadata;
    if let Some(response) = protocol_version_error(request_metadata) {
        return response;
    }

    let node_id = request.node.id;
    let mut registry = match state.registry.write() {
        Ok(registry) => registry,
        Err(error) => {
            tracing::error!(%node_id, %error, "node registry lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            )
                .into_response();
        }
    };
    registry.register(request.node);
    drop(registry);

    tracing::info!(%node_id, "node registered");

    Json(RegisterNodeResponse {
        metadata: ResponseMetadata::for_request(request_metadata),
        node_id,
    })
    .into_response()
}

async fn record_heartbeat(
    State(state): State<ControllerState>,
    Json(request): Json<HeartbeatRequest>,
) -> Response {
    let request_metadata = request.metadata;
    if let Some(response) = protocol_version_error(request_metadata) {
        return response;
    }

    let mut registry = match state.registry.write() {
        Ok(registry) => registry,
        Err(error) => {
            tracing::error!(node_id = %request.node_id, %error, "node registry lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            )
                .into_response();
        }
    };

    let was_ready = registry
        .get(request.node_id)
        .is_some_and(|node| node.state() == NodeState::Ready);
    if let Err(NodeRegistryError::NodeNotFound(node_id)) =
        registry.record_heartbeat(request.node_id, request.snapshot)
    {
        return (
            StatusCode::NOT_FOUND,
            Json(ProtocolErrorResponse {
                metadata: ResponseMetadata::for_request(request_metadata),
                error: ProtocolError::NodeNotRegistered { node_id },
            }),
        )
            .into_response();
    }
    drop(registry);
    if !was_ready {
        state.notify_command_update();
    }

    tracing::debug!(
        node_id = %request.node_id,
        cpu_usage_percent = request.snapshot.cpu_usage_percent,
        available_memory_bytes = request.snapshot.available_memory_bytes,
        running_executions = request.snapshot.running_executions,
        "heartbeat recorded"
    );

    Json(Acknowledgement {
        metadata: ResponseMetadata::for_request(request_metadata),
    })
    .into_response()
}

fn protocol_version_error(metadata: meld_core::RequestMetadata) -> Option<Response> {
    (metadata.protocol_version != CURRENT_PROTOCOL_VERSION).then(|| {
        (
            StatusCode::UPGRADE_REQUIRED,
            Json(ProtocolErrorResponse {
                metadata: ResponseMetadata::for_request(metadata),
                error: ProtocolError::ProtocolVersionMismatch {
                    expected: CURRENT_PROTOCOL_VERSION,
                    received: metadata.protocol_version,
                },
            }),
        )
            .into_response()
    })
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, header},
    };
    use meld_core::{
        CapturedStream, Execution, ExecutionOutput, ExecutionResult, ExecutionState, Job,
        MessageId, NodeDescriptor, NodeId, NodeState, ProtocolVersion, RequestMetadata,
        ResourceCapacity, ResourceRequirements, ResourceSnapshot,
    };
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn valid_job_submission_queues_job() {
        let state = ControllerState::new();

        let response = router(state.clone())
            .oneshot(submit_job_json_request(job_spec()))
            .await
            .expect("job submission should be handled");

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: SubmitJobResponse =
            serde_json::from_slice(&body).expect("response should contain submitted job JSON");
        assert_eq!(response.state, JobState::Queued);

        let jobs = state
            .jobs
            .read()
            .expect("job manager lock should be available");
        assert_eq!(
            jobs.job(response.job_id).map(Job::state),
            Some(JobState::Queued)
        );
    }

    #[tokio::test]
    async fn queued_job_status_explains_current_scheduling_blocker() {
        let state = ControllerState::new();
        let first_job_id = state
            .jobs
            .write()
            .expect("job manager lock should be available")
            .submit(job_spec())
            .expect("valid job should be queued");

        let status = get_job_status(state.clone(), first_job_id).await;
        assert_eq!(status.queue_reason, Some(QueueReason::NoReadyNodes));

        register_ready_node(&state, NodeId::generate());
        let status = get_job_status(state.clone(), first_job_id).await;
        assert_eq!(
            status.queue_reason,
            Some(QueueReason::InsufficientResources)
        );

        let second_job_id = state
            .jobs
            .write()
            .expect("job manager lock should be available")
            .submit(job_spec())
            .expect("valid job should be queued");
        let status = get_job_status(state, second_job_id).await;
        assert_eq!(status.queue_reason, Some(QueueReason::WaitingForEarlierJob));
    }

    #[tokio::test]
    async fn invalid_job_submission_is_rejected_without_storing_job() {
        let state = ControllerState::new();
        let mut invalid_spec = job_spec();
        invalid_spec.program = "  ".to_owned();

        let response = router(state.clone())
            .oneshot(submit_job_json_request(invalid_spec))
            .await
            .expect("invalid job submission should be handled");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: ApiErrorResponse =
            serde_json::from_slice(&body).expect("response should contain an API error");
        assert_eq!(response.error, "job program must not be empty");
    }

    #[tokio::test]
    async fn waiting_command_poll_wakes_when_job_is_submitted() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        register_ready_node(&state, node_id);
        let request = PollNodeCommandRequest {
            metadata: RequestMetadata::new(),
            node_id,
            active_execution_id: None,
        };
        let polling_state = state.clone();
        let poll = tokio::spawn(async move {
            poll_node_command_with_timeout(polling_state, request, Duration::from_secs(1)).await
        });
        while state.command_updates.receiver_count() == 0 {
            tokio::task::yield_now().await;
        }
        assert!(
            !poll.is_finished(),
            "poll should wait while no command exists"
        );

        let mut spec = job_spec();
        spec.requirements.memory_bytes = 8_000;
        let submission = router(state)
            .oneshot(submit_job_json_request(spec))
            .await
            .expect("job submission should be handled");
        assert_eq!(submission.status(), StatusCode::ACCEPTED);

        let response = timeout(Duration::from_secs(1), poll)
            .await
            .expect("poll should wake after job submission")
            .expect("poll task should complete");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: PollNodeCommandResponse =
            serde_json::from_slice(&body).expect("response should contain command JSON");
        assert!(matches!(response.command, Some(NodeCommand::Start { .. })));
    }

    #[tokio::test]
    async fn command_poll_returns_no_command_after_wait_timeout() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        register_ready_node(&state, node_id);

        let response = poll_node_command_with_timeout(
            state,
            PollNodeCommandRequest {
                metadata: RequestMetadata::new(),
                node_id,
                active_execution_id: None,
            },
            Duration::from_millis(10),
        )
        .await;

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: PollNodeCommandResponse =
            serde_json::from_slice(&body).expect("response should contain command JSON");
        assert_eq!(response.command, None);
    }

    #[tokio::test]
    async fn waiting_command_poll_wakes_when_active_job_is_cancelled() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let (job_id, execution_id) = assigned_execution(&state, node_id);
        state
            .jobs
            .write()
            .expect("job manager lock should be available")
            .accept_execution(execution_id)
            .expect("assigned execution should be accepted");

        let polling_state = state.clone();
        let poll = tokio::spawn(async move {
            poll_node_command_with_timeout(
                polling_state,
                PollNodeCommandRequest {
                    metadata: RequestMetadata::new(),
                    node_id,
                    active_execution_id: Some(execution_id),
                },
                Duration::from_secs(1),
            )
            .await
        });
        while state.command_updates.receiver_count() == 0 {
            tokio::task::yield_now().await;
        }
        assert!(!poll.is_finished(), "poll should wait before cancellation");

        let cancellation = router(state)
            .oneshot(post_empty_request(&format!("/v1/jobs/{job_id}/cancel")))
            .await
            .expect("cancellation should be handled");
        assert_eq!(cancellation.status(), StatusCode::ACCEPTED);

        let response = timeout(Duration::from_secs(1), poll)
            .await
            .expect("poll should wake after cancellation")
            .expect("poll task should complete");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: PollNodeCommandResponse =
            serde_json::from_slice(&body).expect("response should contain command JSON");
        assert_eq!(response.command, Some(NodeCommand::Cancel { execution_id }));
    }

    #[tokio::test]
    async fn job_status_and_logs_expose_completed_execution() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let (job_id, execution_id) = assigned_execution(&state, node_id);
        assert_eq!(
            report_event(
                state.clone(),
                node_id,
                execution_id,
                ExecutionEvent::Accepted,
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            report_event(
                state.clone(),
                node_id,
                execution_id,
                ExecutionEvent::Running,
            )
            .await,
            StatusCode::OK
        );
        let output = ExecutionOutput {
            stdout: CapturedStream {
                content: "hello\n".to_owned(),
                truncated: false,
                lossy: false,
            },
            stderr: CapturedStream {
                content: "warning\n".to_owned(),
                truncated: true,
                lossy: false,
            },
        };
        let finished = ReportExecutionEventRequest {
            metadata: RequestMetadata::new(),
            node_id,
            execution_id,
            event: ExecutionEvent::Finished {
                result: ExecutionResult { exit_code: Some(0) },
                output: output.clone(),
            },
        };
        let response = router(state.clone())
            .oneshot(execution_event_json_request(finished))
            .await
            .expect("finished event should be handled");
        assert_eq!(response.status(), StatusCode::OK);

        let response = router(state.clone())
            .oneshot(get_request(&format!("/v1/jobs/{job_id}")))
            .await
            .expect("job status should be handled");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let status: JobStatusResponse =
            serde_json::from_slice(&body).expect("response should contain job status JSON");
        assert_eq!(status.state, JobState::Succeeded);
        let execution = status
            .execution
            .expect("completed job should expose its execution");
        assert_eq!(execution.execution_id, execution_id);
        assert_eq!(
            execution.result,
            Some(ExecutionResult { exit_code: Some(0) })
        );

        let response = router(state)
            .oneshot(get_request(&format!("/v1/jobs/{job_id}/logs")))
            .await
            .expect("job logs should be handled");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let logs: JobLogsResponse =
            serde_json::from_slice(&body).expect("response should contain job logs JSON");
        assert_eq!(logs.execution_id, execution_id);
        assert_eq!(logs.output, output);
    }

    #[tokio::test]
    async fn active_job_cancellation_is_delivered_and_confirmed() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let (job_id, execution_id) = assigned_execution(&state, node_id);
        assert_eq!(
            report_event(
                state.clone(),
                node_id,
                execution_id,
                ExecutionEvent::Accepted,
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            report_event(
                state.clone(),
                node_id,
                execution_id,
                ExecutionEvent::Running,
            )
            .await,
            StatusCode::OK
        );

        let response = router(state.clone())
            .oneshot(post_empty_request(&format!("/v1/jobs/{job_id}/cancel")))
            .await
            .expect("cancel request should be handled");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let response = router(state.clone())
            .oneshot(poll_node_command_json_request(PollNodeCommandRequest {
                metadata: RequestMetadata::new(),
                node_id,
                active_execution_id: Some(execution_id),
            }))
            .await
            .expect("command poll should be handled");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: PollNodeCommandResponse =
            serde_json::from_slice(&body).expect("response should contain command JSON");
        assert_eq!(response.command, Some(NodeCommand::Cancel { execution_id }));

        let output = ExecutionOutput {
            stdout: CapturedStream {
                content: "partial".to_owned(),
                truncated: false,
                lossy: false,
            },
            stderr: CapturedStream::default(),
        };
        assert_eq!(
            report_event(
                state.clone(),
                node_id,
                execution_id,
                ExecutionEvent::Cancelled {
                    output: output.clone(),
                },
            )
            .await,
            StatusCode::OK
        );

        let jobs = state
            .jobs
            .read()
            .expect("job manager lock should be available");
        assert_eq!(jobs.job(job_id).map(Job::state), Some(JobState::Cancelled));
        assert_eq!(
            jobs.execution(execution_id).map(Execution::state),
            Some(ExecutionState::Cancelled)
        );
        assert_eq!(jobs.execution_output(execution_id), Some(&output));
        assert!(!jobs.cancellation_requested(execution_id, node_id));
    }

    #[tokio::test]
    async fn timeout_event_fails_job_and_preserves_partial_output() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let (job_id, execution_id) = assigned_execution(&state, node_id);
        report_event(
            state.clone(),
            node_id,
            execution_id,
            ExecutionEvent::Accepted,
        )
        .await;
        report_event(
            state.clone(),
            node_id,
            execution_id,
            ExecutionEvent::Running,
        )
        .await;
        let output = ExecutionOutput {
            stdout: CapturedStream {
                content: "before timeout".to_owned(),
                truncated: false,
                lossy: false,
            },
            stderr: CapturedStream::default(),
        };

        let status = report_event(
            state.clone(),
            node_id,
            execution_id,
            ExecutionEvent::TimedOut {
                output: output.clone(),
            },
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let jobs = state
            .jobs
            .read()
            .expect("job manager lock should be available");
        assert_eq!(jobs.job(job_id).map(Job::state), Some(JobState::Failed));
        assert_eq!(
            jobs.execution(execution_id).map(Execution::state),
            Some(ExecutionState::TimedOut)
        );
        assert_eq!(jobs.execution_output(execution_id), Some(&output));
    }

    #[tokio::test]
    async fn assignment_poll_schedules_and_repeats_the_same_execution() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        register_ready_node(&state, node_id);
        let mut spec = job_spec();
        spec.requirements.memory_bytes = 8_000;
        let job_id = state
            .jobs
            .write()
            .expect("job manager lock should be available")
            .submit(spec.clone())
            .expect("valid job should be queued");

        let first_request = PollNodeCommandRequest {
            metadata: RequestMetadata::new(),
            node_id,
            active_execution_id: None,
        };
        let first_request_metadata = first_request.metadata;
        let first_response = router(state.clone())
            .oneshot(poll_node_command_json_request(first_request))
            .await
            .expect("assignment poll should be handled");

        assert_eq!(first_response.status(), StatusCode::OK);
        let body = to_bytes(first_response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let first_response: PollNodeCommandResponse =
            serde_json::from_slice(&body).expect("response should contain assignment JSON");
        assert_eq!(
            first_response.metadata.in_reply_to,
            first_request_metadata.message_id
        );
        let NodeCommand::Start { assignment } = first_response
            .command
            .expect("ready node should receive queued job")
        else {
            panic!("idle node should receive a start command");
        };
        assert_eq!(assignment.job_id, job_id);
        assert_eq!(assignment.node_id, node_id);
        assert_eq!(assignment.spec, spec);

        let second_request = PollNodeCommandRequest {
            metadata: RequestMetadata::new(),
            node_id,
            active_execution_id: None,
        };
        let second_response = router(state)
            .oneshot(poll_node_command_json_request(second_request))
            .await
            .expect("repeated assignment poll should be handled");
        let body = to_bytes(second_response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let second_response: PollNodeCommandResponse =
            serde_json::from_slice(&body).expect("response should contain assignment JSON");
        let NodeCommand::Start {
            assignment: repeated,
        } = second_response
            .command
            .expect("unacknowledged assignment should be repeated")
        else {
            panic!("idle node should receive a start command");
        };
        assert_eq!(repeated.execution_id, assignment.execution_id);
    }

    #[tokio::test]
    async fn assignment_poll_from_unregistered_node_is_rejected() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let request = PollNodeCommandRequest {
            metadata: RequestMetadata::new(),
            node_id,
            active_execution_id: None,
        };

        let response = router(state)
            .oneshot(poll_node_command_json_request(request))
            .await
            .expect("assignment poll should be handled");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: ProtocolErrorResponse =
            serde_json::from_slice(&body).expect("response should contain protocol error JSON");
        assert_eq!(response.error, ProtocolError::NodeNotRegistered { node_id });
    }

    #[tokio::test]
    async fn accepted_event_acknowledges_assignment_and_stops_redelivery() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let (_, execution_id) = assigned_execution(&state, node_id);
        let request = ReportExecutionEventRequest {
            metadata: RequestMetadata::new(),
            node_id,
            execution_id,
            event: ExecutionEvent::Accepted,
        };
        let request_metadata = request.metadata;

        let response = router(state.clone())
            .oneshot(execution_event_json_request(request))
            .await
            .expect("accepted event should be handled");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: Acknowledgement =
            serde_json::from_slice(&body).expect("response should contain acknowledgement JSON");
        assert_eq!(response.metadata.in_reply_to, request_metadata.message_id);
        assert_eq!(
            state
                .jobs
                .read()
                .expect("job manager lock should be available")
                .execution(execution_id)
                .map(Execution::state),
            Some(ExecutionState::Accepted)
        );

        let poll_response = poll_node_command_with_timeout(
            state,
            PollNodeCommandRequest {
                metadata: RequestMetadata::new(),
                node_id,
                active_execution_id: None,
            },
            Duration::from_millis(10),
        )
        .await;
        let body = to_bytes(poll_response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let poll_response: PollNodeCommandResponse =
            serde_json::from_slice(&body).expect("response should contain assignment JSON");
        assert_eq!(poll_response.command, None);
    }

    #[tokio::test]
    async fn rejected_event_returns_job_to_fifo_queue() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let (job_id, execution_id) = assigned_execution(&state, node_id);
        let request = ReportExecutionEventRequest {
            metadata: RequestMetadata::new(),
            node_id,
            execution_id,
            event: ExecutionEvent::Rejected {
                reason: "executor is busy".to_owned(),
            },
        };

        let response = router(state.clone())
            .oneshot(execution_event_json_request(request))
            .await
            .expect("rejected event should be handled");

        assert_eq!(response.status(), StatusCode::OK);
        let jobs = state
            .jobs
            .read()
            .expect("job manager lock should be available");
        assert_eq!(
            jobs.execution(execution_id).map(Execution::state),
            Some(ExecutionState::Rejected)
        );
        assert_eq!(jobs.job(job_id).map(Job::state), Some(JobState::Queued));
    }

    #[tokio::test]
    async fn start_failed_event_fails_job_without_requeueing() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let (job_id, execution_id) = assigned_execution(&state, node_id);

        let status = report_event(
            state.clone(),
            node_id,
            execution_id,
            ExecutionEvent::StartFailed {
                reason: "program was not found".to_owned(),
            },
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let jobs = state
            .jobs
            .read()
            .expect("job manager lock should be available");
        assert_eq!(
            jobs.execution(execution_id).map(Execution::state),
            Some(ExecutionState::Failed)
        );
        assert_eq!(jobs.job(job_id).map(Job::state), Some(JobState::Failed));
    }

    #[tokio::test]
    async fn running_and_finished_events_complete_execution_and_job() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let (job_id, execution_id) = assigned_execution(&state, node_id);

        assert_eq!(
            report_event(
                state.clone(),
                node_id,
                execution_id,
                ExecutionEvent::Accepted,
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            report_event(
                state.clone(),
                node_id,
                execution_id,
                ExecutionEvent::Running,
            )
            .await,
            StatusCode::OK
        );
        let result = ExecutionResult { exit_code: Some(0) };
        assert_eq!(
            report_event(
                state.clone(),
                node_id,
                execution_id,
                ExecutionEvent::Finished {
                    result,
                    output: ExecutionOutput::default(),
                },
            )
            .await,
            StatusCode::OK
        );

        let jobs = state
            .jobs
            .read()
            .expect("job manager lock should be available");
        let execution = jobs
            .execution(execution_id)
            .expect("execution should remain stored");
        assert_eq!(execution.state(), ExecutionState::Succeeded);
        assert_eq!(execution.result(), Some(result));
        assert_eq!(jobs.job(job_id).map(Job::state), Some(JobState::Succeeded));
    }

    #[tokio::test]
    async fn event_from_a_different_node_is_rejected() {
        let state = ControllerState::new();
        let assigned_node_id = NodeId::generate();
        let (_, execution_id) = assigned_execution(&state, assigned_node_id);
        let reporting_node_id = NodeId::generate();
        register_ready_node(&state, reporting_node_id);
        let request = ReportExecutionEventRequest {
            metadata: RequestMetadata::new(),
            node_id: reporting_node_id,
            execution_id,
            event: ExecutionEvent::Accepted,
        };

        let response = router(state.clone())
            .oneshot(execution_event_json_request(request))
            .await
            .expect("foreign execution event should be handled");

        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            state
                .jobs
                .read()
                .expect("job manager lock should be available")
                .execution(execution_id)
                .map(Execution::state),
            Some(ExecutionState::Assigned)
        );
    }

    #[tokio::test]
    async fn queued_job_reports_when_matching_node_is_busy() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        assigned_execution(&state, node_id);
        let mut spec = job_spec();
        spec.requirements.memory_bytes = 8_000;
        let job_id = state
            .jobs
            .write()
            .expect("job manager lock should be available")
            .submit(spec)
            .expect("valid job should be queued");

        let status = get_job_status(state, job_id).await;

        assert_eq!(status.queue_reason, Some(QueueReason::NoAvailableNodes));
    }

    #[tokio::test]
    async fn node_list_is_empty_before_registration() {
        let response = router(ControllerState::new())
            .oneshot(list_nodes_request())
            .await
            .expect("node list request should be handled");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: ListNodesResponse =
            serde_json::from_slice(&body).expect("response should contain node list JSON");
        assert!(response.nodes.is_empty());
    }

    #[tokio::test]
    async fn node_list_returns_observed_state_in_node_id_order() {
        let state = ControllerState::new();
        let lower_id = "00000000-0000-0000-0000-000000000001"
            .parse()
            .expect("fixed Node ID should be valid");
        let higher_id = "00000000-0000-0000-0000-000000000002"
            .parse()
            .expect("fixed Node ID should be valid");
        let heartbeat_at = Instant::now();
        {
            let mut registry = state
                .registry
                .write()
                .expect("registry lock should be available");
            registry.register(descriptor(higher_id));
            registry.register(descriptor(lower_id));
            registry
                .record_heartbeat_at(lower_id, snapshot(), heartbeat_at)
                .expect("registered node should accept heartbeat");
        }

        let response = router(state)
            .oneshot(list_nodes_request())
            .await
            .expect("node list request should be handled");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: ListNodesResponse =
            serde_json::from_slice(&body).expect("response should contain node list JSON");

        assert_eq!(response.nodes.len(), 2);
        assert_eq!(response.nodes[0].descriptor.id, lower_id);
        assert_eq!(response.nodes[0].state, NodeState::Ready);
        assert_eq!(response.nodes[0].snapshot, Some(snapshot()));
        assert!(response.nodes[0].last_heartbeat_age_ms.is_some());
        assert_eq!(response.nodes[1].descriptor.id, higher_id);
        assert_eq!(response.nodes[1].state, NodeState::Joining);
        assert_eq!(response.nodes[1].snapshot, None);
        assert_eq!(response.nodes[1].last_heartbeat_age_ms, None);
    }

    #[tokio::test]
    async fn registration_stores_node_and_correlates_response() {
        let state = ControllerState::new();
        let request = registration_request(CURRENT_PROTOCOL_VERSION);
        let request_metadata = request.metadata;
        let node_id = request.node.id;

        let response = router(state.clone())
            .oneshot(registration_json_request(request))
            .await
            .expect("registration request should be handled");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: RegisterNodeResponse =
            serde_json::from_slice(&body).expect("response should contain registration JSON");
        assert_eq!(response.node_id, node_id);
        assert_eq!(response.metadata.in_reply_to, request_metadata.message_id);

        let registry = state
            .registry
            .read()
            .expect("registry lock should be available");
        assert_eq!(
            registry.get(node_id).map(|node| node.state()),
            Some(NodeState::Joining)
        );
    }

    #[tokio::test]
    async fn version_mismatch_is_rejected_without_registering_node() {
        let state = ControllerState::new();
        let request =
            registration_request(ProtocolVersion::new(CURRENT_PROTOCOL_VERSION.value() + 1));
        let request_metadata = request.metadata;
        let node_id = request.node.id;

        let response = router(state.clone())
            .oneshot(registration_json_request(request))
            .await
            .expect("registration request should be handled");

        assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: ProtocolErrorResponse =
            serde_json::from_slice(&body).expect("response should contain protocol error JSON");
        assert_eq!(response.metadata.in_reply_to, request_metadata.message_id);
        assert_eq!(
            response.error,
            ProtocolError::ProtocolVersionMismatch {
                expected: CURRENT_PROTOCOL_VERSION,
                received: request_metadata.protocol_version,
            }
        );

        let registry = state
            .registry
            .read()
            .expect("registry lock should be available");
        assert!(registry.get(node_id).is_none());
    }

    #[tokio::test]
    async fn heartbeat_makes_registered_node_ready_and_updates_snapshot() {
        let state = ControllerState::new();
        let registration = registration_request(CURRENT_PROTOCOL_VERSION);
        let node_id = registration.node.id;
        state
            .registry
            .write()
            .expect("registry lock should be available")
            .register(registration.node);
        let snapshot = snapshot();
        let request = HeartbeatRequest {
            metadata: RequestMetadata::new(),
            node_id,
            snapshot,
        };
        let request_metadata = request.metadata;

        let response = router(state.clone())
            .oneshot(heartbeat_json_request(request))
            .await
            .expect("heartbeat request should be handled");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: Acknowledgement =
            serde_json::from_slice(&body).expect("response should contain acknowledgement JSON");
        assert_eq!(response.metadata.in_reply_to, request_metadata.message_id);

        let registry = state
            .registry
            .read()
            .expect("registry lock should be available");
        let node = registry
            .get(node_id)
            .expect("node should remain registered");
        assert_eq!(node.state(), NodeState::Ready);
        assert_eq!(node.snapshot(), Some(snapshot));
    }

    #[tokio::test]
    async fn heartbeat_from_unregistered_node_is_rejected() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let request = HeartbeatRequest {
            metadata: RequestMetadata::new(),
            node_id,
            snapshot: snapshot(),
        };

        let response = router(state.clone())
            .oneshot(heartbeat_json_request(request))
            .await
            .expect("heartbeat request should be handled");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let response: ProtocolErrorResponse =
            serde_json::from_slice(&body).expect("response should contain protocol error JSON");
        assert_eq!(response.error, ProtocolError::NodeNotRegistered { node_id });
    }

    fn list_nodes_request() -> Request<Body> {
        Request::builder()
            .uri(LIST_NODES_PATH)
            .body(Body::empty())
            .expect("HTTP request should be valid")
    }

    fn get_request(uri: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("HTTP request should be valid")
    }

    fn post_empty_request(uri: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .body(Body::empty())
            .expect("HTTP request should be valid")
    }

    fn submit_job_json_request(spec: JobSpec) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(SUBMIT_JOB_PATH)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&spec).expect("job specification should serialize"),
            ))
            .expect("HTTP request should be valid")
    }

    fn poll_node_command_json_request(request: PollNodeCommandRequest) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(POLL_NODE_COMMAND_PATH)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&request).expect("request should serialize"),
            ))
            .expect("HTTP request should be valid")
    }

    fn execution_event_json_request(request: ReportExecutionEventRequest) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(REPORT_EXECUTION_EVENT_PATH)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&request).expect("request should serialize"),
            ))
            .expect("HTTP request should be valid")
    }

    async fn report_event(
        state: ControllerState,
        node_id: NodeId,
        execution_id: meld_core::ExecutionId,
        event: ExecutionEvent,
    ) -> StatusCode {
        router(state)
            .oneshot(execution_event_json_request(ReportExecutionEventRequest {
                metadata: RequestMetadata::new(),
                node_id,
                execution_id,
                event,
            }))
            .await
            .expect("execution event should be handled")
            .status()
    }

    async fn get_job_status(state: ControllerState, job_id: JobId) -> JobStatusResponse {
        let response = router(state)
            .oneshot(get_request(&format!("/v1/jobs/{job_id}")))
            .await
            .expect("job status should be handled");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        serde_json::from_slice(&body).expect("response should contain job status JSON")
    }

    fn registration_json_request(request: RegisterNodeRequest) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(REGISTER_NODE_PATH)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&request).expect("request should serialize"),
            ))
            .expect("HTTP request should be valid")
    }

    fn heartbeat_json_request(request: HeartbeatRequest) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(HEARTBEAT_PATH)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&request).expect("request should serialize"),
            ))
            .expect("HTTP request should be valid")
    }

    fn registration_request(protocol_version: ProtocolVersion) -> RegisterNodeRequest {
        RegisterNodeRequest {
            metadata: RequestMetadata {
                message_id: MessageId::generate(),
                protocol_version,
            },
            node: descriptor(NodeId::generate()),
        }
    }

    fn descriptor(node_id: NodeId) -> NodeDescriptor {
        NodeDescriptor {
            id: node_id,
            hostname: "worker-1".to_owned(),
            operating_system: "linux".to_owned(),
            architecture: "x86_64".to_owned(),
            capacity: ResourceCapacity {
                logical_cpus: 8,
                memory_bytes: 16_000,
            },
        }
    }

    fn snapshot() -> ResourceSnapshot {
        ResourceSnapshot {
            cpu_usage_percent: 12,
            available_memory_bytes: 8_000,
            running_executions: 0,
        }
    }

    fn register_ready_node(state: &ControllerState, node_id: NodeId) {
        let mut registry = state
            .registry
            .write()
            .expect("registry lock should be available");
        registry.register(descriptor(node_id));
        registry
            .record_heartbeat(node_id, snapshot())
            .expect("registered node should accept heartbeat");
    }

    fn assigned_execution(
        state: &ControllerState,
        node_id: NodeId,
    ) -> (JobId, meld_core::ExecutionId) {
        register_ready_node(state, node_id);
        let mut spec = job_spec();
        spec.requirements.memory_bytes = 8_000;
        let registry = state
            .registry
            .read()
            .expect("registry lock should be available");
        let mut jobs = state
            .jobs
            .write()
            .expect("job manager lock should be available");
        let job_id = jobs.submit(spec).expect("valid job should be queued");
        let execution_id = jobs
            .schedule_next(&Scheduler::new(), &registry)
            .expect("queued job should be schedulable")
            .expect("one job should be queued");
        (job_id, execution_id)
    }

    fn job_spec() -> JobSpec {
        JobSpec {
            program: "rustc".to_owned(),
            args: vec!["--version".to_owned()],
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 256_000_000,
            },
            job_timeout_secs: None,
            execution_timeout_secs: None,
        }
    }
}
