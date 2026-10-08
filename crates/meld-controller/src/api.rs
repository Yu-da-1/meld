//! HTTP boundary for controller-node protocol messages.

use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock, RwLockWriteGuard},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use meld_core::{
    Acknowledgement, ApiErrorResponse, BlobResponse, CURRENT_PROTOCOL_VERSION, CancelJobResponse,
    DataFailure, ExecutionEvent, ExecutionId, ExecutionOutput, ExecutionView, HeartbeatRequest,
    JobId, JobLogsResponse, JobSpec, JobState, JobStatusResponse, ListNodesResponse,
    MissingInputsResponse, NodeCommand, NodeId, NodeState, NodeStateResponse, NodeView, OutputFile,
    PollNodeCommandRequest, PollNodeCommandResponse, ProtocolError, ProtocolErrorResponse,
    QueueReason, RegisterNodeRequest, RegisterNodeResponse, ReportExecutionEventRequest,
    ResponseMetadata, RetryJobRequest, RetryJobResponse, Sha256Digest, SubmitJobResponse,
};
use tokio::{sync::watch, time::timeout};
use tokio_util::io::ReaderStream;

use crate::{
    blob_store::{BlobError, BlobStore, PutOutcome},
    failure_detector::FailureDetector,
    job_manager::{JobManager, JobManagerError},
    node_registry::{NodeRegistry, NodeRegistryError},
    scheduler::{Scheduler, SchedulingFailure},
    store::{Persist, StateStore, StoreError},
};

pub const REGISTER_NODE_PATH: &str = "/v1/nodes/register";
pub const HEARTBEAT_PATH: &str = "/v1/nodes/heartbeat";
pub const LIST_NODES_PATH: &str = "/v1/nodes";
pub const DRAIN_NODE_PATH: &str = "/v1/nodes/{node_id}/drain";
pub const RESUME_NODE_PATH: &str = "/v1/nodes/{node_id}/resume";
pub const SUBMIT_JOB_PATH: &str = "/v1/jobs";
pub const JOB_STATUS_PATH: &str = "/v1/jobs/{job_id}";
pub const JOB_LOGS_PATH: &str = "/v1/jobs/{job_id}/logs";
pub const CANCEL_JOB_PATH: &str = "/v1/jobs/{job_id}/cancel";
pub const RETRY_JOB_PATH: &str = "/v1/jobs/{job_id}/retry";
pub const BLOB_PATH: &str = "/v1/blobs/{sha256}";
pub const POLL_NODE_COMMAND_PATH: &str = "/v1/nodes/commands/poll";
pub const REPORT_EXECUTION_EVENT_PATH: &str = "/v1/nodes/executions/events";
pub const DEFAULT_OUTPUT_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const COMMAND_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(25);

/// Shared controller state exposed to HTTP handlers.
#[derive(Debug, Clone)]
pub struct ControllerState {
    registry: Arc<RwLock<NodeRegistry>>,
    jobs: Arc<RwLock<JobManager>>,
    /// Absent until configured; blob routes and file inputs then report 503.
    blobs: Option<Arc<BlobStore>>,
    /// Absent for a controller that keeps its state in memory only.
    store: Option<Arc<StateStore>>,
    /// How long outputs are protected from eviction after a job finishes.
    output_retention: Duration,
    command_updates: watch::Sender<u64>,
    /// When this controller process started. A node restored from the store
    /// has no heartbeat to measure silence from, so silence counts from here.
    started_at: Instant,
}

impl Default for ControllerState {
    fn default() -> Self {
        let (command_updates, _) = watch::channel(0);
        Self {
            registry: Arc::default(),
            jobs: Arc::default(),
            blobs: None,
            store: None,
            output_retention: DEFAULT_OUTPUT_RETENTION,
            command_updates,
            started_at: Instant::now(),
        }
    }
}

impl ControllerState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets how long finished jobs' outputs are guaranteed to stay available.
    #[must_use]
    pub fn with_output_retention(mut self, output_retention: Duration) -> Self {
        self.output_retention = output_retention;
        self
    }

    /// Enables storage of job input and output files.
    #[must_use]
    pub fn with_blob_store(mut self, blobs: Arc<BlobStore>) -> Self {
        self.blobs = Some(blobs);
        self
    }

    /// Makes controller state durable, first restoring whatever the store holds.
    ///
    /// From then on every change to nodes and jobs is written before the
    /// request that made it is answered.
    pub fn with_store(mut self, store: Arc<StateStore>) -> Result<Self, StoreError> {
        self.registry = Arc::new(RwLock::new(NodeRegistry::restore(&store)?));
        self.jobs = Arc::new(RwLock::new(JobManager::restore(&store)?));
        self.store = Some(store);
        Ok(self)
    }

    /// Locks the node registry for writing; changes are saved when the guard is released.
    fn registry_mut(&self) -> Result<WriteGuard<'_, NodeRegistry>, ControllerStateError> {
        self.write_guard(&self.registry)
    }

    /// Locks the job manager for writing; changes are saved when the guard is released.
    fn jobs_mut(&self) -> Result<WriteGuard<'_, JobManager>, ControllerStateError> {
        self.write_guard(&self.jobs)
    }

    fn write_guard<'a, T: Persist>(
        &'a self,
        lock: &'a RwLock<T>,
    ) -> Result<WriteGuard<'a, T>, ControllerStateError> {
        Ok(WriteGuard {
            guard: lock.write().map_err(|_| ControllerStateError)?,
            store: self.store.as_deref(),
        })
    }

    pub fn detect_unreachable_nodes(
        &self,
        detector: &FailureDetector,
        now: Instant,
    ) -> Result<Vec<NodeId>, ControllerStateError> {
        let mut registry = self.registry.write().map_err(|_| ControllerStateError)?;
        Ok(detector.detect(&mut registry, now))
    }

    /// Gives up on executions of nodes that have been unreachable for `silent_for`.
    ///
    /// Returns the executions marked lost and the ones sent back to the queue.
    pub fn give_up_on_silent_nodes(
        &self,
        now: Instant,
        silent_for: Duration,
    ) -> Result<(Vec<ExecutionId>, Vec<ExecutionId>), ControllerStateError> {
        let silent_nodes = {
            let registry = self.registry.read().map_err(|_| ControllerStateError)?;
            registry
                .nodes()
                .filter(|node| node.state() == NodeState::Unreachable)
                .filter(|node| {
                    let last_seen = node.last_heartbeat_at().unwrap_or(self.started_at);
                    now.saturating_duration_since(last_seen) >= silent_for
                })
                .map(|node| node.descriptor().id)
                .collect::<BTreeSet<_>>()
        };
        if silent_nodes.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let mut jobs = self.jobs_mut()?;
        let (lost, requeued) = jobs.give_up_on_nodes(&silent_nodes).map_err(|error| {
            tracing::error!(%error, "giving up on silent nodes failed");
            ControllerStateError
        })?;
        drop(jobs);
        if !requeued.is_empty() {
            self.notify_command_update();
        }
        Ok((lost, requeued))
    }

    pub fn expire_jobs(&self, now: Instant) -> Result<Vec<JobId>, ControllerStateError> {
        let mut jobs = self.jobs_mut()?;
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

/// A write lock that saves what changed under it when released.
///
/// The save happens while the lock is still held, so no other request can
/// observe, or act on, a change that is not yet durable. Call
/// [`WriteGuard::commit`] to learn whether the save worked; releasing the
/// guard saves too but can only log a failure.
struct WriteGuard<'a, T: Persist> {
    guard: RwLockWriteGuard<'a, T>,
    store: Option<&'a StateStore>,
}

impl<T: Persist> WriteGuard<'_, T> {
    fn commit(&mut self) -> Result<(), StoreError> {
        match self.store {
            Some(store) => self.guard.save(store),
            None => Ok(()),
        }
    }
}

impl<T: Persist> Deref for WriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T: Persist> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

impl<T: Persist> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        if let Err(error) = self.commit() {
            tracing::error!(%error, "controller state could not be saved; it will be retried");
        }
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
        .route(DRAIN_NODE_PATH, post(drain_node))
        .route(RESUME_NODE_PATH, post(resume_node))
        .route(BLOB_PATH, put(put_blob).get(get_blob))
        .route(SUBMIT_JOB_PATH, post(submit_job))
        .route(JOB_STATUS_PATH, get(job_status))
        .route(JOB_LOGS_PATH, get(job_logs))
        .route(CANCEL_JOB_PATH, post(cancel_job))
        .route(RETRY_JOB_PATH, post(retry_job))
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

    let mut jobs = match state.jobs_mut() {
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
    let transition =
        match request.event {
            ExecutionEvent::Accepted => jobs.accept_execution(request.execution_id),
            ExecutionEvent::Running => jobs.start_execution(request.execution_id),
            ExecutionEvent::Finished { output, .. } if job_timeout_requested => {
                jobs.confirm_job_timeout_with_output(request.execution_id, output)
            }
            ExecutionEvent::Finished {
                result,
                output,
                outputs,
            } => match verify_outputs(
                &state,
                &jobs,
                request.execution_id,
                result.exit_code,
                &outputs,
            ) {
                Ok(()) => {
                    let finished =
                        jobs.finish_execution_with_output(request.execution_id, result, output);
                    if finished.is_ok() {
                        jobs.record_output_files(request.execution_id, outputs);
                    }
                    finished
                }
                Err(failure) => {
                    tracing::warn!(
                        node_id = %request.node_id,
                        execution_id = %request.execution_id,
                        %failure,
                        "execution outputs are not available on the controller"
                    );
                    jobs.fail_execution_data(request.execution_id, failure)
                }
            },
            ExecutionEvent::StartFailed { .. } if job_timeout_requested => jobs
                .confirm_job_timeout_with_output(request.execution_id, ExecutionOutput::default()),
            ExecutionEvent::StartFailed { reason } => {
                tracing::warn!(
                    node_id = %request.node_id,
                    execution_id = %request.execution_id,
                    %reason,
                    "execution process failed to start"
                );
                jobs.fail_execution_start(request.execution_id)
            }
            ExecutionEvent::DataFailed { .. } if job_timeout_requested => jobs
                .confirm_job_timeout_with_output(request.execution_id, ExecutionOutput::default()),
            ExecutionEvent::DataFailed { failure } => {
                tracing::warn!(
                    node_id = %request.node_id,
                    execution_id = %request.execution_id,
                    %failure,
                    "execution data could not be moved"
                );
                jobs.fail_execution_data(request.execution_id, failure)
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

    let mut jobs = match state.jobs_mut() {
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

    for &execution_id in &request.active_execution_ids {
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
    }

    match jobs.reconcile_node(request.node_id, &request.active_execution_ids) {
        Ok(lost) => {
            for execution_id in lost {
                tracing::warn!(
                    node_id = %request.node_id,
                    %execution_id,
                    "execution is not running on its node after a controller restart; marked lost"
                );
            }
        }
        Err(error) => {
            tracing::error!(node_id = %request.node_id, %error, "reconciliation failed");
            return Err(Box::new(
                (StatusCode::INTERNAL_SERVER_ERROR, "reconciliation failed").into_response(),
            ));
        }
    }

    let cancellation = request
        .active_execution_ids
        .iter()
        .copied()
        .find(|&execution_id| jobs.cancellation_requested(execution_id, request.node_id));
    let command = if let Some(execution_id) = cancellation {
        Some(NodeCommand::Cancel { execution_id })
    } else {
        let assignment = match jobs
            .pending_assignment_for(request.node_id, &request.active_execution_ids)
        {
            Ok(Some(assignment)) => Some(assignment),
            Ok(None) if node.state() != NodeState::Ready => None,
            Ok(None) => {
                match jobs.schedule_next(&Scheduler::new(), &registry) {
                    Ok(_)
                    | Err(JobManagerError::Scheduling(
                        SchedulingFailure::NoReadyNodes
                        | SchedulingFailure::ConstraintsNotSatisfied
                        | SchedulingFailure::InsufficientResources
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

                match jobs.pending_assignment_for(request.node_id, &request.active_execution_ids) {
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
    let mut jobs = match state.jobs_mut() {
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

async fn retry_job(
    State(state): State<ControllerState>,
    Path(job_id): Path<JobId>,
    Json(request): Json<RetryJobRequest>,
) -> Response {
    let mut jobs = match state.jobs_mut() {
        Ok(jobs) => jobs,
        Err(error) => {
            tracing::error!(%job_id, %error, "job manager lock is poisoned");
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            );
        }
    };
    let Some(job) = jobs.job(job_id) else {
        return api_error(StatusCode::NOT_FOUND, format!("job {job_id} was not found"));
    };
    // The inputs were pinned only while the job was unfinished; they may be gone.
    if let Some(rejection) = check_inputs_are_stored(&state, job.spec()) {
        return rejection;
    }

    let previous_attempts = match jobs.retry_job(job_id, request.allow_duplicate_run) {
        Ok(attempts) => attempts,
        Err(error) => {
            return api_error(StatusCode::CONFLICT, error.to_string());
        }
    };
    if let Err(error) = jobs.commit() {
        tracing::error!(%job_id, %error, "retried job could not be saved");
        return api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "job could not be saved; retry the request",
        );
    }
    drop(jobs);
    state.notify_command_update();

    (
        StatusCode::ACCEPTED,
        Json(RetryJobResponse {
            job_id,
            state: JobState::Queued,
            previous_attempts,
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
    let attempts = jobs
        .attempts(job_id)
        .into_iter()
        .map(|execution| ExecutionView {
            execution_id: execution.id(),
            node_id: execution.node_id(),
            state: execution.state(),
            result: execution.result(),
        })
        .collect();
    let latest_execution_id = execution.map(|view| view.execution_id);
    let data_failure = latest_execution_id.and_then(|id| jobs.data_failure(id).cloned());
    let outputs = latest_execution_id
        .map(|id| jobs.output_files(id).to_vec())
        .unwrap_or_default();
    let queue_reason = if job.state() == JobState::Queued {
        match jobs.pending_position(job_id) {
            Some(_)
                if jobs
                    .is_behind_earlier_job(job_id, &Scheduler::new(), &registry)
                    .expect("queued job should support scheduling diagnosis") =>
            {
                Some(QueueReason::WaitingForEarlierJob)
            }
            Some(_) => Some(
                match jobs
                    .scheduling_failure_for(job_id, &Scheduler::new(), &registry)
                    .expect("queued job should support scheduling diagnosis")
                {
                    None => QueueReason::AwaitingAssignment,
                    Some(SchedulingFailure::NoReadyNodes) => QueueReason::NoReadyNodes,
                    Some(SchedulingFailure::ConstraintsNotSatisfied) => {
                        QueueReason::ConstraintsNotSatisfied
                    }
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

    let placement = jobs
        .placement_for(job_id, &Scheduler::new(), &registry)
        .expect("job was found above");

    Json(JobStatusResponse {
        job_id,
        spec: job.spec().clone(),
        state: job.state(),
        queue_reason,
        execution,
        attempts,
        data_failure,
        outputs,
        placement,
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
    let mut jobs = match state.jobs_mut() {
        Ok(jobs) => jobs,
        Err(error) => {
            tracing::error!(%error, "job manager lock is poisoned");
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            );
        }
    };

    if let Err(error) = spec.validate() {
        return api_error(StatusCode::BAD_REQUEST, error.to_string());
    }
    // Checked while the job lock is held: the lookup also refreshes each
    // blob's retention, which keeps it until the job starts pinning it.
    if let Some(rejection) = check_inputs_are_stored(&state, &spec) {
        return rejection;
    }

    match jobs.submit(spec) {
        Ok(job_id) => {
            // Accepting a job is a promise to run it, so it must be durable first.
            if let Err(error) = jobs.commit() {
                tracing::error!(%error, "submitted job could not be saved");
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "job could not be saved; retry the submission",
                );
            }
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
        Err(JobManagerError::InvalidSpec(error)) => {
            api_error(StatusCode::BAD_REQUEST, error.to_string())
        }
        Err(error) => {
            tracing::error!(%error, "job submission failed");
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "job submission failed")
        }
    }
}

/// Checks that a successful execution left exactly the declared outputs and
/// that each one is stored on the controller.
///
/// Outputs are only expected from a process that succeeded; the node collects
/// nothing otherwise.
fn verify_outputs(
    state: &ControllerState,
    jobs: &JobManager,
    execution_id: ExecutionId,
    exit_code: Option<i32>,
    reported: &[OutputFile],
) -> Result<(), DataFailure> {
    if exit_code != Some(0) {
        return Ok(());
    }
    let Some(job) = jobs
        .execution(execution_id)
        .and_then(|execution| jobs.job(execution.job_id()))
    else {
        return Ok(());
    };

    let unavailable = |path: &str| DataFailure::OutputUploadFailed {
        path: path.to_owned(),
    };
    let declared = &job.spec().data.outputs;
    if let Some(missing) = declared
        .iter()
        .find(|spec| reported.iter().all(|file| file.path != spec.path))
    {
        return Err(DataFailure::OutputMissing {
            path: missing.path.clone(),
        });
    }
    if let Some(extra) = reported
        .iter()
        .find(|file| declared.iter().all(|spec| spec.path != file.path))
    {
        return Err(unavailable(&extra.path));
    }
    for file in reported {
        let stored = state
            .blobs
            .as_ref()
            .and_then(|blobs| blobs.size_of(&file.sha256).ok().flatten());
        if stored != Some(file.size_bytes) {
            return Err(unavailable(&file.path));
        }
    }
    Ok(())
}

/// Returns the rejection for a spec whose input files are not all uploaded.
fn check_inputs_are_stored(state: &ControllerState, spec: &JobSpec) -> Option<Response> {
    if spec.data.is_empty() {
        return None;
    }
    let Some(blobs) = &state.blobs else {
        return Some(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "this controller has no file storage configured",
        ));
    };

    let mut missing = std::collections::BTreeSet::new();
    for input in &spec.data.inputs {
        match blobs.size_of(&input.sha256) {
            Ok(Some(stored)) if stored == input.size_bytes => {}
            Ok(Some(_)) => {
                return Some(api_error(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "declared size of input `{}` differs from its content",
                        input.path
                    ),
                ));
            }
            Ok(None) => {
                missing.insert(input.sha256.clone());
            }
            Err(error) => return Some(blob_error_response(&error)),
        }
    }
    if missing.is_empty() {
        return None;
    }
    Some(
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(MissingInputsResponse {
                error: "input files have not been uploaded".to_owned(),
                missing: missing.into_iter().collect(),
            }),
        )
            .into_response(),
    )
}

async fn put_blob(
    State(state): State<ControllerState>,
    Path(sha256): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (blobs, digest) = match blob_request(&state, &sha256) {
        Ok(parts) => parts,
        Err((status, message)) => return api_error(status, message),
    };
    // Reject early what the limit would reject after the whole transfer.
    let declared_length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let limit_bytes = blobs.limits().max_blob_bytes;
    if declared_length.is_some_and(|length| length > limit_bytes) {
        return blob_error_response(&BlobError::TooLarge { limit_bytes });
    }

    let pinned = match state.jobs.read() {
        Ok(jobs) => jobs.pinned_digests(Instant::now(), state.output_retention),
        Err(error) => {
            tracing::error!(%error, "job manager lock is poisoned");
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "controller state is unavailable",
            );
        }
    };
    match blobs.put(&digest, body.into_data_stream(), &pinned).await {
        Ok(outcome) => {
            let status = match outcome {
                PutOutcome::Created { .. } => StatusCode::CREATED,
                PutOutcome::Existing { .. } => StatusCode::OK,
            };
            (
                status,
                Json(BlobResponse {
                    sha256: digest,
                    size_bytes: outcome.size_bytes(),
                }),
            )
                .into_response()
        }
        Err(error) => blob_error_response(&error),
    }
}

async fn get_blob(State(state): State<ControllerState>, Path(sha256): Path<String>) -> Response {
    let (blobs, digest) = match blob_request(&state, &sha256) {
        Ok(parts) => parts,
        Err((status, message)) => return api_error(status, message),
    };
    match blobs.read(&digest).await {
        Ok(Some(stored)) => Response::builder()
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, stored.size_bytes)
            .body(Body::from_stream(ReaderStream::new(stored.file)))
            .unwrap_or_else(|error| {
                tracing::error!(%error, "failed to build blob response");
                api_error(StatusCode::INTERNAL_SERVER_ERROR, "blob response failed")
            }),
        Ok(None) => api_error(StatusCode::NOT_FOUND, "blob not found"),
        Err(error) => blob_error_response(&error),
    }
}

fn blob_request(
    state: &ControllerState,
    sha256: &str,
) -> Result<(Arc<BlobStore>, Sha256Digest), (StatusCode, String)> {
    let Some(blobs) = &state.blobs else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "this controller has no file storage configured".to_owned(),
        ));
    };
    let digest = sha256
        .parse::<Sha256Digest>()
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok((Arc::clone(blobs), digest))
}

fn blob_error_response(error: &BlobError) -> Response {
    let status = match error {
        BlobError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        BlobError::DigestMismatch | BlobError::Body(_) => StatusCode::BAD_REQUEST,
        BlobError::QuotaExceeded { .. } => StatusCode::INSUFFICIENT_STORAGE,
        BlobError::Io(_) | BlobError::Unavailable => {
            tracing::error!(%error, "blob storage failed");
            return api_error(StatusCode::INTERNAL_SERVER_ERROR, "blob storage failed");
        }
    };
    api_error(status, error.to_string())
}

fn api_error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(ApiErrorResponse {
            error: message.into(),
        }),
    )
        .into_response()
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

async fn drain_node(State(state): State<ControllerState>, Path(node_id): Path<NodeId>) -> Response {
    set_node_draining(state, node_id, true)
}

async fn resume_node(
    State(state): State<ControllerState>,
    Path(node_id): Path<NodeId>,
) -> Response {
    set_node_draining(state, node_id, false)
}

/// Stops or resumes new placements on a node; both directions are idempotent.
fn set_node_draining(state: ControllerState, node_id: NodeId, draining: bool) -> Response {
    let mut registry = match state.registry_mut() {
        Ok(registry) => registry,
        Err(error) => {
            tracing::error!(%node_id, %error, "node registry lock is poisoned");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiErrorResponse {
                    error: "controller state is unavailable".to_owned(),
                }),
            )
                .into_response();
        }
    };
    let node_state = match registry.set_draining(node_id, draining) {
        Ok(node_state) => node_state,
        Err(NodeRegistryError::NodeNotFound(node_id)) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiErrorResponse {
                    error: format!("node {node_id} was not found"),
                }),
            )
                .into_response();
        }
    };
    drop(registry);
    // Resuming makes capacity schedulable again, so wake waiting polls.
    state.notify_command_update();

    tracing::info!(%node_id, draining, state = ?node_state, "node drain setting changed");
    Json(NodeStateResponse {
        node_id,
        state: node_state,
    })
    .into_response()
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
    let mut registry = match state.registry_mut() {
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

    let mut registry = match state.registry_mut() {
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
        .is_some_and(|node| matches!(node.state(), NodeState::Ready | NodeState::Draining));
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
        MessageId, NodeDescriptor, NodeId, NodeState, NodeVerdict, ProtocolVersion,
        RequestMetadata, ResourceCapacity, ResourceRequirements, ResourceSnapshot,
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
        // The first job cannot fit any node, so it does not hold the second back;
        // the second reports its own blocker instead of waiting in line.
        let status = get_job_status(state, second_job_id).await;
        assert_eq!(
            status.queue_reason,
            Some(QueueReason::InsufficientResources)
        );
    }

    #[tokio::test]
    async fn job_behind_one_waiting_for_capacity_reports_waiting_for_earlier_job() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        assigned_execution(&state, node_id);
        let mut spec = job_spec();
        spec.requirements.memory_bytes = 4_000;
        let submit = |spec: JobSpec| {
            state
                .jobs
                .write()
                .expect("job manager lock should be available")
                .submit(spec)
                .expect("valid job should be queued")
        };
        let first = submit(spec.clone());
        let second = submit(spec);

        assert_eq!(
            get_job_status(state.clone(), first).await.queue_reason,
            Some(QueueReason::NoAvailableNodes)
        );
        assert_eq!(
            get_job_status(state.clone(), second).await.queue_reason,
            Some(QueueReason::WaitingForEarlierJob)
        );
    }

    #[tokio::test]
    async fn unplaceable_head_job_does_not_stop_the_node_from_polling_later_jobs() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        register_ready_node(&state, node_id);
        let mut gpu_spec = job_spec();
        gpu_spec.requirements.memory_bytes = 4_000;
        gpu_spec.constraints.capabilities = vec!["gpu".to_owned()];
        let mut plain_spec = job_spec();
        plain_spec.requirements.memory_bytes = 4_000;
        let (gpu_job, plain_job) = {
            let mut jobs = state
                .jobs
                .write()
                .expect("job manager lock should be available");
            (
                jobs.submit(gpu_spec).expect("job should be queued"),
                jobs.submit(plain_spec).expect("job should be queued"),
            )
        };

        let Some(NodeCommand::Start { assignment }) =
            poll_for_command(&state, node_id, vec![]).await
        else {
            panic!("the runnable job should start despite the unplaceable one ahead of it");
        };

        assert_eq!(assignment.job_id, plain_job);
        let waiting = get_job_status(state, gpu_job).await;
        assert_eq!(waiting.state, JobState::Queued);
        assert_eq!(
            waiting.queue_reason,
            Some(QueueReason::ConstraintsNotSatisfied)
        );
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
            active_execution_ids: vec![],
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
                active_execution_ids: vec![],
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
                    active_execution_ids: vec![execution_id],
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
                outputs: vec![],
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
                active_execution_ids: vec![execution_id],
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
            active_execution_ids: vec![],
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
            active_execution_ids: vec![],
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
    async fn busy_node_with_spare_capacity_receives_further_assignments() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        {
            let mut node = descriptor(node_id);
            node.capacity.max_concurrent_executions = 2;
            let mut registry = state
                .registry
                .write()
                .expect("registry lock should be available");
            registry.register(node);
            registry
                .record_heartbeat(node_id, snapshot())
                .expect("registered node should accept heartbeat");
        }
        let mut spec = job_spec();
        spec.requirements.memory_bytes = 4_000;
        let job_ids: Vec<_> = (0..3)
            .map(|_| {
                state
                    .jobs
                    .write()
                    .expect("job manager lock should be available")
                    .submit(spec.clone())
                    .expect("valid job should be queued")
            })
            .collect();

        let first = poll_for_command(&state, node_id, vec![]).await;
        let NodeCommand::Start { assignment: first } = first.expect("first job should start")
        else {
            panic!("idle node should receive a start command");
        };
        assert_eq!(first.job_id, job_ids[0]);

        let repeated = poll_for_command(&state, node_id, vec![]).await;
        let Some(NodeCommand::Start {
            assignment: repeated,
        }) = repeated
        else {
            panic!("unacknowledged assignment should be repeated");
        };
        assert_eq!(repeated.execution_id, first.execution_id);

        let second = poll_for_command(&state, node_id, vec![first.execution_id]).await;
        let Some(NodeCommand::Start { assignment: second }) = second else {
            panic!("node with spare capacity should receive a second job");
        };
        assert_eq!(second.job_id, job_ids[1]);
        assert_ne!(second.execution_id, first.execution_id);

        let both_active = vec![first.execution_id, second.execution_id];
        assert_eq!(
            poll_for_command(&state, node_id, both_active.clone()).await,
            None,
            "a full node must not receive a third job"
        );
        assert_eq!(
            get_job_status(state.clone(), job_ids[2]).await.state,
            JobState::Queued
        );

        state
            .jobs
            .write()
            .expect("job manager lock should be available")
            .request_job_cancellation(job_ids[1])
            .expect("active job should accept cancellation");
        assert_eq!(
            poll_for_command(&state, node_id, both_active).await,
            Some(NodeCommand::Cancel {
                execution_id: second.execution_id
            })
        );
    }

    #[tokio::test]
    async fn draining_node_keeps_running_work_but_receives_no_new_jobs() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let (_, running_execution_id) = assigned_execution(&state, node_id);
        let mut spec = job_spec();
        spec.requirements.memory_bytes = 8_000;
        let queued_job_id = state
            .jobs
            .write()
            .expect("job manager lock should be available")
            .submit(spec)
            .expect("valid job should be queued");

        let response = drain(&state, node_id).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let drained: NodeStateResponse =
            serde_json::from_slice(&body).expect("response should contain node state JSON");
        assert_eq!(drained.state, NodeState::Draining);

        // A heartbeat must not turn the node back into a scheduling target.
        let heartbeat = router(state.clone())
            .oneshot(heartbeat_json_request(HeartbeatRequest {
                metadata: RequestMetadata::new(),
                node_id,
                snapshot: snapshot(),
            }))
            .await
            .expect("heartbeat should be handled");
        assert_eq!(heartbeat.status(), StatusCode::OK);

        // The unacknowledged assignment made before the drain is still delivered.
        let delivered = poll_for_command(&state, node_id, vec![]).await;
        assert!(matches!(delivered, Some(NodeCommand::Start { .. })));

        assert_eq!(
            poll_for_command(&state, node_id, vec![running_execution_id]).await,
            None
        );
        let status = get_job_status(state.clone(), queued_job_id).await;
        assert_eq!(status.state, JobState::Queued);
        assert_eq!(status.queue_reason, Some(QueueReason::NoReadyNodes));

        // The node has one slot, still held by the running execution, so
        // resuming alone cannot start the queued job.
        let response = resume(&state, node_id).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            poll_for_command(&state, node_id, vec![running_execution_id]).await,
            None
        );
        assert_eq!(
            get_job_status(state.clone(), queued_job_id)
                .await
                .queue_reason,
            Some(QueueReason::NoAvailableNodes)
        );
    }

    #[tokio::test]
    async fn resumed_node_receives_queued_jobs_again() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        register_ready_node(&state, node_id);
        let mut spec = job_spec();
        spec.requirements.memory_bytes = 8_000;
        let job_id = state
            .jobs
            .write()
            .expect("job manager lock should be available")
            .submit(spec)
            .expect("valid job should be queued");

        drain(&state, node_id).await;
        assert_eq!(poll_for_command(&state, node_id, vec![]).await, None);

        let response = resume(&state, node_id).await;
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let resumed: NodeStateResponse =
            serde_json::from_slice(&body).expect("response should contain node state JSON");
        assert_eq!(resumed.state, NodeState::Ready);
        let Some(NodeCommand::Start { assignment }) =
            poll_for_command(&state, node_id, vec![]).await
        else {
            panic!("resumed node should receive the queued job");
        };
        assert_eq!(assignment.job_id, job_id);
    }

    #[tokio::test]
    async fn job_with_unmet_constraints_waits_without_breaking_node_polls() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        register_ready_node(&state, node_id);
        let mut spec = job_spec();
        spec.requirements.memory_bytes = 8_000;
        spec.constraints.capabilities = vec!["gpu".to_owned()];
        let job_id = state
            .jobs
            .write()
            .expect("job manager lock should be available")
            .submit(spec)
            .expect("valid job should be queued");

        assert_eq!(poll_for_command(&state, node_id, vec![]).await, None);
        let status = get_job_status(state.clone(), job_id).await;
        assert_eq!(status.state, JobState::Queued);
        assert_eq!(
            status.queue_reason,
            Some(QueueReason::ConstraintsNotSatisfied)
        );

        // A node that advertises the capability joins and receives the job.
        let gpu_node_id = NodeId::generate();
        {
            let mut node = descriptor(gpu_node_id);
            node.capabilities = vec!["gpu".to_owned()];
            let mut registry = state
                .registry
                .write()
                .expect("registry lock should be available");
            registry.register(node);
            registry
                .record_heartbeat(gpu_node_id, snapshot())
                .expect("registered node should accept heartbeat");
        }
        let Some(NodeCommand::Start { assignment }) =
            poll_for_command(&state, gpu_node_id, vec![]).await
        else {
            panic!("node with the capability should receive the job");
        };
        assert_eq!(assignment.job_id, job_id);
    }

    #[tokio::test]
    async fn status_explains_why_a_job_waits_and_where_it_was_placed() {
        let state = ControllerState::new();
        let drained_id = NodeId::generate();
        register_ready_node(&state, drained_id);
        drain(&state, drained_id).await;
        let mut spec = job_spec();
        spec.requirements.memory_bytes = 8_000;
        let job_id = state
            .jobs
            .write()
            .expect("job manager lock should be available")
            .submit(spec)
            .expect("valid job should be queued");

        let waiting = get_job_status(state.clone(), job_id).await;
        assert_eq!(waiting.state, JobState::Queued);
        assert_eq!(waiting.placement.len(), 1);
        assert_eq!(waiting.placement[0].node_id, drained_id);
        assert_eq!(
            waiting.placement[0].verdict,
            NodeVerdict::NotReady {
                state: NodeState::Draining
            }
        );

        resume(&state, drained_id).await;
        let Some(NodeCommand::Start { .. }) = poll_for_command(&state, drained_id, vec![]).await
        else {
            panic!("resumed node should receive the job");
        };

        let placed = get_job_status(state.clone(), job_id).await;
        assert_eq!(placed.state, JobState::Assigned);
        assert_eq!(placed.queue_reason, None);
        assert_eq!(placed.placement.len(), 1);
        assert!(matches!(
            placed.placement[0].verdict,
            NodeVerdict::Selected { .. }
        ));
    }

    #[tokio::test]
    async fn blank_constraint_is_rejected_at_submission() {
        let state = ControllerState::new();
        let mut spec = job_spec();
        spec.constraints.operating_system = Some("  ".to_owned());

        let response = router(state)
            .oneshot(submit_job_json_request(spec))
            .await
            .expect("submission should be handled");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn draining_an_unknown_node_returns_not_found() {
        let response = drain(&ControllerState::new(), NodeId::generate()).await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    async fn drain(state: &ControllerState, node_id: NodeId) -> Response {
        router(state.clone())
            .oneshot(post_empty_request(&format!("/v1/nodes/{node_id}/drain")))
            .await
            .expect("drain should be handled")
    }

    async fn resume(state: &ControllerState, node_id: NodeId) -> Response {
        router(state.clone())
            .oneshot(post_empty_request(&format!("/v1/nodes/{node_id}/resume")))
            .await
            .expect("resume should be handled")
    }

    #[tokio::test]
    async fn poll_reporting_an_execution_of_another_node_is_rejected() {
        let state = ControllerState::new();
        let other_node_id = NodeId::generate();
        let (_, execution_id) = assigned_execution(&state, other_node_id);
        let node_id = NodeId::generate();
        register_ready_node(&state, node_id);

        let response = poll_node_command_with_timeout(
            state,
            PollNodeCommandRequest {
                metadata: RequestMetadata::new(),
                node_id,
                active_execution_ids: vec![execution_id],
            },
            Duration::from_millis(20),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    async fn poll_for_command(
        state: &ControllerState,
        node_id: NodeId,
        active_execution_ids: Vec<meld_core::ExecutionId>,
    ) -> Option<NodeCommand> {
        let response = poll_node_command_with_timeout(
            state.clone(),
            PollNodeCommandRequest {
                metadata: RequestMetadata::new(),
                node_id,
                active_execution_ids,
            },
            Duration::from_millis(20),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        serde_json::from_slice::<PollNodeCommandResponse>(&body)
            .expect("response should contain command JSON")
            .command
    }

    #[tokio::test]
    async fn assignment_poll_from_unregistered_node_is_rejected() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let request = PollNodeCommandRequest {
            metadata: RequestMetadata::new(),
            node_id,
            active_execution_ids: vec![],
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
                active_execution_ids: vec![],
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
    async fn data_failed_event_fails_an_accepted_execution_and_its_job() {
        let state = ControllerState::new();
        let node_id = NodeId::generate();
        let (job_id, execution_id) = assigned_execution(&state, node_id);
        assert_eq!(
            report_event(
                state.clone(),
                node_id,
                execution_id,
                ExecutionEvent::Accepted
            )
            .await,
            StatusCode::OK
        );

        let status = report_event(
            state.clone(),
            node_id,
            execution_id,
            ExecutionEvent::DataFailed {
                failure: meld_core::DataFailure::ChecksumMismatch {
                    path: "data.csv".to_owned(),
                },
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
                    outputs: vec![],
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
                max_concurrent_executions: 1,
            },
            capabilities: vec![],
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

    mod retry {
        use meld_core::{ExecutionOutput, Sha256Digest};

        use super::*;

        async fn json<T: serde::de::DeserializeOwned>(response: Response) -> T {
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("response body should be readable");
            serde_json::from_slice(&body).expect("response should contain JSON")
        }

        fn retry_request(job_id: JobId, allow_duplicate_run: bool) -> Request<Body> {
            Request::builder()
                .method("POST")
                .uri(format!("/v1/jobs/{job_id}/retry"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&RetryJobRequest {
                        allow_duplicate_run,
                    })
                    .expect("request should serialize"),
                ))
                .expect("HTTP request should be valid")
        }

        async fn retry(state: &ControllerState, job_id: JobId, force: bool) -> Response {
            router(state.clone())
                .oneshot(retry_request(job_id, force))
                .await
                .expect("retry should be handled")
        }

        async fn fail(state: &ControllerState, node_id: NodeId, execution_id: ExecutionId) {
            for event in [
                ExecutionEvent::Accepted,
                ExecutionEvent::Running,
                ExecutionEvent::Finished {
                    result: ExecutionResult { exit_code: Some(2) },
                    output: ExecutionOutput::default(),
                    outputs: vec![],
                },
            ] {
                assert_eq!(
                    report_event(state.clone(), node_id, execution_id, event).await,
                    StatusCode::OK
                );
            }
        }

        #[tokio::test]
        async fn failed_job_is_retried_as_a_second_attempt_on_the_same_job() {
            let state = ControllerState::new();
            let node_id = NodeId::generate();
            let (job_id, first) = assigned_execution(&state, node_id);
            fail(&state, node_id, first).await;
            assert_eq!(
                get_job_status(state.clone(), job_id).await.state,
                JobState::Failed
            );

            let response = retry(&state, job_id, false).await;

            assert_eq!(response.status(), StatusCode::ACCEPTED);
            let body: RetryJobResponse = json(response).await;
            assert_eq!(body.state, JobState::Queued);
            assert_eq!(body.previous_attempts, 1);
            let Some(NodeCommand::Start { assignment }) =
                poll_for_command(&state, node_id, vec![]).await
            else {
                panic!("the retried job should be assigned");
            };
            assert_eq!(assignment.job_id, job_id);
            assert_ne!(assignment.execution_id, first);
            let status = get_job_status(state.clone(), job_id).await;
            assert_eq!(
                status
                    .attempts
                    .iter()
                    .map(|attempt| attempt.execution_id)
                    .collect::<Vec<_>>(),
                vec![first, assignment.execution_id]
            );
            assert_eq!(status.attempts[0].state, ExecutionState::Failed);
        }

        #[tokio::test]
        async fn lost_job_is_retried_only_with_explicit_consent() {
            let state = ControllerState::new();
            let node_id = NodeId::generate();
            let (job_id, execution_id) = assigned_execution(&state, node_id);
            for event in [ExecutionEvent::Accepted, ExecutionEvent::Running] {
                report_event(state.clone(), node_id, execution_id, event).await;
            }
            state
                .jobs_mut()
                .expect("job lock")
                .mark_execution_lost(execution_id)
                .expect("lost");

            let refused = retry(&state, job_id, false).await;
            assert_eq!(refused.status(), StatusCode::CONFLICT);
            let message: ApiErrorResponse = json(refused).await;
            assert!(
                message.error.contains("may still be running"),
                "{}",
                message.error
            );
            assert_eq!(
                get_job_status(state.clone(), job_id).await.state,
                JobState::Lost
            );

            let accepted = retry(&state, job_id, true).await;
            assert_eq!(accepted.status(), StatusCode::ACCEPTED);
            assert_eq!(
                get_job_status(state.clone(), job_id).await.state,
                JobState::Queued
            );
        }

        #[tokio::test]
        async fn running_and_unknown_jobs_cannot_be_retried() {
            let state = ControllerState::new();
            let node_id = NodeId::generate();
            let (job_id, _) = assigned_execution(&state, node_id);

            assert_eq!(
                retry(&state, job_id, true).await.status(),
                StatusCode::CONFLICT
            );
            assert_eq!(
                retry(&state, JobId::generate(), true).await.status(),
                StatusCode::NOT_FOUND
            );
        }

        #[tokio::test]
        async fn retry_is_refused_when_the_job_inputs_are_no_longer_stored() {
            let directory = tempfile::tempdir().expect("temp dir");
            let blobs = crate::blob_store::BlobStore::new(
                directory.path(),
                crate::blob_store::BlobLimits::default(),
            )
            .expect("blob store");
            let state = ControllerState::new().with_blob_store(Arc::new(blobs));
            let node_id = NodeId::generate();
            register_ready_node(&state, node_id);
            let mut spec = job_spec();
            spec.requirements.memory_bytes = 8_000;
            spec.data.inputs.push(meld_core::InputFile {
                path: "in.txt".to_owned(),
                sha256: Sha256Digest::from_bytes([9; 32]),
                size_bytes: 3,
                executable: false,
            });
            // Put the job straight into a failed state; its input was never stored.
            let job_id = {
                let mut jobs = state.jobs_mut().expect("job lock");
                let job_id = jobs.submit(spec).expect("submit");
                let registry = state.registry.read().expect("registry lock");
                let execution = jobs
                    .schedule(job_id, &Scheduler::new(), &registry)
                    .expect("schedule");
                jobs.fail_execution_start(execution).expect("fail");
                job_id
            };

            let response = retry(&state, job_id, false).await;

            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            assert_eq!(
                get_job_status(state.clone(), job_id).await.state,
                JobState::Failed
            );
        }
    }

    mod silent_nodes {
        use meld_core::ExecutionOutput;

        use super::*;

        const TIMEOUT: Duration = Duration::from_secs(15);
        const SILENT_FOR: Duration = Duration::from_secs(45);

        #[tokio::test]
        async fn node_that_stays_silent_loses_its_execution_and_a_late_result_still_counts() {
            let state = ControllerState::new();
            let node_id = NodeId::generate();
            let (job_id, execution_id) = assigned_execution(&state, node_id);
            for event in [ExecutionEvent::Accepted, ExecutionEvent::Running] {
                assert_eq!(
                    report_event(state.clone(), node_id, execution_id, event).await,
                    StatusCode::OK
                );
            }
            let last_heartbeat = state
                .registry
                .read()
                .expect("registry lock")
                .get(node_id)
                .and_then(|node| node.last_heartbeat_at())
                .expect("node has heartbeated");
            let detector = FailureDetector::new(TIMEOUT);

            // Unreachable, but still within the grace period: nothing is decided.
            let unreachable = state
                .detect_unreachable_nodes(&detector, last_heartbeat + TIMEOUT)
                .expect("detection");
            assert_eq!(unreachable, vec![node_id]);
            let (lost, _) = state
                .give_up_on_silent_nodes(
                    last_heartbeat + SILENT_FOR - Duration::from_secs(1),
                    SILENT_FOR,
                )
                .expect("giving up");
            assert!(lost.is_empty());
            assert_eq!(
                get_job_status(state.clone(), job_id).await.state,
                JobState::Running
            );

            let (lost, requeued) = state
                .give_up_on_silent_nodes(last_heartbeat + SILENT_FOR, SILENT_FOR)
                .expect("giving up");
            assert_eq!(lost, vec![execution_id]);
            assert!(requeued.is_empty());
            assert_eq!(
                get_job_status(state.clone(), job_id).await.state,
                JobState::Lost
            );

            // The process was never dead, only unreachable. Its result still counts.
            assert_eq!(
                report_event(
                    state.clone(),
                    node_id,
                    execution_id,
                    ExecutionEvent::Finished {
                        result: ExecutionResult { exit_code: Some(0) },
                        output: ExecutionOutput::default(),
                        outputs: vec![],
                    },
                )
                .await,
                StatusCode::OK
            );
            assert_eq!(
                get_job_status(state.clone(), job_id).await.state,
                JobState::Succeeded
            );
        }

        #[tokio::test]
        async fn unacknowledged_assignment_of_a_silent_node_goes_to_another_node() {
            let state = ControllerState::new();
            let silent = NodeId::generate();
            let (job_id, _) = assigned_execution(&state, silent);
            let last_heartbeat = state
                .registry
                .read()
                .expect("registry lock")
                .get(silent)
                .and_then(|node| node.last_heartbeat_at())
                .expect("node has heartbeated");
            state
                .detect_unreachable_nodes(&FailureDetector::new(TIMEOUT), last_heartbeat + TIMEOUT)
                .expect("detection");

            let (lost, requeued) = state
                .give_up_on_silent_nodes(last_heartbeat + SILENT_FOR, SILENT_FOR)
                .expect("giving up");
            assert!(lost.is_empty());
            assert_eq!(requeued.len(), 1);
            assert_eq!(
                get_job_status(state.clone(), job_id).await.state,
                JobState::Queued
            );

            let other = NodeId::generate();
            register_ready_node(&state, other);
            assert!(matches!(
                poll_for_command(&state, other, vec![]).await,
                Some(NodeCommand::Start { .. })
            ));
        }
    }

    mod restart {
        use meld_core::ExecutionOutput;
        use tempfile::TempDir;

        use super::*;

        fn state_over(directory: &TempDir) -> ControllerState {
            let store = StateStore::open(&directory.path().join("state.db"))
                .expect("state database should open");
            ControllerState::new()
                .with_store(Arc::new(store))
                .expect("stored state should restore")
        }

        async fn submit(state: &ControllerState) -> JobId {
            let mut spec = job_spec();
            spec.requirements.memory_bytes = 8_000;
            let response = router(state.clone())
                .oneshot(submit_job_json_request(spec))
                .await
                .expect("job submission should be handled");
            assert_eq!(response.status(), StatusCode::ACCEPTED);
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("response body should be readable");
            serde_json::from_slice::<SubmitJobResponse>(&body)
                .expect("response should contain submitted job JSON")
                .job_id
        }

        #[tokio::test]
        async fn jobs_nodes_and_drain_intent_survive_a_controller_restart() {
            let directory = tempfile::tempdir().expect("temp dir");
            let node_id = NodeId::generate();
            let before = state_over(&directory);
            {
                let mut registry = before.registry_mut().expect("registry lock");
                registry.register(descriptor(node_id));
                registry
                    .record_heartbeat(node_id, snapshot())
                    .expect("registered node should accept heartbeat");
            }
            let running_job = submit(&before).await;
            let queued_job = submit(&before).await;
            let Some(NodeCommand::Start { assignment }) =
                poll_for_command(&before, node_id, vec![]).await
            else {
                panic!("the first job should be assigned");
            };
            let execution_id = assignment.execution_id;
            for event in [ExecutionEvent::Accepted, ExecutionEvent::Running] {
                assert_eq!(
                    report_event(before.clone(), node_id, execution_id, event).await,
                    StatusCode::OK
                );
            }
            assert_eq!(drain(&before, node_id).await.status(), StatusCode::OK);
            drop(before);

            let after = state_over(&directory);

            let running = get_job_status(after.clone(), running_job).await;
            assert_eq!(running.state, JobState::Running);
            let queued = get_job_status(after.clone(), queued_job).await;
            assert_eq!(queued.state, JobState::Queued);
            // The node was last seen before the restart, so it is not yet Ready.
            let nodes = after.node_views_at(Instant::now()).expect("node views");
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].state, NodeState::Unreachable);
            // Its drain intent was kept: reporting in again does not make it schedulable.
            let heartbeat = router(after.clone())
                .oneshot(heartbeat_json_request(HeartbeatRequest {
                    metadata: RequestMetadata::new(),
                    node_id,
                    snapshot: snapshot(),
                }))
                .await
                .expect("heartbeat should be handled");
            assert_eq!(heartbeat.status(), StatusCode::OK);
            let nodes = after.node_views_at(Instant::now()).expect("node views");
            assert_eq!(nodes[0].state, NodeState::Draining);

            // The restored execution carries on where it left off.
            assert_eq!(
                report_event(
                    after.clone(),
                    node_id,
                    execution_id,
                    ExecutionEvent::Finished {
                        result: ExecutionResult { exit_code: Some(0) },
                        output: ExecutionOutput::default(),
                        outputs: vec![],
                    },
                )
                .await,
                StatusCode::OK
            );
            assert_eq!(
                get_job_status(after.clone(), running_job).await.state,
                JobState::Succeeded
            );
            assert_eq!(poll_for_command(&after, node_id, vec![]).await, None);
            assert_eq!(resume(&after, node_id).await.status(), StatusCode::OK);
            assert!(matches!(
                poll_for_command(&after, node_id, vec![]).await,
                Some(NodeCommand::Start { .. })
            ));
        }

        #[tokio::test]
        async fn execution_that_vanished_while_the_controller_was_down_is_marked_lost() {
            let directory = tempfile::tempdir().expect("temp dir");
            let node_id = NodeId::generate();
            let before = state_over(&directory);
            {
                let mut registry = before.registry_mut().expect("registry lock");
                registry.register(descriptor(node_id));
                registry
                    .record_heartbeat(node_id, snapshot())
                    .expect("registered node should accept heartbeat");
            }
            let job_id = submit(&before).await;
            let Some(NodeCommand::Start { assignment }) =
                poll_for_command(&before, node_id, vec![]).await
            else {
                panic!("the job should be assigned");
            };
            for event in [ExecutionEvent::Accepted, ExecutionEvent::Running] {
                assert_eq!(
                    report_event(before.clone(), node_id, assignment.execution_id, event).await,
                    StatusCode::OK
                );
            }
            drop(before);

            // The node restarted meanwhile and runs nothing.
            let after = state_over(&directory);
            assert_eq!(poll_for_command(&after, node_id, vec![]).await, None);

            assert_eq!(
                get_job_status(after.clone(), job_id).await.state,
                JobState::Lost
            );
            drop(after);
            // The verdict is durable.
            let again = state_over(&directory);
            assert_eq!(get_job_status(again, job_id).await.state, JobState::Lost);
        }

        #[tokio::test]
        async fn results_reported_after_a_restart_are_durable_too() {
            let directory = tempfile::tempdir().expect("temp dir");
            let node_id = NodeId::generate();
            let first = state_over(&directory);
            let (job_id, execution_id) = {
                {
                    let mut registry = first.registry_mut().expect("registry lock");
                    registry.register(descriptor(node_id));
                    registry
                        .record_heartbeat(node_id, snapshot())
                        .expect("registered node should accept heartbeat");
                }
                let job_id = submit(&first).await;
                let Some(NodeCommand::Start { assignment }) =
                    poll_for_command(&first, node_id, vec![]).await
                else {
                    panic!("the job should be assigned");
                };
                (job_id, assignment.execution_id)
            };
            drop(first);

            let second = state_over(&directory);
            for event in [ExecutionEvent::Accepted, ExecutionEvent::Running] {
                assert_eq!(
                    report_event(second.clone(), node_id, execution_id, event).await,
                    StatusCode::OK
                );
            }
            drop(second);

            let third = state_over(&directory);
            assert_eq!(get_job_status(third, job_id).await.state, JobState::Running);
        }
    }

    mod blobs {
        use meld_core::{DataSpec, InputFile};
        use sha2::{Digest, Sha256};
        use tempfile::TempDir;

        use crate::blob_store::BlobLimits;

        use super::*;

        fn limits() -> BlobLimits {
            BlobLimits {
                max_blob_bytes: 100,
                quota_bytes: 1000,
                min_retention: Duration::ZERO,
            }
        }

        fn state_with_blobs(directory: &TempDir, limits: BlobLimits) -> ControllerState {
            ControllerState::new().with_blob_store(Arc::new(
                BlobStore::new(directory.path(), limits).expect("blob store should open"),
            ))
        }

        fn digest_of(data: &[u8]) -> Sha256Digest {
            Sha256Digest::from_bytes(Sha256::digest(data).into())
        }

        fn blob_http_request(method: &str, digest: &str, body: &[u8]) -> Request<Body> {
            Request::builder()
                .method(method)
                .uri(format!("/v1/blobs/{digest}"))
                .body(Body::from(body.to_vec()))
                .expect("HTTP request should be valid")
        }

        async fn send(state: &ControllerState, request: Request<Body>) -> Response {
            router(state.clone())
                .oneshot(request)
                .await
                .expect("request should be handled")
        }

        async fn upload(state: &ControllerState, data: &[u8]) -> StatusCode {
            send(
                state,
                blob_http_request("PUT", digest_of(data).as_str(), data),
            )
            .await
            .status()
        }

        async fn json<T: serde::de::DeserializeOwned>(response: Response) -> T {
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("response body should be readable");
            serde_json::from_slice(&body).expect("response should contain JSON")
        }

        fn spec_with_input(data: &[u8]) -> JobSpec {
            let mut spec = job_spec();
            spec.data = DataSpec {
                inputs: vec![InputFile {
                    path: "input.txt".to_owned(),
                    sha256: digest_of(data),
                    size_bytes: data.len() as u64,
                    executable: false,
                }],
                outputs: vec![],
            };
            spec
        }

        #[tokio::test]
        async fn uploaded_blob_can_be_downloaded() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            let digest = digest_of(b"payload");

            let created = send(
                &state,
                blob_http_request("PUT", digest.as_str(), b"payload"),
            )
            .await;
            assert_eq!(created.status(), StatusCode::CREATED);
            let info: BlobResponse = json(created).await;
            assert_eq!(info.sha256, digest);
            assert_eq!(info.size_bytes, 7);

            let repeated = upload(&state, b"payload").await;
            assert_eq!(repeated, StatusCode::OK);

            let download = send(&state, blob_http_request("GET", digest.as_str(), b"")).await;
            assert_eq!(download.status(), StatusCode::OK);
            assert_eq!(download.headers()[header::CONTENT_LENGTH], "7");
            let body = to_bytes(download.into_body(), usize::MAX)
                .await
                .expect("body should be readable");
            assert_eq!(&body[..], b"payload");
        }

        #[tokio::test]
        async fn head_reports_size_without_a_body() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            upload(&state, b"payload").await;

            let response = send(
                &state,
                blob_http_request("HEAD", digest_of(b"payload").as_str(), b""),
            )
            .await;

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CONTENT_LENGTH], "7");
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body should be readable");
            assert!(body.is_empty());
        }

        #[tokio::test]
        async fn missing_blob_is_not_found() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());

            let response = send(
                &state,
                blob_http_request("GET", digest_of(b"absent").as_str(), b""),
            )
            .await;

            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }

        #[tokio::test]
        async fn content_that_does_not_match_the_url_digest_is_rejected() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            let claimed = digest_of(b"expected");

            let response = send(
                &state,
                blob_http_request("PUT", claimed.as_str(), b"different"),
            )
            .await;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let lookup = send(&state, blob_http_request("GET", claimed.as_str(), b"")).await;
            assert_eq!(lookup.status(), StatusCode::NOT_FOUND);
        }

        #[tokio::test]
        async fn malformed_digest_is_rejected() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());

            for digest in ["short", "..%2F..%2Fetc%2Fpasswd", &"G".repeat(64)] {
                let response = send(&state, blob_http_request("GET", digest, b"")).await;
                assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{digest}");
            }
        }

        #[tokio::test]
        async fn oversized_upload_is_rejected() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());

            let status = upload(&state, &[1u8; 101]).await;

            assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        }

        #[tokio::test]
        async fn upload_beyond_quota_is_refused() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(
                &directory,
                BlobLimits {
                    quota_bytes: 3,
                    ..limits()
                },
            );

            let status = upload(&state, b"four").await;

            assert_eq!(status, StatusCode::INSUFFICIENT_STORAGE);
        }

        #[tokio::test]
        async fn blob_routes_report_unavailable_without_storage() {
            let state = ControllerState::new();

            let put = upload(&state, b"data").await;
            let get = send(
                &state,
                blob_http_request("GET", digest_of(b"data").as_str(), b""),
            )
            .await
            .status();

            assert_eq!(put, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(get, StatusCode::SERVICE_UNAVAILABLE);
        }

        #[tokio::test]
        async fn job_with_unuploaded_input_is_rejected_with_the_missing_digests() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());

            let response = send(&state, submit_job_json_request(spec_with_input(b"data"))).await;

            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let rejection: MissingInputsResponse = json(response).await;
            assert_eq!(rejection.missing, vec![digest_of(b"data")]);
            assert!(
                state
                    .jobs
                    .read()
                    .expect("job manager lock should be available")
                    .pinned_digests(Instant::now(), DEFAULT_OUTPUT_RETENTION)
                    .is_empty(),
                "a rejected job must not be queued"
            );
        }

        #[tokio::test]
        async fn job_is_accepted_once_its_inputs_are_uploaded() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            upload(&state, b"data").await;

            let response = send(&state, submit_job_json_request(spec_with_input(b"data"))).await;

            assert_eq!(response.status(), StatusCode::ACCEPTED);
        }

        #[tokio::test]
        async fn declared_size_must_match_the_stored_content() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            upload(&state, b"data").await;
            let mut spec = spec_with_input(b"data");
            spec.data.inputs[0].size_bytes = 99;

            let response = send(&state, submit_job_json_request(spec)).await;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        #[tokio::test]
        async fn job_with_inputs_is_unavailable_without_storage() {
            let state = ControllerState::new();

            let response = send(&state, submit_job_json_request(spec_with_input(b"data"))).await;

            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }

        #[tokio::test]
        async fn job_with_unsafe_data_path_is_rejected_before_storage_is_consulted() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            let mut spec = spec_with_input(b"data");
            spec.data.inputs[0].path = "../escape".to_owned();

            let response = send(&state, submit_job_json_request(spec)).await;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        fn spec_with_output(path: &str) -> JobSpec {
            let mut spec = job_spec();
            spec.requirements.memory_bytes = 8_000;
            spec.data.outputs = vec![meld_core::OutputSpec {
                path: path.to_owned(),
            }];
            spec
        }

        /// Queues, places and starts a job, returning it ready to finish.
        async fn running_execution(
            state: &ControllerState,
            spec: JobSpec,
        ) -> (NodeId, JobId, meld_core::ExecutionId) {
            let node_id = NodeId::generate();
            register_ready_node(state, node_id);
            let (job_id, execution_id) = {
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
            };
            for event in [ExecutionEvent::Accepted, ExecutionEvent::Running] {
                assert_eq!(
                    report_event(state.clone(), node_id, execution_id, event).await,
                    StatusCode::OK
                );
            }
            (node_id, job_id, execution_id)
        }

        fn finished(exit_code: i32, outputs: Vec<OutputFile>) -> ExecutionEvent {
            ExecutionEvent::Finished {
                result: ExecutionResult {
                    exit_code: Some(exit_code),
                },
                output: ExecutionOutput::default(),
                outputs,
            }
        }

        fn output_file(path: &str, data: &[u8]) -> OutputFile {
            OutputFile {
                path: path.to_owned(),
                sha256: digest_of(data),
                size_bytes: data.len() as u64,
            }
        }

        #[tokio::test]
        async fn uploaded_outputs_are_recorded_and_shown_in_status() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            upload(&state, b"{}").await;
            let (node_id, job_id, execution_id) =
                running_execution(&state, spec_with_output("result.json")).await;
            let produced = vec![output_file("result.json", b"{}")];

            let status = report_event(
                state.clone(),
                node_id,
                execution_id,
                finished(0, produced.clone()),
            )
            .await;

            assert_eq!(status, StatusCode::OK);
            let job = get_job_status(state.clone(), job_id).await;
            assert_eq!(job.state, JobState::Succeeded);
            assert_eq!(job.outputs, produced);
            assert_eq!(job.data_failure, None);
            // A repeated report, as a node retries after a lost reply, is harmless.
            assert_eq!(
                report_event(state.clone(), node_id, execution_id, finished(0, produced)).await,
                StatusCode::OK
            );
        }

        #[tokio::test]
        async fn declared_output_that_was_not_reported_fails_the_job() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            let (node_id, job_id, execution_id) =
                running_execution(&state, spec_with_output("result.json")).await;

            let status =
                report_event(state.clone(), node_id, execution_id, finished(0, vec![])).await;

            assert_eq!(status, StatusCode::OK);
            let job = get_job_status(state, job_id).await;
            assert_eq!(job.state, JobState::Failed);
            assert_eq!(
                job.data_failure,
                Some(DataFailure::OutputMissing {
                    path: "result.json".to_owned()
                })
            );
            assert!(job.outputs.is_empty());
        }

        #[tokio::test]
        async fn reported_output_that_is_not_stored_fails_the_job() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            let (node_id, job_id, execution_id) =
                running_execution(&state, spec_with_output("result.json")).await;

            report_event(
                state.clone(),
                node_id,
                execution_id,
                finished(0, vec![output_file("result.json", b"never uploaded")]),
            )
            .await;

            let job = get_job_status(state, job_id).await;
            assert_eq!(job.state, JobState::Failed);
            assert_eq!(
                job.data_failure,
                Some(DataFailure::OutputUploadFailed {
                    path: "result.json".to_owned()
                })
            );
        }

        #[tokio::test]
        async fn reported_size_must_match_the_stored_content() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            upload(&state, b"{}").await;
            let (node_id, job_id, execution_id) =
                running_execution(&state, spec_with_output("result.json")).await;
            let mut lying = output_file("result.json", b"{}");
            lying.size_bytes = 999;

            report_event(
                state.clone(),
                node_id,
                execution_id,
                finished(0, vec![lying]),
            )
            .await;

            let job = get_job_status(state, job_id).await;
            assert_eq!(job.state, JobState::Failed);
            assert!(matches!(
                job.data_failure,
                Some(DataFailure::OutputUploadFailed { .. })
            ));
        }

        #[tokio::test]
        async fn undeclared_output_is_refused() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            upload(&state, b"{}").await;
            upload(&state, b"secret").await;
            let (node_id, job_id, execution_id) =
                running_execution(&state, spec_with_output("result.json")).await;

            report_event(
                state.clone(),
                node_id,
                execution_id,
                finished(
                    0,
                    vec![
                        output_file("result.json", b"{}"),
                        output_file("extra.txt", b"secret"),
                    ],
                ),
            )
            .await;

            let job = get_job_status(state, job_id).await;
            assert_eq!(job.state, JobState::Failed);
            assert_eq!(
                job.data_failure,
                Some(DataFailure::OutputUploadFailed {
                    path: "extra.txt".to_owned()
                })
            );
        }

        #[tokio::test]
        async fn failed_process_is_not_expected_to_leave_outputs() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            let (node_id, job_id, execution_id) =
                running_execution(&state, spec_with_output("result.json")).await;

            let status =
                report_event(state.clone(), node_id, execution_id, finished(2, vec![])).await;

            assert_eq!(status, StatusCode::OK);
            let job = get_job_status(state, job_id).await;
            assert_eq!(job.state, JobState::Failed);
            assert_eq!(job.data_failure, None, "the process failed, not the data");
        }

        #[tokio::test]
        async fn data_failure_is_kept_and_shown_in_status() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(&directory, limits());
            let mut spec = job_spec();
            spec.requirements.memory_bytes = 8_000;
            let (node_id, job_id, execution_id) = running_execution(&state, spec).await;
            let failure = DataFailure::OutputUploadFailed {
                path: "result.json".to_owned(),
            };

            for _ in 0..2 {
                let status = report_event(
                    state.clone(),
                    node_id,
                    execution_id,
                    ExecutionEvent::DataFailed {
                        failure: failure.clone(),
                    },
                )
                .await;
                assert_eq!(status, StatusCode::OK, "reports are idempotent");
            }

            let job = get_job_status(state, job_id).await;
            assert_eq!(job.state, JobState::Failed);
            assert_eq!(job.data_failure, Some(failure));
        }

        /// Finishes a job that left `{}` as its output, in a store that holds 8 bytes.
        async fn state_holding_a_finished_output(
            retention: Duration,
        ) -> (ControllerState, TempDir) {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(
                &directory,
                BlobLimits {
                    quota_bytes: 8,
                    ..limits()
                },
            )
            .with_output_retention(retention);
            upload(&state, b"{}").await;
            let (node_id, _, execution_id) =
                running_execution(&state, spec_with_output("result.json")).await;
            report_event(
                state.clone(),
                node_id,
                execution_id,
                finished(0, vec![output_file("result.json", b"{}")]),
            )
            .await;
            (state, directory)
        }

        async fn is_stored(state: &ControllerState, data: &[u8]) -> bool {
            send(
                state,
                blob_http_request("GET", digest_of(data).as_str(), b""),
            )
            .await
            .status()
                == StatusCode::OK
        }

        #[tokio::test]
        async fn recent_outputs_survive_eviction_pressure() {
            let (state, _directory) =
                state_holding_a_finished_output(Duration::from_secs(3600)).await;
            upload(&state, b"aaaa").await;

            // 2 + 4 + 4 bytes do not fit in 8: only the unprotected blob can go.
            assert_eq!(upload(&state, b"bbbb").await, StatusCode::CREATED);

            assert!(is_stored(&state, b"{}").await, "the output must be kept");
            assert!(!is_stored(&state, b"aaaa").await);
        }

        #[tokio::test]
        async fn outputs_past_retention_can_be_evicted() {
            let (state, _directory) = state_holding_a_finished_output(Duration::ZERO).await;
            upload(&state, b"aaaa").await;
            is_stored(&state, b"aaaa").await;

            // `{}` is now the least recently used and unprotected.
            assert_eq!(upload(&state, b"bbbbbb").await, StatusCode::CREATED);

            assert!(!is_stored(&state, b"{}").await);
        }

        #[tokio::test]
        async fn job_with_only_outputs_is_unavailable_without_storage() {
            let state = ControllerState::new();

            let response = send(
                &state,
                submit_job_json_request(spec_with_output("result.json")),
            )
            .await;

            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }

        #[tokio::test]
        async fn inputs_of_unfinished_jobs_survive_eviction() {
            let directory = TempDir::new().expect("temp dir");
            let state = state_with_blobs(
                &directory,
                BlobLimits {
                    quota_bytes: 8,
                    ..limits()
                },
            );
            upload(&state, b"aaaa").await;
            let submitted = send(&state, submit_job_json_request(spec_with_input(b"aaaa"))).await;
            assert_eq!(submitted.status(), StatusCode::ACCEPTED);
            upload(&state, b"bbbb").await;

            // Full store: `bbbb` is unreferenced and goes, `aaaa` is needed.
            assert_eq!(upload(&state, b"cccc").await, StatusCode::CREATED);

            let kept = send(
                &state,
                blob_http_request("GET", digest_of(b"aaaa").as_str(), b""),
            )
            .await;
            let evicted = send(
                &state,
                blob_http_request("GET", digest_of(b"bbbb").as_str(), b""),
            )
            .await;
            assert_eq!(kept.status(), StatusCode::OK);
            assert_eq!(evicted.status(), StatusCode::NOT_FOUND);
        }
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
            constraints: meld_core::PlacementConstraints::default(),
            data: meld_core::DataSpec::default(),
        }
    }
}
