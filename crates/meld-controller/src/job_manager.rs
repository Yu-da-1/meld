//! In-memory ownership of logical jobs and execution attempts.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error,
    fmt,
    time::{Duration, Instant},
};

use meld_core::{
    DataFailure, Execution, ExecutionAssignment, ExecutionCompletionError, ExecutionId,
    ExecutionOutput, ExecutionResult, ExecutionState, InvalidStateTransition, Job, JobId, JobSpec,
    JobSpecValidationError, JobState, NodeAssessment, NodeId, OutputFile, Sha256Digest,
};

use crate::{
    node_registry::NodeRegistry,
    scheduler::{NodeAllocation, Scheduler, SchedulingFailure},
};

/// Owns controller-authoritative Job and Execution records.
#[derive(Debug, Default)]
pub struct JobManager {
    jobs: BTreeMap<JobId, Job>,
    executions: BTreeMap<ExecutionId, Execution>,
    execution_ids_by_job: BTreeMap<JobId, Vec<ExecutionId>>,
    outputs: BTreeMap<ExecutionId, ExecutionOutput>,
    /// Why an execution failed to move its job's data.
    data_failures: BTreeMap<ExecutionId, DataFailure>,
    /// Files each execution left behind for collection.
    output_files: BTreeMap<ExecutionId, Vec<OutputFile>>,
    /// When each execution's outputs were recorded, to bound how long they are kept.
    outputs_recorded_at: BTreeMap<ExecutionId, Instant>,
    cancellation_requests: BTreeSet<ExecutionId>,
    job_timeout_requests: BTreeSet<ExecutionId>,
    job_submitted_at: BTreeMap<JobId, Instant>,
    pending_jobs: VecDeque<JobId>,
    /// The scheduler's verdict on every node at the latest placement of each job.
    placements: BTreeMap<JobId, Vec<NodeAssessment>>,
}

impl JobManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and queues a newly submitted job.
    pub fn submit(&mut self, spec: JobSpec) -> Result<JobId, JobManagerError> {
        self.submit_at(spec, Instant::now())
    }

    fn submit_at(
        &mut self,
        spec: JobSpec,
        submitted_at: Instant,
    ) -> Result<JobId, JobManagerError> {
        let has_job_timeout = spec.job_timeout_secs.is_some();
        let mut job = Job::new(spec).map_err(JobManagerError::InvalidSpec)?;
        job.queue().map_err(JobManagerError::InvalidJobState)?;
        let job_id = job.id();
        self.jobs.insert(job_id, job);
        self.pending_jobs.push_back(job_id);
        if has_job_timeout {
            self.job_submitted_at.insert(job_id, submitted_at);
        }
        Ok(job_id)
    }

    /// Schedules the oldest queued job that can be placed.
    ///
    /// Order is FIFO, with one exception: a job that no current node could
    /// ever take (see [`SchedulingFailure::can_be_overtaken`]) does not hold
    /// back later jobs. A job that is merely waiting for capacity does, so
    /// large jobs are not starved by a stream of small ones.
    ///
    /// When nothing can be placed, the error is the first blocker found, so a
    /// queue holding only unplaceable jobs still reports why.
    pub fn schedule_next(
        &mut self,
        scheduler: &Scheduler,
        registry: &NodeRegistry,
    ) -> Result<Option<ExecutionId>, JobManagerError> {
        let mut first_overtaken = None;
        for job_id in self.pending_jobs.clone() {
            match self.schedule(job_id, scheduler, registry) {
                Ok(execution_id) => return Ok(Some(execution_id)),
                Err(JobManagerError::Scheduling(failure)) if failure.can_be_overtaken() => {
                    first_overtaken.get_or_insert(failure);
                }
                Err(error) => return Err(error),
            }
        }

        first_overtaken.map_or(Ok(None), |failure| {
            Err(JobManagerError::Scheduling(failure))
        })
    }

    /// Whether an earlier queued job will be placed before this one, or is
    /// waiting for capacity that this job must not take.
    pub fn is_behind_earlier_job(
        &self,
        job_id: JobId,
        scheduler: &Scheduler,
        registry: &NodeRegistry,
    ) -> Result<bool, JobManagerError> {
        for &earlier in self
            .pending_jobs
            .iter()
            .take_while(|queued| **queued != job_id)
        {
            let overtakable = self
                .scheduling_failure_for(earlier, scheduler, registry)?
                .is_some_and(SchedulingFailure::can_be_overtaken);
            if !overtakable {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Selects a node and creates one assigned execution attempt.
    pub fn schedule(
        &mut self,
        job_id: JobId,
        scheduler: &Scheduler,
        registry: &NodeRegistry,
    ) -> Result<ExecutionId, JobManagerError> {
        let job = self
            .jobs
            .get(&job_id)
            .ok_or(JobManagerError::JobNotFound(job_id))?;

        if job.state() != JobState::Queued {
            return Err(JobManagerError::JobNotQueued {
                job_id,
                state: job.state(),
            });
        }

        let allocations = self.node_allocations();
        let placement = scheduler.place(
            job.spec().requirements,
            &job.spec().constraints,
            registry,
            &allocations,
        );
        let Some(node_id) = placement.selected else {
            return Err(JobManagerError::Scheduling(
                placement
                    .failure()
                    .expect("an unselected placement always has a failure"),
            ));
        };

        let execution = loop {
            let candidate = Execution::new(job_id, node_id);
            if !self.executions.contains_key(&candidate.id()) {
                break candidate;
            }
        };
        let execution_id = execution.id();

        self.jobs
            .get_mut(&job_id)
            .expect("job was verified above")
            .assign()
            .map_err(JobManagerError::InvalidJobState)?;
        self.executions.insert(execution_id, execution);
        self.execution_ids_by_job
            .entry(job_id)
            .or_default()
            .push(execution_id);
        self.placements.insert(job_id, placement.assessments);
        self.remove_pending_job(job_id);

        Ok(execution_id)
    }

    /// Per-node reasoning for a job: what the scheduler would decide right now
    /// while the job is queued, otherwise what it decided when placing the job.
    pub fn placement_for(
        &self,
        job_id: JobId,
        scheduler: &Scheduler,
        registry: &NodeRegistry,
    ) -> Result<Vec<NodeAssessment>, JobManagerError> {
        let job = self
            .jobs
            .get(&job_id)
            .ok_or(JobManagerError::JobNotFound(job_id))?;
        if job.state() != JobState::Queued {
            return Ok(self.placements.get(&job_id).cloned().unwrap_or_default());
        }

        Ok(scheduler
            .place(
                job.spec().requirements,
                &job.spec().constraints,
                registry,
                &self.node_allocations(),
            )
            .assessments)
    }

    /// Content that must not be evicted at `now`: the inputs of jobs that have
    /// not finished, and outputs recorded less than `output_retention` ago.
    ///
    /// Retention is a guaranteed minimum. Older outputs are only removed when
    /// the store needs the room.
    pub fn pinned_digests(
        &self,
        now: Instant,
        output_retention: Duration,
    ) -> BTreeSet<Sha256Digest> {
        let recent_outputs = self
            .outputs_recorded_at
            .iter()
            .filter(|(_, recorded_at)| {
                now.saturating_duration_since(**recorded_at) < output_retention
            })
            .filter_map(|(execution_id, _)| self.output_files.get(execution_id))
            .flatten()
            .map(|file| file.sha256.clone());
        let unfinished_inputs = self
            .jobs
            .values()
            .filter(|job| {
                !matches!(
                    job.state(),
                    JobState::Succeeded
                        | JobState::Failed
                        | JobState::Cancelled
                        | JobState::TimedOut
                        | JobState::Lost
                )
            })
            .flat_map(|job| job.spec().data.inputs.iter())
            .map(|input| input.sha256.clone());
        unfinished_inputs.chain(recent_outputs).collect()
    }

    pub fn job(&self, job_id: JobId) -> Option<&Job> {
        self.jobs.get(&job_id)
    }

    pub fn execution(&self, execution_id: ExecutionId) -> Option<&Execution> {
        self.executions.get(&execution_id)
    }

    pub fn latest_execution_for_job(&self, job_id: JobId) -> Option<&Execution> {
        self.execution_ids_by_job
            .get(&job_id)
            .and_then(|execution_ids| execution_ids.last())
            .and_then(|execution_id| self.executions.get(execution_id))
    }

    pub fn execution_output(&self, execution_id: ExecutionId) -> Option<&ExecutionOutput> {
        self.outputs.get(&execution_id)
    }

    pub fn pending_position(&self, job_id: JobId) -> Option<usize> {
        self.pending_jobs
            .iter()
            .position(|pending| *pending == job_id)
    }

    pub fn scheduling_failure_for(
        &self,
        job_id: JobId,
        scheduler: &Scheduler,
        registry: &NodeRegistry,
    ) -> Result<Option<SchedulingFailure>, JobManagerError> {
        let job = self
            .jobs
            .get(&job_id)
            .ok_or(JobManagerError::JobNotFound(job_id))?;
        if job.state() != JobState::Queued {
            return Err(JobManagerError::JobNotQueued {
                job_id,
                state: job.state(),
            });
        }
        let allocations = self.node_allocations();
        Ok(scheduler
            .select_node(
                job.spec().requirements,
                &job.spec().constraints,
                registry,
                &allocations,
            )
            .err())
    }

    pub fn request_job_cancellation(
        &mut self,
        job_id: JobId,
    ) -> Result<Option<ExecutionId>, JobManagerError> {
        let state = self
            .jobs
            .get(&job_id)
            .ok_or(JobManagerError::JobNotFound(job_id))?
            .state();

        if matches!(state, JobState::Submitted | JobState::Queued) {
            self.jobs
                .get_mut(&job_id)
                .expect("job was verified above")
                .request_cancellation()
                .map_err(JobManagerError::InvalidJobState)?;
            self.remove_pending_job(job_id);
            self.job_submitted_at.remove(&job_id);
            return Ok(None);
        }

        let execution_id = self
            .latest_execution_for_job(job_id)
            .map(Execution::id)
            .ok_or(JobManagerError::JobHasNoExecution(job_id))?;
        self.jobs
            .get_mut(&job_id)
            .expect("job was verified above")
            .request_cancellation()
            .map_err(JobManagerError::InvalidJobState)?;
        self.cancellation_requests.insert(execution_id);
        self.job_submitted_at.remove(&job_id);
        Ok(Some(execution_id))
    }

    pub fn cancellation_requested(&self, execution_id: ExecutionId, node_id: NodeId) -> bool {
        self.cancellation_requests.contains(&execution_id)
            && self
                .executions
                .get(&execution_id)
                .is_some_and(|execution| execution.node_id() == node_id)
    }

    pub fn job_timeout_requested(&self, execution_id: ExecutionId) -> bool {
        self.job_timeout_requests.contains(&execution_id)
    }

    pub fn job_timeout_applies(&self, execution_id: ExecutionId) -> bool {
        self.job_timeout_requested(execution_id)
            || self
                .executions
                .get(&execution_id)
                .and_then(|execution| self.jobs.get(&execution.job_id()))
                .is_some_and(|job| matches!(job.state(), JobState::TimingOut | JobState::TimedOut))
    }

    pub fn expire_jobs_at(&mut self, now: Instant) -> Result<Vec<JobId>, JobManagerError> {
        let expired = self
            .job_submitted_at
            .iter()
            .filter_map(|(job_id, submitted_at)| {
                let timeout_secs = self.jobs.get(job_id)?.spec().job_timeout_secs?;
                (now.saturating_duration_since(*submitted_at) >= Duration::from_secs(timeout_secs))
                    .then_some(*job_id)
            })
            .collect::<Vec<_>>();
        let mut timed_out = Vec::with_capacity(expired.len());

        for job_id in expired {
            let state = self
                .jobs
                .get(&job_id)
                .ok_or(JobManagerError::JobNotFound(job_id))?
                .state();
            let timeout_started = match state {
                JobState::Submitted | JobState::Queued => {
                    self.jobs
                        .get_mut(&job_id)
                        .expect("job was verified above")
                        .mark_timed_out()
                        .map_err(JobManagerError::InvalidJobState)?;
                    self.remove_pending_job(job_id);
                    true
                }
                JobState::Assigned | JobState::Running => {
                    let execution_id = self
                        .latest_execution_for_job(job_id)
                        .map(Execution::id)
                        .ok_or(JobManagerError::JobHasNoExecution(job_id))?;
                    let execution_state = self
                        .executions
                        .get(&execution_id)
                        .expect("latest execution should remain stored")
                        .state();
                    if execution_state == ExecutionState::Assigned {
                        self.executions
                            .get_mut(&execution_id)
                            .expect("execution was verified above")
                            .mark_timed_out()
                            .map_err(JobManagerError::InvalidExecutionState)?;
                        self.jobs
                            .get_mut(&job_id)
                            .expect("job was verified above")
                            .mark_timed_out()
                            .map_err(JobManagerError::InvalidJobState)?;
                    } else {
                        self.request_execution_job_timeout(execution_id)?;
                    }
                    true
                }
                JobState::Cancelling
                | JobState::TimingOut
                | JobState::Succeeded
                | JobState::Failed
                | JobState::Cancelled
                | JobState::TimedOut
                | JobState::Lost => false,
            };

            self.job_submitted_at.remove(&job_id);
            if timeout_started {
                timed_out.push(job_id);
            }
        }

        Ok(timed_out)
    }

    fn request_execution_job_timeout(
        &mut self,
        execution_id: ExecutionId,
    ) -> Result<(), JobManagerError> {
        let job_id = self.preflight_linked_transition(
            execution_id,
            ExecutionState::Cancelling,
            JobState::TimingOut,
        )?;
        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .request_cancellation()
            .expect("execution transition was preflighted");
        self.jobs
            .get_mut(&job_id)
            .expect("linked job was verified above")
            .request_timeout()
            .expect("job transition was preflighted");
        self.cancellation_requests.insert(execution_id);
        self.job_timeout_requests.insert(execution_id);
        Ok(())
    }

    /// Returns an assignment that still awaits acknowledgement from one node.
    ///
    /// Executions the node already reports as active are skipped, so a node
    /// that has received an assignment but not yet acknowledged it is not
    /// handed the same execution twice.
    pub fn pending_assignment_for(
        &self,
        node_id: NodeId,
        active_execution_ids: &[ExecutionId],
    ) -> Result<Option<ExecutionAssignment>, JobManagerError> {
        let Some(execution) = self.executions.values().find(|execution| {
            execution.node_id() == node_id
                && execution.state() == ExecutionState::Assigned
                && !active_execution_ids.contains(&execution.id())
        }) else {
            return Ok(None);
        };
        let job = self
            .jobs
            .get(&execution.job_id())
            .ok_or(JobManagerError::JobNotFound(execution.job_id()))?;

        Ok(Some(ExecutionAssignment {
            execution_id: execution.id(),
            job_id: job.id(),
            node_id,
            spec: job.spec().clone(),
        }))
    }

    /// Records that the selected node accepted an assignment.
    pub fn accept_execution(&mut self, execution_id: ExecutionId) -> Result<(), JobManagerError> {
        self.executions
            .get_mut(&execution_id)
            .ok_or(JobManagerError::ExecutionNotFound(execution_id))?
            .accept()
            .map_err(JobManagerError::InvalidExecutionState)
    }

    /// Records process start and moves the logical job to Running.
    pub fn start_execution(&mut self, execution_id: ExecutionId) -> Result<(), JobManagerError> {
        let execution = self
            .executions
            .get(&execution_id)
            .ok_or(JobManagerError::ExecutionNotFound(execution_id))?;
        if !execution.state().can_transition_to(ExecutionState::Running) {
            return Err(JobManagerError::InvalidExecutionState(
                InvalidStateTransition::new(execution.state(), ExecutionState::Running),
            ));
        }
        let job_id = execution.job_id();
        let job_state = self
            .jobs
            .get(&job_id)
            .ok_or(JobManagerError::JobNotFound(job_id))?
            .state();
        if job_state != JobState::Cancelling && !job_state.can_transition_to(JobState::Running) {
            return Err(JobManagerError::InvalidJobState(
                InvalidStateTransition::new(job_state, JobState::Running),
            ));
        }

        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .start()
            .expect("execution transition was preflighted");
        if job_state != JobState::Cancelling {
            self.jobs
                .get_mut(&job_id)
                .expect("linked job was verified above")
                .mark_running()
                .expect("job transition was preflighted");
        }

        Ok(())
    }

    /// Records assignment rejection and returns the job to the queue.
    pub fn reject_execution(&mut self, execution_id: ExecutionId) -> Result<(), JobManagerError> {
        let job_id = self.preflight_linked_transition(
            execution_id,
            ExecutionState::Rejected,
            JobState::Queued,
        )?;

        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .reject()
            .expect("execution transition was preflighted");
        self.jobs
            .get_mut(&job_id)
            .expect("linked job was verified above")
            .queue()
            .expect("job transition was preflighted");
        if !self.pending_jobs.contains(&job_id) {
            self.pending_jobs.push_front(job_id);
        }

        Ok(())
    }

    /// Requests cancellation without assuming that the remote process stopped.
    pub fn request_execution_cancellation(
        &mut self,
        execution_id: ExecutionId,
    ) -> Result<(), JobManagerError> {
        let job_id = self.preflight_linked_transition(
            execution_id,
            ExecutionState::Cancelling,
            JobState::Cancelling,
        )?;

        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .request_cancellation()
            .expect("execution transition was preflighted");
        self.jobs
            .get_mut(&job_id)
            .expect("linked job was verified above")
            .request_cancellation()
            .expect("job transition was preflighted");

        Ok(())
    }

    /// Confirms that the node stopped the process after cancellation.
    pub fn confirm_execution_cancellation(
        &mut self,
        execution_id: ExecutionId,
    ) -> Result<(), JobManagerError> {
        self.confirm_execution_cancellation_with_output(execution_id, ExecutionOutput::default())
    }

    pub fn confirm_execution_cancellation_with_output(
        &mut self,
        execution_id: ExecutionId,
        output: ExecutionOutput,
    ) -> Result<(), JobManagerError> {
        let state = self
            .executions
            .get(&execution_id)
            .ok_or(JobManagerError::ExecutionNotFound(execution_id))?
            .state();
        if state != ExecutionState::Cancelling {
            self.executions
                .get_mut(&execution_id)
                .expect("execution was verified above")
                .request_cancellation()
                .map_err(JobManagerError::InvalidExecutionState)?;
        }
        let job_id = self.preflight_linked_transition(
            execution_id,
            ExecutionState::Cancelled,
            JobState::Cancelled,
        )?;

        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .confirm_cancellation()
            .expect("execution transition was preflighted");
        self.jobs
            .get_mut(&job_id)
            .expect("linked job was verified above")
            .confirm_cancellation()
            .expect("job transition was preflighted");
        self.outputs.entry(execution_id).or_insert(output);
        self.cancellation_requests.remove(&execution_id);
        self.job_timeout_requests.remove(&execution_id);
        self.job_submitted_at.remove(&job_id);

        Ok(())
    }

    pub fn confirm_job_timeout_with_output(
        &mut self,
        execution_id: ExecutionId,
        output: ExecutionOutput,
    ) -> Result<(), JobManagerError> {
        let job_id = self.preflight_linked_transition(
            execution_id,
            ExecutionState::TimedOut,
            JobState::TimedOut,
        )?;
        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .mark_timed_out()
            .expect("execution transition was preflighted");
        self.jobs
            .get_mut(&job_id)
            .expect("linked job was verified above")
            .mark_timed_out()
            .expect("job transition was preflighted");
        self.outputs.entry(execution_id).or_insert(output);
        self.cancellation_requests.remove(&execution_id);
        self.job_timeout_requests.remove(&execution_id);
        self.job_submitted_at.remove(&job_id);
        Ok(())
    }

    pub fn mark_execution_timed_out(
        &mut self,
        execution_id: ExecutionId,
        output: ExecutionOutput,
    ) -> Result<(), JobManagerError> {
        let job_id = self.preflight_linked_transition(
            execution_id,
            ExecutionState::TimedOut,
            JobState::Failed,
        )?;
        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .mark_timed_out()
            .expect("execution transition was preflighted");
        self.jobs
            .get_mut(&job_id)
            .expect("linked job was verified above")
            .mark_failed()
            .expect("job transition was preflighted");
        self.outputs.entry(execution_id).or_insert(output);
        self.cancellation_requests.remove(&execution_id);
        self.job_timeout_requests.remove(&execution_id);
        self.job_submitted_at.remove(&job_id);
        Ok(())
    }

    /// Records that the controller can no longer determine process state.
    pub fn mark_execution_lost(
        &mut self,
        execution_id: ExecutionId,
    ) -> Result<(), JobManagerError> {
        let job_id =
            self.preflight_linked_transition(execution_id, ExecutionState::Lost, JobState::Lost)?;

        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .mark_lost()
            .expect("execution transition was preflighted");
        self.jobs
            .get_mut(&job_id)
            .expect("linked job was verified above")
            .mark_lost()
            .expect("job transition was preflighted");
        self.job_submitted_at.remove(&job_id);

        Ok(())
    }

    /// Records process completion and updates the linked logical job.
    pub fn finish_execution(
        &mut self,
        execution_id: ExecutionId,
        result: ExecutionResult,
    ) -> Result<(), JobManagerError> {
        self.finish_execution_with_output(execution_id, result, ExecutionOutput::default())
    }

    /// Records process completion together with its bounded output.
    pub fn finish_execution_with_output(
        &mut self,
        execution_id: ExecutionId,
        result: ExecutionResult,
        output: ExecutionOutput,
    ) -> Result<(), JobManagerError> {
        let execution = self
            .executions
            .get(&execution_id)
            .ok_or(JobManagerError::ExecutionNotFound(execution_id))?;
        let execution_state = if result.exit_code == Some(0) {
            ExecutionState::Succeeded
        } else {
            ExecutionState::Failed
        };
        let job_state = if result.exit_code == Some(0) {
            JobState::Succeeded
        } else {
            JobState::Failed
        };

        if execution.state() == ExecutionState::Assigned {
            return Err(JobManagerError::InvalidExecutionState(
                InvalidStateTransition::new(execution.state(), execution_state),
            ));
        }

        if let Some(recorded) = execution.result()
            && recorded != result
        {
            return Err(JobManagerError::ExecutionCompletion(
                ExecutionCompletionError::ConflictingResult {
                    recorded,
                    received: result,
                },
            ));
        }
        if self
            .outputs
            .get(&execution_id)
            .is_some_and(|recorded| recorded != &output)
        {
            return Err(JobManagerError::ConflictingExecutionOutput(execution_id));
        }

        let job_id = self.preflight_linked_transition(execution_id, execution_state, job_state)?;

        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .finish(result)
            .map_err(JobManagerError::ExecutionCompletion)?;

        let job = self
            .jobs
            .get_mut(&job_id)
            .expect("linked job was verified above");
        if job_state == JobState::Succeeded {
            job.mark_succeeded()
                .expect("job transition was preflighted");
        } else {
            job.mark_failed().expect("job transition was preflighted");
        }
        self.outputs.entry(execution_id).or_insert(output);
        self.cancellation_requests.remove(&execution_id);
        self.job_timeout_requests.remove(&execution_id);
        self.job_submitted_at.remove(&job_id);

        Ok(())
    }

    /// Records that the execution failed because its data could not be moved.
    ///
    /// The reason is kept for the first report only, so a repeated report of
    /// the same failure changes nothing.
    pub fn fail_execution_data(
        &mut self,
        execution_id: ExecutionId,
        failure: DataFailure,
    ) -> Result<(), JobManagerError> {
        self.fail_execution_start(execution_id)?;
        self.data_failures.entry(execution_id).or_insert(failure);
        Ok(())
    }

    /// Records the files a finished execution left for collection.
    pub fn record_output_files(&mut self, execution_id: ExecutionId, files: Vec<OutputFile>) {
        if !files.is_empty() {
            self.output_files.insert(execution_id, files);
            self.outputs_recorded_at
                .entry(execution_id)
                .or_insert_with(Instant::now);
        }
    }

    pub fn data_failure(&self, execution_id: ExecutionId) -> Option<&DataFailure> {
        self.data_failures.get(&execution_id)
    }

    pub fn output_files(&self, execution_id: ExecutionId) -> &[OutputFile] {
        self.output_files
            .get(&execution_id)
            .map_or(&[], Vec::as_slice)
    }

    /// Records that the assigned process could not be started on the node.
    pub fn fail_execution_start(
        &mut self,
        execution_id: ExecutionId,
    ) -> Result<(), JobManagerError> {
        let job_id = self.preflight_linked_transition(
            execution_id,
            ExecutionState::Failed,
            JobState::Failed,
        )?;

        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .finish(ExecutionResult { exit_code: None })
            .map_err(JobManagerError::ExecutionCompletion)?;
        self.jobs
            .get_mut(&job_id)
            .expect("linked job was verified above")
            .mark_failed()
            .expect("job transition was preflighted");
        self.job_submitted_at.remove(&job_id);

        Ok(())
    }

    fn preflight_linked_transition(
        &self,
        execution_id: ExecutionId,
        execution_target: ExecutionState,
        job_target: JobState,
    ) -> Result<JobId, JobManagerError> {
        let execution = self
            .executions
            .get(&execution_id)
            .ok_or(JobManagerError::ExecutionNotFound(execution_id))?;
        if !execution.state().can_transition_to(execution_target) {
            return Err(JobManagerError::InvalidExecutionState(
                InvalidStateTransition::new(execution.state(), execution_target),
            ));
        }

        let job_id = execution.job_id();
        let job = self
            .jobs
            .get(&job_id)
            .ok_or(JobManagerError::JobNotFound(job_id))?;
        if !job.state().can_transition_to(job_target) {
            return Err(JobManagerError::InvalidJobState(
                InvalidStateTransition::new(job.state(), job_target),
            ));
        }

        Ok(job_id)
    }

    fn remove_pending_job(&mut self, job_id: JobId) {
        if let Some(index) = self
            .pending_jobs
            .iter()
            .position(|queued| *queued == job_id)
        {
            self.pending_jobs.remove(index);
        }
    }

    /// Sums the requirements of every execution still holding node resources.
    fn node_allocations(&self) -> BTreeMap<NodeId, NodeAllocation> {
        let mut allocations = BTreeMap::<NodeId, NodeAllocation>::new();
        for execution in self.executions.values().filter(|execution| {
            matches!(
                execution.state(),
                ExecutionState::Assigned
                    | ExecutionState::Accepted
                    | ExecutionState::Running
                    | ExecutionState::Cancelling
            )
        }) {
            if let Some(job) = self.jobs.get(&execution.job_id()) {
                allocations
                    .entry(execution.node_id())
                    .or_default()
                    .reserve(job.spec().requirements);
            }
        }
        allocations
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobManagerError {
    InvalidSpec(JobSpecValidationError),
    JobNotFound(JobId),
    JobHasNoExecution(JobId),
    ExecutionNotFound(ExecutionId),
    JobNotQueued { job_id: JobId, state: JobState },
    InvalidJobState(InvalidStateTransition<JobState>),
    InvalidExecutionState(InvalidStateTransition<ExecutionState>),
    ExecutionCompletion(ExecutionCompletionError),
    ConflictingExecutionOutput(ExecutionId),
    Scheduling(SchedulingFailure),
}

impl fmt::Display for JobManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSpec(error) => error.fmt(formatter),
            Self::JobNotFound(job_id) => write!(formatter, "job {job_id} was not found"),
            Self::JobHasNoExecution(job_id) => {
                write!(formatter, "job {job_id} has no execution")
            }
            Self::ExecutionNotFound(execution_id) => {
                write!(formatter, "execution {execution_id} was not found")
            }
            Self::JobNotQueued { job_id, state } => {
                write!(
                    formatter,
                    "job {job_id} is not queued; current state is {state:?}"
                )
            }
            Self::InvalidJobState(error) => error.fmt(formatter),
            Self::InvalidExecutionState(error) => error.fmt(formatter),
            Self::ExecutionCompletion(error) => error.fmt(formatter),
            Self::ConflictingExecutionOutput(execution_id) => {
                write!(
                    formatter,
                    "execution {execution_id} reported conflicting output"
                )
            }
            Self::Scheduling(error) => error.fmt(formatter),
        }
    }
}

impl Error for JobManagerError {}

#[cfg(test)]
mod tests {
    use meld_core::{
        CapturedStream, NodeDescriptor, NodeId, ResourceCapacity, ResourceRequirements,
        ResourceSnapshot,
    };

    use super::*;

    #[test]
    fn submit_queues_a_valid_job() {
        let mut manager = JobManager::new();

        let job_id = manager.submit(spec()).expect("valid job should be queued");

        assert_eq!(manager.job(job_id).map(Job::state), Some(JobState::Queued));
    }

    #[test]
    fn schedule_creates_execution_for_selected_node() {
        let mut manager = JobManager::new();
        let job_id = manager.submit(spec()).expect("valid job should be queued");
        let (registry, node_id) = ready_registry();

        let execution_id = manager
            .schedule(job_id, &Scheduler::new(), &registry)
            .expect("ready node should receive execution");

        let execution = manager
            .execution(execution_id)
            .expect("execution should be stored");
        assert_eq!(execution.job_id(), job_id);
        assert_eq!(execution.node_id(), node_id);
        assert_eq!(
            manager.job(job_id).map(Job::state),
            Some(JobState::Assigned)
        );
    }

    #[test]
    fn assigned_execution_is_exposed_until_acknowledged() {
        let mut manager = JobManager::new();
        let expected_spec = spec();
        let job_id = manager
            .submit(expected_spec.clone())
            .expect("valid job should be queued");
        let (registry, node_id) = ready_registry();
        let execution_id = manager
            .schedule_next(&Scheduler::new(), &registry)
            .expect("queued job should be schedulable")
            .expect("one job should be queued");

        let assignment = manager
            .pending_assignment_for(node_id, &[])
            .expect("linked job should exist")
            .expect("assigned execution should be pending");

        assert_eq!(assignment.execution_id, execution_id);
        assert_eq!(assignment.job_id, job_id);
        assert_eq!(assignment.node_id, node_id);
        assert_eq!(assignment.spec, expected_spec);

        manager
            .accept_execution(execution_id)
            .expect("assignment acknowledgement should be accepted");
        assert_eq!(
            manager
                .pending_assignment_for(node_id, &[])
                .expect("linked job should still exist"),
            None
        );
    }

    #[test]
    fn scheduling_failure_leaves_job_queued() {
        let mut manager = JobManager::new();
        let job_id = manager.submit(spec()).expect("valid job should be queued");

        let error = manager
            .schedule(job_id, &Scheduler::new(), &NodeRegistry::new())
            .expect_err("job cannot be scheduled without ready nodes");

        assert_eq!(
            error,
            JobManagerError::Scheduling(SchedulingFailure::NoReadyNodes)
        );
        assert_eq!(manager.job(job_id).map(Job::state), Some(JobState::Queued));
    }

    #[test]
    fn active_node_does_not_receive_another_execution() {
        let mut manager = JobManager::new();
        let first_job_id = manager.submit(spec()).expect("first job should be queued");
        let (registry, _) = ready_registry();
        manager
            .schedule(first_job_id, &Scheduler::new(), &registry)
            .expect("first job should be assigned");
        let second_job_id = manager.submit(spec()).expect("second job should be queued");

        let error = manager
            .schedule_next(&Scheduler::new(), &registry)
            .expect_err("active node must not receive another execution");

        assert_eq!(
            error,
            JobManagerError::Scheduling(SchedulingFailure::NoAvailableNodes)
        );
        assert_eq!(
            manager.job(second_job_id).map(Job::state),
            Some(JobState::Queued)
        );
    }

    #[test]
    fn executions_share_a_node_until_reservations_fill_its_capacity() {
        let scheduler = Scheduler::new();
        let (registry, node_id) = ready_registry_with_limit(8);
        let mut manager = JobManager::new();
        let mut half = spec();
        half.requirements.logical_cpus = 4;
        half.requirements.memory_bytes = 8_000;
        for _ in 0..2 {
            manager.submit(half.clone()).expect("job should be queued");
        }
        let third = manager.submit(half).expect("job should be queued");

        for _ in 0..2 {
            let execution_id = manager
                .schedule_next(&scheduler, &registry)
                .expect("reservation should fit")
                .expect("job should be pending");
            assert_eq!(
                manager.execution(execution_id).map(Execution::node_id),
                Some(node_id)
            );
        }
        let error = manager
            .schedule_next(&scheduler, &registry)
            .expect_err("node capacity is fully reserved");

        assert_eq!(
            error,
            JobManagerError::Scheduling(SchedulingFailure::NoAvailableNodes)
        );
        assert_eq!(manager.job(third).map(Job::state), Some(JobState::Queued));
    }

    #[test]
    fn schedule_next_selects_jobs_in_submission_order() {
        let mut manager = JobManager::new();
        let first_job_id = manager
            .submit(spec())
            .expect("first valid job should be queued");
        let mut second_spec = spec();
        second_spec.program = "cargo".to_owned();
        let second_job_id = manager
            .submit(second_spec)
            .expect("second valid job should be queued");
        let (registry, _) = ready_registry();

        let execution_id = manager
            .schedule_next(&Scheduler::new(), &registry)
            .expect("oldest queued job should be schedulable")
            .expect("one job should be queued");

        assert_eq!(
            manager.execution(execution_id).map(Execution::job_id),
            Some(first_job_id)
        );
        assert_eq!(
            manager.job(second_job_id).map(Job::state),
            Some(JobState::Queued)
        );
    }

    #[test]
    fn head_job_no_node_can_ever_take_is_overtaken() {
        let mut manager = JobManager::new();
        let mut oversized_spec = spec();
        oversized_spec.requirements.logical_cpus = 9;
        let oversized_job_id = manager
            .submit(oversized_spec)
            .expect("oversized job should still be queued");
        let mut gpu_spec = spec();
        gpu_spec.constraints.capabilities = vec!["gpu".to_owned()];
        let gpu_job_id = manager
            .submit(gpu_spec)
            .expect("constrained job should be queued");
        let runnable_job_id = manager
            .submit(spec())
            .expect("runnable job should be queued");
        let (registry, _) = ready_registry();

        let execution_id = manager
            .schedule_next(&Scheduler::new(), &registry)
            .expect("runnable job should not be held back")
            .expect("a job should have been scheduled");

        assert_eq!(
            manager.execution(execution_id).map(Execution::job_id),
            Some(runnable_job_id)
        );
        for blocked in [oversized_job_id, gpu_job_id] {
            assert_eq!(manager.job(blocked).map(Job::state), Some(JobState::Queued));
        }
    }

    #[test]
    fn queue_of_only_unplaceable_jobs_reports_the_first_blocker() {
        let mut manager = JobManager::new();
        let mut oversized_spec = spec();
        oversized_spec.requirements.logical_cpus = 9;
        manager
            .submit(oversized_spec)
            .expect("oversized job should still be queued");
        let mut gpu_spec = spec();
        gpu_spec.constraints.capabilities = vec!["gpu".to_owned()];
        manager
            .submit(gpu_spec)
            .expect("constrained job should be queued");
        let (registry, _) = ready_registry();

        let error = manager
            .schedule_next(&Scheduler::new(), &registry)
            .expect_err("nothing in the queue can be placed");

        assert_eq!(
            error,
            JobManagerError::Scheduling(SchedulingFailure::InsufficientResources)
        );
    }

    #[test]
    fn job_waiting_for_busy_capacity_is_not_overtaken() {
        let mut manager = JobManager::new();
        let mut large = spec();
        large.requirements.logical_cpus = 8;
        let small = spec();
        let (registry, _) = ready_registry_with_limit(8);
        let scheduler = Scheduler::new();
        let running_id = manager
            .submit(spec())
            .expect("running job should be queued");
        manager
            .schedule(running_id, &scheduler, &registry)
            .expect("first job should be assigned");
        let large_id = manager.submit(large).expect("large job should be queued");
        let small_id = manager.submit(small).expect("small job should be queued");

        let error = manager
            .schedule_next(&scheduler, &registry)
            .expect_err("the large job must keep its place in line");

        assert_eq!(
            error,
            JobManagerError::Scheduling(SchedulingFailure::NoAvailableNodes)
        );
        assert_eq!(
            manager.job(large_id).map(Job::state),
            Some(JobState::Queued)
        );
        assert_eq!(
            manager.job(small_id).map(Job::state),
            Some(JobState::Queued)
        );
        assert_eq!(
            manager.is_behind_earlier_job(small_id, &scheduler, &registry),
            Ok(true)
        );
        assert_eq!(
            manager.is_behind_earlier_job(large_id, &scheduler, &registry),
            Ok(false)
        );
    }

    #[test]
    fn schedule_next_returns_none_when_queue_is_empty() {
        let mut manager = JobManager::new();

        let execution_id = manager
            .schedule_next(&Scheduler::new(), &NodeRegistry::new())
            .expect("an empty queue is not a scheduling failure");

        assert_eq!(execution_id, None);
    }

    #[test]
    fn same_job_is_not_assigned_twice() {
        let mut manager = JobManager::new();
        let job_id = manager.submit(spec()).expect("valid job should be queued");
        let (registry, _) = ready_registry();
        manager
            .schedule(job_id, &Scheduler::new(), &registry)
            .expect("first assignment should succeed");

        let error = manager
            .schedule(job_id, &Scheduler::new(), &registry)
            .expect_err("assigned job must not get another execution");

        assert_eq!(
            error,
            JobManagerError::JobNotQueued {
                job_id,
                state: JobState::Assigned,
            }
        );
    }

    #[test]
    fn successful_execution_completes_linked_job() {
        let (mut manager, job_id, execution_id) = assigned_job();

        manager
            .accept_execution(execution_id)
            .expect("assignment should be accepted");
        manager
            .start_execution(execution_id)
            .expect("execution should start");
        manager
            .finish_execution(execution_id, ExecutionResult { exit_code: Some(0) })
            .expect("execution should finish");

        assert_eq!(
            manager.execution(execution_id).map(Execution::state),
            Some(ExecutionState::Succeeded)
        );
        assert_eq!(
            manager.job(job_id).map(Job::state),
            Some(JobState::Succeeded)
        );
    }

    #[test]
    fn finished_execution_stores_output_for_latest_attempt() {
        let (mut manager, job_id, execution_id) = assigned_job();
        manager
            .accept_execution(execution_id)
            .expect("assignment should be accepted");
        manager
            .start_execution(execution_id)
            .expect("execution should start");
        let output = ExecutionOutput {
            stdout: CapturedStream {
                content: "done\n".to_owned(),
                truncated: false,
                lossy: false,
            },
            stderr: CapturedStream::default(),
        };

        manager
            .finish_execution_with_output(
                execution_id,
                ExecutionResult { exit_code: Some(0) },
                output.clone(),
            )
            .expect("execution output should be recorded");

        assert_eq!(
            manager.latest_execution_for_job(job_id).map(Execution::id),
            Some(execution_id)
        );
        assert_eq!(manager.execution_output(execution_id), Some(&output));
    }

    #[test]
    fn failed_execution_fails_linked_job() {
        let (mut manager, job_id, execution_id) = assigned_job();
        manager
            .accept_execution(execution_id)
            .expect("assignment should be accepted");
        manager
            .start_execution(execution_id)
            .expect("execution should start");

        manager
            .finish_execution(execution_id, ExecutionResult { exit_code: Some(1) })
            .expect("failed result should be recorded");

        assert_eq!(
            manager.execution(execution_id).map(Execution::state),
            Some(ExecutionState::Failed)
        );
        assert_eq!(manager.job(job_id).map(Job::state), Some(JobState::Failed));
    }

    #[test]
    fn process_start_failure_fails_job_without_requeueing() {
        let (mut manager, job_id, execution_id) = assigned_job();

        manager
            .fail_execution_start(execution_id)
            .expect("process start failure should be recorded");

        assert_eq!(
            manager.execution(execution_id).map(Execution::state),
            Some(ExecutionState::Failed)
        );
        assert_eq!(manager.job(job_id).map(Job::state), Some(JobState::Failed));
        assert_eq!(
            manager
                .schedule_next(&Scheduler::new(), &NodeRegistry::new())
                .expect("failed job must not remain queued"),
            None
        );
    }

    #[test]
    fn ordinary_finished_event_cannot_skip_process_start() {
        let (mut manager, _, execution_id) = assigned_job();

        let error = manager
            .finish_execution(execution_id, ExecutionResult { exit_code: Some(1) })
            .expect_err("ordinary completion must not be accepted before process start");

        assert!(matches!(error, JobManagerError::InvalidExecutionState(_)));
    }

    #[test]
    fn rejected_execution_returns_job_to_queue() {
        let (mut manager, job_id, execution_id) = assigned_job();

        manager
            .reject_execution(execution_id)
            .expect("assignment rejection should be recorded");

        assert_eq!(
            manager.execution(execution_id).map(Execution::state),
            Some(ExecutionState::Rejected)
        );
        assert_eq!(manager.job(job_id).map(Job::state), Some(JobState::Queued));
    }

    #[test]
    fn queued_job_timeout_removes_job_from_fifo_queue() {
        let submitted_at = Instant::now();
        let mut manager = JobManager::new();
        let mut timed_spec = spec();
        timed_spec.job_timeout_secs = Some(5);
        let job_id = manager
            .submit_at(timed_spec, submitted_at)
            .expect("valid job should be queued");

        assert!(
            manager
                .expire_jobs_at(submitted_at + Duration::from_secs(4))
                .expect("timeout scan should succeed")
                .is_empty()
        );
        assert_eq!(
            manager
                .expire_jobs_at(submitted_at + Duration::from_secs(5))
                .expect("timeout scan should succeed"),
            vec![job_id]
        );
        assert_eq!(
            manager.job(job_id).map(Job::state),
            Some(JobState::TimedOut)
        );
        assert_eq!(manager.pending_position(job_id), None);
    }

    #[test]
    fn unacknowledged_assignment_times_out_without_remote_confirmation() {
        let submitted_at = Instant::now();
        let mut manager = JobManager::new();
        let mut timed_spec = spec();
        timed_spec.job_timeout_secs = Some(5);
        let job_id = manager
            .submit_at(timed_spec, submitted_at)
            .expect("valid job should be queued");
        let (registry, _) = ready_registry();
        let execution_id = manager
            .schedule(job_id, &Scheduler::new(), &registry)
            .expect("job should be assigned");

        manager
            .expire_jobs_at(submitted_at + Duration::from_secs(5))
            .expect("timeout scan should succeed");

        assert_eq!(
            manager.job(job_id).map(Job::state),
            Some(JobState::TimedOut)
        );
        assert_eq!(
            manager.execution(execution_id).map(Execution::state),
            Some(ExecutionState::TimedOut)
        );
        assert!(!manager.job_timeout_requested(execution_id));
    }

    #[test]
    fn running_job_timeout_waits_for_node_and_preserves_partial_output() {
        let submitted_at = Instant::now();
        let mut manager = JobManager::new();
        let mut timed_spec = spec();
        timed_spec.job_timeout_secs = Some(5);
        let job_id = manager
            .submit_at(timed_spec, submitted_at)
            .expect("valid job should be queued");
        let (registry, node_id) = ready_registry();
        let execution_id = manager
            .schedule(job_id, &Scheduler::new(), &registry)
            .expect("job should be assigned");
        manager
            .accept_execution(execution_id)
            .expect("assignment should be accepted");
        manager
            .start_execution(execution_id)
            .expect("execution should start");

        manager
            .expire_jobs_at(submitted_at + Duration::from_secs(5))
            .expect("timeout scan should succeed");
        assert_eq!(
            manager.job(job_id).map(Job::state),
            Some(JobState::TimingOut)
        );
        assert_eq!(
            manager.execution(execution_id).map(Execution::state),
            Some(ExecutionState::Cancelling)
        );
        assert!(manager.cancellation_requested(execution_id, node_id));
        assert!(manager.job_timeout_requested(execution_id));

        let output = ExecutionOutput {
            stdout: CapturedStream {
                content: "before job timeout\n".to_owned(),
                truncated: false,
                lossy: false,
            },
            stderr: CapturedStream::default(),
        };
        manager
            .confirm_job_timeout_with_output(execution_id, output.clone())
            .expect("node confirmation should complete timeout");

        assert_eq!(
            manager.job(job_id).map(Job::state),
            Some(JobState::TimedOut)
        );
        assert_eq!(
            manager.execution(execution_id).map(Execution::state),
            Some(ExecutionState::TimedOut)
        );
        assert_eq!(manager.execution_output(execution_id), Some(&output));
    }

    #[test]
    fn cancellation_waits_for_node_confirmation() {
        let (mut manager, job_id, execution_id) = assigned_job();
        manager
            .accept_execution(execution_id)
            .expect("assignment should be accepted");
        manager
            .start_execution(execution_id)
            .expect("execution should start");

        manager
            .request_execution_cancellation(execution_id)
            .expect("cancellation request should be recorded");
        assert_eq!(
            manager.job(job_id).map(Job::state),
            Some(JobState::Cancelling)
        );

        manager
            .confirm_execution_cancellation(execution_id)
            .expect("cancellation confirmation should be recorded");
        assert_eq!(
            manager.execution(execution_id).map(Execution::state),
            Some(ExecutionState::Cancelled)
        );
        assert_eq!(
            manager.job(job_id).map(Job::state),
            Some(JobState::Cancelled)
        );
    }

    #[test]
    fn lost_execution_marks_linked_job_lost() {
        let (mut manager, job_id, execution_id) = assigned_job();
        manager
            .accept_execution(execution_id)
            .expect("assignment should be accepted");
        manager
            .start_execution(execution_id)
            .expect("execution should start");

        manager
            .mark_execution_lost(execution_id)
            .expect("lost execution should be recorded");

        assert_eq!(
            manager.execution(execution_id).map(Execution::state),
            Some(ExecutionState::Lost)
        );
        assert_eq!(manager.job(job_id).map(Job::state), Some(JobState::Lost));
    }

    fn assigned_job() -> (JobManager, JobId, ExecutionId) {
        let mut manager = JobManager::new();
        let job_id = manager.submit(spec()).expect("valid job should be queued");
        let (registry, _) = ready_registry();
        let execution_id = manager
            .schedule(job_id, &Scheduler::new(), &registry)
            .expect("ready node should receive execution");
        (manager, job_id, execution_id)
    }

    fn output_file(path: &str, fill: char) -> OutputFile {
        OutputFile {
            path: path.to_owned(),
            sha256: fill.to_string().repeat(64).parse().expect("valid digest"),
            size_bytes: 1,
        }
    }

    #[test]
    fn recent_outputs_are_pinned_until_retention_ends() {
        let mut manager = JobManager::new();
        let execution_id = ExecutionId::generate();
        let file = output_file("result.json", 'a');
        manager.record_output_files(execution_id, vec![file.clone()]);
        let retention = Duration::from_secs(3600);
        let now = Instant::now();

        let within = manager.pinned_digests(now, retention);
        let after = manager.pinned_digests(now + retention + Duration::from_secs(1), retention);

        assert!(within.contains(&file.sha256));
        assert!(
            after.is_empty(),
            "retention is a minimum, not a promise forever"
        );
    }

    #[test]
    fn recording_outputs_again_does_not_extend_retention() {
        let mut manager = JobManager::new();
        let execution_id = ExecutionId::generate();
        let file = output_file("result.json", 'b');
        manager.record_output_files(execution_id, vec![file.clone()]);
        let first = manager.outputs_recorded_at[&execution_id];

        manager.record_output_files(execution_id, vec![file]);

        assert_eq!(manager.outputs_recorded_at[&execution_id], first);
    }

    #[test]
    fn inputs_are_pinned_only_while_their_job_is_unfinished() {
        let mut manager = JobManager::new();
        let mut spec = spec();
        let digest: Sha256Digest = "c".repeat(64).parse().expect("valid digest");
        spec.data.inputs = vec![meld_core::InputFile {
            path: "in.txt".to_owned(),
            sha256: digest.clone(),
            size_bytes: 1,
            executable: false,
        }];
        let job_id = manager.submit(spec).expect("job should be queued");
        let now = Instant::now();

        assert!(
            manager
                .pinned_digests(now, Duration::ZERO)
                .contains(&digest)
        );

        manager
            .request_job_cancellation(job_id)
            .expect("queued job should be cancellable");
        assert!(manager.pinned_digests(now, Duration::ZERO).is_empty());
    }

    fn spec() -> JobSpec {
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

    fn ready_registry() -> (NodeRegistry, NodeId) {
        ready_registry_with_limit(1)
    }

    fn ready_registry_with_limit(max_concurrent_executions: u32) -> (NodeRegistry, NodeId) {
        let node_id = NodeId::generate();
        let mut registry = NodeRegistry::new();
        registry.register(NodeDescriptor {
            id: node_id,
            hostname: "worker".to_owned(),
            operating_system: "windows".to_owned(),
            architecture: "x86_64".to_owned(),
            capacity: ResourceCapacity {
                logical_cpus: 8,
                memory_bytes: 16_000_000_000,
                max_concurrent_executions,
            },
            capabilities: vec![],
        });
        registry
            .record_heartbeat(
                node_id,
                ResourceSnapshot {
                    cpu_usage_percent: 10,
                    available_memory_bytes: 12_000_000_000,
                    running_executions: 0,
                },
            )
            .expect("registered node should accept heartbeat");
        (registry, node_id)
    }
}
