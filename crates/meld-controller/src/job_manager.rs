//! In-memory ownership of logical jobs and execution attempts.

use std::{collections::BTreeMap, error::Error, fmt};

use meld_core::{
    Execution, ExecutionCompletionError, ExecutionId, ExecutionResult, ExecutionState,
    InvalidStateTransition, Job, JobId, JobSpec, JobSpecValidationError, JobState,
};

use crate::{
    node_registry::NodeRegistry,
    scheduler::{Scheduler, SchedulingFailure},
};

/// Owns controller-authoritative Job and Execution records.
#[derive(Debug, Default)]
pub struct JobManager {
    jobs: BTreeMap<JobId, Job>,
    executions: BTreeMap<ExecutionId, Execution>,
}

impl JobManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and queues a newly submitted job.
    pub fn submit(&mut self, spec: JobSpec) -> Result<JobId, JobManagerError> {
        let mut job = Job::new(spec).map_err(JobManagerError::InvalidSpec)?;
        job.queue().map_err(JobManagerError::InvalidJobState)?;
        let job_id = job.id();
        self.jobs.insert(job_id, job);
        Ok(job_id)
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

        let node_id = scheduler
            .select_node(job.spec().requirements, registry)
            .map_err(JobManagerError::Scheduling)?;

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

        Ok(execution_id)
    }

    pub fn job(&self, job_id: JobId) -> Option<&Job> {
        self.jobs.get(&job_id)
    }

    pub fn execution(&self, execution_id: ExecutionId) -> Option<&Execution> {
        self.executions.get(&execution_id)
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
        let job_id = self.preflight_linked_transition(
            execution_id,
            ExecutionState::Running,
            JobState::Running,
        )?;

        self.executions
            .get_mut(&execution_id)
            .expect("execution was verified above")
            .start()
            .expect("execution transition was preflighted");
        self.jobs
            .get_mut(&job_id)
            .expect("linked job was verified above")
            .mark_running()
            .expect("job transition was preflighted");

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

        Ok(())
    }

    /// Records process completion and updates the linked logical job.
    pub fn finish_execution(
        &mut self,
        execution_id: ExecutionId,
        result: ExecutionResult,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobManagerError {
    InvalidSpec(JobSpecValidationError),
    JobNotFound(JobId),
    ExecutionNotFound(ExecutionId),
    JobNotQueued { job_id: JobId, state: JobState },
    InvalidJobState(InvalidStateTransition<JobState>),
    InvalidExecutionState(InvalidStateTransition<ExecutionState>),
    ExecutionCompletion(ExecutionCompletionError),
    Scheduling(SchedulingFailure),
}

impl fmt::Display for JobManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSpec(error) => error.fmt(formatter),
            Self::JobNotFound(job_id) => write!(formatter, "job {job_id} was not found"),
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
            Self::Scheduling(error) => error.fmt(formatter),
        }
    }
}

impl Error for JobManagerError {}

#[cfg(test)]
mod tests {
    use meld_core::{
        NodeDescriptor, NodeId, ResourceCapacity, ResourceRequirements, ResourceSnapshot,
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

    fn spec() -> JobSpec {
        JobSpec {
            program: "rustc".to_owned(),
            args: vec!["--version".to_owned()],
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 256_000_000,
            },
        }
    }

    fn ready_registry() -> (NodeRegistry, NodeId) {
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
            },
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
