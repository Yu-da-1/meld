//! User-provided job specifications.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

use crate::{InvalidStateTransition, JobId, ResourceRequirements};

/// Defines a program invocation and the resources it requires.
///
/// The program is executed directly. Arguments are kept separate so neither
/// a Unix shell nor a Windows command interpreter needs to parse the command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSpec {
    /// Executable name or path.
    pub program: String,
    /// Arguments passed to the executable in their original boundaries.
    pub args: Vec<String>,
    pub requirements: ResourceRequirements,
}

impl JobSpec {
    /// Rejects values that cannot describe a schedulable process.
    pub fn validate(&self) -> Result<(), JobSpecValidationError> {
        if self.program.trim().is_empty() {
            return Err(JobSpecValidationError::EmptyProgram);
        }
        if self.requirements.logical_cpus == 0 {
            return Err(JobSpecValidationError::ZeroLogicalCpus);
        }
        if self.requirements.memory_bytes == 0 {
            return Err(JobSpecValidationError::ZeroMemory);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobSpecValidationError {
    EmptyProgram,
    ZeroLogicalCpus,
    ZeroMemory,
}

impl fmt::Display for JobSpecValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyProgram => formatter.write_str("job program must not be empty"),
            Self::ZeroLogicalCpus => {
                formatter.write_str("job must request at least one logical CPU")
            }
            Self::ZeroMemory => formatter.write_str("job must request at least one byte of memory"),
        }
    }
}

impl Error for JobSpecValidationError {}

/// A logical unit of work managed by the controller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Job {
    id: JobId,
    spec: JobSpec,
    state: JobState,
}

impl Job {
    /// Creates a submitted job with a newly generated identity.
    pub fn new(spec: JobSpec) -> Result<Self, JobSpecValidationError> {
        spec.validate()?;
        Ok(Self {
            id: JobId::generate(),
            spec,
            state: JobState::Submitted,
        })
    }

    pub const fn id(&self) -> JobId {
        self.id
    }

    pub const fn spec(&self) -> &JobSpec {
        &self.spec
    }

    pub const fn state(&self) -> JobState {
        self.state
    }

    pub fn queue(&mut self) -> Result<(), InvalidStateTransition<JobState>> {
        self.state.transition_to(JobState::Queued)
    }

    pub fn assign(&mut self) -> Result<(), InvalidStateTransition<JobState>> {
        self.state.transition_to(JobState::Assigned)
    }

    pub fn mark_running(&mut self) -> Result<(), InvalidStateTransition<JobState>> {
        self.state.transition_to(JobState::Running)
    }

    /// Records a cancellation request without assuming a remote process stopped.
    pub fn request_cancellation(&mut self) -> Result<(), InvalidStateTransition<JobState>> {
        let next = match self.state {
            JobState::Submitted | JobState::Queued | JobState::Cancelled => JobState::Cancelled,
            JobState::Assigned | JobState::Running | JobState::Cancelling => JobState::Cancelling,
            JobState::Succeeded | JobState::Failed | JobState::Lost => JobState::Cancelling,
        };

        self.state.transition_to(next)
    }

    pub fn confirm_cancellation(&mut self) -> Result<(), InvalidStateTransition<JobState>> {
        self.state.transition_to(JobState::Cancelled)
    }

    pub fn mark_succeeded(&mut self) -> Result<(), InvalidStateTransition<JobState>> {
        self.state.transition_to(JobState::Succeeded)
    }

    pub fn mark_failed(&mut self) -> Result<(), InvalidStateTransition<JobState>> {
        self.state.transition_to(JobState::Failed)
    }

    pub fn mark_lost(&mut self) -> Result<(), InvalidStateTransition<JobState>> {
        self.state.transition_to(JobState::Lost)
    }
}

/// Lifecycle state of a logical job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Submitted,
    Queued,
    Assigned,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    /// The controller can no longer determine whether the process is running.
    Lost,
}

impl JobState {
    /// Returns whether moving to `next` is valid.
    ///
    /// Repeating the current state is accepted to make duplicate events
    /// idempotent.
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Submitted, Self::Queued | Self::Cancelled)
                    | (Self::Queued, Self::Assigned | Self::Cancelled)
                    | (
                        Self::Assigned,
                        Self::Queued | Self::Running | Self::Cancelling | Self::Lost
                    )
                    | (
                        Self::Running,
                        Self::Succeeded | Self::Failed | Self::Cancelling | Self::Lost
                    )
                    | (
                        Self::Cancelling,
                        Self::Succeeded | Self::Failed | Self::Cancelled | Self::Lost
                    )
            )
    }

    /// Applies a valid transition without changing state on failure.
    pub fn transition_to(&mut self, next: Self) -> Result<(), InvalidStateTransition<Self>> {
        if self.can_transition_to(next) {
            *self = next;
            Ok(())
        } else {
            Err(InvalidStateTransition::new(*self, next))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_spec_preserves_argument_boundaries_in_json() {
        let spec = JobSpec {
            program: "rustc".to_owned(),
            args: vec!["--version".to_owned(), "argument with spaces".to_owned()],
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 256_000_000,
            },
        };

        let json = serde_json::to_string(&spec).expect("job spec should serialize");
        let deserialized = serde_json::from_str(&json).expect("job spec should deserialize");

        assert_eq!(spec, deserialized);
        assert_eq!(spec.args.len(), 2);
        assert_eq!(spec.args[1], "argument with spaces");
    }

    #[test]
    fn job_can_complete_successfully() {
        let mut state = JobState::Submitted;

        for next in [
            JobState::Queued,
            JobState::Assigned,
            JobState::Running,
            JobState::Succeeded,
        ] {
            state
                .transition_to(next)
                .expect("transition should succeed");
        }

        assert_eq!(state, JobState::Succeeded);
    }

    #[test]
    fn running_job_waits_for_cancellation_confirmation() {
        let mut state = JobState::Running;

        state
            .transition_to(JobState::Cancelling)
            .expect("cancellation request should be accepted");
        state
            .transition_to(JobState::Cancelled)
            .expect("cancellation confirmation should be accepted");

        assert_eq!(state, JobState::Cancelled);
    }

    #[test]
    fn invalid_job_transition_preserves_current_state() {
        let mut state = JobState::Submitted;

        let error = state
            .transition_to(JobState::Running)
            .expect_err("submitted job cannot run directly");

        assert_eq!(state, JobState::Submitted);
        assert_eq!(
            error,
            InvalidStateTransition::new(JobState::Submitted, JobState::Running)
        );
    }

    #[test]
    fn new_job_starts_submitted() {
        let spec = JobSpec {
            program: "rustc".to_owned(),
            args: vec!["--version".to_owned()],
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 256_000_000,
            },
        };

        let job = Job::new(spec.clone()).expect("valid spec should create a job");

        assert_eq!(job.spec(), &spec);
        assert_eq!(job.state(), JobState::Submitted);
    }

    #[test]
    fn queued_job_can_be_cancelled_without_remote_confirmation() {
        let mut job = Job::new(JobSpec {
            program: "rustc".to_owned(),
            args: Vec::new(),
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 256_000_000,
            },
        })
        .expect("valid spec should create a job");
        job.queue().expect("job should be queued");

        job.request_cancellation()
            .expect("queued job should be cancelled");

        assert_eq!(job.state(), JobState::Cancelled);
    }

    #[test]
    fn invalid_job_spec_is_rejected() {
        let error = Job::new(JobSpec {
            program: "  ".to_owned(),
            args: Vec::new(),
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 1,
            },
        })
        .expect_err("blank program must be rejected");

        assert_eq!(error, JobSpecValidationError::EmptyProgram);
    }
}
