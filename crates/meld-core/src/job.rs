//! User-provided job specifications.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

use crate::{
    DataSpec, DataSpecError, InvalidStateTransition, JobId, NodeDescriptor, ResourceRequirements,
};

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
    /// Maximum time from controller submission until terminal completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_timeout_secs: Option<u64>,
    /// Maximum process runtime after the node starts execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_timeout_secs: Option<u64>,
    /// Restrictions on which nodes may run the job.
    #[serde(default, skip_serializing_if = "PlacementConstraints::is_empty")]
    pub constraints: PlacementConstraints,
    /// Files moved to the node before the run and collected after it.
    #[serde(default, skip_serializing_if = "DataSpec::is_empty")]
    pub data: DataSpec,
}

/// Node properties a job requires beyond CPU and memory.
///
/// Every field that is set must match; an empty value accepts any node.
/// Comparison ignores ASCII case.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementConstraints {
    /// Required operating system, as reported by the Rust target (`linux`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operating_system: Option<String>,
    /// Required CPU architecture, as reported by the Rust target (`aarch64`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub architecture: Option<String>,
    /// Labels the node must advertise, such as `gpu`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

impl PlacementConstraints {
    pub fn is_empty(&self) -> bool {
        self.operating_system.is_none()
            && self.architecture.is_none()
            && self.capabilities.is_empty()
    }

    /// Rejects blank values, which could never be matched intentionally.
    pub fn validate(&self) -> Result<(), JobSpecValidationError> {
        let blank = |value: &str| value.trim().is_empty();
        if self.operating_system.as_deref().is_some_and(blank)
            || self.architecture.as_deref().is_some_and(blank)
            || self.capabilities.iter().any(|label| blank(label))
        {
            return Err(JobSpecValidationError::BlankConstraint);
        }
        Ok(())
    }

    /// Returns whether the node's static properties satisfy every constraint.
    pub fn is_satisfied_by(&self, node: &NodeDescriptor) -> bool {
        let matches = |required: &Option<String>, actual: &str| {
            required
                .as_deref()
                .is_none_or(|required| required.trim().eq_ignore_ascii_case(actual))
        };

        matches(&self.operating_system, &node.operating_system)
            && matches(&self.architecture, &node.architecture)
            && self.capabilities.iter().all(|required| {
                node.capabilities
                    .iter()
                    .any(|offered| offered.eq_ignore_ascii_case(required.trim()))
            })
    }
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
        if self.job_timeout_secs == Some(0) {
            return Err(JobSpecValidationError::ZeroJobTimeout);
        }
        if self.execution_timeout_secs == Some(0) {
            return Err(JobSpecValidationError::ZeroExecutionTimeout);
        }
        self.constraints.validate()?;
        self.data.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobSpecValidationError {
    InvalidData(DataSpecError),
    EmptyProgram,
    ZeroLogicalCpus,
    ZeroMemory,
    ZeroJobTimeout,
    ZeroExecutionTimeout,
    BlankConstraint,
}

impl fmt::Display for JobSpecValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidData(error) => error.fmt(formatter),
            Self::EmptyProgram => formatter.write_str("job program must not be empty"),
            Self::ZeroLogicalCpus => {
                formatter.write_str("job must request at least one logical CPU")
            }
            Self::ZeroMemory => formatter.write_str("job must request at least one byte of memory"),
            Self::ZeroJobTimeout => formatter.write_str("job timeout must be greater than zero"),
            Self::ZeroExecutionTimeout => {
                formatter.write_str("execution timeout must be greater than zero")
            }
            Self::BlankConstraint => {
                formatter.write_str("placement constraints must not contain blank values")
            }
        }
    }
}

impl From<DataSpecError> for JobSpecValidationError {
    fn from(error: DataSpecError) -> Self {
        Self::InvalidData(error)
    }
}

impl Error for JobSpecValidationError {}

/// A logical unit of work managed by the controller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
            JobState::TimingOut
            | JobState::Succeeded
            | JobState::Failed
            | JobState::TimedOut
            | JobState::Lost => JobState::Cancelling,
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

    pub fn request_timeout(&mut self) -> Result<(), InvalidStateTransition<JobState>> {
        self.state.transition_to(JobState::TimingOut)
    }

    pub fn mark_timed_out(&mut self) -> Result<(), InvalidStateTransition<JobState>> {
        self.state.transition_to(JobState::TimedOut)
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
    TimingOut,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
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
                        Self::Queued
                            | Self::Running
                            | Self::Failed
                            | Self::Cancelling
                            | Self::TimingOut
                            | Self::TimedOut
                            | Self::Lost
                    )
                    | (
                        Self::Running,
                        Self::Succeeded
                            | Self::Failed
                            | Self::Cancelling
                            | Self::TimingOut
                            | Self::Lost
                    )
                    | (
                        Self::Cancelling,
                        Self::Succeeded | Self::Failed | Self::Cancelled | Self::Lost
                    )
                    | (Self::Submitted | Self::Queued, Self::TimedOut)
                    | (Self::TimingOut, Self::TimedOut | Self::Lost)
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
    use crate::{NodeId, ResourceCapacity};

    use super::*;

    fn node(operating_system: &str, architecture: &str, capabilities: &[&str]) -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId::generate(),
            hostname: "worker".to_owned(),
            operating_system: operating_system.to_owned(),
            architecture: architecture.to_owned(),
            capacity: ResourceCapacity {
                logical_cpus: 8,
                memory_bytes: 16_000,
                max_concurrent_executions: 8,
            },
            capabilities: capabilities
                .iter()
                .map(|label| (*label).to_owned())
                .collect(),
        }
    }

    #[test]
    fn empty_constraints_accept_any_node() {
        let constraints = PlacementConstraints::default();

        assert!(constraints.is_empty());
        assert!(constraints.is_satisfied_by(&node("linux", "x86_64", &[])));
    }

    #[test]
    fn every_set_constraint_must_match_ignoring_case() {
        let constraints = PlacementConstraints {
            operating_system: Some("Linux".to_owned()),
            architecture: Some("aarch64".to_owned()),
            capabilities: vec!["GPU".to_owned(), "docker".to_owned()],
        };

        assert!(constraints.is_satisfied_by(&node("linux", "aarch64", &["docker", "gpu"])));
        assert!(!constraints.is_satisfied_by(&node("macos", "aarch64", &["docker", "gpu"])));
        assert!(!constraints.is_satisfied_by(&node("linux", "x86_64", &["docker", "gpu"])));
        assert!(!constraints.is_satisfied_by(&node("linux", "aarch64", &["gpu"])));
    }

    #[test]
    fn blank_constraint_values_are_rejected() {
        for constraints in [
            PlacementConstraints {
                operating_system: Some(" ".to_owned()),
                ..PlacementConstraints::default()
            },
            PlacementConstraints {
                architecture: Some(String::new()),
                ..PlacementConstraints::default()
            },
            PlacementConstraints {
                capabilities: vec!["gpu".to_owned(), "  ".to_owned()],
                ..PlacementConstraints::default()
            },
        ] {
            assert_eq!(
                constraints.validate(),
                Err(JobSpecValidationError::BlankConstraint)
            );
        }
    }

    #[test]
    fn job_spec_without_constraints_deserializes_and_omits_them_when_empty() {
        let json =
            r#"{"program":"rustc","args":[],"requirements":{"logical_cpus":1,"memory_bytes":1}}"#;

        let spec: JobSpec = serde_json::from_str(json).expect("older specs should deserialize");

        assert!(spec.constraints.is_empty());
        assert!(
            !serde_json::to_string(&spec)
                .expect("spec should serialize")
                .contains("constraints")
        );
    }

    fn spec_with_data(data: DataSpec) -> JobSpec {
        JobSpec {
            program: "python3".to_owned(),
            args: Vec::new(),
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 1,
            },
            job_timeout_secs: None,
            execution_timeout_secs: None,
            constraints: PlacementConstraints::default(),
            data,
        }
    }

    #[test]
    fn job_spec_data_round_trips_and_is_omitted_when_empty() {
        let data = DataSpec {
            inputs: vec![crate::InputFile {
                path: "data.csv".to_owned(),
                sha256: "a".repeat(64).parse().expect("valid digest"),
                size_bytes: 3,
                executable: true,
            }],
            outputs: vec![crate::OutputSpec {
                path: "result.json".to_owned(),
            }],
        };
        let spec = spec_with_data(data);

        let json = serde_json::to_string(&spec).expect("spec should serialize");
        let deserialized: JobSpec = serde_json::from_str(&json).expect("spec should deserialize");

        assert_eq!(deserialized, spec);
        assert!(
            !serde_json::to_string(&spec_with_data(DataSpec::default()))
                .expect("spec should serialize")
                .contains("data")
        );
    }

    #[test]
    fn job_with_unsafe_data_path_is_rejected() {
        let error = Job::new(spec_with_data(DataSpec {
            inputs: vec![],
            outputs: vec![crate::OutputSpec {
                path: "../escape".to_owned(),
            }],
        }))
        .expect_err("path traversal must be rejected");

        assert!(matches!(error, JobSpecValidationError::InvalidData(_)));
    }

    #[test]
    fn job_spec_preserves_argument_boundaries_in_json() {
        let spec = JobSpec {
            program: "rustc".to_owned(),
            args: vec!["--version".to_owned(), "argument with spaces".to_owned()],
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 256_000_000,
            },
            job_timeout_secs: None,
            execution_timeout_secs: None,
            constraints: PlacementConstraints::default(),
            data: DataSpec::default(),
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
            job_timeout_secs: None,
            execution_timeout_secs: None,
            constraints: PlacementConstraints::default(),
            data: DataSpec::default(),
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
            job_timeout_secs: None,
            execution_timeout_secs: None,
            constraints: PlacementConstraints::default(),
            data: DataSpec::default(),
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
            job_timeout_secs: None,
            execution_timeout_secs: None,
            constraints: PlacementConstraints::default(),
            data: DataSpec::default(),
        })
        .expect_err("blank program must be rejected");

        assert_eq!(error, JobSpecValidationError::EmptyProgram);
    }

    #[test]
    fn zero_execution_timeout_is_rejected() {
        let error = Job::new(JobSpec {
            program: "rustc".to_owned(),
            args: Vec::new(),
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 1,
            },
            job_timeout_secs: None,
            execution_timeout_secs: Some(0),
            constraints: PlacementConstraints::default(),
            data: DataSpec::default(),
        })
        .expect_err("zero execution timeout must be rejected");

        assert_eq!(error, JobSpecValidationError::ZeroExecutionTimeout);
    }

    #[test]
    fn zero_job_timeout_is_rejected() {
        let error = Job::new(JobSpec {
            program: "rustc".to_owned(),
            args: Vec::new(),
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 1,
            },
            job_timeout_secs: Some(0),
            execution_timeout_secs: None,
            constraints: PlacementConstraints::default(),
            data: DataSpec::default(),
        })
        .expect_err("zero job timeout must be rejected");

        assert_eq!(error, JobSpecValidationError::ZeroJobTimeout);
    }
}
