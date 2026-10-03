//! State and result of one physical job execution attempt.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

use crate::{ExecutionId, InvalidStateTransition, JobId, NodeId};

/// One physical attempt to run a job on a selected node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Execution {
    id: ExecutionId,
    job_id: JobId,
    node_id: NodeId,
    state: ExecutionState,
    result: Option<ExecutionResult>,
}

impl Execution {
    /// Creates an assigned execution with a newly generated identity.
    pub fn new(job_id: JobId, node_id: NodeId) -> Self {
        Self {
            id: ExecutionId::generate(),
            job_id,
            node_id,
            state: ExecutionState::Assigned,
            result: None,
        }
    }

    pub const fn id(&self) -> ExecutionId {
        self.id
    }

    pub const fn job_id(&self) -> JobId {
        self.job_id
    }

    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub const fn state(&self) -> ExecutionState {
        self.state
    }

    pub const fn result(&self) -> Option<ExecutionResult> {
        self.result
    }

    pub fn accept(&mut self) -> Result<(), InvalidStateTransition<ExecutionState>> {
        self.state.transition_to(ExecutionState::Accepted)
    }

    pub fn start(&mut self) -> Result<(), InvalidStateTransition<ExecutionState>> {
        self.state.transition_to(ExecutionState::Running)
    }

    pub fn request_cancellation(&mut self) -> Result<(), InvalidStateTransition<ExecutionState>> {
        self.state.transition_to(ExecutionState::Cancelling)
    }

    pub fn confirm_cancellation(&mut self) -> Result<(), InvalidStateTransition<ExecutionState>> {
        self.state.transition_to(ExecutionState::Cancelled)
    }

    pub fn reject(&mut self) -> Result<(), InvalidStateTransition<ExecutionState>> {
        self.state.transition_to(ExecutionState::Rejected)
    }

    pub fn mark_lost(&mut self) -> Result<(), InvalidStateTransition<ExecutionState>> {
        self.state.transition_to(ExecutionState::Lost)
    }

    pub fn mark_timed_out(&mut self) -> Result<(), InvalidStateTransition<ExecutionState>> {
        self.state.transition_to(ExecutionState::TimedOut)?;
        self.result = Some(ExecutionResult { exit_code: None });
        Ok(())
    }

    /// Records process completion and derives the terminal state from its exit code.
    pub fn finish(&mut self, result: ExecutionResult) -> Result<(), ExecutionCompletionError> {
        if let Some(recorded) = self.result
            && recorded != result
        {
            return Err(ExecutionCompletionError::ConflictingResult {
                recorded,
                received: result,
            });
        }

        let next = if result.exit_code == Some(0) {
            ExecutionState::Succeeded
        } else {
            ExecutionState::Failed
        };

        self.state
            .transition_to(next)
            .map_err(ExecutionCompletionError::InvalidTransition)?;
        self.result = Some(result);

        Ok(())
    }
}

/// Lifecycle state of one execution attempt on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    Assigned,
    Accepted,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Rejected,
    /// The controller can no longer determine whether the process is running.
    Lost,
}

impl ExecutionState {
    /// Returns whether moving to `next` is valid.
    ///
    /// Repeating the current state is accepted to make duplicate events
    /// idempotent.
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (
                    Self::Assigned,
                    Self::Accepted
                        | Self::Failed
                        | Self::Cancelling
                        | Self::TimedOut
                        | Self::Rejected
                        | Self::Lost
                ) | (
                    Self::Accepted,
                    Self::Running | Self::Failed | Self::Cancelling | Self::Lost
                ) | (
                    Self::Running,
                    Self::Succeeded | Self::Failed | Self::Cancelling | Self::TimedOut | Self::Lost
                ) | (
                    Self::Cancelling,
                    Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut | Self::Lost
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

/// Process result reported by a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionResult {
    /// Process exit code, or `None` when no code is available.
    pub exit_code: Option<i32>,
}

/// Text captured from one process output stream.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedStream {
    pub content: String,
    pub truncated: bool,
    pub lossy: bool,
}

/// Bounded stdout and stderr captured for one execution.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionOutput {
    pub stdout: CapturedStream,
    pub stderr: CapturedStream,
}

/// Failure to apply a process completion report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionCompletionError {
    InvalidTransition(InvalidStateTransition<ExecutionState>),
    ConflictingResult {
        recorded: ExecutionResult,
        received: ExecutionResult,
    },
}

impl fmt::Display for ExecutionCompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTransition(error) => error.fmt(formatter),
            Self::ConflictingResult { recorded, received } => write!(
                formatter,
                "conflicting execution result: recorded {recorded:?}, received {received:?}"
            ),
        }
    }
}

impl Error for ExecutionCompletionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_can_complete_successfully() {
        let mut state = ExecutionState::Assigned;

        for next in [
            ExecutionState::Accepted,
            ExecutionState::Running,
            ExecutionState::Succeeded,
        ] {
            state
                .transition_to(next)
                .expect("transition should succeed");
        }

        assert_eq!(state, ExecutionState::Succeeded);
    }

    #[test]
    fn rejected_execution_is_terminal() {
        let mut state = ExecutionState::Assigned;

        state
            .transition_to(ExecutionState::Rejected)
            .expect("assigned execution can be rejected");
        let error = state
            .transition_to(ExecutionState::Accepted)
            .expect_err("rejected execution must remain terminal");

        assert_eq!(state, ExecutionState::Rejected);
        assert_eq!(
            error,
            InvalidStateTransition::new(ExecutionState::Rejected, ExecutionState::Accepted)
        );
    }

    #[test]
    fn cancellation_can_race_with_process_completion() {
        let mut state = ExecutionState::Running;

        state
            .transition_to(ExecutionState::Cancelling)
            .expect("cancellation request should be accepted");
        state
            .transition_to(ExecutionState::Succeeded)
            .expect("process may finish before cancellation");

        assert_eq!(state, ExecutionState::Succeeded);
    }

    #[test]
    fn duplicate_state_event_is_idempotent() {
        let mut state = ExecutionState::Running;

        state
            .transition_to(ExecutionState::Running)
            .expect("duplicate event should be accepted");

        assert_eq!(state, ExecutionState::Running);
    }

    #[test]
    fn execution_links_job_to_selected_node() {
        let job_id = JobId::generate();
        let node_id = NodeId::generate();

        let execution = Execution::new(job_id, node_id);

        assert_eq!(execution.job_id(), job_id);
        assert_eq!(execution.node_id(), node_id);
        assert_eq!(execution.state(), ExecutionState::Assigned);
        assert_eq!(execution.result(), None);
    }

    #[test]
    fn zero_exit_code_completes_execution_successfully() {
        let mut execution = running_execution();
        let result = ExecutionResult { exit_code: Some(0) };

        execution
            .finish(result)
            .expect("successful result should be accepted");

        assert_eq!(execution.state(), ExecutionState::Succeeded);
        assert_eq!(execution.result(), Some(result));
    }

    #[test]
    fn nonzero_exit_code_fails_execution() {
        let mut execution = running_execution();
        let result = ExecutionResult { exit_code: Some(1) };

        execution
            .finish(result)
            .expect("failed result should be accepted");

        assert_eq!(execution.state(), ExecutionState::Failed);
        assert_eq!(execution.result(), Some(result));
    }

    #[test]
    fn duplicate_result_is_idempotent_but_conflicting_result_is_rejected() {
        let mut execution = running_execution();
        let successful = ExecutionResult { exit_code: Some(0) };
        execution
            .finish(successful)
            .expect("initial result should be accepted");

        execution
            .finish(successful)
            .expect("duplicate result should be accepted");
        let error = execution
            .finish(ExecutionResult { exit_code: Some(1) })
            .expect_err("conflicting result should be rejected");

        assert_eq!(
            error,
            ExecutionCompletionError::ConflictingResult {
                recorded: successful,
                received: ExecutionResult { exit_code: Some(1) },
            }
        );
        assert_eq!(execution.result(), Some(successful));
    }

    fn running_execution() -> Execution {
        let mut execution = Execution::new(JobId::generate(), NodeId::generate());
        execution.accept().expect("execution should be accepted");
        execution.start().expect("execution should start");
        execution
    }
}
