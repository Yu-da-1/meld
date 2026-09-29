//! Errors shared by domain state machines.

use std::{error::Error, fmt};

/// Describes an attempted transition that is not allowed by a state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidStateTransition<S> {
    pub from: S,
    pub to: S,
}

impl<S> InvalidStateTransition<S> {
    pub const fn new(from: S, to: S) -> Self {
        Self { from, to }
    }
}

impl<S: fmt::Debug> fmt::Display for InvalidStateTransition<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid state transition from {:?} to {:?}",
            self.from, self.to
        )
    }
}

impl<S: fmt::Debug> Error for InvalidStateTransition<S> {}
