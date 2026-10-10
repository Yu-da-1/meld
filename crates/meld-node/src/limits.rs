//! Watching how much memory an execution's processes hold.
//!
//! The node does not ask the operating system to enforce a limit, which
//! differs between systems and often needs privileges. It measures instead,
//! and stops the execution when it has used more than it reserved. That is the
//! same on every system, at the price of noticing a moment after the fact.

use std::{collections::HashMap, time::Duration};

use meld_core::{LimitBreach, memory_limit_bytes};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// How often the processes of a running execution are measured.
pub const MEMORY_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// What an execution may use, and how closely it is watched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryWatch {
    reserved_bytes: u64,
    limit_bytes: u64,
    interval: Duration,
}

impl MemoryWatch {
    /// The job's reservation, with the usual margin above it.
    pub fn for_reservation(reserved_bytes: u64, interval: Duration) -> Self {
        Self {
            reserved_bytes,
            limit_bytes: memory_limit_bytes(reserved_bytes),
            interval,
        }
    }

    pub const fn interval(&self) -> Duration {
        self.interval
    }

    /// The breach, if `used_bytes` is over what the job may use.
    pub fn breach(&self, used_bytes: u64) -> Option<LimitBreach> {
        (used_bytes > self.limit_bytes).then_some(LimitBreach::Memory {
            reserved_bytes: self.reserved_bytes,
            limit_bytes: self.limit_bytes,
            used_bytes,
        })
    }
}

/// Measures the memory of a process and the processes it started.
#[derive(Debug, Default)]
pub struct MemoryMeter {
    system: System,
}

impl MemoryMeter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resident memory of `root` and all its descendants together, in bytes.
    /// `None` once `root` is gone.
    ///
    /// Memory shared between processes is counted in each, so a job that
    /// forks many workers can look larger than it is. The margin above the
    /// reservation absorbs a little of this, not all of it.
    pub fn tree_bytes(&mut self, root: u32) -> Option<u64> {
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().with_memory(),
        );
        sum_tree(
            Pid::from_u32(root),
            self.system
                .processes()
                .iter()
                .map(|(pid, process)| (*pid, process.parent(), process.memory())),
        )
    }
}

/// The memory of `root` and everything below it in `processes`, given as
/// `(process, parent, bytes)`. `None` if `root` is not among them.
fn sum_tree(root: Pid, processes: impl Iterator<Item = (Pid, Option<Pid>, u64)>) -> Option<u64> {
    let mut memory = HashMap::new();
    let mut children: HashMap<Pid, Vec<Pid>> = HashMap::new();
    for (pid, parent, bytes) in processes {
        memory.insert(pid, bytes);
        if let Some(parent) = parent {
            children.entry(parent).or_default().push(pid);
        }
    }

    let mut total = *memory.get(&root)?;
    // A process is visited once even if the data were to loop.
    let mut seen = vec![root];
    let mut pending = vec![root];
    while let Some(pid) = pending.pop() {
        for child in children.get(&pid).into_iter().flatten() {
            if !seen.contains(child) {
                seen.push(*child);
                pending.push(*child);
                total = total.saturating_add(memory.get(child).copied().unwrap_or(0));
            }
        }
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(value: u32) -> Pid {
        Pid::from_u32(value)
    }

    #[test]
    fn a_process_and_all_its_descendants_are_counted_and_nothing_else() {
        let processes = vec![
            (pid(1), None, 1_000_000),      // somebody else's
            (pid(10), Some(pid(1)), 100),   // the job
            (pid(11), Some(pid(10)), 20),   // its child
            (pid(12), Some(pid(11)), 3),    // its grandchild
            (pid(20), Some(pid(1)), 5_000), // a sibling, not the job's
        ];

        assert_eq!(sum_tree(pid(10), processes.into_iter()), Some(123));
    }

    #[test]
    fn a_process_that_is_gone_has_no_memory() {
        let processes = vec![(pid(1), None, 10)];

        assert_eq!(sum_tree(pid(99), processes.into_iter()), None);
    }

    #[test]
    fn a_cycle_in_the_data_does_not_loop_forever() {
        let processes = vec![(pid(10), Some(pid(11)), 1), (pid(11), Some(pid(10)), 2)];

        assert_eq!(sum_tree(pid(10), processes.into_iter()), Some(3));
    }

    #[test]
    fn a_watch_reports_a_breach_only_above_the_limit() {
        let watch = MemoryWatch::for_reservation(1_000, MEMORY_CHECK_INTERVAL);

        assert_eq!(watch.breach(1_100), None);
        assert_eq!(
            watch.breach(1_101),
            Some(LimitBreach::Memory {
                reserved_bytes: 1_000,
                limit_bytes: 1_100,
                used_bytes: 1_101,
            })
        );
    }

    #[test]
    fn the_meter_sees_this_process() {
        let mut meter = MemoryMeter::new();

        let bytes = meter
            .tree_bytes(std::process::id())
            .expect("the running test process is measured");

        assert!(bytes > 0);
    }
}
