//! Resource capacity, requirements, and observed usage.

use serde::{Deserialize, Serialize};

/// Static resources provided by a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceCapacity {
    /// Number of logical processors available to Meld.
    pub logical_cpus: u32,
    /// Total memory capacity in bytes.
    pub memory_bytes: u64,
    /// Upper bound on executions the node runs at the same time.
    pub max_concurrent_executions: u32,
}

impl ResourceCapacity {
    /// Returns whether this capacity meets all requested resources.
    pub const fn satisfies(self, requirements: ResourceRequirements) -> bool {
        self.logical_cpus >= requirements.logical_cpus
            && self.memory_bytes >= requirements.memory_bytes
    }
}

/// Minimum resources required to run a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRequirements {
    /// Minimum number of logical processors.
    pub logical_cpus: u32,
    /// Minimum memory in bytes.
    pub memory_bytes: u64,
}

/// Dynamic resource usage observed on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    /// Whole-node CPU utilization from 0 through 100.
    pub cpu_usage_percent: u8,
    /// Memory currently available in bytes.
    pub available_memory_bytes: u64,
    /// Number of executions currently managed by Meld.
    pub running_executions: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAPACITY: ResourceCapacity = ResourceCapacity {
        logical_cpus: 8,
        memory_bytes: 16_000,
        max_concurrent_executions: 8,
    };

    #[test]
    fn capacity_satisfies_equal_requirements() {
        let requirements = ResourceRequirements {
            logical_cpus: 8,
            memory_bytes: 16_000,
        };

        assert!(CAPACITY.satisfies(requirements));
    }

    #[test]
    fn capacity_rejects_insufficient_cpu() {
        let requirements = ResourceRequirements {
            logical_cpus: 9,
            memory_bytes: 16_000,
        };

        assert!(!CAPACITY.satisfies(requirements));
    }

    #[test]
    fn capacity_rejects_insufficient_memory() {
        let requirements = ResourceRequirements {
            logical_cpus: 8,
            memory_bytes: 16_001,
        };

        assert!(!CAPACITY.satisfies(requirements));
    }

    #[test]
    fn resources_round_trip_through_json() {
        let requirements = ResourceRequirements {
            logical_cpus: 4,
            memory_bytes: 8_000,
        };

        let json = serde_json::to_string(&requirements).expect("resources should serialize");
        let deserialized = serde_json::from_str(&json).expect("resources should deserialize");

        assert_eq!(requirements, deserialized);
    }
}
