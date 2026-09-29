//! Node identity and static capabilities.

use serde::{Deserialize, Serialize};

use crate::{NodeId, ResourceCapacity};

/// Controller-observed availability of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    /// Registered but not yet confirmed by a heartbeat.
    Joining,
    Ready,
    /// Finishing existing work without accepting new executions.
    Draining,
    /// No heartbeat was observed within the configured timeout.
    Unreachable,
}

/// Describes a node independently of its current liveness or resource usage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    /// Stable identity reused when the node reconnects.
    pub id: NodeId,
    pub hostname: String,
    /// Operating system name reported by the Rust target.
    pub operating_system: String,
    /// CPU architecture reported by the Rust target.
    pub architecture: String,
    pub capacity: ResourceCapacity,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_descriptor_round_trips_through_json() {
        let descriptor = NodeDescriptor {
            id: NodeId::generate(),
            hostname: "worker-1".to_owned(),
            operating_system: "windows".to_owned(),
            architecture: "x86_64".to_owned(),
            capacity: ResourceCapacity {
                logical_cpus: 8,
                memory_bytes: 16_000,
            },
        };

        let json = serde_json::to_string(&descriptor).expect("node descriptor should serialize");
        let deserialized = serde_json::from_str(&json).expect("node descriptor should deserialize");

        assert_eq!(descriptor, deserialized);
    }
}
