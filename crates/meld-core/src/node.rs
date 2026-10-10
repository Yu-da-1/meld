//! Node identity and static capabilities.

use serde::{Deserialize, Serialize};

use crate::{NodeId, ResourceCapacity};

/// Whether the operator has let a node take part in the cluster.
///
/// This is a decision, not an observation, and is kept apart from whether
/// the node is alive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Admission {
    /// Known to the controller, but waiting for an operator to approve it.
    Pending,
    Approved,
    /// Removed by an operator. It may be approved again.
    Revoked,
}

/// Controller-observed availability of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    /// Not yet approved by an operator. It receives no work.
    Pending,
    /// Revoked by an operator. It receives no work and is not listened to.
    Revoked,
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
    /// Operator-assigned labels (for example `gpu`) that jobs may require.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
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
                max_concurrent_executions: 8,
            },
            capabilities: vec![],
        };

        let json = serde_json::to_string(&descriptor).expect("node descriptor should serialize");
        let deserialized = serde_json::from_str(&json).expect("node descriptor should deserialize");

        assert_eq!(descriptor, deserialized);
    }
}
