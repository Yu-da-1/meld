//! In-memory source of truth for registered nodes.

use std::{collections::BTreeMap, error::Error, fmt};

use meld_core::{NodeDescriptor, NodeId, NodeState, ResourceSnapshot};

/// Controller-owned view of one registered node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredNode {
    descriptor: NodeDescriptor,
    state: NodeState,
    snapshot: Option<ResourceSnapshot>,
}

impl RegisteredNode {
    pub const fn descriptor(&self) -> &NodeDescriptor {
        &self.descriptor
    }

    pub const fn state(&self) -> NodeState {
        self.state
    }

    pub const fn snapshot(&self) -> Option<ResourceSnapshot> {
        self.snapshot
    }
}

/// Stores the latest controller-observed state for every node.
#[derive(Debug, Default)]
pub struct NodeRegistry {
    nodes: BTreeMap<NodeId, RegisteredNode>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a node as joining, replacing stale data for the same identity.
    pub fn register(&mut self, descriptor: NodeDescriptor) {
        self.nodes.insert(
            descriptor.id,
            RegisteredNode {
                descriptor,
                state: NodeState::Joining,
                snapshot: None,
            },
        );
    }

    /// Records a heartbeat and makes the node available for scheduling.
    pub fn record_heartbeat(
        &mut self,
        node_id: NodeId,
        snapshot: ResourceSnapshot,
    ) -> Result<(), NodeRegistryError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(NodeRegistryError::NodeNotFound(node_id))?;

        node.state = NodeState::Ready;
        node.snapshot = Some(snapshot);
        Ok(())
    }

    pub fn get(&self, node_id: NodeId) -> Option<&RegisteredNode> {
        self.nodes.get(&node_id)
    }

    /// Iterates in Node ID order to keep scheduling deterministic.
    pub fn nodes(&self) -> impl Iterator<Item = &RegisteredNode> {
        self.nodes.values()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRegistryError {
    NodeNotFound(NodeId),
}

impl fmt::Display for NodeRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeNotFound(node_id) => write!(formatter, "node {node_id} is not registered"),
        }
    }
}

impl Error for NodeRegistryError {}

#[cfg(test)]
mod tests {
    use meld_core::ResourceCapacity;

    use super::*;

    #[test]
    fn registered_node_becomes_ready_after_heartbeat() {
        let descriptor = descriptor();
        let node_id = descriptor.id;
        let mut registry = NodeRegistry::new();
        registry.register(descriptor);

        assert_eq!(
            registry.get(node_id).map(RegisteredNode::state),
            Some(NodeState::Joining)
        );

        registry
            .record_heartbeat(node_id, snapshot())
            .expect("registered node should accept heartbeat");

        let node = registry
            .get(node_id)
            .expect("node should remain registered");
        assert_eq!(node.state(), NodeState::Ready);
        assert_eq!(node.snapshot(), Some(snapshot()));
    }

    #[test]
    fn heartbeat_from_unknown_node_is_rejected() {
        let mut registry = NodeRegistry::new();
        let node_id = NodeId::generate();

        let error = registry
            .record_heartbeat(node_id, snapshot())
            .expect_err("unknown node must be rejected");

        assert_eq!(error, NodeRegistryError::NodeNotFound(node_id));
    }

    fn descriptor() -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId::generate(),
            hostname: "worker-1".to_owned(),
            operating_system: "windows".to_owned(),
            architecture: "x86_64".to_owned(),
            capacity: ResourceCapacity {
                logical_cpus: 8,
                memory_bytes: 16_000,
            },
        }
    }

    fn snapshot() -> ResourceSnapshot {
        ResourceSnapshot {
            cpu_usage_percent: 10,
            available_memory_bytes: 12_000,
            running_executions: 0,
        }
    }
}
