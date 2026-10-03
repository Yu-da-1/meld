//! Deterministic node placement policy.

use std::{collections::BTreeSet, error::Error, fmt};

use meld_core::{NodeId, NodeState, ResourceRequirements};

use crate::node_registry::NodeRegistry;

/// Selects the first suitable node in deterministic Node ID order.
#[derive(Debug, Default)]
pub struct Scheduler;

impl Scheduler {
    pub const fn new() -> Self {
        Self
    }

    pub fn select_node(
        &self,
        requirements: ResourceRequirements,
        registry: &NodeRegistry,
        unavailable_nodes: &BTreeSet<NodeId>,
    ) -> Result<NodeId, SchedulingFailure> {
        let mut ready_nodes = registry
            .nodes()
            .filter(|node| node.state() == NodeState::Ready)
            .peekable();

        if ready_nodes.peek().is_none() {
            return Err(SchedulingFailure::NoReadyNodes);
        }

        let mut matching_nodes = ready_nodes
            .filter(|node| node.descriptor().capacity.satisfies(requirements))
            .peekable();
        if matching_nodes.peek().is_none() {
            return Err(SchedulingFailure::InsufficientResources);
        }

        matching_nodes
            .find(|node| !unavailable_nodes.contains(&node.descriptor().id))
            .map(|node| node.descriptor().id)
            .ok_or(SchedulingFailure::NoAvailableNodes)
    }
}

/// Explains why no node could be selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingFailure {
    NoReadyNodes,
    InsufficientResources,
    NoAvailableNodes,
}

impl fmt::Display for SchedulingFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoReadyNodes => formatter.write_str("no nodes are ready"),
            Self::InsufficientResources => {
                formatter.write_str("ready nodes do not satisfy the resource requirements")
            }
            Self::NoAvailableNodes => {
                formatter.write_str("matching nodes are already running an execution")
            }
        }
    }
}

impl Error for SchedulingFailure {}

#[cfg(test)]
mod tests {
    use meld_core::{NodeDescriptor, ResourceCapacity, ResourceSnapshot};

    use super::*;

    #[test]
    fn selects_a_ready_node_with_sufficient_capacity() {
        let descriptor = descriptor(8, 16_000);
        let expected = descriptor.id;
        let registry = ready_registry(descriptor);

        let selected = Scheduler::new()
            .select_node(requirements(4, 8_000), &registry, &BTreeSet::new())
            .expect("matching node should be selected");

        assert_eq!(selected, expected);
    }

    #[test]
    fn joining_nodes_are_not_selected() {
        let mut registry = NodeRegistry::new();
        registry.register(descriptor(8, 16_000));

        let error = Scheduler::new()
            .select_node(requirements(1, 1_000), &registry, &BTreeSet::new())
            .expect_err("joining node must not be selected");

        assert_eq!(error, SchedulingFailure::NoReadyNodes);
    }

    #[test]
    fn reports_when_ready_nodes_lack_capacity() {
        let registry = ready_registry(descriptor(2, 4_000));

        let error = Scheduler::new()
            .select_node(requirements(4, 8_000), &registry, &BTreeSet::new())
            .expect_err("undersized node must not be selected");

        assert_eq!(error, SchedulingFailure::InsufficientResources);
    }

    #[test]
    fn reports_when_all_matching_nodes_are_unavailable() {
        let descriptor = descriptor(8, 16_000);
        let node_id = descriptor.id;
        let registry = ready_registry(descriptor);

        let error = Scheduler::new()
            .select_node(
                requirements(1, 1_000),
                &registry,
                &BTreeSet::from([node_id]),
            )
            .expect_err("busy node must not receive another execution");

        assert_eq!(error, SchedulingFailure::NoAvailableNodes);
    }

    #[test]
    fn selection_is_deterministic_regardless_of_registration_order() {
        let lower_id = "00000000-0000-0000-0000-000000000001"
            .parse()
            .expect("fixed Node ID should be valid");
        let higher_id = "00000000-0000-0000-0000-000000000002"
            .parse()
            .expect("fixed Node ID should be valid");
        let mut registry = NodeRegistry::new();

        for node_id in [higher_id, lower_id] {
            let mut node = descriptor(8, 16_000);
            node.id = node_id;
            registry.register(node);
            registry
                .record_heartbeat(
                    node_id,
                    ResourceSnapshot {
                        cpu_usage_percent: 0,
                        available_memory_bytes: 16_000,
                        running_executions: 0,
                    },
                )
                .expect("registered node should accept heartbeat");
        }

        let selected = Scheduler::new()
            .select_node(requirements(1, 1_000), &registry, &BTreeSet::new())
            .expect("matching node should be selected");

        assert_eq!(selected, lower_id);
    }

    fn ready_registry(descriptor: NodeDescriptor) -> NodeRegistry {
        let node_id = descriptor.id;
        let mut registry = NodeRegistry::new();
        registry.register(descriptor);
        registry
            .record_heartbeat(
                node_id,
                ResourceSnapshot {
                    cpu_usage_percent: 10,
                    available_memory_bytes: 12_000,
                    running_executions: 0,
                },
            )
            .expect("registered node should accept heartbeat");
        registry
    }

    fn descriptor(logical_cpus: u32, memory_bytes: u64) -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId::generate(),
            hostname: "worker".to_owned(),
            operating_system: "windows".to_owned(),
            architecture: "x86_64".to_owned(),
            capacity: ResourceCapacity {
                logical_cpus,
                memory_bytes,
            },
        }
    }

    const fn requirements(logical_cpus: u32, memory_bytes: u64) -> ResourceRequirements {
        ResourceRequirements {
            logical_cpus,
            memory_bytes,
        }
    }
}
