//! Deterministic node placement policy.

use std::{collections::BTreeMap, error::Error, fmt};

use meld_core::{
    LimitedResource, NodeAssessment, NodeDescriptor, NodeId, NodeState, NodeVerdict,
    PlacementConstraints, ResourceRequirements,
};

use crate::node_registry::NodeRegistry;

/// Resources already reserved on one node by active executions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeAllocation {
    pub logical_cpus: u32,
    pub memory_bytes: u64,
    pub executions: u32,
}

impl NodeAllocation {
    /// Adds one execution's requirements to the reserved total.
    pub fn reserve(&mut self, requirements: ResourceRequirements) {
        self.logical_cpus = self.logical_cpus.saturating_add(requirements.logical_cpus);
        self.memory_bytes = self.memory_bytes.saturating_add(requirements.memory_bytes);
        self.executions = self.executions.saturating_add(1);
    }
}

/// Chooses the least-loaded node that can take a job.
///
/// Load is judged on reservations, not live usage: the highest of the CPU and
/// memory fractions reserved on a node once the job is added. Ties go to the
/// lowest Node ID so a decision is a pure function of its inputs.
#[derive(Debug, Default)]
pub struct Scheduler;

/// Every node's verdict for one job, plus the node chosen if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    pub selected: Option<NodeId>,
    /// In Node ID order.
    pub assessments: Vec<NodeAssessment>,
}

impl Placement {
    /// Collapses the per-node verdicts into the first filter that emptied the
    /// candidate set: liveness, then fixed properties, then total capacity,
    /// then free capacity.
    pub fn failure(&self) -> Option<SchedulingFailure> {
        if self.selected.is_some() {
            return None;
        }
        // Each stage keeps the nodes that got past the previous ones.
        let ready = |verdict: &NodeVerdict| !matches!(verdict, NodeVerdict::NotReady { .. });
        let allowed = |verdict: &NodeVerdict| {
            ready(verdict) && !matches!(verdict, NodeVerdict::ConstraintsNotSatisfied)
        };
        let large_enough = |verdict: &NodeVerdict| {
            allowed(verdict) && !matches!(verdict, NodeVerdict::InsufficientCapacity)
        };
        let any = |keep: &dyn Fn(&NodeVerdict) -> bool| {
            self.assessments
                .iter()
                .any(|assessment| keep(&assessment.verdict))
        };

        Some(if !any(&ready) {
            SchedulingFailure::NoReadyNodes
        } else if !any(&allowed) {
            SchedulingFailure::ConstraintsNotSatisfied
        } else if !any(&large_enough) {
            SchedulingFailure::InsufficientResources
        } else {
            SchedulingFailure::NoAvailableNodes
        })
    }
}

impl Scheduler {
    pub const fn new() -> Self {
        Self
    }

    pub fn select_node(
        &self,
        requirements: ResourceRequirements,
        constraints: &PlacementConstraints,
        registry: &NodeRegistry,
        allocations: &BTreeMap<NodeId, NodeAllocation>,
    ) -> Result<NodeId, SchedulingFailure> {
        let placement = self.place(requirements, constraints, registry, allocations);
        match (placement.selected, placement.failure()) {
            (Some(node_id), _) => Ok(node_id),
            (None, Some(failure)) => Err(failure),
            (None, None) => unreachable!("an unselected placement always has a failure"),
        }
    }

    /// Judges every node in Node ID order and picks the least-loaded eligible one.
    pub fn place(
        &self,
        requirements: ResourceRequirements,
        constraints: &PlacementConstraints,
        registry: &NodeRegistry,
        allocations: &BTreeMap<NodeId, NodeAllocation>,
    ) -> Placement {
        let mut assessments: Vec<NodeAssessment> = registry
            .nodes()
            .map(|node| {
                let descriptor = node.descriptor();
                let allocation = allocations.get(&descriptor.id).copied().unwrap_or_default();
                NodeAssessment {
                    node_id: descriptor.id,
                    hostname: descriptor.hostname.clone(),
                    verdict: Self::judge(
                        node.state(),
                        descriptor,
                        allocation,
                        requirements,
                        constraints,
                    ),
                }
            })
            .collect();

        // Strict `<` keeps the earlier (lower) Node ID on equal load.
        let mut best: Option<(usize, u32)> = None;
        for (index, assessment) in assessments.iter().enumerate() {
            if let NodeVerdict::Eligible { load_permille } = assessment.verdict
                && best.is_none_or(|(_, best_load)| load_permille < best_load)
            {
                best = Some((index, load_permille));
            }
        }
        let selected = best.map(|(index, load_permille)| {
            assessments[index].verdict = NodeVerdict::Selected { load_permille };
            assessments[index].node_id
        });

        Placement {
            selected,
            assessments,
        }
    }

    /// Applies the filters in the order a user would reason about a refusal.
    fn judge(
        state: NodeState,
        descriptor: &NodeDescriptor,
        allocation: NodeAllocation,
        requirements: ResourceRequirements,
        constraints: &PlacementConstraints,
    ) -> NodeVerdict {
        let capacity = descriptor.capacity;
        if state != NodeState::Ready {
            return NodeVerdict::NotReady { state };
        }
        if !constraints.is_satisfied_by(descriptor) {
            return NodeVerdict::ConstraintsNotSatisfied;
        }
        if !capacity.satisfies(requirements) {
            return NodeVerdict::InsufficientCapacity;
        }
        if allocation.executions >= capacity.max_concurrent_executions {
            return NodeVerdict::NoFreeCapacity {
                resource: LimitedResource::ConcurrentExecutions,
            };
        }
        let Some(cpus) = allocation
            .logical_cpus
            .checked_add(requirements.logical_cpus)
            .filter(|cpus| *cpus <= capacity.logical_cpus)
        else {
            return NodeVerdict::NoFreeCapacity {
                resource: LimitedResource::Cpu,
            };
        };
        let Some(memory) = allocation
            .memory_bytes
            .checked_add(requirements.memory_bytes)
            .filter(|memory| *memory <= capacity.memory_bytes)
        else {
            return NodeVerdict::NoFreeCapacity {
                resource: LimitedResource::Memory,
            };
        };

        NodeVerdict::Eligible {
            load_permille: load_permille(
                u128::from(cpus),
                u128::from(capacity.logical_cpus),
                u128::from(memory),
                u128::from(capacity.memory_bytes),
            ),
        }
    }
}

/// The larger of the CPU and memory reservations, in thousandths of capacity.
fn load_permille(cpus: u128, total_cpus: u128, memory: u128, total_memory: u128) -> u32 {
    let fraction = |reserved: u128, total: u128| reserved * 1000 / total.max(1);
    u32::try_from(fraction(cpus, total_cpus).max(fraction(memory, total_memory)))
        .unwrap_or(u32::MAX)
}

/// Explains why no node could be selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingFailure {
    NoReadyNodes,
    ConstraintsNotSatisfied,
    InsufficientResources,
    NoAvailableNodes,
}

impl SchedulingFailure {
    /// True when waiting cannot help with the nodes that exist now, so later
    /// jobs may be placed first. Busy nodes free up and no-ready-nodes affects
    /// every job alike, so those keep their place in line.
    pub const fn can_be_overtaken(self) -> bool {
        matches!(
            self,
            Self::ConstraintsNotSatisfied | Self::InsufficientResources
        )
    }
}

impl fmt::Display for SchedulingFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoReadyNodes => formatter.write_str("no nodes are ready"),
            Self::ConstraintsNotSatisfied => {
                formatter.write_str("ready nodes do not satisfy the placement constraints")
            }
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
            .select_node(
                requirements(4, 8_000),
                &PlacementConstraints::default(),
                &registry,
                &BTreeMap::new(),
            )
            .expect("matching node should be selected");

        assert_eq!(selected, expected);
    }

    #[test]
    fn joining_nodes_are_not_selected() {
        let mut registry = NodeRegistry::new();
        registry.register(descriptor(8, 16_000));

        let error = Scheduler::new()
            .select_node(
                requirements(1, 1_000),
                &PlacementConstraints::default(),
                &registry,
                &BTreeMap::new(),
            )
            .expect_err("joining node must not be selected");

        assert_eq!(error, SchedulingFailure::NoReadyNodes);
    }

    #[test]
    fn reports_when_ready_nodes_lack_capacity() {
        let registry = ready_registry(descriptor(2, 4_000));

        let error = Scheduler::new()
            .select_node(
                requirements(4, 8_000),
                &PlacementConstraints::default(),
                &registry,
                &BTreeMap::new(),
            )
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
                &PlacementConstraints::default(),
                &registry,
                &BTreeMap::from([(
                    node_id,
                    NodeAllocation {
                        logical_cpus: 1,
                        memory_bytes: 1_000,
                        executions: 1,
                    },
                )]),
            )
            .expect_err("busy node must not receive another execution");

        assert_eq!(error, SchedulingFailure::NoAvailableNodes);
    }

    #[test]
    fn least_loaded_node_wins_even_when_it_has_a_higher_node_id() {
        let busy = fixed_node(1, 8, 16_000, 8);
        let idle = fixed_node(2, 8, 16_000, 8);
        let (busy_id, idle_id) = (busy.id, idle.id);
        let registry = ready_registry_of([busy, idle]);
        let mut busy_allocation = NodeAllocation::default();
        busy_allocation.reserve(requirements(4, 2_000));
        let allocations = BTreeMap::from([(busy_id, busy_allocation)]);

        let placement = Scheduler::new().place(
            requirements(2, 2_000),
            &PlacementConstraints::default(),
            &registry,
            &allocations,
        );

        assert_eq!(placement.selected, Some(idle_id));
        // idle: 2/8 cpus = 250; busy would be 6/8 cpus = 750.
        assert_eq!(
            verdicts(&placement),
            vec![
                NodeVerdict::Eligible { load_permille: 750 },
                NodeVerdict::Selected { load_permille: 250 },
            ]
        );
    }

    #[test]
    fn load_is_the_larger_of_cpu_and_memory_fractions() {
        let node = fixed_node(1, 10, 10_000, 8);
        let registry = ready_registry_of([node]);

        let placement = Scheduler::new().place(
            requirements(1, 6_000),
            &PlacementConstraints::default(),
            &registry,
            &BTreeMap::new(),
        );

        // cpu 1/10 = 100, memory 6000/10000 = 600.
        assert_eq!(
            verdicts(&placement),
            vec![NodeVerdict::Selected { load_permille: 600 }]
        );
    }

    #[test]
    fn equal_load_goes_to_the_lowest_node_id() {
        let (low, high) = (fixed_node(1, 8, 16_000, 8), fixed_node(2, 8, 16_000, 8));
        let low_id = low.id;
        let registry = ready_registry_of([high, low]);

        let placement = Scheduler::new().place(
            requirements(1, 1_000),
            &PlacementConstraints::default(),
            &registry,
            &BTreeMap::new(),
        );

        assert_eq!(placement.selected, Some(low_id));
    }

    #[test]
    fn each_rejection_is_explained_per_node() {
        let full_cpu = fixed_node(1, 4, 16_000, 8);
        let full_memory = fixed_node(2, 8, 4_000, 8);
        let full_slots = fixed_node(3, 8, 16_000, 1);
        let too_small = fixed_node(4, 1, 16_000, 8);
        let ids = [full_cpu.id, full_memory.id, full_slots.id, too_small.id];
        let mut draining = fixed_node(5, 8, 16_000, 8);
        draining.capabilities = vec!["gpu".to_owned()];
        let draining_id = draining.id;
        let mut registry =
            ready_registry_of([full_cpu, full_memory, full_slots, too_small, draining]);
        registry
            .set_draining(draining_id, true)
            .expect("registered node can be drained");
        let allocations = BTreeMap::from([
            (
                ids[0],
                NodeAllocation {
                    logical_cpus: 3,
                    memory_bytes: 0,
                    executions: 1,
                },
            ),
            (
                ids[1],
                NodeAllocation {
                    logical_cpus: 0,
                    memory_bytes: 3_000,
                    executions: 1,
                },
            ),
            (
                ids[2],
                NodeAllocation {
                    logical_cpus: 1,
                    memory_bytes: 1_000,
                    executions: 1,
                },
            ),
        ]);

        let placement = Scheduler::new().place(
            requirements(2, 2_000),
            &PlacementConstraints::default(),
            &registry,
            &allocations,
        );

        assert_eq!(placement.selected, None);
        assert_eq!(
            verdicts(&placement),
            vec![
                NodeVerdict::NoFreeCapacity {
                    resource: LimitedResource::Cpu
                },
                NodeVerdict::NoFreeCapacity {
                    resource: LimitedResource::Memory
                },
                NodeVerdict::NoFreeCapacity {
                    resource: LimitedResource::ConcurrentExecutions
                },
                NodeVerdict::InsufficientCapacity,
                NodeVerdict::NotReady {
                    state: NodeState::Draining
                },
            ]
        );
        assert_eq!(
            placement.failure(),
            Some(SchedulingFailure::NoAvailableNodes)
        );
    }

    fn verdicts(placement: &Placement) -> Vec<NodeVerdict> {
        placement
            .assessments
            .iter()
            .map(|assessment| assessment.verdict)
            .collect()
    }

    /// A node whose ID orders by `index`, so tests can state expected ordering.
    fn fixed_node(
        index: u128,
        logical_cpus: u32,
        memory_bytes: u64,
        max_concurrent_executions: u32,
    ) -> NodeDescriptor {
        let mut node = descriptor_with_limit(logical_cpus, memory_bytes, max_concurrent_executions);
        node.id = format!("00000000-0000-0000-0000-{index:012}")
            .parse()
            .expect("fixed Node ID should be valid");
        node
    }

    fn ready_registry_of(nodes: impl IntoIterator<Item = NodeDescriptor>) -> NodeRegistry {
        let mut registry = NodeRegistry::new();
        for node in nodes {
            let node_id = node.id;
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
        registry
    }

    #[test]
    fn constraints_exclude_ready_nodes_that_do_not_match() {
        let mut gpu_node = descriptor(8, 16_000);
        gpu_node.capabilities = vec!["gpu".to_owned()];
        let gpu_node_id = gpu_node.id;
        let mut registry = ready_registry(descriptor(8, 16_000));
        registry.register(gpu_node);
        registry
            .record_heartbeat(
                gpu_node_id,
                ResourceSnapshot {
                    cpu_usage_percent: 0,
                    available_memory_bytes: 16_000,
                    running_executions: 0,
                },
            )
            .expect("registered node should accept heartbeat");
        let wants_gpu = PlacementConstraints {
            capabilities: vec!["gpu".to_owned()],
            ..PlacementConstraints::default()
        };
        let wants_arm = PlacementConstraints {
            architecture: Some("aarch64".to_owned()),
            ..PlacementConstraints::default()
        };

        assert_eq!(
            Scheduler::new().select_node(
                requirements(1, 1_000),
                &wants_gpu,
                &registry,
                &BTreeMap::new()
            ),
            Ok(gpu_node_id)
        );
        assert_eq!(
            Scheduler::new().select_node(
                requirements(1, 1_000),
                &wants_arm,
                &registry,
                &BTreeMap::new()
            ),
            Err(SchedulingFailure::ConstraintsNotSatisfied)
        );
    }

    #[test]
    fn constraint_mismatch_is_reported_before_capacity_problems() {
        let registry = ready_registry(descriptor(2, 4_000));
        let wants_gpu = PlacementConstraints {
            capabilities: vec!["gpu".to_owned()],
            ..PlacementConstraints::default()
        };

        assert_eq!(
            Scheduler::new().select_node(
                requirements(64, 1_000_000),
                &wants_gpu,
                &registry,
                &BTreeMap::new()
            ),
            Err(SchedulingFailure::ConstraintsNotSatisfied)
        );
    }

    #[test]
    fn reservations_are_summed_against_capacity() {
        let descriptor = descriptor_with_limit(8, 16_000, 8);
        let node_id = descriptor.id;
        let registry = ready_registry(descriptor);
        let scheduler = Scheduler::new();
        let mut allocation = NodeAllocation::default();
        allocation.reserve(requirements(4, 8_000));
        allocation.reserve(requirements(2, 4_000));
        let allocations = BTreeMap::from([(node_id, allocation)]);

        assert_eq!(
            scheduler.select_node(
                requirements(2, 4_000),
                &PlacementConstraints::default(),
                &registry,
                &allocations
            ),
            Ok(node_id)
        );
        assert_eq!(
            scheduler.select_node(
                requirements(3, 1_000),
                &PlacementConstraints::default(),
                &registry,
                &allocations
            ),
            Err(SchedulingFailure::NoAvailableNodes)
        );
        assert_eq!(
            scheduler.select_node(
                requirements(1, 5_000),
                &PlacementConstraints::default(),
                &registry,
                &allocations
            ),
            Err(SchedulingFailure::NoAvailableNodes)
        );
    }

    #[test]
    fn concurrent_execution_limit_applies_even_with_free_resources() {
        let descriptor = descriptor_with_limit(8, 16_000, 2);
        let node_id = descriptor.id;
        let registry = ready_registry(descriptor);
        let mut allocation = NodeAllocation::default();
        allocation.reserve(requirements(1, 1_000));
        allocation.reserve(requirements(1, 1_000));
        let allocations = BTreeMap::from([(node_id, allocation)]);

        let error = Scheduler::new()
            .select_node(
                requirements(1, 1_000),
                &PlacementConstraints::default(),
                &registry,
                &allocations,
            )
            .expect_err("limit reached");

        assert_eq!(error, SchedulingFailure::NoAvailableNodes);
    }

    #[test]
    fn overflowing_reservation_is_rejected_without_panic() {
        let descriptor = descriptor_with_limit(8, 16_000, 2);
        let node_id = descriptor.id;
        let registry = ready_registry(descriptor);
        let allocations = BTreeMap::from([(
            node_id,
            NodeAllocation {
                logical_cpus: u32::MAX,
                memory_bytes: u64::MAX,
                executions: 0,
            },
        )]);

        let error = Scheduler::new()
            .select_node(
                requirements(1, 1_000),
                &PlacementConstraints::default(),
                &registry,
                &allocations,
            )
            .expect_err("no room");

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
            .select_node(
                requirements(1, 1_000),
                &PlacementConstraints::default(),
                &registry,
                &BTreeMap::new(),
            )
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
        descriptor_with_limit(logical_cpus, memory_bytes, 1)
    }

    fn descriptor_with_limit(
        logical_cpus: u32,
        memory_bytes: u64,
        max_concurrent_executions: u32,
    ) -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId::generate(),
            hostname: "worker".to_owned(),
            operating_system: "windows".to_owned(),
            architecture: "x86_64".to_owned(),
            capacity: ResourceCapacity {
                logical_cpus,
                memory_bytes,
                max_concurrent_executions,
            },
            capabilities: vec![],
        }
    }

    const fn requirements(logical_cpus: u32, memory_bytes: u64) -> ResourceRequirements {
        ResourceRequirements {
            logical_cpus,
            memory_bytes,
        }
    }
}
