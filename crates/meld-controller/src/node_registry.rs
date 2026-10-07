//! In-memory source of truth for registered nodes.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    time::Instant,
};

use meld_core::{NodeDescriptor, NodeId, NodeState, ResourceSnapshot};
use serde::{Deserialize, Serialize};

use crate::store::{Change, Kind, Persist, StateStore, StoreError, decode, encode};

/// Controller-owned view of one registered node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredNode {
    descriptor: NodeDescriptor,
    /// Liveness observed from heartbeats; never `Draining`.
    state: NodeState,
    /// Operator intent. Kept separately so it survives heartbeats, timeouts
    /// and re-registration instead of being overwritten by liveness changes.
    draining: bool,
    snapshot: Option<ResourceSnapshot>,
    last_heartbeat_at: Option<Instant>,
}

impl RegisteredNode {
    pub const fn descriptor(&self) -> &NodeDescriptor {
        &self.descriptor
    }

    /// Effective state: a live node the operator is draining reports `Draining`.
    pub const fn state(&self) -> NodeState {
        match self.state {
            NodeState::Ready if self.draining => NodeState::Draining,
            state => state,
        }
    }

    pub const fn snapshot(&self) -> Option<ResourceSnapshot> {
        self.snapshot
    }

    pub const fn last_heartbeat_at(&self) -> Option<Instant> {
        self.last_heartbeat_at
    }

    pub(crate) fn mark_unreachable(&mut self) {
        self.state = NodeState::Unreachable;
    }
}

/// Stores the latest controller-observed state for every node.
#[derive(Debug, Default)]
pub struct NodeRegistry {
    nodes: BTreeMap<NodeId, RegisteredNode>,
    /// Nodes whose persisted fields changed since the last flush.
    dirty: BTreeSet<NodeId>,
}

/// What survives a controller restart. Liveness and usage do not: they are
/// observations, and are rebuilt from the next heartbeat.
#[derive(Debug, Serialize, Deserialize)]
struct StoredNode {
    descriptor: NodeDescriptor,
    draining: bool,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuilds the registry from the store.
    ///
    /// Restored nodes are `Unreachable` until they report in again: the
    /// controller has not observed them since it started.
    pub fn restore(store: &StateStore) -> Result<Self, StoreError> {
        let mut registry = Self::default();
        for row in store.load(Kind::Node)? {
            let stored: StoredNode = decode(Kind::Node, &row.id, &row.body)?;
            if stored.descriptor.id.to_string() != row.id {
                return Err(StoreError::Corrupt(format!(
                    "node row {} holds a record with id {}",
                    row.id, stored.descriptor.id
                )));
            }
            registry.nodes.insert(
                stored.descriptor.id,
                RegisteredNode {
                    descriptor: stored.descriptor,
                    state: NodeState::Unreachable,
                    draining: stored.draining,
                    snapshot: None,
                    last_heartbeat_at: None,
                },
            );
        }
        Ok(registry)
    }

    /// Writes nodes changed since the last flush; on failure they stay unsaved.
    pub fn flush(&mut self, store: &StateStore) -> Result<(), StoreError> {
        let changes = self
            .dirty
            .iter()
            .filter_map(|node_id| self.nodes.get(node_id))
            .map(|node| {
                Change::put(
                    Kind::Node,
                    node.descriptor.id,
                    encode(&StoredNode {
                        descriptor: node.descriptor.clone(),
                        draining: node.draining,
                    }),
                )
            })
            .collect::<Vec<_>>();
        store.apply(&changes)?;
        self.dirty.clear();
        Ok(())
    }

    pub fn has_unsaved_changes(&self) -> bool {
        !self.dirty.is_empty()
    }

    /// Registers a node as joining, replacing stale data for the same identity.
    ///
    /// A drain requested for this identity is kept, so restarting a node does
    /// not silently put it back into scheduling.
    pub fn register(&mut self, descriptor: NodeDescriptor) {
        let draining = self
            .nodes
            .get(&descriptor.id)
            .is_some_and(|node| node.draining);
        self.dirty.insert(descriptor.id);
        self.nodes.insert(
            descriptor.id,
            RegisteredNode {
                descriptor,
                state: NodeState::Joining,
                draining,
                snapshot: None,
                last_heartbeat_at: None,
            },
        );
    }

    /// Stops (or resumes) new placements on a node and returns its effective state.
    pub fn set_draining(
        &mut self,
        node_id: NodeId,
        draining: bool,
    ) -> Result<NodeState, NodeRegistryError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(NodeRegistryError::NodeNotFound(node_id))?;
        node.draining = draining;
        let state = node.state();
        self.dirty.insert(node_id);
        Ok(state)
    }

    /// Records a heartbeat and makes the node available for scheduling.
    pub fn record_heartbeat(
        &mut self,
        node_id: NodeId,
        snapshot: ResourceSnapshot,
    ) -> Result<(), NodeRegistryError> {
        self.record_heartbeat_at(node_id, snapshot, Instant::now())
    }

    pub(crate) fn record_heartbeat_at(
        &mut self,
        node_id: NodeId,
        snapshot: ResourceSnapshot,
        received_at: Instant,
    ) -> Result<(), NodeRegistryError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(NodeRegistryError::NodeNotFound(node_id))?;

        node.state = NodeState::Ready;
        node.snapshot = Some(snapshot);
        node.last_heartbeat_at = Some(received_at);
        Ok(())
    }

    pub fn get(&self, node_id: NodeId) -> Option<&RegisteredNode> {
        self.nodes.get(&node_id)
    }

    /// Iterates in Node ID order to keep scheduling deterministic.
    pub fn nodes(&self) -> impl Iterator<Item = &RegisteredNode> {
        self.nodes.values()
    }

    pub(crate) fn nodes_mut(&mut self) -> impl Iterator<Item = &mut RegisteredNode> {
        self.nodes.values_mut()
    }
}

impl Persist for NodeRegistry {
    fn save(&mut self, store: &StateStore) -> Result<(), StoreError> {
        self.flush(store)
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
    fn drain_persists_across_heartbeats_and_resume_restores_ready() {
        let descriptor = descriptor();
        let node_id = descriptor.id;
        let mut registry = NodeRegistry::new();
        registry.register(descriptor);
        registry
            .record_heartbeat(node_id, snapshot())
            .expect("registered node should accept heartbeat");

        assert_eq!(
            registry.set_draining(node_id, true),
            Ok(NodeState::Draining)
        );
        registry
            .record_heartbeat(node_id, snapshot())
            .expect("draining node should accept heartbeat");
        assert_eq!(
            registry.get(node_id).map(RegisteredNode::state),
            Some(NodeState::Draining)
        );

        assert_eq!(registry.set_draining(node_id, false), Ok(NodeState::Ready));
    }

    #[test]
    fn drain_survives_unreachable_and_reregistration() {
        let descriptor = descriptor();
        let node_id = descriptor.id;
        let mut registry = NodeRegistry::new();
        registry.register(descriptor.clone());
        registry
            .record_heartbeat(node_id, snapshot())
            .expect("registered node should accept heartbeat");
        registry
            .set_draining(node_id, true)
            .expect("registered node can be drained");

        registry
            .nodes_mut()
            .for_each(RegisteredNode::mark_unreachable);
        assert_eq!(
            registry.get(node_id).map(RegisteredNode::state),
            Some(NodeState::Unreachable)
        );

        registry.register(descriptor);
        registry
            .record_heartbeat(node_id, snapshot())
            .expect("re-registered node should accept heartbeat");
        assert_eq!(
            registry.get(node_id).map(RegisteredNode::state),
            Some(NodeState::Draining)
        );
    }

    #[test]
    fn draining_an_unknown_node_is_rejected() {
        let node_id = NodeId::generate();

        assert_eq!(
            NodeRegistry::new().set_draining(node_id, true),
            Err(NodeRegistryError::NodeNotFound(node_id))
        );
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
                max_concurrent_executions: 1,
            },
            capabilities: vec![],
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
