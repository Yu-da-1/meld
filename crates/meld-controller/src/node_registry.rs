//! In-memory source of truth for registered nodes.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    time::Instant,
};

use meld_core::{Admission, NodeDescriptor, NodeId, NodeState, ResourceSnapshot, Sha256Digest};
use serde::{Deserialize, Serialize};

use crate::store::{Change, Kind, Persist, StateStore, StoreError, decode, encode};

/// Controller-owned view of one registered node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredNode {
    descriptor: NodeDescriptor,
    /// The operator's decision on whether the node takes part. It outranks
    /// liveness: a node that is not approved is never schedulable.
    admission: Admission,
    /// Digest of the secret that speaks for this node, bound when the node
    /// first registered. `None` for a node stored before secrets existed,
    /// which takes the first one it presents.
    credential: Option<Sha256Digest>,
    /// Liveness observed from heartbeats; never `Draining`, `Pending` or `Revoked`.
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

    /// Effective state: a node that is not approved reports why, and a live
    /// node the operator is draining reports `Draining`.
    pub const fn state(&self) -> NodeState {
        match (self.admission, self.state) {
            (Admission::Pending, _) => NodeState::Pending,
            (Admission::Revoked, _) => NodeState::Revoked,
            (Admission::Approved, NodeState::Ready) if self.draining => NodeState::Draining,
            (Admission::Approved, state) => state,
        }
    }

    pub const fn admission(&self) -> Admission {
        self.admission
    }

    /// Whether `presented` is the digest of this node's secret.
    pub fn has_credential(&self, presented: &Sha256Digest) -> bool {
        self.credential
            .as_ref()
            .is_some_and(|bound| digests_equal(bound, presented))
    }

    /// Whether heartbeats have stopped, whatever the operator decided. The
    /// executions of a revoked node are given up on by this, not by `state`.
    pub const fn is_silent(&self) -> bool {
        matches!(self.state, NodeState::Unreachable)
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
    /// Absent in records written before nodes needed approval, which were
    /// all in use.
    #[serde(default = "approved")]
    admission: Admission,
    #[serde(default)]
    credential: Option<Sha256Digest>,
}

const fn approved() -> Admission {
    Admission::Approved
}

/// Compares without stopping at the first difference.
pub(crate) fn digests_equal(left: &Sha256Digest, right: &Sha256Digest) -> bool {
    let (left, right) = (left.as_str().as_bytes(), right.as_str().as_bytes());
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (left, right)| difference | (left ^ right))
            == 0
}

/// A node tried to register under an identity bound to another secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialMismatch(pub NodeId);

impl fmt::Display for CredentialMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "node {} is bound to a different secret", self.0)
    }
}

impl Error for CredentialMismatch {}

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
                    admission: stored.admission,
                    credential: stored.credential,
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
                        admission: node.admission,
                        credential: node.credential.clone(),
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
    /// not silently put it back into scheduling. So is the admission: only
    /// a node seen for the first time starts as `first_admission`.
    pub fn register(&mut self, descriptor: NodeDescriptor) {
        self.register_as(descriptor, Admission::Approved);
    }

    pub fn register_as(&mut self, descriptor: NodeDescriptor, first_admission: Admission) {
        self.insert(descriptor, first_admission, None);
    }

    /// Registers a node that speaks with `credential`.
    ///
    /// The first credential seen for an identity is bound to it. Later
    /// registrations must present the same one, so that holding the
    /// controller's shared token is not enough to take over a node.
    pub fn register_with(
        &mut self,
        descriptor: NodeDescriptor,
        first_admission: Admission,
        credential: Sha256Digest,
    ) -> Result<(), CredentialMismatch> {
        if let Some(bound) = self
            .nodes
            .get(&descriptor.id)
            .and_then(|node| node.credential.as_ref())
            && !digests_equal(bound, &credential)
        {
            return Err(CredentialMismatch(descriptor.id));
        }
        self.insert(descriptor, first_admission, Some(credential));
        Ok(())
    }

    fn insert(
        &mut self,
        descriptor: NodeDescriptor,
        first_admission: Admission,
        credential: Option<Sha256Digest>,
    ) {
        let previous = self.nodes.get(&descriptor.id);
        let draining = previous.is_some_and(|node| node.draining);
        let admission = previous.map_or(first_admission, |node| node.admission);
        let credential = credential.or_else(|| previous.and_then(|node| node.credential.clone()));
        self.dirty.insert(descriptor.id);
        self.nodes.insert(
            descriptor.id,
            RegisteredNode {
                descriptor,
                admission,
                credential,
                state: NodeState::Joining,
                draining,
                snapshot: None,
                last_heartbeat_at: None,
            },
        );
    }

    /// The node that speaks with `presented`, whatever its admission.
    pub fn find_by_credential(&self, presented: &Sha256Digest) -> Option<&RegisteredNode> {
        self.nodes
            .values()
            .find(|node| node.has_credential(presented))
    }

    /// Lets a node take part, whether it is waiting or was revoked.
    /// Approving an approved node changes nothing.
    pub fn approve(&mut self, node_id: NodeId) -> Result<NodeState, NodeRegistryError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(NodeRegistryError::NodeNotFound(node_id))?;
        if node.admission != Admission::Approved {
            if node.admission == Admission::Revoked {
                // Taking a node back is how one that lost its secret is
                // recovered, so it may bind a new one.
                node.credential = None;
            }
            node.admission = Admission::Approved;
            // Nothing was observed while it was kept out.
            node.state = NodeState::Joining;
            node.snapshot = None;
            self.dirty.insert(node_id);
        }
        Ok(node.state())
    }

    /// Removes a node from the cluster. Its liveness stops counting at once,
    /// so what it was running is given up on like any silent node's.
    pub fn revoke(&mut self, node_id: NodeId) -> Result<NodeState, NodeRegistryError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(NodeRegistryError::NodeNotFound(node_id))?;
        if node.admission != Admission::Revoked {
            node.admission = Admission::Revoked;
            // Its secret is kept: the node must still be told it was revoked,
            // and nobody else may bind one to this identity meanwhile.
            node.state = NodeState::Unreachable;
            node.snapshot = None;
            self.dirty.insert(node_id);
        }
        Ok(node.state())
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

    #[test]
    fn a_node_stored_before_approval_existed_is_approved() {
        let descriptor = descriptor();
        let node_id = descriptor.id;
        let store = StateStore::open_in_memory().expect("in-memory store");
        // The record as an earlier version wrote it: no `admission`.
        let legacy = serde_json::json!({ "descriptor": descriptor, "draining": false });
        store
            .apply(&[Change::put(Kind::Node, node_id, legacy.to_string())])
            .expect("legacy record is stored");

        let registry = NodeRegistry::restore(&store).expect("legacy record restores");

        assert_eq!(
            registry.get(node_id).map(RegisteredNode::admission),
            Some(Admission::Approved)
        );
    }

    #[test]
    fn admission_is_kept_when_a_node_registers_again() {
        let descriptor = descriptor();
        let node_id = descriptor.id;
        let mut registry = NodeRegistry::new();
        registry.register_as(descriptor.clone(), Admission::Pending);
        assert_eq!(
            registry.get(node_id).map(RegisteredNode::state),
            Some(NodeState::Pending)
        );

        registry.approve(node_id).expect("known node");
        // Registering again names a different first admission; it only
        // applies to a node that was never seen.
        registry.register_as(descriptor.clone(), Admission::Pending);
        assert_eq!(
            registry.get(node_id).map(RegisteredNode::admission),
            Some(Admission::Approved)
        );

        registry.revoke(node_id).expect("known node");
        registry.register_as(descriptor, Admission::Approved);
        assert_eq!(
            registry.get(node_id).map(RegisteredNode::admission),
            Some(Admission::Revoked)
        );
    }

    #[test]
    fn revoking_a_live_node_makes_it_silent_and_drops_what_was_observed() {
        let descriptor = descriptor();
        let node_id = descriptor.id;
        let mut registry = NodeRegistry::new();
        registry.register(descriptor);
        registry
            .record_heartbeat(node_id, snapshot())
            .expect("registered node should accept heartbeat");

        registry.revoke(node_id).expect("known node");

        let node = registry.get(node_id).expect("node stays known");
        assert_eq!(node.state(), NodeState::Revoked);
        assert!(node.is_silent());
        assert_eq!(node.snapshot(), None);
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
