//! Detects nodes whose heartbeat deadline has elapsed.

use std::time::{Duration, Instant};

use meld_core::{NodeId, NodeState};

use crate::node_registry::NodeRegistry;

#[derive(Debug, Clone, Copy)]
pub struct FailureDetector {
    heartbeat_timeout: Duration,
}

impl FailureDetector {
    pub fn new(heartbeat_timeout: Duration) -> Self {
        Self { heartbeat_timeout }
    }

    /// Marks timed-out nodes unreachable and returns identities that changed state.
    pub fn detect(&self, registry: &mut NodeRegistry, now: Instant) -> Vec<NodeId> {
        registry
            .nodes_mut()
            .filter_map(|node| {
                let monitored = matches!(node.state(), NodeState::Ready | NodeState::Draining);
                let timed_out = node.last_heartbeat_at().is_some_and(|last| {
                    now.saturating_duration_since(last) >= self.heartbeat_timeout
                });

                (monitored && timed_out).then(|| {
                    let node_id = node.descriptor().id;
                    node.mark_unreachable();
                    node_id
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use meld_core::{NodeDescriptor, ResourceCapacity, ResourceSnapshot};

    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(15);

    #[test]
    fn node_becomes_unreachable_at_timeout_boundary() {
        let heartbeat_at = Instant::now();
        let (mut registry, node_id) = ready_registry(heartbeat_at);
        let detector = FailureDetector::new(TIMEOUT);

        assert!(
            detector
                .detect(
                    &mut registry,
                    heartbeat_at + TIMEOUT - Duration::from_nanos(1)
                )
                .is_empty()
        );

        assert_eq!(
            detector.detect(&mut registry, heartbeat_at + TIMEOUT),
            vec![node_id]
        );
        assert_eq!(
            registry.get(node_id).map(|node| node.state()),
            Some(NodeState::Unreachable)
        );
    }

    #[test]
    fn detector_only_reports_a_state_change_once() {
        let heartbeat_at = Instant::now();
        let (mut registry, node_id) = ready_registry(heartbeat_at);
        let detector = FailureDetector::new(TIMEOUT);
        let timed_out_at = heartbeat_at + TIMEOUT;

        assert_eq!(detector.detect(&mut registry, timed_out_at), vec![node_id]);
        assert!(
            detector
                .detect(&mut registry, timed_out_at + TIMEOUT)
                .is_empty()
        );
    }

    #[test]
    fn new_heartbeat_restores_an_unreachable_node() {
        let heartbeat_at = Instant::now();
        let (mut registry, node_id) = ready_registry(heartbeat_at);
        let detector = FailureDetector::new(TIMEOUT);
        let restored_at = heartbeat_at + TIMEOUT + Duration::from_secs(1);
        detector.detect(&mut registry, heartbeat_at + TIMEOUT);

        registry
            .record_heartbeat_at(node_id, snapshot(), restored_at)
            .expect("registered node should accept heartbeat");

        let node = registry
            .get(node_id)
            .expect("node should remain registered");
        assert_eq!(node.state(), NodeState::Ready);
        assert_eq!(node.last_heartbeat_at(), Some(restored_at));
    }

    fn ready_registry(heartbeat_at: Instant) -> (NodeRegistry, NodeId) {
        let descriptor = descriptor();
        let node_id = descriptor.id;
        let mut registry = NodeRegistry::new();
        registry.register(descriptor);
        registry
            .record_heartbeat_at(node_id, snapshot(), heartbeat_at)
            .expect("registered node should accept heartbeat");
        (registry, node_id)
    }

    fn descriptor() -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId::generate(),
            hostname: "worker".to_owned(),
            operating_system: "linux".to_owned(),
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
