//! Writing the job manager's records to the state store and reading them back.
//!
//! Only authoritative records are stored. What can be derived (the attempt
//! list of a job, resource reservations) is rebuilt on restore, so it cannot
//! disagree with the records it came from.

use std::{collections::BTreeSet, fmt::Display, str::FromStr};

use serde::de::DeserializeOwned;

use super::JobManager;
use crate::{
    store::{
        Change, Kind, StateStore, StoreError, decode, encode, instant_to_unix_ms,
        unix_ms_to_instant,
    },
    tracked::Tracked,
};
use meld_core::{Execution, ExecutionId, ExecutionState, JobId, JobState};

const QUEUE_ROW: &str = "queue";
const FLAG_BODY: &str = "1";

impl JobManager {
    /// Rebuilds a manager from everything the store holds.
    pub fn restore(store: &StateStore) -> Result<Self, StoreError> {
        let mut manager = Self::default();

        load(store, Kind::Job, &mut manager.jobs, |job| job.id())?;
        load(
            store,
            Kind::Execution,
            &mut manager.executions,
            |execution| execution.id(),
        )?;
        for execution in manager.executions.values() {
            if !manager.jobs.contains_key(&execution.job_id()) {
                return Err(StoreError::Corrupt(format!(
                    "execution {} belongs to unknown job {}",
                    execution.id(),
                    execution.job_id()
                )));
            }
        }
        // The map is ordered by id, which says nothing about age. The rows
        // come back in creation order, which is attempt order.
        for row in store.load(Kind::Execution)? {
            let execution_id: ExecutionId = parse_id(Kind::Execution, &row.id)?;
            manager
                .execution_order
                .insert(execution_id, manager.next_execution_order);
            manager.next_execution_order += 1;
            if let Some(execution) = manager.executions.get(&execution_id) {
                manager
                    .execution_ids_by_job
                    .entry(execution.job_id())
                    .or_default()
                    .push(execution_id);
            }
        }

        load_by_id(store, Kind::Output, &mut manager.outputs)?;
        load_by_id(store, Kind::DataFailure, &mut manager.data_failures)?;
        load_by_id(store, Kind::OutputFiles, &mut manager.output_files)?;
        load_by_id(store, Kind::Placement, &mut manager.placements)?;
        load_instants(
            store,
            Kind::OutputsRecordedAt,
            &mut manager.outputs_recorded_at,
        )?;
        load_instants(store, Kind::JobSubmittedAt, &mut manager.job_submitted_at)?;
        load_flags(
            store,
            Kind::CancellationRequest,
            &mut manager.cancellation_requests,
        )?;
        load_flags(
            store,
            Kind::JobTimeoutRequest,
            &mut manager.job_timeout_requests,
        )?;

        manager.restore_queue(store)?;
        manager.unconfirmed = manager
            .executions
            .values()
            .filter(|execution| {
                matches!(
                    execution.state(),
                    ExecutionState::Accepted | ExecutionState::Running | ExecutionState::Cancelling
                )
            })
            .map(Execution::id)
            .collect();
        Ok(manager)
    }

    /// The stored order is kept, but the job records decide who is queued: a
    /// job that is no longer `Queued` is dropped, and a `Queued` job missing
    /// from the stored order goes to the back.
    fn restore_queue(&mut self, store: &StateStore) -> Result<(), StoreError> {
        let stored: Vec<JobId> = match store.load(Kind::Queue)?.into_iter().next() {
            Some(row) => decode(Kind::Queue, &row.id, &row.body)?,
            None => Vec::new(),
        };
        let mut seen = BTreeSet::new();
        for job_id in stored {
            let queued = self
                .jobs
                .get(&job_id)
                .is_some_and(|job| job.state() == JobState::Queued);
            if queued && seen.insert(job_id) {
                self.pending_jobs.push_back(job_id);
            }
        }
        let missing = self
            .jobs
            .values()
            .filter(|job| job.state() == JobState::Queued && !seen.contains(&job.id()))
            .map(|job| job.id())
            .collect::<Vec<_>>();
        self.pending_jobs.extend(missing);
        Ok(())
    }

    /// Whether anything changed since the last successful [`Self::flush`].
    pub fn has_unsaved_changes(&self) -> bool {
        self.queue_dirty
            || self.jobs.dirty().next().is_some()
            || self.executions.dirty().next().is_some()
            || self.outputs.dirty().next().is_some()
            || self.data_failures.dirty().next().is_some()
            || self.output_files.dirty().next().is_some()
            || self.outputs_recorded_at.dirty().next().is_some()
            || self.cancellation_requests.dirty().next().is_some()
            || self.job_timeout_requests.dirty().next().is_some()
            || self.job_submitted_at.dirty().next().is_some()
            || self.placements.dirty().next().is_some()
    }

    /// Writes every record changed since the last flush in one transaction.
    ///
    /// On failure nothing is marked as saved, so the next flush retries it.
    pub fn flush(&mut self, store: &StateStore) -> Result<(), StoreError> {
        if !self.has_unsaved_changes() {
            return Ok(());
        }

        let mut changes = Vec::new();
        collect(&mut changes, Kind::Job, &self.jobs, encode);
        // New executions are written oldest first, since the store keeps rows
        // in write order and that order is the attempt order.
        let mut executions = self.executions.dirty().collect::<Vec<_>>();
        executions.sort_by_key(|(id, _)| self.execution_order.get(id).copied());
        changes.extend(
            executions
                .into_iter()
                .map(|(id, execution)| match execution {
                    Some(execution) => Change::put(Kind::Execution, id, encode(execution)),
                    None => Change::delete(Kind::Execution, id),
                }),
        );
        collect(&mut changes, Kind::Output, &self.outputs, |value| {
            encode(value)
        });
        collect(&mut changes, Kind::DataFailure, &self.data_failures, encode);
        collect(&mut changes, Kind::OutputFiles, &self.output_files, encode);
        collect(&mut changes, Kind::Placement, &self.placements, |value| {
            encode(value)
        });
        collect(
            &mut changes,
            Kind::OutputsRecordedAt,
            &self.outputs_recorded_at,
            |instant| encode(&instant_to_unix_ms(*instant)),
        );
        collect(
            &mut changes,
            Kind::JobSubmittedAt,
            &self.job_submitted_at,
            |instant| encode(&instant_to_unix_ms(*instant)),
        );
        collect(
            &mut changes,
            Kind::CancellationRequest,
            &self.cancellation_requests,
            |()| FLAG_BODY.to_owned(),
        );
        collect(
            &mut changes,
            Kind::JobTimeoutRequest,
            &self.job_timeout_requests,
            |()| FLAG_BODY.to_owned(),
        );
        if self.queue_dirty {
            changes.push(Change::put(
                Kind::Queue,
                QUEUE_ROW,
                encode(&self.pending_jobs.iter().collect::<Vec<_>>()),
            ));
        }

        store.apply(&changes)?;

        self.jobs.clear_dirty();
        self.executions.clear_dirty();
        self.outputs.clear_dirty();
        self.data_failures.clear_dirty();
        self.output_files.clear_dirty();
        self.placements.clear_dirty();
        self.outputs_recorded_at.clear_dirty();
        self.job_submitted_at.clear_dirty();
        self.cancellation_requests.clear_dirty();
        self.job_timeout_requests.clear_dirty();
        self.queue_dirty = false;
        Ok(())
    }
}

fn collect<K: Ord + Copy + Display, V>(
    changes: &mut Vec<Change>,
    kind: Kind,
    records: &Tracked<K, V>,
    encode_value: impl Fn(&V) -> String,
) {
    for (key, value) in records.dirty() {
        changes.push(match value {
            Some(value) => Change::put(kind, key, encode_value(value)),
            None => Change::delete(kind, key),
        });
    }
}

fn parse_id<K: FromStr>(kind: Kind, id: &str) -> Result<K, StoreError>
where
    K::Err: Display,
{
    id.parse().map_err(|error| {
        StoreError::Corrupt(format!("{kind:?} row has an invalid id {id:?}: {error}"))
    })
}

/// Loads records whose key is carried inside the body.
///
/// A record filed under an id that is not its own would be silently
/// misattributed, so the two must agree.
fn load<K: Ord + Copy + Display, V: DeserializeOwned>(
    store: &StateStore,
    kind: Kind,
    target: &mut Tracked<K, V>,
    key_of: impl Fn(&V) -> K,
) -> Result<(), StoreError> {
    for row in store.load(kind)? {
        let value = decode(kind, &row.id, &row.body)?;
        let key = key_of(&value);
        if key.to_string() != row.id {
            return Err(StoreError::Corrupt(format!(
                "{kind:?} row {} holds a record with id {key}",
                row.id
            )));
        }
        target.load(key, value);
    }
    Ok(())
}

/// Loads records keyed by their row id.
fn load_by_id<K, V>(
    store: &StateStore,
    kind: Kind,
    target: &mut Tracked<K, V>,
) -> Result<(), StoreError>
where
    K: Ord + Copy + FromStr,
    K::Err: Display,
    V: DeserializeOwned,
{
    for row in store.load(kind)? {
        let key = parse_id(kind, &row.id)?;
        target.load(key, decode(kind, &row.id, &row.body)?);
    }
    Ok(())
}

fn load_instants<K>(
    store: &StateStore,
    kind: Kind,
    target: &mut Tracked<K, std::time::Instant>,
) -> Result<(), StoreError>
where
    K: Ord + Copy + FromStr,
    K::Err: Display,
{
    for row in store.load(kind)? {
        let key = parse_id(kind, &row.id)?;
        let unix_ms: u64 = decode(kind, &row.id, &row.body)?;
        target.load(key, unix_ms_to_instant(unix_ms));
    }
    Ok(())
}

fn load_flags(
    store: &StateStore,
    kind: Kind,
    target: &mut Tracked<ExecutionId, ()>,
) -> Result<(), StoreError> {
    for row in store.load(kind)? {
        target.load(parse_id(kind, &row.id)?, ());
    }
    Ok(())
}

impl crate::store::Persist for JobManager {
    fn save(&mut self, store: &StateStore) -> Result<(), StoreError> {
        self.flush(store)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use meld_core::{
        CapturedStream, DataFailure, DataSpec, ExecutionOutput, ExecutionResult, ExecutionState,
        JobSpec, NodeDescriptor, NodeId, OutputFile, PlacementConstraints, ResourceCapacity,
        ResourceRequirements, ResourceSnapshot, Sha256Digest,
    };

    use super::*;
    use crate::{node_registry::NodeRegistry, scheduler::Scheduler};

    fn spec() -> JobSpec {
        JobSpec {
            program: "rustc".to_owned(),
            args: vec!["--version".to_owned()],
            requirements: ResourceRequirements {
                logical_cpus: 1,
                memory_bytes: 1_000,
            },
            job_timeout_secs: None,
            execution_timeout_secs: None,
            constraints: PlacementConstraints::default(),
            data: DataSpec::default(),
        }
    }

    fn registry(slots: u32) -> NodeRegistry {
        let node_id = NodeId::generate();
        let mut registry = NodeRegistry::new();
        registry.register(NodeDescriptor {
            id: node_id,
            hostname: "worker".to_owned(),
            operating_system: "linux".to_owned(),
            architecture: "x86_64".to_owned(),
            capacity: ResourceCapacity {
                logical_cpus: 8,
                memory_bytes: 1_000_000,
                max_concurrent_executions: slots,
            },
            capabilities: vec![],
        });
        registry
            .record_heartbeat(
                node_id,
                ResourceSnapshot {
                    cpu_usage_percent: 0,
                    available_memory_bytes: 1_000_000,
                    running_executions: 0,
                },
            )
            .expect("node is registered");
        registry
    }

    fn restored(manager: &mut JobManager, store: &StateStore) -> JobManager {
        manager.flush(store).expect("flush should succeed");
        JobManager::restore(store).expect("restore should succeed")
    }

    #[test]
    fn every_kind_of_record_round_trips() {
        let store = StateStore::open_in_memory().expect("store");
        let registry = registry(4);
        let scheduler = Scheduler::new();
        let mut manager = JobManager::new();

        let finished_job = manager.submit(spec()).expect("submit");
        let finished = manager
            .schedule(finished_job, &scheduler, &registry)
            .expect("schedule");
        manager.accept_execution(finished).expect("accept");
        manager.start_execution(finished).expect("start");
        let output = ExecutionOutput {
            stdout: CapturedStream {
                content: "hello".to_owned(),
                truncated: false,
                lossy: false,
            },
            stderr: CapturedStream::default(),
        };
        manager
            .finish_execution_with_output(
                finished,
                ExecutionResult { exit_code: Some(0) },
                output.clone(),
            )
            .expect("finish");
        let file = OutputFile {
            path: "out.txt".to_owned(),
            sha256: Sha256Digest::from_bytes([7; 32]),
            size_bytes: 3,
        };
        manager.record_output_files(finished, vec![file.clone()]);

        let failed_job = manager.submit(spec()).expect("submit");
        let failed = manager
            .schedule(failed_job, &scheduler, &registry)
            .expect("schedule");
        manager.accept_execution(failed).expect("accept");
        manager
            .fail_execution_data(
                failed,
                DataFailure::OutputMissing {
                    path: "x".to_owned(),
                },
            )
            .expect("data failure");

        let cancelling_job = manager.submit(spec()).expect("submit");
        let cancelling = manager
            .schedule(cancelling_job, &scheduler, &registry)
            .expect("schedule");
        manager.accept_execution(cancelling).expect("accept");
        manager.start_execution(cancelling).expect("start");
        manager
            .request_job_cancellation(cancelling_job)
            .expect("cancel request");

        let queued_first = manager.submit(spec()).expect("submit");
        let queued_second = manager.submit(spec()).expect("submit");

        let after = restored(&mut manager, &store);

        assert_eq!(
            after.job(finished_job).map(|j| j.state()),
            Some(JobState::Succeeded)
        );
        assert_eq!(
            after.execution(finished).map(|e| e.state()),
            Some(ExecutionState::Succeeded)
        );
        assert_eq!(after.execution_output(finished), Some(&output));
        assert_eq!(after.output_files(finished), std::slice::from_ref(&file));
        assert!(
            after
                .pinned_digests(Instant::now(), Duration::from_secs(60))
                .contains(&file.sha256),
            "the recording time of an output must survive, or retention restarts or lapses"
        );
        assert!(after.data_failure(failed).is_some());
        assert_eq!(
            after.job(failed_job).map(|j| j.state()),
            Some(JobState::Failed)
        );
        assert!(
            after
                .cancellation_requested(cancelling, after.execution(cancelling).unwrap().node_id())
        );
        assert_eq!(
            after.job(cancelling_job).map(|j| j.state()),
            Some(JobState::Cancelling)
        );
        assert_eq!(after.pending_position(queued_first), Some(0));
        assert_eq!(after.pending_position(queued_second), Some(1));
        assert_eq!(
            after.latest_execution_for_job(finished_job).map(|e| e.id()),
            Some(finished)
        );
    }

    #[test]
    fn a_rejected_job_returns_to_the_front_of_the_restored_queue() {
        let store = StateStore::open_in_memory().expect("store");
        let registry = registry(1);
        let mut manager = JobManager::new();
        let first = manager.submit(spec()).expect("submit");
        let second = manager.submit(spec()).expect("submit");
        let execution = manager
            .schedule(first, &Scheduler::new(), &registry)
            .expect("schedule");
        manager.reject_execution(execution).expect("reject");

        let after = restored(&mut manager, &store);

        assert_eq!(after.pending_position(first), Some(0));
        assert_eq!(after.pending_position(second), Some(1));
    }

    #[test]
    fn attempts_of_one_job_keep_their_order() {
        let store = StateStore::open_in_memory().expect("store");
        let registry = registry(1);
        let scheduler = Scheduler::new();
        let mut manager = JobManager::new();
        let job = manager.submit(spec()).expect("submit");
        let first = manager
            .schedule(job, &scheduler, &registry)
            .expect("schedule");
        manager.reject_execution(first).expect("reject");
        let second = manager
            .schedule(job, &scheduler, &registry)
            .expect("schedule");

        let after = restored(&mut manager, &store);

        assert_eq!(
            after.latest_execution_for_job(job).map(|e| e.id()),
            Some(second)
        );
        assert_eq!(
            after.execution(first).map(|e| e.state()),
            Some(ExecutionState::Rejected)
        );
    }

    #[test]
    fn the_job_deadline_keeps_counting_while_the_controller_is_down() {
        let store = StateStore::open_in_memory().expect("store");
        let mut manager = JobManager::new();
        let mut timed = spec();
        timed.job_timeout_secs = Some(60);
        let job = manager
            .submit_at(timed, Instant::now() - Duration::from_secs(59))
            .expect("submit");

        let mut after = restored(&mut manager, &store);

        assert!(
            after
                .expire_jobs_at(Instant::now())
                .expect("expire")
                .is_empty()
        );
        let later = Instant::now() + Duration::from_secs(2);
        assert_eq!(after.expire_jobs_at(later).expect("expire"), vec![job]);
    }

    #[test]
    fn a_flush_writes_only_what_changed() {
        let store = StateStore::open_in_memory().expect("store");
        let mut manager = JobManager::new();
        manager.submit(spec()).expect("submit");
        assert!(manager.has_unsaved_changes());

        manager.flush(&store).expect("flush");

        assert!(!manager.has_unsaved_changes());
        let reading_changes_nothing = manager.pending_position(JobId::generate());
        assert_eq!(reading_changes_nothing, None);
        assert!(!manager.has_unsaved_changes());
    }

    #[test]
    fn an_execution_of_an_unknown_job_is_reported_as_corruption() {
        let store = StateStore::open_in_memory().expect("store");
        let registry = registry(1);
        let mut manager = JobManager::new();
        let job = manager.submit(spec()).expect("submit");
        manager
            .schedule(job, &Scheduler::new(), &registry)
            .expect("schedule");
        manager.flush(&store).expect("flush");
        store
            .apply(&[crate::store::Change::delete(Kind::Job, job)])
            .expect("delete");

        let error = JobManager::restore(&store).expect_err("dangling execution must be refused");

        assert!(matches!(error, StoreError::Corrupt(_)), "{error}");
    }

    #[test]
    fn a_record_filed_under_the_wrong_id_is_reported_as_corruption() {
        let store = StateStore::open_in_memory().expect("store");
        let mut manager = JobManager::new();
        let job = manager.submit(spec()).expect("submit");
        manager.flush(&store).expect("flush");
        let body = store.load(Kind::Job).expect("load").remove(0).body;
        store
            .apply(&[
                crate::store::Change::delete(Kind::Job, job),
                crate::store::Change::put(Kind::Job, JobId::generate(), body),
            ])
            .expect("rewrite");

        let error = JobManager::restore(&store).expect_err("misfiled record must be refused");

        assert!(matches!(error, StoreError::Corrupt(_)), "{error}");
    }

    #[test]
    fn a_queue_row_that_disagrees_with_job_states_is_repaired() {
        let store = StateStore::open_in_memory().expect("store");
        let mut manager = JobManager::new();
        let first = manager.submit(spec()).expect("submit");
        let second = manager.submit(spec()).expect("submit");
        manager.flush(&store).expect("flush");
        store
            .apply(&[crate::store::Change::put(
                Kind::Queue,
                QUEUE_ROW,
                encode(&vec![JobId::generate(), second, second]),
            )])
            .expect("rewrite");

        let after = JobManager::restore(&store).expect("restore");

        assert_eq!(after.pending_position(second), Some(0));
        assert_eq!(after.pending_position(first), Some(1));
    }

    /// A job running on `node`, saved and restored as after a controller restart.
    fn running_then_restarted() -> (JobManager, JobId, ExecutionId, NodeId) {
        let store = StateStore::open_in_memory().expect("store");
        let registry = registry(2);
        let mut manager = JobManager::new();
        let job = manager.submit(spec()).expect("submit");
        let execution = manager
            .schedule(job, &Scheduler::new(), &registry)
            .expect("schedule");
        manager.accept_execution(execution).expect("accept");
        manager.start_execution(execution).expect("start");
        let node = manager.execution(execution).expect("stored").node_id();
        (restored(&mut manager, &store), job, execution, node)
    }

    #[test]
    fn a_restored_execution_the_node_still_runs_is_confirmed() {
        let (mut manager, job, execution, node) = running_then_restarted();

        let lost = manager
            .reconcile_node(node, &[execution])
            .expect("reconcile");

        assert!(lost.is_empty());
        assert_eq!(manager.job(job).map(|j| j.state()), Some(JobState::Running));
        // Once confirmed, a later list without it is not judged: it may be stale.
        assert!(
            manager
                .reconcile_node(node, &[])
                .expect("reconcile")
                .is_empty()
        );
        assert_eq!(manager.job(job).map(|j| j.state()), Some(JobState::Running));
    }

    #[test]
    fn a_restored_execution_missing_from_its_node_is_lost() {
        let (mut manager, job, execution, node) = running_then_restarted();

        let lost = manager.reconcile_node(node, &[]).expect("reconcile");

        assert_eq!(lost, vec![execution]);
        assert_eq!(manager.job(job).map(|j| j.state()), Some(JobState::Lost));
        assert_eq!(
            manager.execution(execution).map(|e| e.state()),
            Some(ExecutionState::Lost)
        );
        assert!(
            !manager
                .pinned_digests(Instant::now(), Duration::ZERO)
                .contains(&Sha256Digest::from_bytes([0; 32]))
        );
    }

    #[test]
    fn another_nodes_report_does_not_decide_the_fate_of_an_execution() {
        let (mut manager, job, _, _) = running_then_restarted();

        let lost = manager
            .reconcile_node(NodeId::generate(), &[])
            .expect("reconcile");

        assert!(lost.is_empty());
        assert_eq!(manager.job(job).map(|j| j.state()), Some(JobState::Running));
    }

    #[test]
    fn a_cancelling_execution_that_vanished_is_lost_and_its_request_dropped() {
        let store = StateStore::open_in_memory().expect("store");
        let registry = registry(1);
        let mut manager = JobManager::new();
        let job = manager.submit(spec()).expect("submit");
        let execution = manager
            .schedule(job, &Scheduler::new(), &registry)
            .expect("schedule");
        manager.accept_execution(execution).expect("accept");
        manager.start_execution(execution).expect("start");
        manager.request_job_cancellation(job).expect("cancel");
        let node = manager.execution(execution).expect("stored").node_id();
        let mut manager = restored(&mut manager, &store);

        manager.reconcile_node(node, &[]).expect("reconcile");

        assert_eq!(manager.job(job).map(|j| j.state()), Some(JobState::Lost));
        assert!(!manager.cancellation_requested(execution, node));
        manager.flush(&store).expect("flush");
        let after = JobManager::restore(&store).expect("restore");
        assert!(!after.cancellation_requested(execution, node));
        assert_eq!(after.job(job).map(|j| j.state()), Some(JobState::Lost));
    }

    #[test]
    fn an_assigned_execution_is_left_for_redelivery() {
        let store = StateStore::open_in_memory().expect("store");
        let registry = registry(1);
        let mut manager = JobManager::new();
        let job = manager.submit(spec()).expect("submit");
        let execution = manager
            .schedule(job, &Scheduler::new(), &registry)
            .expect("schedule");
        let node = manager.execution(execution).expect("stored").node_id();
        let mut manager = restored(&mut manager, &store);

        assert!(
            manager
                .reconcile_node(node, &[])
                .expect("reconcile")
                .is_empty()
        );

        assert_eq!(
            manager.job(job).map(|j| j.state()),
            Some(JobState::Assigned)
        );
        assert!(
            manager
                .pending_assignment_for(node, &[])
                .expect("lookup")
                .is_some()
        );
    }

    #[test]
    fn a_retry_and_its_attempt_history_survive_a_restart() {
        let store = StateStore::open_in_memory().expect("store");
        let registry = registry(1);
        let scheduler = Scheduler::new();
        let mut manager = JobManager::new();
        let job = manager.submit(spec()).expect("submit");
        let first = manager
            .schedule(job, &scheduler, &registry)
            .expect("schedule");
        manager.accept_execution(first).expect("accept");
        manager.start_execution(first).expect("start");
        manager
            .finish_execution(first, ExecutionResult { exit_code: Some(1) })
            .expect("finish");
        manager.retry_job(job, false).expect("retry");
        let other = manager.submit(spec()).expect("submit");

        let mut after = restored(&mut manager, &store);

        assert_eq!(after.job(job).map(|j| j.state()), Some(JobState::Queued));
        assert_eq!(after.pending_position(job), Some(0));
        assert_eq!(after.pending_position(other), Some(1));
        let second = after
            .schedule(job, &scheduler, &registry)
            .expect("schedule");
        assert_eq!(
            after
                .attempts(job)
                .iter()
                .map(|e| e.id())
                .collect::<Vec<_>>(),
            vec![first, second]
        );
    }
}
