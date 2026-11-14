//! Event aggregation: a repeated crash bumps the `series` count of one
//! Event instead of creating a new Event each time.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tracing::warn;

use super::event::Reason;

/// Aggregation TTL: bounds how long a crash-loop's Event `series` count keeps
/// growing before a fresh Event (and count) starts. Matches the API server's
/// default Event TTL, so this never outlives the object it's tracking.
pub(super) const AGGREGATION_TTL: Duration = Duration::from_hours(1);

/// Aggregation key: repeats of the same (pod, reason) bump a `series` count
/// on one Event object instead of creating a new one per crash.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct AggKey {
    pub(super) namespace: String,
    pub(super) pod_uid: String,
    pub(super) reason: Reason,
}

pub(super) struct AggEntry {
    pub(super) event_name: String,
    pub(super) count: u32,
    pub(super) created_at: Instant,
}

/// In-memory `(namespace, pod_uid, reason) -> Event` aggregation map. Entries
/// older than [`AGGREGATION_TTL`] are dropped and re-created rather than kept
/// alive forever - simpler than tracking the API server's actual Event TTL.
#[derive(Default)]
pub(super) struct Aggregator {
    pub(super) entries: HashMap<AggKey, AggEntry>,
}

/// Maximum number of distinct `(namespace, pod_uid, reason)` aggregation
/// keys held in memory. Beyond this, new keys are dropped rather than
/// allowing unbounded growth.
pub(super) const MAX_AGGREGATION_KEYS: usize = 10_000;

/// How often the aggregator is swept for expired entries, even when no new
/// capture events arrive. Quiet daemons must not hold stale entries forever.
pub(super) const AGGREGATION_SWEEP_INTERVAL: Duration = Duration::from_mins(5);

/// Outcome of recording one capture against the aggregator.
pub(super) struct Recorded {
    pub(super) event_name: String,
    pub(super) is_new: bool,
    pub(super) count: u32,
}

impl Aggregator {
    pub(super) fn prune(&mut self, now: Instant) {
        self.entries
            .retain(|_, e| now.duration_since(e.created_at) < AGGREGATION_TTL);
    }

    /// Sweep expired entries. Public so `run_listener` can call it on a
    /// periodic timer even when no events are being recorded.
    pub(super) fn sweep(&mut self, now: Instant) {
        self.prune(now);
    }

    /// Record one occurrence for `key`. `make_name` is only called when a new
    /// entry is created (first occurrence, or the previous one expired).
    /// Returns `None` when the in-memory key cap has been reached and `key`
    /// is not already tracked.
    pub(super) fn record(
        &mut self,
        key: AggKey,
        now: Instant,
        make_name: impl FnOnce() -> String,
    ) -> Option<Recorded> {
        self.prune(now);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.count += 1;
            return Some(Recorded {
                event_name: entry.event_name.clone(),
                is_new: false,
                count: entry.count,
            });
        }
        if self.entries.len() >= MAX_AGGREGATION_KEYS {
            warn!(
                cap = MAX_AGGREGATION_KEYS,
                "aggregator key cap reached; dropping capture event"
            );
            return None;
        }
        let event_name = make_name();
        self.entries.insert(
            key,
            AggEntry {
                event_name: event_name.clone(),
                count: 1,
                created_at: now,
            },
        );
        Some(Recorded {
            event_name,
            is_new: true,
            count: 1,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn agg_key(pod_uid: &str, reason: Reason) -> AggKey {
        AggKey {
            namespace: "default".into(),
            pod_uid: pod_uid.into(),
            reason,
        }
    }

    #[test]
    fn aggregator_bumps_count_for_repeats_within_ttl() {
        let mut agg = Aggregator::default();
        let key = agg_key("abc-123", Reason::CoreDumped);
        let t0 = Instant::now();

        let first = agg.record(key.clone(), t0, || "ev-1".to_string()).unwrap();
        assert!(first.is_new);
        assert_eq!(first.count, 1);

        let second = agg
            .record(key.clone(), t0 + Duration::from_secs(10), || {
                "ev-2".to_string()
            })
            .unwrap();
        assert!(!second.is_new);
        assert_eq!(second.count, 2);
        assert_eq!(
            second.event_name, first.event_name,
            "a repeat must patch the original Event, not name a new one"
        );

        let third = agg
            .record(key, t0 + Duration::from_secs(20), || "ev-3".to_string())
            .unwrap();
        assert!(!third.is_new);
        assert_eq!(third.count, 3);
    }

    #[test]
    fn aggregator_separates_distinct_keys() {
        let mut agg = Aggregator::default();
        let t0 = Instant::now();
        // Different pod, and same pod with a different reason: both are their
        // own Event series.
        assert!(
            agg.record(agg_key("pod-a", Reason::CoreDumped), t0, || "a".into())
                .unwrap()
                .is_new
        );
        assert!(
            agg.record(agg_key("pod-b", Reason::CoreDumped), t0, || "b".into())
                .unwrap()
                .is_new
        );
        assert!(
            agg.record(agg_key("pod-a", Reason::CoreDumpSuppressed), t0, || "c"
                .into())
                .unwrap()
                .is_new
        );
    }

    #[test]
    fn aggregator_re_creates_after_ttl_expiry() {
        let mut agg = Aggregator::default();
        let key = agg_key("abc-123", Reason::CoreDumped);
        let t0 = Instant::now();
        assert!(
            agg.record(key.clone(), t0, || "ev-1".to_string())
                .unwrap()
                .is_new
        );

        let after_ttl = t0 + AGGREGATION_TTL + Duration::from_secs(1);
        let renewed = agg.record(key, after_ttl, || "ev-2".to_string()).unwrap();
        assert!(renewed.is_new, "entry past its TTL should be re-created");
        assert_eq!(renewed.count, 1);
        assert_eq!(renewed.event_name, "ev-2");
    }

    #[test]
    fn aggregator_stops_inserting_new_keys_at_cap() {
        let mut agg = Aggregator::default();
        let t0 = Instant::now();

        for i in 0..MAX_AGGREGATION_KEYS {
            let recorded = agg
                .record(agg_key(&format!("pod-{i}"), Reason::CoreDumped), t0, || {
                    format!("ev-{i}")
                })
                .unwrap();
            assert!(recorded.is_new);
        }
        assert_eq!(agg.entries.len(), MAX_AGGREGATION_KEYS);

        assert!(
            agg.record(agg_key("overflow", Reason::CoreDumped), t0, || "overflow"
                .into())
                .is_none()
        );
        assert_eq!(agg.entries.len(), MAX_AGGREGATION_KEYS);

        // Repeating an existing key still bumps its count.
        let recorded = agg
            .record(agg_key("pod-0", Reason::CoreDumped), t0, || {
                "should-not-run".into()
            })
            .unwrap();
        assert!(!recorded.is_new);
        assert_eq!(recorded.count, 2);
    }

    #[test]
    fn aggregator_sweep_removes_expired_entries() {
        // The sweep exists for quiet daemons: without it, entries only expire
        // when a new capture happens to call `record`.
        let mut agg = Aggregator::default();
        let key = agg_key("abc-123", Reason::CoreDumped);
        let t0 = Instant::now();
        agg.record(key.clone(), t0, || "ev-1".into()).unwrap();
        assert_eq!(agg.entries.len(), 1);

        let after_ttl = t0 + AGGREGATION_TTL + Duration::from_secs(1);
        agg.sweep(after_ttl);
        assert!(agg.entries.is_empty());

        let recorded = agg.record(key, after_ttl, || "ev-2".into()).unwrap();
        assert!(recorded.is_new);
        assert_eq!(recorded.event_name, "ev-2");
    }
}
