//! The k8s Event objects: the reason for each outcome, the Event name,
//! body and `series` patch.

use serde_json::Value;

use crate::events::{CaptureEventPayload, Outcome};

use super::EventTarget;

/// The k8s Event `reason` a capture produces.
// Variant names match the `reason` strings users filter on.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Reason {
    CoreDumped,
    CoreDumpSuppressed,
    CoreDumpSpoolFull,
    CoreDumpDrainDeadline,
    CoreDiscardedNoStore,
}

impl Reason {
    /// Map a capture outcome to its Event reason. Outcomes with no Event
    /// (`spooled`, which is not final, `skipped-non-k8s`, `failed`) yield
    /// `None` - the caller drops the payload.
    pub(super) fn from_outcome(outcome: Outcome) -> Option<Self> {
        match outcome {
            Outcome::Uploaded => Some(Self::CoreDumped),
            Outcome::SuppressedRateLimit => Some(Self::CoreDumpSuppressed),
            Outcome::SpoolFull => Some(Self::CoreDumpSpoolFull),
            Outcome::DrainDeadline => Some(Self::CoreDumpDrainDeadline),
            Outcome::NoStoreDiscard => Some(Self::CoreDiscardedNoStore),
            Outcome::Spooled | Outcome::SkippedNonK8s | Outcome::Filtered | Outcome::Failed => None,
        }
    }

    /// The `reason` field value.
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::CoreDumped => "CoreDumped",
            Self::CoreDumpSuppressed => "CoreDumpSuppressed",
            Self::CoreDumpSpoolFull => "CoreDumpSpoolFull",
            Self::CoreDumpDrainDeadline => "CoreDumpDrainDeadline",
            Self::CoreDiscardedNoStore => "CoreDiscardedNoStore",
        }
    }

    /// Lowercase form for Event object names, which must be DNS subdomains.
    pub(super) fn slug(self) -> &'static str {
        match self {
            Self::CoreDumped => "coredumped",
            Self::CoreDumpSuppressed => "coredumpsuppressed",
            Self::CoreDumpSpoolFull => "coredumpspoolfull",
            Self::CoreDumpDrainDeadline => "coredumpdraindeadline",
            Self::CoreDiscardedNoStore => "corediscardednostore",
        }
    }
}

/// DNS-1123-subdomain-safe Event name: pod UID is already a lowercase UUID,
/// the reason lowercased has no invalid characters, and the timestamp keeps
/// re-created (post-TTL-expiry) entries for the same pod+reason unique.
pub(super) fn event_name(pod_uid: &str, reason: Reason, timestamp: i64) -> String {
    format!("coredrop-{pod_uid}-{}-{timestamp}", reason.slug())
}

pub(super) fn event_body(
    node: &str,
    name: &str,
    reason: Reason,
    target: &EventTarget<'_>,
    payload: &CaptureEventPayload,
) -> Value {
    serde_json::json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {
            "name": name,
            "namespace": target.namespace,
        },
        "eventTime": rfc3339_micro(payload.timestamp),
        "reportingController": "coredrop",
        "reportingInstance": node,
        "action": "CoreCaptured",
        "reason": reason.as_str(),
        "regarding": {
            "kind": "Pod",
            "apiVersion": "v1",
            "namespace": target.namespace,
            "name": target.pod_name,
            "uid": target.pod_uid,
        },
        "note": note(payload),
        "type": "Warning",
    })
}

pub(super) fn series_patch(count: u32, last_observed: &str) -> Value {
    serde_json::json!({
        "series": {
            "count": count,
            "lastObservedTime": last_observed,
        }
    })
}

pub(super) fn note(payload: &CaptureEventPayload) -> String {
    let signal = payload
        .signal_name
        .clone()
        .unwrap_or_else(|| payload.signal.to_string());
    match &payload.manifest_key {
        Some(key) => format!("core dumped (signal {signal}); artifacts at {key}"),
        None => format!("core dumped (signal {signal})"),
    }
}

/// Format as `metav1.MicroTime` expects: exactly 6 fractional-second digits
/// and a literal `Z`. Go's JSON decoder for `MicroTime` parses against a
/// fixed-width reference layout (`.000000`), so a value with zero or a
/// different number of fractional digits - which plain `to_rfc3339()`
/// produces for a whole-second timestamp - fails to decode and the API
/// server 400s the request.
pub(super) fn rfc3339_micro(epoch_secs: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(epoch_secs, 0).map_or_else(
        || epoch_secs.to_string(),
        |dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::tests::payload;
    use super::*;

    #[test]
    fn reason_maps_only_reportable_outcomes() {
        let cases = [
            (Outcome::Uploaded, "CoreDumped"),
            (Outcome::SuppressedRateLimit, "CoreDumpSuppressed"),
            (Outcome::SpoolFull, "CoreDumpSpoolFull"),
            (Outcome::DrainDeadline, "CoreDumpDrainDeadline"),
            (Outcome::NoStoreDiscard, "CoreDiscardedNoStore"),
        ];
        for (outcome, reason) in cases {
            let got = Reason::from_outcome(outcome).unwrap();
            assert_eq!(got.as_str(), reason);
            // The name slug is the reason lowercased.
            assert_eq!(got.slug(), reason.to_lowercase());
        }
        // No Event: `spooled` is not final, `skipped-non-k8s` has no pod to
        // target, and a failed capture has no artifacts to point at.
        assert_eq!(Reason::from_outcome(Outcome::Spooled), None);
        assert_eq!(Reason::from_outcome(Outcome::SkippedNonK8s), None);
        assert_eq!(Reason::from_outcome(Outcome::Filtered), None);
        assert_eq!(Reason::from_outcome(Outcome::Failed), None);
    }

    /// A whole-second timestamp must still have six fractional digits - see
    /// `rfc3339_micro`.
    #[test]
    fn rfc3339_micro_always_has_six_fractional_digits() {
        assert_eq!(rfc3339_micro(1_749_600_000), "2025-06-11T00:00:00.000000Z");
    }

    #[test]
    fn event_name_is_dns_subdomain_safe() {
        let name = event_name("abc-123-def", Reason::CoreDumped, 1_749_600_000);
        assert_eq!(name, "coredrop-abc-123-def-coredumped-1749600000");
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        );
    }

    /// The body is what the API server validates; a wrong field name or a
    /// mistyped `eventTime` is a 400 that only shows up in production.
    #[test]
    fn event_body_targets_the_pod_and_carries_a_decodable_event_time() {
        let payload = payload("default", "abc-123");
        let target = EventTarget {
            namespace: "default",
            pod_name: "my-pod",
            pod_uid: "abc-123",
        };
        let name = event_name(target.pod_uid, Reason::CoreDumped, payload.timestamp);
        let body = event_body("node-a", &name, Reason::CoreDumped, &target, &payload);

        assert_eq!(body["apiVersion"], "events.k8s.io/v1");
        assert_eq!(body["kind"], "Event");
        assert_eq!(body["metadata"]["name"], name.as_str());
        assert_eq!(body["metadata"]["namespace"], "default");
        assert_eq!(body["reportingController"], "coredrop");
        assert_eq!(body["reportingInstance"], "node-a");
        assert_eq!(body["reason"], "CoreDumped");
        assert_eq!(body["type"], "Warning");
        assert_eq!(body["eventTime"], "2025-06-11T00:00:00.000000Z");

        // `regarding` is what makes this show up under `kubectl describe pod`.
        assert_eq!(body["regarding"]["kind"], "Pod");
        assert_eq!(body["regarding"]["apiVersion"], "v1");
        assert_eq!(body["regarding"]["namespace"], "default");
        assert_eq!(body["regarding"]["name"], "my-pod");
        assert_eq!(body["regarding"]["uid"], "abc-123");
    }

    #[test]
    fn note_points_at_the_artifacts_when_a_manifest_was_written() {
        let mut payload = payload("default", "abc-123");
        assert_eq!(
            note(&payload),
            "core dumped (signal SIGSEGV); artifacts at local/abc-123/def456/1234567890-manifest.json"
        );

        // Manifest write failed: the note still reports the crash.
        payload.manifest_key = None;
        assert_eq!(note(&payload), "core dumped (signal SIGSEGV)");

        // Unmapped signal number: fall back to the raw number, never omit it.
        payload.signal = 31;
        payload.signal_name = None;
        assert_eq!(note(&payload), "core dumped (signal 31)");
    }

    #[test]
    fn series_patch_carries_the_running_count() {
        let body = series_patch(7, "2025-06-11T00:00:00.000000Z");
        assert_eq!(body["series"]["count"], 7);
        assert_eq!(
            body["series"]["lastObservedTime"],
            "2025-06-11T00:00:00.000000Z"
        );
    }
}
