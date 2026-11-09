//! Object store access: the key scheme, building the store from the daemon's
//! environment, and the uploads the daemon makes from the spool.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tokio::io::AsyncWriteExt;
use tokio::time::{Instant, timeout_at};
use tracing::warn;

/// Object keys for one capture. Every object of a capture shares the prefix
/// `{cluster}/{podUID}/{containerID}/{timestamp}-{hostPid}`; the suffix names
/// the object. Handler-derivable from the cgroup - no UUIDs; the manifest
/// carries the human-readable identity from crictl. The host PID
/// disambiguates two crashes in the same container within the same second
/// (possible before the rate limiter engages), which would otherwise
/// overwrite each other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectKeys {
    prefix: String,
}

impl ObjectKeys {
    /// Build the key prefix for one capture:
    /// `{cluster}/{pod_uid}/{container_id}/{timestamp}-{host_pid}`.
    #[must_use]
    pub fn new(
        cluster: &str,
        pod_uid: &str,
        container_id: &str,
        timestamp: i64,
        host_pid: i32,
    ) -> Self {
        Self {
            prefix: format!("{cluster}/{pod_uid}/{container_id}/{timestamp}-{host_pid}"),
        }
    }

    /// `<prefix>-core.zst`
    #[must_use]
    pub fn core(&self) -> String {
        format!("{}-core.zst", self.prefix)
    }

    /// `<prefix>-procsnapshot.tar`
    #[must_use]
    pub fn proc_snapshot(&self) -> String {
        format!("{}-procsnapshot.tar", self.prefix)
    }

    /// `<prefix>-manifest.json`, written last (blob-first ordering).
    #[must_use]
    pub fn manifest(&self) -> String {
        format!("{}-manifest.json", self.prefix)
    }
}

/// Buffered single-shot PUT of a small object (the proc snapshot tar or the
/// manifest JSON). Unlike the multi-GB core, these are bounded and fit in
/// memory.
///
/// # Errors
///
/// Fails when the object store PUT fails.
pub async fn put_object(store: &Arc<dyn ObjectStore>, key: &str, bytes: Vec<u8>) -> Result<()> {
    store
        .put(&ObjectPath::from(key), PutPayload::from(bytes))
        .await
        .with_context(|| format!("putting object {key}"))?;
    Ok(())
}

/// Stream the file at `path` to `key`, as a multipart upload when it is large.
/// The file is never held in memory whole.
///
/// # Errors
///
/// Fails when the file cannot be read, the upload fails, or `deadline_at`
/// passes. A failed upload is aborted, except when the deadline passes while
/// the upload completes: those parts stay until the bucket's lifecycle rule
/// removes them.
pub async fn upload_file(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    key: &str,
    deadline_at: Option<Instant>,
) -> Result<()> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    let mut writer = BufWriter::new(store.clone(), ObjectPath::from(key));
    let copied = match deadline_at {
        Some(dl) => timeout_at(dl, tokio::io::copy(&mut file, &mut writer))
            .await
            .map_err(|_| anyhow!("upload deadline passed")),
        None => Ok(tokio::io::copy(&mut file, &mut writer).await),
    };
    // `BufWriter` does not abort its multipart upload on drop, and S3/GCS
    // keep (and bill) the parts of an upload nobody completes or aborts.
    // `abort` panics once `shutdown` has been polled, so abort only here.
    let copied = copied.and_then(|r| r.map_err(anyhow::Error::from));
    if let Err(e) = copied {
        if let Err(abort) = writer.abort().await {
            warn!(error = %abort, key, "aborting multipart upload failed; orphaned parts may remain in the object store");
        }
        return Err(e).with_context(|| format!("uploading {key}"));
    }
    let shutdown = match deadline_at {
        Some(dl) => timeout_at(dl, writer.shutdown())
            .await
            .map_err(|_| anyhow!("upload deadline passed while completing")),
        None => Ok(writer.shutdown().await),
    };
    shutdown
        .and_then(|r| r.map_err(anyhow::Error::from))
        .with_context(|| format!("completing upload of {key}"))
}

/// The `AWS_*` and GCP/Azure keys `object_store` recognizes. Forwarding only
/// these (an allowlist) keeps an unknown env key from making `parse_url_opts`
/// error out and needlessly disabling capture.
// The workload-identity keys are the env vars each cloud's webhook injects
// into the pod, and `object_store` passes each to a credential provider:
//
// - IRSA (EKS): the pod-identity webhook injects `AWS_ROLE_ARN` +
//   `AWS_WEB_IDENTITY_TOKEN_FILE` (+ `AWS_DEFAULT_REGION`).
//   `AmazonS3Builder` reads both and feeds them to `WebIdentityProvider`
//   (assume-role-with-web-identity).
// - AKS workload identity: the webhook injects `AZURE_CLIENT_ID`,
//   `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_AUTHORITY_HOST`.
//   `MicrosoftAzureBuilder` feeds client id + tenant id + federated token
//   file to `WorkloadIdentityOAuthProvider`.
// - GKE workload identity: `GoogleCloudStorageBuilder` falls back to the GCE
//   metadata server (`InstanceCredentialProvider`) with no env vars at all
//   when no service-account key/path is configured - nothing to add
//   to this allowlist for GCP WI to work.
//
// `AWS_STS_REGIONAL_ENDPOINTS` is not in the list: it is an AWS SDK setting,
// not an `AmazonS3ConfigKey`, so `object_store` never reads it.
pub const ALLOWED_STORE_OPTS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT",
    "AWS_ALLOW_HTTP",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
    "AWS_ROLE_ARN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "GOOGLE_SERVICE_ACCOUNT",
    "GOOGLE_SERVICE_ACCOUNT_KEY",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "AZURE_STORAGE_ACCESS_KEY",
    "AZURE_STORAGE_CLIENT_ID",
    "AZURE_STORAGE_CLIENT_SECRET",
    "AZURE_STORAGE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "AZURE_FEDERATED_TOKEN_FILE",
    "AZURE_AUTHORITY_HOST",
];

/// Collect the `object_store` config options ([`ALLOWED_STORE_OPTS`]) present
/// in the process environment. The daemon builds its object store from them;
/// they never reach the handler.
#[must_use]
pub fn store_options_from_env() -> Vec<(String, String)> {
    std::env::vars()
        .filter(|(k, _)| ALLOWED_STORE_OPTS.contains(&k.as_str()))
        .collect()
}

/// Whether the forwarded store options point at a plaintext endpoint. Cores
/// are secret-bearing, so an `http://` endpoint (or a blanket `AWS_ALLOW_HTTP`)
/// puts them on the network in the clear - acceptable for an in-cluster dev
/// store. Anywhere else the daemon logs a startup warning.
#[must_use]
pub fn is_plaintext_endpoint(opts: &[(String, String)]) -> bool {
    let get = |key: &str| {
        opts.iter()
            .find(|(k, _)| k == key)
            .map_or("", |(_, v)| v.as_str())
    };
    get("AWS_ENDPOINT").to_lowercase().starts_with("http://")
        || matches!(
            get("AWS_ALLOW_HTTP").to_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
}

/// Retry policy for one request. `object_store` defaults to 10 retries over
/// 3 minutes. The daemon's uploader retries a failed bundle with its own
/// backoff, so a request gives up sooner and frees its upload slot.
fn retry_config() -> object_store::RetryConfig {
    object_store::RetryConfig {
        max_retries: 3,
        retry_timeout: std::time::Duration::from_mins(1),
        ..Default::default()
    }
}

/// Build an object store from a store URL (e.g. `s3://crash-artifacts`) plus
/// `object_store` config options. `None` when the URL is invalid or the store
/// can't be built; the reason is logged.
///
/// Dispatches on scheme rather than `parse_url_opts` because retry config is
/// builder-only; opts are folded the same way `parse_url_opts` does (lowercase
/// key -> config key, unknown keys skipped).
pub fn object_store_from_url_opts(
    raw_url: &str,
    opts: Vec<(String, String)>,
) -> Option<Arc<dyn ObjectStore>> {
    let url = match url::Url::parse(raw_url) {
        Ok(u) => u,
        Err(e) => {
            warn!(error = %e, url = %raw_url, "store url is not a valid URL");
            return None;
        }
    };
    let scheme = match object_store::ObjectStoreScheme::parse(&url) {
        Ok((scheme, _path)) => scheme,
        Err(e) => {
            warn!(error = %e, url = %raw_url, "unrecognized store url scheme");
            return None;
        }
    };
    let built: object_store::Result<Arc<dyn ObjectStore>> = match scheme {
        object_store::ObjectStoreScheme::AmazonS3 => {
            let mut builder = object_store::aws::AmazonS3Builder::new()
                .with_url(url.to_string())
                .with_retry(retry_config());
            for (k, v) in opts {
                if let Ok(key) = k.to_ascii_lowercase().parse() {
                    builder = builder.with_config(key, v);
                }
            }
            builder.build().map(|s| Arc::new(s) as _)
        }
        object_store::ObjectStoreScheme::GoogleCloudStorage => {
            let mut builder = object_store::gcp::GoogleCloudStorageBuilder::new()
                .with_url(url.to_string())
                .with_retry(retry_config());
            for (k, v) in opts {
                if let Ok(key) = k.to_ascii_lowercase().parse() {
                    builder = builder.with_config(key, v);
                }
            }
            builder.build().map(|s| Arc::new(s) as _)
        }
        object_store::ObjectStoreScheme::MicrosoftAzure => {
            let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
                .with_url(url.to_string())
                .with_retry(retry_config());
            for (k, v) in opts {
                if let Ok(key) = k.to_ascii_lowercase().parse() {
                    builder = builder.with_config(key, v);
                }
            }
            builder.build().map(|s| Arc::new(s) as _)
        }
        // Local backends: no HTTP, no retry policy to apply.
        object_store::ObjectStoreScheme::Memory => {
            Ok(Arc::new(object_store::memory::InMemory::new()) as _)
        }
        object_store::ObjectStoreScheme::Local => {
            object_store::local::LocalFileSystem::new_with_prefix(url.path())
                .map(|s| Arc::new(s) as _)
        }
        other => {
            warn!(scheme = ?other, url = %raw_url, "unsupported store url scheme");
            return None;
        }
    };
    match built {
        Ok(store) => Some(store),
        Err(e) => {
            warn!(error = %e, url = %raw_url, "building object store failed");
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::testutil::ENV_LOCK;
    use object_store::memory::InMemory;

    /// The Helm chart hard-codes a copy of [`ALLOWED_STORE_OPTS`] so it can
    /// `fail` on a typo'd key at render time. Nothing links the two lists, so
    /// this test keeps the copy in sync: without it, adding a key here
    /// makes the chart reject a legitimate value.
    #[test]
    fn helpers_store_opts_match_allowlist() {
        const HELPERS: &str = include_str!("../charts/coredrop/templates/_helpers.tpl");

        let block = HELPERS
            .split_once(r#"{{- define "coredrop.allowedStoreOpts" -}}"#)
            .expect("_helpers.tpl defines coredrop.allowedStoreOpts")
            .1
            .split_once("{{- end -}}")
            .expect("the allowedStoreOpts define is terminated")
            .0;

        let chart: std::collections::BTreeSet<&str> = block
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect();
        let rust: std::collections::BTreeSet<&str> = ALLOWED_STORE_OPTS.iter().copied().collect();

        assert_eq!(
            chart, rust,
            "coredrop.allowedStoreOpts in _helpers.tpl drifted from ALLOWED_STORE_OPTS"
        );
    }

    #[test]
    fn builds_object_keys() {
        assert_eq!(
            ObjectKeys::new("prod", "pod-uid-123", "abc123def", 1_749_600_000, 4242).core(),
            "prod/pod-uid-123/abc123def/1749600000-4242-core.zst"
        );
        assert_eq!(
            ObjectKeys::new("prod", "pod-uid-123", "abc123def", 1_749_600_000, 4242)
                .proc_snapshot(),
            "prod/pod-uid-123/abc123def/1749600000-4242-procsnapshot.tar"
        );
        assert_eq!(
            ObjectKeys::new("prod", "pod-uid-123", "abc123def", 1_749_600_000, 4242).manifest(),
            "prod/pod-uid-123/abc123def/1749600000-4242-manifest.json"
        );
    }

    #[test]
    fn distinct_pids_yield_distinct_keys_within_the_same_second() {
        let a = ObjectKeys::new("prod", "pod-uid-123", "abc123def", 1_749_600_000, 111).core();
        let b = ObjectKeys::new("prod", "pod-uid-123", "abc123def", 1_749_600_000, 222).core();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn put_object_round_trips_a_buffered_blob() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let key = ObjectKeys::new("local", "pod-a", "cid-b", 7, 4242).proc_snapshot();
        let tar = b"a-small-tar-bundle".to_vec();
        put_object(&store, &key, tar.clone()).await.unwrap();

        let stored = store
            .get(&ObjectPath::from(key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(stored.as_ref(), tar.as_slice());
    }

    #[test]
    fn object_store_from_url_opts_dispatches_by_scheme() {
        // Unknown option keys are skipped rather than failing the build, so a
        // stray env var can never disable capture.
        let s3_opts = vec![
            ("AWS_ACCESS_KEY_ID".to_string(), "ak".to_string()),
            ("AWS_SECRET_ACCESS_KEY".to_string(), "sk".to_string()),
            ("AWS_REGION".to_string(), "us-east-1".to_string()),
            ("NOT_A_REAL_KEY".to_string(), "ignored".to_string()),
        ];
        assert!(object_store_from_url_opts("s3://some-bucket", s3_opts).is_some());
        assert!(object_store_from_url_opts("memory:///", vec![]).is_some());

        // Not a URL, and a URL whose scheme has no backend.
        assert!(object_store_from_url_opts("not a url", vec![]).is_none());
        assert!(object_store_from_url_opts("bogus://x", vec![]).is_none());
    }

    #[test]
    fn store_options_from_env_forwards_workload_identity_keys() {
        let _guard = ENV_LOCK.lock().unwrap();
        let wi_keys = [
            "AWS_ROLE_ARN",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_DEFAULT_REGION",
            "AZURE_CLIENT_ID",
            "AZURE_TENANT_ID",
            "AZURE_FEDERATED_TOKEN_FILE",
            "AZURE_AUTHORITY_HOST",
        ];
        // SAFETY: every env-mutating test in this binary holds ENV_LOCK, so no
        // other thread reads or writes the environment here.
        unsafe {
            for key in wi_keys {
                std::env::set_var(key, "test-value");
            }
            std::env::set_var("NOT_AN_ALLOWED_KEY", "leak-me-not");
        }

        let opts: std::collections::HashMap<_, _> = store_options_from_env().into_iter().collect();

        // SAFETY: as above.
        unsafe {
            for key in wi_keys {
                assert_eq!(opts.get(key).map(String::as_str), Some("test-value"));
                std::env::remove_var(key);
            }
            std::env::remove_var("NOT_AN_ALLOWED_KEY");
        }
        assert!(!opts.contains_key("NOT_AN_ALLOWED_KEY"));
    }

    #[test]
    fn plaintext_endpoint_detection() {
        let opts = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect()
        };

        assert!(is_plaintext_endpoint(&opts(&[(
            "AWS_ENDPOINT",
            "http://minio:9000"
        )])));
        assert!(is_plaintext_endpoint(&opts(&[("AWS_ALLOW_HTTP", "true")])));
        assert!(!is_plaintext_endpoint(&opts(&[(
            "AWS_ENDPOINT",
            "https://s3.example.com"
        )])));
        // Explicitly disabled is not a plaintext endpoint.
        assert!(!is_plaintext_endpoint(&opts(&[(
            "AWS_ALLOW_HTTP",
            "false"
        )])));
        assert!(!is_plaintext_endpoint(&[]));
    }
}
