//! Object store access: building the store from the daemon's environment,
//! and the uploads the daemon makes from the spool. The key scheme is in
//! [`crate::keys`].

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::warn;

/// Tells uploads in progress to stop. Once the value is `true`, a running
/// [`upload_file`] aborts its multipart upload and returns an error.
pub type Cancel = watch::Receiver<bool>;

/// Run `fut` until it completes, `deadline_at` passes or `cancel` turns true.
/// `what` names the step in the error.
async fn bounded<T>(
    fut: impl std::future::Future<Output = T>,
    deadline_at: Option<Instant>,
    cancel: &mut Cancel,
    what: &str,
) -> Result<T> {
    let deadline = async {
        match deadline_at {
            Some(dl) => tokio::time::sleep_until(dl).await,
            None => std::future::pending().await,
        }
    };
    let cancelled = async {
        // An error means the sender is gone: nothing can stop the upload
        // then, so let it run.
        if cancel.wait_for(|stop| *stop).await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        out = fut => Ok(out),
        () = deadline => Err(anyhow!("upload deadline passed while {what}")),
        () = cancelled => Err(anyhow!("upload stopped while {what}")),
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
/// Fails when the file cannot be read, the upload fails, `deadline_at`
/// passes or `cancel` turns true. A failed upload is aborted, except when it
/// fails while the upload completes: those parts stay until the bucket's
/// lifecycle rule removes them.
pub async fn upload_file(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    key: &str,
    deadline_at: Option<Instant>,
    cancel: &mut Cancel,
) -> Result<()> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    let mut writer = BufWriter::new(store.clone(), ObjectPath::from(key));
    let copied = bounded(
        tokio::io::copy(&mut file, &mut writer),
        deadline_at,
        cancel,
        "sending",
    )
    .await;
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
    bounded(writer.shutdown(), deadline_at, cancel, "completing")
        .await
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
/// `object_store` config options.
///
/// Dispatches on scheme rather than `parse_url_opts` because retry config is
/// builder-only; opts are folded the same way `parse_url_opts` does (lowercase
/// key -> config key, unknown keys skipped).
///
/// # Errors
///
/// Fails when the URL is invalid, its scheme has no supported store, or the
/// store cannot be built.
pub fn object_store_from_url_opts(
    raw_url: &str,
    opts: Vec<(String, String)>,
) -> Result<Arc<dyn ObjectStore>> {
    use object_store::ObjectStoreScheme as Scheme;
    use object_store::{aws, azure, gcp};

    let url = url::Url::parse(raw_url).context("the store URL is not a valid URL")?;
    let (scheme, _path) =
        Scheme::parse(&url).context("the store URL scheme has no object store")?;
    let url_str = url.to_string();
    let store: Arc<dyn ObjectStore> = match scheme {
        Scheme::AmazonS3 => {
            let builder = aws::AmazonS3Builder::new()
                .with_url(url_str)
                .with_retry(retry_config());
            Arc::new(with_opts(builder, opts, aws::AmazonS3Builder::with_config).build()?)
        }
        Scheme::GoogleCloudStorage => {
            let builder = gcp::GoogleCloudStorageBuilder::new()
                .with_url(url_str)
                .with_retry(retry_config());
            Arc::new(with_opts(builder, opts, gcp::GoogleCloudStorageBuilder::with_config).build()?)
        }
        Scheme::MicrosoftAzure => {
            let builder = azure::MicrosoftAzureBuilder::new()
                .with_url(url_str)
                .with_retry(retry_config());
            Arc::new(with_opts(builder, opts, azure::MicrosoftAzureBuilder::with_config).build()?)
        }
        // No HTTP, no retry policy to apply.
        Scheme::Local => Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
            url.path(),
        )?),
        // `memory://` is not offered: it would keep every core in the
        // daemon's RAM and lose it on exit.
        other => bail!("the store URL scheme {other:?} is not supported"),
    };
    Ok(store)
}

/// Apply each option whose lowercase key names a config key of the builder;
/// skip the rest.
fn with_opts<B, K: std::str::FromStr>(
    builder: B,
    opts: Vec<(String, String)>,
    set: fn(B, K, String) -> B,
) -> B {
    opts.into_iter()
        .fold(builder, |b, (k, v)| match k.to_ascii_lowercase().parse() {
            Ok(key) => set(b, key, v),
            Err(_) => b,
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::keys::ObjectKeys;
    use crate::testutil::ENV_LOCK;
    use object_store::memory::InMemory;

    /// A store whose multipart part uploads never finish. Counts aborts.
    #[derive(Debug, Default)]
    struct StalledParts {
        aborts: Arc<std::sync::atomic::AtomicUsize>,
        started: Arc<tokio::sync::Notify>,
    }

    impl std::fmt::Display for StalledParts {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "StalledParts")
        }
    }

    #[derive(Debug)]
    struct StalledUpload {
        aborts: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl object_store::MultipartUpload for StalledUpload {
        fn put_part(&mut self, _data: PutPayload) -> object_store::UploadPart {
            Box::pin(std::future::pending())
        }

        async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
            std::future::pending().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for StalledParts {
        async fn put_opts(
            &self,
            _location: &ObjectPath,
            _payload: PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            std::future::pending().await
        }

        async fn put_multipart_opts(
            &self,
            _location: &ObjectPath,
            _opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.started.notify_one();
            Ok(Box::new(StalledUpload {
                aborts: self.aborts.clone(),
            }))
        }

        async fn get_opts(
            &self,
            _location: &ObjectPath,
            _options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }

        fn delete_stream(
            &self,
            _locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            unimplemented!()
        }

        fn list(
            &self,
            _prefix: Option<&ObjectPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            unimplemented!()
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            unimplemented!()
        }

        async fn copy_opts(
            &self,
            _from: &ObjectPath,
            _to: &ObjectPath,
            _options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn a_stopped_upload_aborts_its_multipart_upload() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("core.zst");
        // Larger than `BufWriter`'s buffer, so the upload goes multipart.
        std::fs::write(&path, vec![0u8; 12 << 20]).unwrap();
        let stalled = StalledParts::default();
        let aborts = stalled.aborts.clone();
        let started = stalled.started.clone();
        let store: Arc<dyn ObjectStore> = Arc::new(stalled);
        let (stop, mut cancel) = watch::channel(false);

        let upload =
            tokio::spawn(
                async move { upload_file(&store, &path, "core.zst", None, &mut cancel).await },
            );
        started.notified().await;
        stop.send(true).unwrap();

        let err = upload.await.unwrap().unwrap_err();
        assert!(format!("{err:#}").contains("upload stopped"), "{err:#}");
        assert_eq!(aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// The Helm chart hard-codes a copy of [`ALLOWED_STORE_OPTS`] so it can
    /// `fail` on a typo'd key at render time. Nothing links the two lists, so
    /// this test keeps the copy in sync: without it, adding a key here
    /// makes the chart reject a legitimate value.
    #[test]
    fn chart_schema_store_keys_match_allowlist() {
        const SCHEMA: &str = include_str!("../../charts/coredrop/values.schema.json");
        let schema: serde_json::Value = serde_json::from_str(SCHEMA).unwrap();
        let keys = |def: &str| -> std::collections::BTreeSet<String> {
            schema["definitions"][def]["enum"]
                .as_array()
                .unwrap_or_else(|| panic!("values.schema.json defines {def}"))
                .iter()
                .map(|k| k.as_str().unwrap().to_string())
                .collect()
        };
        let config = keys("storeConfigKey");
        let secret = keys("storeSecretKey");
        assert!(config.is_disjoint(&secret), "a key is both secret and not");
        let chart: std::collections::BTreeSet<String> = config.union(&secret).cloned().collect();
        let rust: std::collections::BTreeSet<String> =
            ALLOWED_STORE_OPTS.iter().map(ToString::to_string).collect();
        assert_eq!(
            chart, rust,
            "the store keys in values.schema.json drifted from ALLOWED_STORE_OPTS"
        );
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
        assert!(object_store_from_url_opts("s3://some-bucket", s3_opts).is_ok());

        // Not a URL, a URL whose scheme has no backend, and RAM.
        assert!(object_store_from_url_opts("not a url", vec![]).is_err());
        assert!(object_store_from_url_opts("memory:///", vec![]).is_err());
        assert!(object_store_from_url_opts("bogus://x", vec![]).is_err());
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
