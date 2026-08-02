//! `environ` and `cmdline` redaction: redact-by-default with a curated keyword
//! list plus a value-shape/entropy heuristic, `--no-redact` opts into raw.
//!
//! `/proc/<pid>/environ` is NUL-separated `KEY=VALUE` entries. We redact an
//! entry's value (never the key) when either the key matches a curated
//! secret keyword *or* the value looks secret-shaped (high-entropy token, JWT,
//! or PEM block). The match is deliberately not a greedy substring over
//! the whole entry: `API_TIMEOUT=30` must survive while `API_KEY=...` must not.
//!
//! Values that parse as URLs get a second pass ([`Redactor::redact_url`]):
//! userinfo passwords, a lone high-entropy userinfo token, and query-parameter
//! values under secret-keyword names are rewritten in place, so
//! `DATABASE_URL=postgres://u:pw@h/db?password=x` leaks neither.
//!
//! `/proc/<pid>/cmdline` is NUL-separated argv and gets the same per-entry
//! treatment for `key=value`-shaped arguments, plus a lookahead rule: an
//! argument shaped `--secret-keyword` redacts the argument that follows it.
//! cmdline redaction is deliberately best-effort and biased to over-redact -
//! argv has no schema, so `--keyspace default` loses its value rather than
//! risk `--key <token>` keeping one. Positional arguments are left alone.
//!
//! Cores are secret-bearing regardless - this only governs the small `environ`
//! and `cmdline` blobs that travel in the `/proc` snapshot.

const REDACTED: &str = "<redacted>";

const DEFAULT_KEYWORDS: &[&str] = &[
    "PASSWORD",
    "PASSWD",
    "PASS",
    "PWD",
    "SECRET",
    "TOKEN",
    "KEY",
    "CRED",
    "AUTH",
    "PRIVATE",
    "SALT",
    "PIN",
    "CERT",
    "SSH",
    "GPG",
    "SESSION",
    "COOKIE",
    "BEARER",
    "SIGNATURE",
    "DSN",
];

const MIN_HEURISTIC_LEN: usize = 20;
const ENTROPY_BITS: f64 = 3.5;

/// Decides which `environ` values to redact. `enabled: false` is the
/// `--no-redact` passthrough.
#[derive(Debug, Clone)]
pub struct Redactor {
    enabled: bool,
    keywords: Vec<String>,
}

impl Default for Redactor {
    fn default() -> Self {
        Self {
            enabled: true,
            keywords: DEFAULT_KEYWORDS
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
        }
    }
}

impl Redactor {
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            keywords: Vec::new(),
        }
    }

    /// Redact secret values in a raw `/proc/<pid>/environ` blob.
    #[must_use]
    pub fn redact_environ(&self, raw: &[u8]) -> Vec<u8> {
        if !self.enabled {
            return raw.to_vec();
        }
        let mut out = Vec::with_capacity(raw.len());
        for entry in raw.split(|&b| b == 0) {
            if entry.is_empty() {
                continue;
            }
            out.extend_from_slice(&self.redact_entry(entry));
            out.push(0);
        }
        out
    }

    /// Redact secrets in a raw `/proc/<pid>/cmdline` blob (NUL-separated argv).
    ///
    /// Best-effort: `key=value` arguments get the same treatment as `environ`
    /// entries, and an argument that is itself a secret-keyword flag redacts
    /// the argument after it. Positionals are untouched.
    #[must_use]
    pub fn redact_cmdline(&self, raw: &[u8]) -> Vec<u8> {
        if !self.enabled {
            return raw.to_vec();
        }
        let mut out = Vec::with_capacity(raw.len());
        let mut redact_next = false;
        for arg in raw.split(|&b| b == 0) {
            if arg.is_empty() {
                continue;
            }
            if redact_next {
                out.extend_from_slice(REDACTED.as_bytes());
                redact_next = false;
            } else {
                redact_next = self.is_secret_flag(arg);
                out.extend_from_slice(&self.redact_entry(arg));
            }
            out.push(0);
        }
        out
    }

    fn redact_entry(&self, entry: &[u8]) -> Vec<u8> {
        let Ok(text) = std::str::from_utf8(entry) else {
            return entry.to_vec();
        };
        let Some((key, value)) = text.split_once('=') else {
            return entry.to_vec();
        };
        if self.key_is_secret(key) || looks_secret_value(value) {
            return format!("{key}={REDACTED}").into_bytes();
        }
        if let Some(rewritten) = self.redact_url(value) {
            return format!("{key}={rewritten}").into_bytes();
        }
        entry.to_vec()
    }

    /// A valueless flag whose name matches a secret keyword (`--password`), so
    /// the *next* argv entry is its value and must go.
    fn is_secret_flag(&self, arg: &[u8]) -> bool {
        let Ok(text) = std::str::from_utf8(arg) else {
            return false;
        };
        text.starts_with('-') && !text.contains('=') && self.key_is_secret(text)
    }

    fn key_is_secret(&self, key: &str) -> bool {
        let upper = key.to_ascii_uppercase();
        self.keywords.iter().any(|kw| upper.contains(kw.as_str()))
    }

    /// Redact credentials inside a `scheme://...` URL: a userinfo password, a
    /// userinfo that is itself a secret-shaped token, and query-parameter
    /// values whose name matches a secret keyword. Returns `None` when the
    /// value isn't a URL or holds nothing worth redacting.
    fn redact_url(&self, value: &str) -> Option<String> {
        let scheme_end = value.find("://")?;
        let scheme = &value[..scheme_end];
        let rest = &value[scheme_end + 3..];
        let auth_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let authority = &rest[..auth_end];
        let tail = &rest[auth_end..];

        let mut changed = false;
        let authority = match authority.split_once('@') {
            // `user:pass@host` - keep the user, drop the password.
            Some((userinfo, host)) => match userinfo.split_once(':') {
                Some((user, password)) if !password.is_empty() => {
                    changed = true;
                    format!("{user}:{REDACTED}@{host}")
                }
                // `token@host` - no password field, but a long high-entropy
                // userinfo is a bearer credential in disguise.
                None if looks_secret_value(userinfo) => {
                    changed = true;
                    format!("{REDACTED}@{host}")
                }
                _ => authority.to_string(),
            },
            None => authority.to_string(),
        };
        let tail = match self.redact_query(tail) {
            Some(rewritten) => {
                changed = true;
                rewritten
            }
            None => tail.to_string(),
        };

        changed.then(|| format!("{scheme}://{authority}{tail}"))
    }

    /// Redact secret-keyword query-parameter values in a URL's path+query+
    /// fragment tail. `None` when there is no query or nothing matched.
    fn redact_query(&self, tail: &str) -> Option<String> {
        let start = tail.find('?')?;
        let after = &tail[start + 1..];
        let end = after.find('#').unwrap_or(after.len());
        let fragment = &after[end..];

        let mut changed = false;
        let query = after[..end]
            .split('&')
            .map(|pair| match pair.split_once('=') {
                Some((key, value)) if !value.is_empty() && self.key_is_secret(key) => {
                    changed = true;
                    format!("{key}={REDACTED}")
                }
                _ => pair.to_string(),
            })
            .collect::<Vec<_>>()
            .join("&");

        changed.then(|| format!("{path}?{query}{fragment}", path = &tail[..start]))
    }
}

fn looks_secret_value(value: &str) -> bool {
    if value.starts_with("-----BEGIN") {
        return true;
    }
    if is_jwt(value) {
        return true;
    }
    value.len() >= MIN_HEURISTIC_LEN
        && is_token_shaped(value)
        && shannon_entropy(value) >= ENTROPY_BITS
}

fn is_jwt(value: &str) -> bool {
    if value.len() < MIN_HEURISTIC_LEN {
        return false;
    }
    let parts: Vec<&str> = value.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| p.len() >= 4 && p.bytes().all(is_base64url_byte))
}

fn is_token_shaped(value: &str) -> bool {
    value.bytes().all(is_base64url_byte)
}

fn is_base64url_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'+' | b'=')
}

// Precision loss casting len to f64 is irrelevant: env values are far below 2^52 bytes.
#[allow(clippy::cast_precision_loss)]
fn shannon_entropy(s: &str) -> f64 {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return 0.0;
    }
    let mut counts = [0u32; 256];
    for &b in bytes {
        counts[b as usize] += 1;
    }
    let len = bytes.len() as f64;
    counts
        .iter()
        .filter(|&&c| c > 0)
        .map(|&c| {
            let p = f64::from(c) / len;
            -p * p.log2()
        })
        .sum()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn environ(entries: &[&str]) -> Vec<u8> {
        let mut v = Vec::new();
        for e in entries {
            v.extend_from_slice(e.as_bytes());
            v.push(0);
        }
        v
    }

    fn parse(raw: &[u8]) -> Vec<String> {
        raw.split(|&b| b == 0)
            .filter(|e| !e.is_empty())
            .map(|e| String::from_utf8_lossy(e).into_owned())
            .collect()
    }

    #[test]
    fn redacts_keyword_keys_keeps_innocuous_ones() {
        let raw = environ(&[
            "DB_PASSWORD=hunter2",
            "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLE",
            "GITHUB_TOKEN=ghp_AbC123",
            "API_TIMEOUT=30",
            "PATH=/usr/local/bin:/usr/bin",
            "LANG=en_US.UTF-8",
        ]);
        assert_eq!(
            parse(&Redactor::default().redact_environ(&raw)),
            vec![
                "DB_PASSWORD=<redacted>",
                "AWS_SECRET_ACCESS_KEY=<redacted>",
                "GITHUB_TOKEN=<redacted>",
                "API_TIMEOUT=30",
                "PATH=/usr/local/bin:/usr/bin",
                "LANG=en_US.UTF-8",
            ],
        );
    }

    #[test]
    fn redacts_secret_shaped_values_under_innocuous_keys() {
        let jwt = "CONTEXT=eyJhbGciOiJI.eyJzdWIiOiIxMjM.SflKxwRJSMeKKF2QT4";
        let blob = "STATE=Zm9vYmFyYmF6cXV4MTIzNDU2Nzg5MGFiYw";
        let pem = "CA=-----BEGIN CERTIFICATE-----MIIB";
        let raw = environ(&[jwt, blob, pem]);
        let got = parse(&Redactor::default().redact_environ(&raw));
        assert_eq!(
            got,
            vec!["CONTEXT=<redacted>", "STATE=<redacted>", "CA=<redacted>",]
        );
    }

    #[test]
    fn entropy_heuristic_spares_long_low_entropy_values() {
        let raw = environ(&["GREETING=aaaaaaaaaaaaaaaaaaaaaaaa", "VERSION=1.2.3"]);
        assert_eq!(
            parse(&Redactor::default().redact_environ(&raw)),
            vec!["GREETING=aaaaaaaaaaaaaaaaaaaaaaaa", "VERSION=1.2.3"],
        );
    }

    #[test]
    fn redacts_password_in_connection_url() {
        let raw = environ(&[
            "DATABASE_URL=postgres://user:s3cr3t@db.host:5432/app?sslmode=require",
            "REDIS_URL=redis://cache.host:6379/0",
            "PROXY=http://justuser@proxy.host:8080",
        ]);
        assert_eq!(
            parse(&Redactor::default().redact_environ(&raw)),
            vec![
                "DATABASE_URL=postgres://user:<redacted>@db.host:5432/app?sslmode=require",
                "REDIS_URL=redis://cache.host:6379/0",
                "PROXY=http://justuser@proxy.host:8080",
            ],
        );
    }

    #[test]
    fn redacts_secret_query_parameters_keeps_the_rest_of_the_url() {
        let raw = environ(&[
            "DATABASE_URL=postgres://host/db?password=x&sslpassword=y&sslmode=require",
            "CALLBACK=https://host/cb?token=abc#frag",
            "SERVICE_URL=https://host/path?retries=3&timeout=30",
        ]);
        assert_eq!(
            parse(&Redactor::default().redact_environ(&raw)),
            vec![
                "DATABASE_URL=postgres://host/db?password=<redacted>&sslpassword=<redacted>&sslmode=require",
                "CALLBACK=https://host/cb?token=<redacted>#frag",
                "SERVICE_URL=https://host/path?retries=3&timeout=30",
            ],
        );
    }

    #[test]
    fn redacts_colonless_userinfo_that_looks_like_a_token() {
        let raw = environ(&[
            "HOOK=https://Zm9vYmFyYmF6cXV4MTIzNDU2Nzg5MGFiYw@internal.host/path",
            "PROXY=http://justuser@proxy.host:8080",
        ]);
        assert_eq!(
            parse(&Redactor::default().redact_environ(&raw)),
            vec![
                "HOOK=https://<redacted>@internal.host/path",
                "PROXY=http://justuser@proxy.host:8080",
            ],
        );
    }

    #[test]
    fn redacts_cmdline_keyword_values_and_flag_pairs() {
        let raw = environ(&[
            "myapp",
            "run",
            "--db-password=hunter2",
            "--token",
            "ghp_AbC123",
            "--verbose",
            "--url=postgres://u:pw@h/db",
            "/etc/config.yaml",
        ]);
        assert_eq!(
            parse(&Redactor::default().redact_cmdline(&raw)),
            vec![
                "myapp",
                "run",
                "--db-password=<redacted>",
                "--token",
                "<redacted>",
                "--verbose",
                "--url=postgres://u:<redacted>@h/db",
                "/etc/config.yaml",
            ],
        );
    }

    #[test]
    fn cmdline_spares_innocuous_arguments() {
        let raw = environ(&[
            "myapp",
            "--verbose",
            "--workers=8",
            "--config=/etc/app/config.yaml",
            "-v",
            "serve",
        ]);
        assert_eq!(Redactor::default().redact_cmdline(&raw), raw);
    }

    #[test]
    fn no_redact_passes_everything_through() {
        let raw = environ(&["DB_PASSWORD=hunter2", "API_TIMEOUT=30"]);
        assert_eq!(Redactor::disabled().redact_environ(&raw), raw);
        let argv = environ(&["myapp", "--password", "hunter2"]);
        assert_eq!(Redactor::disabled().redact_cmdline(&argv), argv);
    }

    #[test]
    fn preserves_framing_and_skips_malformed_entries() {
        let raw = environ(&["NOEQUALS", "DB_TOKEN=abc"]);
        assert_eq!(
            parse(&Redactor::default().redact_environ(&raw)),
            vec!["NOEQUALS", "DB_TOKEN=<redacted>"],
        );
    }
}
