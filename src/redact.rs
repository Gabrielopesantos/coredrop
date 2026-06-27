//! `environ` redaction: redact-by-default with a curated keyword list plus a
//! value-shape/entropy heuristic, `--no-redact` opts into raw.
//!
//! `/proc/<pid>/environ` is NUL-separated `KEY=VALUE` entries. We redact an
//! entry's value (never the key) when either the key matches a curated
//! secret keyword *or* the value looks secret-shaped (high-entropy token, JWT,
//! or PEM block). The match is deliberately not a greedy substring over
//! the whole entry: `API_TIMEOUT=30` must survive while `API_KEY=…` must not.
//!
//! Cores are secret-bearing regardless - this only governs the small `environ`
//! blob that travels in the `/proc` snapshot.

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
            keywords: DEFAULT_KEYWORDS.iter().map(|k| k.to_string()).collect(),
        }
    }
}

impl Redactor {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            keywords: Vec::new(),
        }
    }

    /// Redact secret values in a raw `/proc/<pid>/environ` blob.
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
        if let Some(rewritten) = redact_url_userinfo(value) {
            return format!("{key}={rewritten}").into_bytes();
        }
        entry.to_vec()
    }

    fn key_is_secret(&self, key: &str) -> bool {
        let upper = key.to_ascii_uppercase();
        self.keywords.iter().any(|kw| upper.contains(kw.as_str()))
    }
}

/// Redact the password in a `scheme://user:pass@host…` connection string,
/// returning the rewritten value or `None` when there is no userinfo password.
fn redact_url_userinfo(value: &str) -> Option<String> {
    let scheme_end = value.find("://")?;
    let rest = &value[scheme_end + 3..];
    let auth_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..auth_end];
    let at = authority.find('@')?;
    let userinfo = &authority[..at];
    let colon = userinfo.find(':')?;
    let password = &userinfo[colon + 1..];
    if password.is_empty() {
        return None;
    }
    let user = &userinfo[..colon];
    Some(format!(
        "{scheme}://{user}:{REDACTED}{tail}",
        scheme = &value[..scheme_end],
        tail = &rest[at..],
    ))
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
            let p = c as f64 / len;
            -p * p.log2()
        })
        .sum()
}

#[cfg(test)]
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
    fn no_redact_passes_everything_through() {
        let raw = environ(&["DB_PASSWORD=hunter2", "API_TIMEOUT=30"]);
        assert_eq!(Redactor::disabled().redact_environ(&raw), raw);
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
