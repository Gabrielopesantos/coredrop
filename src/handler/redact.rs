//! `environ` and `cmdline` redaction: redact-by-default with a curated keyword
//! list plus a value-shape/entropy heuristic, `--no-redact` opts into raw.
//!
//! `/proc/<pid>/environ` is NUL-separated `KEY=VALUE` entries. We redact an
//! entry's value (never the key) when either the key matches a curated
//! secret keyword *or* the value looks secret-shaped (high-entropy token, JWT,
//! or PEM block). The match is not a greedy substring over
//! the whole entry: `API_TIMEOUT=30` must survive while `API_KEY=...` must not.
//!
//! Values that parse as URLs get a second pass (`Redactor::redact_url`):
//! userinfo passwords, a lone high-entropy userinfo token, and query-parameter
//! values under secret-keyword names are rewritten in place, so
//! `DATABASE_URL=postgres://u:pw@h/db?password=x` leaks neither.
//!
//! `/proc/<pid>/cmdline` is NUL-separated argv and gets the same per-entry
//! treatment for `key=value`-shaped arguments, plus a lookahead rule: an
//! argument shaped `--secret-keyword` redacts the argument that follows it.
//! cmdline redaction is best-effort and biased to over-redact -
//! argv has no schema, so `--keyspace default` loses its value rather than
//! risk `--key <token>` keeping one. Positional arguments are left alone.
//!
//! Cores are secret-bearing regardless - this only governs the small `environ`
//! and `cmdline` blobs that travel in the `/proc` snapshot.

const REDACTED: &str = "<redacted>";

/// Key words that mark a secret, matched as substrings of the upper-cased
/// key (`PASS` also matches `PASSWORD` and `PASSWD`).
const KEYWORDS: &[&str] = &[
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
}

impl Default for Redactor {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl Redactor {
    /// A redactor that never redacts.
    #[must_use]
    pub fn disabled() -> Self {
        Self { enabled: false }
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
            out.extend_from_slice(&Self::redact_entry(entry));
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
        // Each arg ends with a NUL. Empty args stay: `--password ""` takes
        // the empty value, and dropping it would redact the next arg and
        // change the argv layout.
        let args = raw.strip_suffix(&[0]).unwrap_or(raw);
        for (i, arg) in args.split(|&b| b == 0).enumerate() {
            if i > 0 {
                out.push(0);
            }
            if redact_next {
                out.extend_from_slice(REDACTED.as_bytes());
                redact_next = false;
            } else {
                redact_next = Self::is_secret_flag(arg);
                out.extend_from_slice(&Self::redact_entry(arg));
            }
        }
        if raw.ends_with(&[0]) {
            out.push(0);
        }
        out
    }

    fn redact_entry(entry: &[u8]) -> Vec<u8> {
        let Ok(text) = std::str::from_utf8(entry) else {
            return entry.to_vec();
        };
        let Some((key, value)) = text.split_once('=') else {
            return entry.to_vec();
        };
        // The shell's working directories match `PWD` but hold no secret,
        // and they help to read a crash.
        if matches!(key, "PWD" | "OLDPWD") {
            return entry.to_vec();
        }
        if key_is_secret(key) || looks_secret_value(value) {
            return format!("{key}={REDACTED}").into_bytes();
        }
        // A connection string can hold a URL and secret pairs: run both.
        let url = Self::redact_url(value);
        let pairs = Self::redact_pairs(url.as_deref().unwrap_or(value));
        match pairs.or(url) {
            Some(rewritten) => format!("{key}={rewritten}").into_bytes(),
            None => entry.to_vec(),
        }
    }

    /// Redact secret-keyword values in a `;`-separated connection string
    /// (`Server=db;Password=x`). `None` when there is nothing to redact.
    fn redact_pairs(value: &str) -> Option<String> {
        if !value.contains(';') {
            return None;
        }
        let mut changed = false;
        let pairs = value
            .split(';')
            .map(|pair| match pair.split_once('=') {
                Some((key, v)) if !v.is_empty() && key_is_secret(key.trim()) => {
                    changed = true;
                    format!("{key}={REDACTED}")
                }
                _ => pair.to_string(),
            })
            .collect::<Vec<_>>()
            .join(";");
        changed.then_some(pairs)
    }

    /// A valueless flag whose name matches a secret keyword (`--password`), so
    /// the *next* argv entry is its value and must go.
    fn is_secret_flag(arg: &[u8]) -> bool {
        let Ok(text) = std::str::from_utf8(arg) else {
            return false;
        };
        text.starts_with('-') && !text.contains('=') && key_is_secret(text)
    }

    /// Redact credentials inside a `scheme://...` URL: a userinfo password, a
    /// userinfo that is itself a secret-shaped token, and query-parameter
    /// values whose name matches a secret keyword. Returns `None` when the
    /// value isn't a URL or holds nothing worth redacting.
    fn redact_url(value: &str) -> Option<String> {
        let scheme_end = value.find("://")?;
        let scheme = &value[..scheme_end];
        let rest = &value[scheme_end + 3..];
        // The authority normally ends at the first `/`, and its userinfo at
        // the last `@` in it (a password can hold `@`). A password can also
        // hold `/`: when no `@` comes before the first `/`, a `user:` before
        // the first `@` still marks userinfo. A path can hold `@` too, so
        // that fallback takes the first `@`, not the last.
        let tail_start = rest.find(['?', '#']).unwrap_or(rest.len());
        let head = &rest[..tail_start];
        let first_slash = head.find('/').unwrap_or(head.len());
        let host_start = match head[..first_slash].rfind('@') {
            Some(at) => at + 1,
            None => match head.find('@') {
                Some(at)
                    if head[..at]
                        .split_once(':')
                        .is_some_and(|(user, _)| !user.contains('/')) =>
                {
                    at + 1
                }
                _ => 0,
            },
        };
        let auth_end = head[host_start..]
            .find('/')
            .map_or(tail_start, |slash| host_start + slash);
        let authority = &rest[..auth_end];
        let tail = &rest[auth_end..];

        let mut changed = false;
        let authority = match authority.rsplit_once('@') {
            // `user:pass@host` - keep the user, drop the password.
            Some((userinfo, host)) => match userinfo.split_once(':') {
                Some((user, password)) if !password.is_empty() => {
                    changed = true;
                    format!("{user}:{REDACTED}@{host}")
                }
                // `token@host` - no password field, but a long high-entropy
                // userinfo is likely a bearer credential.
                None if looks_secret_value(userinfo) => {
                    changed = true;
                    format!("{REDACTED}@{host}")
                }
                _ => authority.to_string(),
            },
            None => authority.to_string(),
        };
        let tail = match Self::redact_query(tail) {
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
    fn redact_query(tail: &str) -> Option<String> {
        let start = tail.find('?')?;
        let after = &tail[start + 1..];
        let end = after.find('#').unwrap_or(after.len());
        let fragment = &after[end..];

        let mut changed = false;
        let query = after[..end]
            .split('&')
            .map(|pair| match pair.split_once('=') {
                Some((key, value)) if !value.is_empty() && key_is_secret(key) => {
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

fn key_is_secret(key: &str) -> bool {
    let upper = key.to_ascii_uppercase();
    KEYWORDS.iter().any(|kw| upper.contains(kw))
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
    fn redacts_passwords_in_a_semicolon_connection_string() {
        let raw = environ(&[
            "CONN=Server=db;User Id=app;Password=hunter2;Encrypt=true",
            "SQL=Server=db;Pwd=hunter2",
            "MIXED=Endpoint=https://u:p@h/db;Password=hunter2",
            "QUERY=https://h/db?pwd=hunter2",
            "PWD=/srv/app",
            "OLDPWD=/root",
            "MYSQL_PWD=hunter2",
        ]);
        assert_eq!(
            parse(&Redactor::default().redact_environ(&raw)),
            vec![
                "CONN=Server=db;User Id=app;Password=<redacted>;Encrypt=true",
                "SQL=Server=db;Pwd=<redacted>",
                "MIXED=Endpoint=https://u:<redacted>@h/db;Password=<redacted>",
                "QUERY=https://h/db?pwd=<redacted>",
                "PWD=/srv/app",
                "OLDPWD=/root",
                "MYSQL_PWD=<redacted>",
            ]
        );
    }

    #[test]
    fn redacts_a_url_password_that_holds_at_or_slash() {
        let raw = environ(&[
            "A=postgres://u:p@ss@h/db",
            "B=postgres://u:pa/ss@h/db?sslmode=require",
            "C=https://Zm9vYmFyYmF6cXV4MTIzNDU2Nzg5MGFiYw@h/path@v1",
            "D=https://h/users/@me",
        ]);
        assert_eq!(
            parse(&Redactor::default().redact_environ(&raw)),
            vec![
                "A=postgres://u:<redacted>@h/db",
                "B=postgres://u:<redacted>@h/db?sslmode=require",
                "C=https://<redacted>@h/path@v1",
                "D=https://h/users/@me",
            ]
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
    fn cmdline_keeps_empty_arguments() {
        let raw = environ(&["app", "--password", "", "--verbose", ""]);
        assert_eq!(
            Redactor::default().redact_cmdline(&raw),
            environ(&["app", "--password", "<redacted>", "--verbose", ""]),
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

    /// Known limitation, pinned by this test: `/proc`
    /// guarantees no encoding, and a non-UTF-8 entry is passed through
    /// verbatim - keyword matching needs `str`. A secret in such an entry is
    /// not redacted. The NUL framing around it still survives, so one bad
    /// entry never corrupts the rest of the blob.
    #[test]
    fn non_utf8_entries_pass_through_unredacted() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"DB_PASSWORD=");
        raw.extend_from_slice(&[0xff, 0xfe]); // invalid UTF-8
        raw.push(0);
        raw.extend_from_slice(b"API_TOKEN=plain-secret\0");

        let got = Redactor::default().redact_environ(&raw);
        let entries: Vec<&[u8]> = got.split(|&b| b == 0).filter(|e| !e.is_empty()).collect();

        assert_eq!(entries.len(), 2, "framing preserved");
        assert_eq!(entries[0], b"DB_PASSWORD=\xff\xfe");
        assert_eq!(
            entries[1], b"API_TOKEN=<redacted>",
            "a following valid entry is still redacted"
        );
    }
}
