//! Capture filter: which crashes the handler keeps.
//!
//! The filter is two lists of [`Matcher`]s. A crash is kept when it matches
//! an `include` entry (or `include` is empty) and matches no `exclude` entry.
//! One entry matches when all of its fields match; a field is a glob (`*`
//! matches any run of characters, `?` one character).
//!
//! The handler decides before it drains the core, so a filtered core is never
//! read. Namespace, pod, container and image come from `crictl inspect`; when
//! the filter uses them, the handler runs crictl before the drain. A field
//! whose value is unknown (crictl failed) never excludes a crash and never
//! keeps it out of `include`: the filter fails open.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

/// Glob matchers on one crash's identity. Every set field must match.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Matcher {
    /// Pod namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Pod name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod: Option<String>,
    /// Container name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    /// Container image, as the runtime reports it (for example
    /// `docker.io/library/nginx:1.27`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Executable path of the crashed process, for example `/usr/bin/server`
    /// (`*/server` matches it in any directory). The kernel gives it with
    /// `!` for `/` (`%E`); the handler turns `!` back into `/` first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exe: Option<String>,
}

/// The identity values one crash is matched on. `None` is a value that is
/// not known.
#[derive(Debug, Clone, Copy, Default)]
#[allow(missing_docs)] // Same fields as `Matcher`.
pub struct Subject<'a> {
    pub namespace: Option<&'a str>,
    pub pod: Option<&'a str>,
    pub container: Option<&'a str>,
    pub image: Option<&'a str>,
    pub exe: Option<&'a str>,
}

impl Matcher {
    fn patterns(&self) -> [(&'static str, Option<&str>); 5] {
        [
            ("namespace", self.namespace.as_deref()),
            ("pod", self.pod.as_deref()),
            ("container", self.container.as_deref()),
            ("image", self.image.as_deref()),
            ("exe", self.exe.as_deref()),
        ]
    }

    /// Whether every set field matches `subject`. A field whose value is
    /// unknown counts as `unknown`.
    fn matches(&self, subject: &Subject<'_>, unknown: bool) -> bool {
        let values = [
            subject.namespace,
            subject.pod,
            subject.container,
            subject.image,
            subject.exe,
        ];
        self.patterns()
            .iter()
            .zip(values)
            .all(|((_, pattern), value)| match (pattern, value) {
                (None, _) => true,
                (Some(_), None) => unknown,
                (Some(pattern), Some(value)) => glob_match(pattern, value),
            })
    }

    fn needs_crictl(&self) -> bool {
        self.namespace.is_some()
            || self.pod.is_some()
            || self.container.is_some()
            || self.image.is_some()
    }

    fn validate(&self) -> Result<()> {
        let mut set = 0;
        for (name, pattern) in self.patterns() {
            match pattern {
                Some("") => bail!("{name} must not be empty"),
                Some(_) => set += 1,
                None => {}
            }
        }
        if set == 0 {
            bail!("an entry needs at least one of namespace, pod, container, image, exe");
        }
        Ok(())
    }
}

/// Which crashes the handler keeps. The default keeps every crash.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureFilter {
    /// Keep only crashes that match one of these; empty keeps every crash.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<Matcher>,
    /// Drop crashes that match one of these. An exclude match wins over an
    /// include match.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<Matcher>,
}

impl CaptureFilter {
    /// Parse the daemon's `--capture-filter` JSON. An empty string is the
    /// default filter.
    ///
    /// # Errors
    ///
    /// Fails on invalid JSON, an unknown key, an empty pattern, or an entry
    /// with no fields.
    pub fn parse(s: &str) -> Result<Self, String> {
        if s.trim().is_empty() {
            return Ok(Self::default());
        }
        let filter: Self = serde_json::from_str(s).map_err(|e| format!("{s:?}: {e}"))?;
        filter.validate().map_err(|e| format!("{s:?}: {e:#}"))?;
        Ok(filter)
    }

    /// Check every entry: at least one field, and no empty pattern.
    ///
    /// # Errors
    ///
    /// Names the first bad entry.
    pub fn validate(&self) -> Result<()> {
        for (list, entries) in [("include", &self.include), ("exclude", &self.exclude)] {
            for (i, entry) in entries.iter().enumerate() {
                entry
                    .validate()
                    .map_err(|e| anyhow::anyhow!("{list}[{i}]: {e}"))?;
            }
        }
        Ok(())
    }

    /// Whether the filter keeps every crash.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.include.is_empty() && self.exclude.is_empty()
    }

    /// Whether an entry matches on a field only `crictl inspect` knows.
    #[must_use]
    pub fn needs_crictl(&self) -> bool {
        self.include
            .iter()
            .chain(&self.exclude)
            .any(Matcher::needs_crictl)
    }

    /// Whether the handler keeps the crash of `subject`.
    #[must_use]
    pub fn keeps(&self, subject: &Subject<'_>) -> bool {
        let included =
            self.include.is_empty() || self.include.iter().any(|m| m.matches(subject, true));
        included && !self.exclude.iter().any(|m| m.matches(subject, false))
    }
}

/// Glob match of the whole `text`: `*` matches any run of characters
/// (also none), `?` one character, every other character itself.
#[must_use]
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let (mut p, mut t) = (0, 0);
    // Where the last `*` is, and the text position it now covers up to.
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        match pattern.get(p) {
            Some('*') => {
                star = Some((p, t));
                p += 1;
            }
            Some(&c) if c == '?' || c == text[t] => {
                p += 1;
                t += 1;
            }
            _ => match star {
                // Let the last `*` take one more character and retry.
                Some((sp, st)) => {
                    star = Some((sp, st + 1));
                    p = sp + 1;
                    t = st + 1;
                }
                None => return false,
            },
        }
    }
    pattern[p..].iter().all(|&c| c == '*')
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn subject<'a>(namespace: &'a str, pod: &'a str, container: &'a str) -> Subject<'a> {
        Subject {
            namespace: Some(namespace),
            pod: Some(pod),
            container: Some(container),
            image: Some("docker.io/library/app:1.0"),
            exe: Some("server"),
        }
    }

    fn ns(pattern: &str) -> Matcher {
        Matcher {
            namespace: Some(pattern.into()),
            ..Default::default()
        }
    }

    #[test]
    fn glob_matches_whole_text() {
        let cases = [
            ("kube-system", "kube-system", true),
            ("kube-system", "kube-system2", false),
            ("kube-*", "kube-system", true),
            ("kube-*", "kube-", true),
            ("kube-*", "kube", false),
            ("*", "", true),
            ("", "", true),
            ("", "a", false),
            ("?", "a", true),
            ("?", "", false),
            ("a?c", "abc", true),
            ("*-test-*", "ci-test-42", true),
            ("*-test-*", "ci-tests", false),
            ("*a*b", "xaxxab", true),
            ("*a*b", "xaxxa", false),
            ("**", "anything", true),
            ("*/nginx:*", "docker.io/library/nginx:1.27", true),
            ("ünï*", "ünïcode", true),
        ];
        for (pattern, text, want) in cases {
            assert_eq!(glob_match(pattern, text), want, "{pattern:?} vs {text:?}");
        }
    }

    #[test]
    fn empty_filter_keeps_everything() {
        let filter = CaptureFilter::default();
        assert!(filter.is_empty());
        assert!(!filter.needs_crictl());
        assert!(filter.keeps(&subject("a", "b", "c")));
        assert!(filter.keeps(&Subject::default()));
    }

    #[test]
    fn exclude_drops_a_match_and_keeps_the_rest() {
        let filter = CaptureFilter {
            exclude: vec![ns("kube-system")],
            ..Default::default()
        };
        assert!(!filter.keeps(&subject("kube-system", "dns", "coredns")));
        assert!(filter.keeps(&subject("default", "dns", "coredns")));
    }

    #[test]
    fn include_keeps_only_a_match() {
        let filter = CaptureFilter {
            include: vec![ns("team-*")],
            ..Default::default()
        };
        assert!(filter.keeps(&subject("team-a", "api", "server")));
        assert!(!filter.keeps(&subject("default", "api", "server")));
    }

    #[test]
    fn exclude_wins_over_include() {
        let filter = CaptureFilter {
            include: vec![ns("team-*")],
            exclude: vec![Matcher {
                container: Some("istio-proxy".into()),
                ..Default::default()
            }],
        };
        assert!(filter.keeps(&subject("team-a", "api", "server")));
        assert!(!filter.keeps(&subject("team-a", "api", "istio-proxy")));
    }

    #[test]
    fn an_entry_needs_all_its_fields_and_a_list_needs_one_entry() {
        let filter = CaptureFilter {
            exclude: vec![
                Matcher {
                    namespace: Some("ci".into()),
                    pod: Some("flaky-*".into()),
                    ..Default::default()
                },
                Matcher {
                    exe: Some("crashy".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(!filter.keeps(&subject("ci", "flaky-1", "c")));
        assert!(filter.keeps(&subject("ci", "stable-1", "c")));
        assert!(filter.keeps(&subject("prod", "flaky-1", "c")));
        let crashy = Subject {
            exe: Some("crashy"),
            ..subject("prod", "api", "c")
        };
        assert!(!filter.keeps(&crashy));
    }

    #[test]
    fn unknown_identity_fails_open() {
        // crictl failed: only the exe is known.
        let unknown = Subject {
            exe: Some("server"),
            ..Default::default()
        };
        let exclude = CaptureFilter {
            exclude: vec![ns("*")],
            ..Default::default()
        };
        assert!(exclude.keeps(&unknown), "unknown must not be excluded");
        let include = CaptureFilter {
            include: vec![ns("team-*")],
            ..Default::default()
        };
        assert!(include.keeps(&unknown), "unknown must not be left out");
        // A known field still decides.
        let exe = CaptureFilter {
            exclude: vec![Matcher {
                exe: Some("serv*".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(!exe.keeps(&unknown));
    }

    #[test]
    fn needs_crictl_only_for_crictl_fields() {
        let exe_only = CaptureFilter {
            exclude: vec![Matcher {
                exe: Some("x".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(!exe_only.needs_crictl());
        let image = CaptureFilter {
            include: vec![Matcher {
                image: Some("*".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(image.needs_crictl());
    }

    #[test]
    fn parse_reads_json_and_empty_input() {
        assert_eq!(CaptureFilter::parse("").unwrap(), CaptureFilter::default());
        assert_eq!(
            CaptureFilter::parse("{}").unwrap(),
            CaptureFilter::default()
        );
        let filter = CaptureFilter::parse(
            r#"{"include":[{"namespace":"team-*"}],"exclude":[{"container":"istio-proxy"}]}"#,
        )
        .unwrap();
        assert_eq!(filter.include, vec![ns("team-*")]);
        assert_eq!(filter.exclude[0].container.as_deref(), Some("istio-proxy"));
    }

    #[test]
    fn parse_rejects_bad_input() {
        for bad in [
            "not json",
            r#"{"exclude":[{}]}"#,
            r#"{"exclude":[{"namespace":""}]}"#,
            r#"{"exclude":[{"pods":"x"}]}"#,
            r#"{"excludes":[]}"#,
            r#"{"exclude":{"namespace":"x"}}"#,
        ] {
            assert!(CaptureFilter::parse(bad).is_err(), "{bad} parsed");
        }
    }

    #[test]
    fn round_trips_through_json() {
        let filter = CaptureFilter {
            include: vec![ns("team-*")],
            exclude: vec![Matcher {
                pod: Some("p".into()),
                exe: Some("e".into()),
                ..Default::default()
            }],
        };
        let json = serde_json::to_string(&filter).unwrap();
        assert_eq!(CaptureFilter::parse(&json).unwrap(), filter);
    }
}
