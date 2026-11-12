//! The daemon's `--config` file: a YAML mapping of long flag names to
//! values. [`apply`] turns its values into the defaults of a clap
//! [`Command`], so a flag or its env var still wins over the file.

use std::path::Path;

use clap::Command;

/// Make the values in the config file at `path` the defaults of `cmd`.
///
/// # Errors
///
/// A clap error that names the file: it cannot be read, it is not a mapping,
/// a key is not a flag of `cmd`, or a flag does not accept its value.
pub(super) fn apply(mut cmd: Command, path: &Path) -> Result<Command, clap::Error> {
    let file_error = |msg: String| {
        clap::Error::raw(
            clap::error::ErrorKind::InvalidValue,
            format!("config file {}: {msg}\n", path.display()),
        )
    };
    let text = std::fs::read_to_string(path).map_err(|e| file_error(e.to_string()))?;
    for (key, value) in config_file_values(&text).map_err(file_error)? {
        let arg = cmd
            .get_arguments()
            .find(|a| {
                a.get_long() == Some(key.as_str()) && !FILE_ONLY_FLAGS.contains(&key.as_str())
            })
            .ok_or_else(|| file_error(format!("unknown key `{key}`")))?;
        check_file_value(arg, &value).map_err(|e| file_error(format!("`{key}`: {e}")))?;
        let id = arg.get_id().clone();
        cmd = cmd.mut_arg(id, |a| a.default_value(value));
    }
    Ok(cmd)
}

/// Flags the config file cannot set.
const FILE_ONLY_FLAGS: [&str; 3] = ["config", "help", "version"];

/// Whether `arg` accepts `value`. Checked before the value becomes a default:
/// clap checks defaults only in debug builds, and then with a panic.
fn check_file_value(arg: &clap::Arg, value: &str) -> Result<(), String> {
    if !arg.get_action().takes_values() {
        return match value {
            "true" | "false" => Ok(()),
            _ => Err(format!("{value:?} is not true or false")),
        };
    }
    let long = arg.get_long().unwrap_or_default();
    clap::Command::new("coredrop")
        .no_binary_name(true)
        .arg(arg.clone())
        .try_get_matches_from([format!("--{long}"), value.to_string()])
        .map(drop)
        .map_err(|e| {
            let msg = e.to_string();
            let first = msg.lines().next().unwrap_or_default();
            first.trim_start_matches("error: ").to_string()
        })
}

/// The `key: value` pairs of a config file, each value as its flag would
/// take it. A list or mapping (`capture-filter`) becomes JSON; a `null`
/// value leaves the default. An empty file sets nothing.
fn config_file_values(text: &str) -> Result<Vec<(String, String)>, String> {
    use serde_norway::Value;
    let mapping = match serde_norway::from_str::<Value>(text).map_err(|e| e.to_string())? {
        Value::Null => return Ok(Vec::new()),
        Value::Mapping(mapping) => mapping,
        _ => return Err("the top level must be a mapping of flag names to values".into()),
    };
    let mut values = Vec::new();
    for (key, value) in mapping {
        let Value::String(key) = key else {
            return Err(format!("key {key:?} is not a string"));
        };
        let value = match value {
            Value::Null => continue,
            Value::String(s) => s,
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::Sequence(_) | Value::Mapping(_) => {
                serde_json::to_string(&value).map_err(|e| format!("`{key}`: {e}"))?
            }
            Value::Tagged(_) => return Err(format!("`{key}`: YAML tags are not supported")),
        };
        values.push((key, value));
    }
    Ok(values)
}
