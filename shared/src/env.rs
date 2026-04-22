use std::collections::HashMap;
use std::path::PathBuf;

/// Returns a platform-appropriate path for the loganalyser SQLite state file.
///
/// - Linux/macOS: `/var/lib/loganalyser/state.db` if writable, otherwise
///   `~/.local/share/loganalyser/state.db` (or `%LOCALAPPDATA%\loganalyser\state.db` on Windows).
/// - Fallback when no home dir is detectable: `/tmp/loganalyser/state.db`.
///
/// This function only computes the path — it does **not** create directories.
pub fn default_state_db_path() -> PathBuf {
    #[cfg(unix)]
    {
        // Probe whether /var/lib/loganalyser is writable by trying to create it
        // and then touching a sentinel file.
        let var_lib = PathBuf::from("/var/lib/loganalyser");
        if std::fs::create_dir_all(&var_lib).is_ok() {
            let sentinel = var_lib.join(".write_check");
            if std::fs::OpenOptions::new().write(true).create(true).truncate(true).open(&sentinel).is_ok() {
                let _ = std::fs::remove_file(sentinel);
                return var_lib.join("state.db");
            }
        }
    }

    // Windows, macOS, and Linux fallback: user data directory.
    if let Some(data_dir) = dirs::data_local_dir() {
        return data_dir.join("loganalyser").join("state.db");
    }

    // Last resort (headless server with no HOME).
    PathBuf::from("/tmp/loganalyser/state.db")
}

/// Expands `${VAR}` placeholders in `s` using the provided lookup function.
///
/// Returns an error if a `${` is unclosed or the lookup returns `None` for a variable name.
pub fn expand_vars<F>(s: &str, lookup: F) -> Result<String, ExpandError>
where
    F: Fn(&str) -> Option<String>,
{
    let mut result = s.to_string();
    let mut search_from = 0;
    while let Some(rel_start) = result[search_from..].find("${") {
        let start = search_from + rel_start;
        let end = result[start..].find('}').ok_or(ExpandError::UnclosedBrace { pos: start })? + start;
        let var_name = &result[start + 2..end];
        let value = lookup(var_name).ok_or_else(|| ExpandError::MissingVar { name: var_name.to_string() })?;
        result.replace_range(start..=end, &value);
        search_from = start + value.len();
    }
    Ok(result)
}

/// Expands `${VAR}` placeholders using environment variables.
pub fn expand_env_vars(s: &str) -> Result<String, ExpandError> {
    expand_vars(s, |name| std::env::var(name).ok())
}

/// Expands `${VAR}` placeholders using a `HashMap`.
pub fn expand_map_vars<K, V>(s: &str, vars: &HashMap<K, V>) -> Result<String, ExpandError>
where
    K: std::borrow::Borrow<str> + std::hash::Hash + Eq,
    V: AsRef<str>,
{
    expand_vars(s, |name| vars.get(name).map(|v| v.as_ref().to_string()))
}

#[derive(Debug, PartialEq)]
pub enum ExpandError {
    UnclosedBrace { pos: usize },
    MissingVar { name: String },
}

impl std::fmt::Display for ExpandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpandError::UnclosedBrace { pos } => {
                write!(f, "unclosed '${{' at position {pos}")
            }
            ExpandError::MissingVar { name } => {
                write!(f, "variable '{name}' is not set")
            }
        }
    }
}

impl std::error::Error for ExpandError {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use super::*;

    fn vars() -> HashMap<&'static str, &'static str> {
        HashMap::from([("HOST", "db.internal"), ("PORT", "3306"), ("PASS", "s3cr3t")])
    }

    #[rstest]
    #[case("no placeholders", "no placeholders")]
    #[case("", "")]
    #[case("${HOST}", "db.internal")]
    #[case("mysql://user:${PASS}@${HOST}:${PORT}/db", "mysql://user:s3cr3t@db.internal:3306/db")]
    #[case("prefix_${HOST}_suffix", "prefix_db.internal_suffix")]
    #[case("${HOST}${PORT}", "db.internal3306")] // adjacent placeholders
    #[case("${HOST}:${HOST}", "db.internal:db.internal")] // same var twice
    fn test_expand_map_vars(#[case] input: &str, #[case] expected: &str) {
        let result = expand_map_vars(input, &vars()).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case("${MISSING}", "MISSING")]
    #[case("ok_${HOST}_then_${NOPE}", "NOPE")]
    fn test_expand_missing_var(#[case] input: &str, #[case] missing_name: &str) {
        let err = expand_map_vars(input, &vars()).unwrap_err();
        assert_eq!(err, ExpandError::MissingVar { name: missing_name.to_string() });
    }

    #[rstest]
    #[case("${unclosed", 0)]
    #[case("ok ${unclosed", 3)]
    fn test_expand_unclosed_brace(#[case] input: &str, #[case] pos: usize) {
        let err = expand_map_vars(input, &vars()).unwrap_err();
        assert_eq!(err, ExpandError::UnclosedBrace { pos });
    }

    #[test]
    fn test_expand_env_vars() {
        temp_env::with_var("TEST_EXPAND_HOST", Some("localhost"), || {
            let result = expand_env_vars("connect to ${TEST_EXPAND_HOST}").unwrap();
            assert_eq!(result, "connect to localhost");
        });
    }

    #[test]
    fn test_expand_env_vars_missing() {
        temp_env::with_var("TEST_EXPAND_HOST", None::<&str>, || {
            let err = expand_env_vars("connect to ${TEST_EXPAND_HOST}").unwrap_err();
            assert_eq!(err, ExpandError::MissingVar { name: "TEST_EXPAND_HOST".to_string() });
        });
    }
}
