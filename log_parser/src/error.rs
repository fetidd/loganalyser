#[derive(Clone, Debug)]
pub enum LogParserError {
    ConfigParseError(String),
}

pub type LogParserResult<T> = std::result::Result<T, LogParserError>;

impl std::error::Error for LogParserError {}
impl std::fmt::Display for LogParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogParserError::ConfigParseError(e) => write!(f, "Failed to parse config file: {e}"),
        }
    }
}

impl From<regex::Error> for LogParserError {
    fn from(value: regex::Error) -> Self {
        LogParserError::ConfigParseError(format!("Config parsing failed due to bad regex: {value}"))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("missing name", "Failed to parse config file: missing name")]
    #[case("", "Failed to parse config file: ")]
    #[case("field 'x' not a string", "Failed to parse config file: field 'x' not a string")]
    fn test_error_display(#[case] msg: &str, #[case] expected: &str) {
        let err = LogParserError::ConfigParseError(msg.to_owned());
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn test_from_regex_error() {
        let regex_err = regex::Regex::new("[invalid").unwrap_err();
        let err: LogParserError = regex_err.into();
        let LogParserError::ConfigParseError(msg) = &err;
        assert!(msg.contains("bad regex"), "expected 'bad regex' in: {msg}");
        assert!(err.to_string().starts_with("Failed to parse config file:"));
    }
}
