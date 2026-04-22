use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Failed to parse config file: {0}")]
    ConfigParse(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<regex::Error> for Error {
    fn from(e: regex::Error) -> Self {
        Error::ConfigParse(format!("Config parsing failed due to bad regex: {e}"))
    }
}

impl From<toml::de::Error> for Error {
    fn from(e: toml::de::Error) -> Self {
        Error::ConfigParse(format!("Config parsing failed due to invalid toml: {e}"))
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
        let err = Error::ConfigParse(msg.to_owned());
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn test_from_regex_error() {
        let regex_err = regex::Regex::new("[invalid").unwrap_err();
        let err: Error = regex_err.into();
        let Error::ConfigParse(msg) = &err;
        assert!(msg.contains("bad regex"), "expected 'bad regex' in: {msg}");
        assert!(err.to_string().starts_with("Failed to parse config file:"));
    }
}
