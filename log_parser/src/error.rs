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
