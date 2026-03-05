use std::collections::HashMap;

use chrono::Duration;

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Span {
        source: String,
        timestamp: chrono::NaiveDateTime,
        data: HashMap<String, String>,
        duration: Duration,
    },
    Single {
        name: String,
        timestamp: chrono::NaiveDateTime,
        data: HashMap<String, String>,
    },
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn test() {}
}
