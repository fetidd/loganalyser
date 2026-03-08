use shared::event::Event;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait EventStorage {
    fn store(&self, events: &[Event]) -> Result<()>;
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_new_in_memory_storage() {
        todo!()
    }
}
