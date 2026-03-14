use serde::Deserialize;

pub use event_storage::{make_storage, StorageConfig};

#[derive(Deserialize)]
pub struct ViewerConfig {
    #[serde(default)]
    pub storage: StorageConfig,
}
