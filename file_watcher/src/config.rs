use event_storage::StorageConfig;
use serde::Deserialize;

use crate::Settings;

#[derive(Deserialize, Debug)]
pub(crate) struct Config {
    #[serde(default)]
    pub(crate) settings: Settings,
    #[serde(default)]
    pub(crate) storage: StorageConfig,
    pub(crate) state_db_path: Option<String>,
}
