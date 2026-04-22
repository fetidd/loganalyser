use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;

use crate::SqliteEventStore;

/// An in-memory event store backed by a single-connection SQLite `:memory:` database.
///
/// `max_connections(1)` + `min_connections(1)` + disabled timeouts force sqlx to
/// hold exactly one connection open for the pool's lifetime, so the in-memory
/// database persists as long as the store exists.
impl SqliteEventStore {
    pub async fn new_in_memory() -> Self {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(SqliteConnectOptions::from_str(":memory:").expect("valid :memory: connection string").create_if_missing(true))
            .await
            .expect("failed to open in-memory SQLite");
        SqliteEventStore::from_pool(pool).await
    }
}

/// Type alias so existing code using `MemoryEventStore` continues to compile.
pub type MemoryEventStore = SqliteEventStore;
