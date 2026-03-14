mod app;
mod config;

use anyhow::anyhow;
use config::ViewerConfig;
use tokio::fs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("Usage: log_viewer <config.toml>"))?;

    let config_bytes = fs::read(&config_path).await?;
    let config: ViewerConfig = toml::from_slice(&config_bytes)?;

    let storage = config::make_storage(&config.storage).await?;

    app::run(storage).await?;

    Ok(())
}
