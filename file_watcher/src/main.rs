use file_watcher::FileWatcher;
use tokio::fs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let config_path = std::env::args()
        .nth(1)
        .ok_or(anyhow::anyhow!("Usage: file_watcher <config_path>"))?;
    let config_file = fs::read(config_path).await?;
    let mut watcher = FileWatcher::new(config_file).await?;
    watcher.run().await
}
