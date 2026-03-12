use file_watcher::FileWatcher;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut watcher = FileWatcher::new("../gateway_config.toml").await?;
    watcher.run().await
}
