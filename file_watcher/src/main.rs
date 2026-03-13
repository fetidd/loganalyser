use file_watcher::FileWatcher;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args().nth(1).expect("Usage: file_watcher <config_path>");
    let mut watcher = FileWatcher::new(&config_path).await?;
    watcher.run().await
}
