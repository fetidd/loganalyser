use file_watcher::FileWatcher;
use tokio::{
    fs,
    signal::{
        ctrl_c,
        unix::{SignalKind, signal},
    },
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let config_path = std::env::args()
        .nth(1)
        .ok_or(anyhow::anyhow!("Usage: file_watcher <config_path>"))?;
    let config_file = fs::read(config_path).await?;
    // watcher.run().await
    let (tx, rx) = tokio::sync::oneshot::channel::<bool>();
    let mut watcher = FileWatcher::new(config_file).await?.with_receiver(rx);
    let main_join_handle = tokio::spawn(async move { watcher.run().await });
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = sigterm.recv() => { println!("exiting..."); let _ = tx.send(true); }
        _ = ctrl_c() => { println!("exiting..."); let _ = tx.send(true); }
    }
    main_join_handle.await?
}
