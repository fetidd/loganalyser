use file_watcher::file_watcher::FileWatcher;
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
    let config_path = std::env::args().nth(1).ok_or(anyhow::anyhow!("Usage: file_watcher <config_path>"))?;
    let config_file = fs::read(config_path).await?;

    // TODO the below needs to be in a loop so that the main_join_handle returning a datqbase issue can make this rebuild the watcher and restart parsing
    let (interrupt_transmitter, interrupt_receiver) = tokio::sync::oneshot::channel::<bool>();
    let mut watcher = FileWatcher::new(config_file).await?.with_receiver(interrupt_receiver);
    let main_join_handle = tokio::spawn(async move { watcher.run().await });
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = sigterm.recv() => { println!("received SIGTERM, exiting..."); let _ = interrupt_transmitter.send(true); }
        _ = ctrl_c() => { println!("CTRL-C pressed, exiting..."); let _ = interrupt_transmitter.send(true); }
    }
    main_join_handle.await??;
    Ok(())
}
