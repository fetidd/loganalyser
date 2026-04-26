use file_watcher::file_watcher::FileWatcher;
use shared::ExitReason;
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

    'main: loop {
        // load the config file on every loop, in case a db issue is actually fixed by a config change // TEST
        let config_file = fs::read(&config_path).await?;

        // Set up the watcher with a listener that allows for graceful shutdowns on sigint
        let (interrupt_tx, interrupt_rx) = tokio::sync::oneshot::channel::<ExitReason>();
        let mut watcher = FileWatcher::new(&config_file).await?.with_receiver(interrupt_rx);

        // Spawn a task to run the watcher in
        let mut join_handle = tokio::spawn(async move { watcher.run().await });

        // Wait here for ctrl-c, sigterm, or the watcher task to complete
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = sigterm.recv() => { println!("received SIGTERM, exiting..."); let _ = interrupt_tx.send(ExitReason::Interrupt); }
            _ = ctrl_c()       => { println!("CTRL-C pressed, exiting...");   let _ = interrupt_tx.send(ExitReason::Interrupt); }
            _ = &mut join_handle => {}
        };

        // If the watcher returns a DatabaseFailure as the exit reason, start the loop again
        match join_handle.await {
            Ok(Ok(ExitReason::DatabaseFailure)) => continue 'main,
            _ => break 'main,
        }
    }
    Ok(())
}
