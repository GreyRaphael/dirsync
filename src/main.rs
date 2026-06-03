use anyhow::Result;
use clap::Parser;
use dirsync::cli::Cli;
use dirsync::sync::SyncEngine;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing subscriber
    let level = match cli.verbose() {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level)),
        )
        .init();

    info!("dirsync starting");
    info!("  Role:       {}", cli.role().as_str());
    info!("  Directory:  {}", cli.input().display());
    info!("  SHM name:   {}", cli.shm_name());
    info!("  SHM size:   {} bytes", cli.shm_size());

    // Ensure the input directory exists
    if !cli.input().exists() {
        anyhow::bail!("Input directory does not exist: {}", cli.input().display());
    }
    if !cli.input().is_dir() {
        anyhow::bail!("Input path is not a directory: {}", cli.input().display());
    }

    // Shared flag for graceful shutdown
    let running = Arc::new(AtomicBool::new(true));

    // Register Ctrl+C handler
    let r = running.clone();
    ctrlc::set_handler(move || {
        info!("Shutdown signal received, stopping...");
        r.store(false, Ordering::Relaxed);
    })
    .expect("Error setting Ctrl-C handler");

    let instance_id = cli.instance_id();
    info!("Assigned instance_id: {}", instance_id);

    // Create sync engine
    let mut engine = SyncEngine::new(instance_id, &cli, running)?;

    // Initial sync can exceed the ring buffer if the peer has not started yet.
    engine.wait_for_peer()?;

    // Perform initial sync
    engine.initial_sync()?;

    // Run the sync loop (blocks until shutdown signal)
    engine.run_sync_loop()?;

    info!("dirsync stopped");
    Ok(())
}
