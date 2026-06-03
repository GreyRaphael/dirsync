mod apply;
mod chunker;
mod cli;
mod event;
mod shm;
mod sync;
mod watcher;

use anyhow::Result;
use clap::Parser;
use cli::Cli;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing subscriber
    let level = match cli.verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level)),
        )
        .init();

    info!("dirsync starting");
    info!("  Directory: {}", cli.input.display());
    info!("  SHM name:  {}", cli.shm_name);
    info!("  SHM size:  {} bytes", cli.shm_size);

    // Ensure the input directory exists
    if !cli.input.exists() {
        anyhow::bail!("Input directory does not exist: {}", cli.input.display());
    }
    if !cli.input.is_dir() {
        anyhow::bail!("Input path is not a directory: {}", cli.input.display());
    }

    // Determine instance ID based on which process connects first
    // Instance 0 = first to create SHM, Instance 1 = second to open
    let instance_id = if shm::ShmTransport::open(&cli.shm_name).is_err() {
        0
    } else {
        1
    };

    info!("Assigned instance_id: {}", instance_id);

    // Create sync engine
    let mut engine = sync::SyncEngine::new(instance_id, &cli)?;

    // Perform initial sync
    engine.initial_sync()?;

    // Run the sync loop (blocks forever)
    engine.run_sync_loop()?;

    Ok(())
}
