use clap::Parser;
use std::path::PathBuf;

/// dirsync - Directory synchronization over shared memory
///
/// Two instances of dirsync monitor their respective directories and sync
/// file/folder changes to each other via shared memory.
///
/// Usage:
///   dirsync -i /path/to/dir1    # Instance 1
///   dirsync -i /path/to/dir2    # Instance 2
#[derive(Parser, Debug)]
#[command(name = "dirsync", version, about, long_about = None)]
pub struct Cli {
    /// Directory to monitor and sync
    #[arg(short, long)]
    pub input: PathBuf,

    /// Shared memory segment name (must be the same for both instances)
    #[arg(long, default_value = "dirsync_shm")]
    pub shm_name: String,

    /// Shared memory size in bytes (default: 64MB)
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pub shm_size: usize,

    /// Verbose output (repeat for more: -v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Conflict resolution strategy
    #[arg(long, value_enum, default_value_t = ConflictStrategy::LastWriteWins)]
    pub conflict: ConflictStrategy,

    /// Debounce interval in milliseconds for file change events
    #[arg(long, default_value_t = 100)]
    pub debounce_ms: u64,

    /// Directories to ignore (can be specified multiple times)
    #[arg(long)]
    pub ignore: Vec<String>,
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum ConflictStrategy {
    /// Last write wins (by timestamp)
    LastWriteWins,
    /// Keep both copies (file.txt.a / file.txt.b)
    KeepBoth,
}
