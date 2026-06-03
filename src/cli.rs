use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

/// dirsync - Directory synchronization over shared memory
///
/// Two instances of dirsync monitor their respective directories and sync
/// file/folder changes to each other via shared memory.
///
/// Usage:
///   dirsync host -i /path/to/dir1
///   dirsync join -i /path/to/dir2
#[derive(Parser, Debug)]
#[command(name = "dirsync", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Start the first process in a sync pair.
    Host(RunArgs),
    /// Join an existing sync pair.
    Join(RunArgs),
}

#[derive(Args, Debug)]
pub struct RunArgs {
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

    /// Conflict handling mode (conflicts are currently reported and local copies are preserved)
    #[arg(long, value_enum, default_value_t = ConflictStrategy::LastWriteWins)]
    pub conflict: ConflictStrategy,

    /// Debounce interval in milliseconds for file change events
    #[arg(long, default_value_t = 100)]
    pub debounce_ms: u64,

    /// Directories to ignore (can be specified multiple times)
    #[arg(long)]
    pub ignore: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Host,
    Join,
}

impl Role {
    pub fn instance_id(self) -> u64 {
        match self {
            Self::Host => 0,
            Self::Join => 1,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Join => "join",
        }
    }
}

impl Cli {
    pub fn role(&self) -> Role {
        match &self.command {
            Command::Host(_) => Role::Host,
            Command::Join(_) => Role::Join,
        }
    }

    pub fn args(&self) -> &RunArgs {
        match &self.command {
            Command::Host(args) | Command::Join(args) => args,
        }
    }

    pub fn input(&self) -> &PathBuf {
        &self.args().input
    }

    pub fn shm_name(&self) -> &str {
        &self.args().shm_name
    }

    pub fn shm_size(&self) -> usize {
        self.args().shm_size
    }

    pub fn verbose(&self) -> u8 {
        self.args().verbose
    }

    pub fn conflict(&self) -> &ConflictStrategy {
        &self.args().conflict
    }

    pub fn debounce_ms(&self) -> u64 {
        self.args().debounce_ms
    }

    pub fn ignore(&self) -> &[String] {
        &self.args().ignore
    }

    pub fn instance_id(&self) -> u64 {
        self.role().instance_id()
    }
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum ConflictStrategy {
    /// Reserved for last-write-wins conflict resolution.
    LastWriteWins,
    /// Reserved for keep-both conflict resolution.
    KeepBoth,
}
