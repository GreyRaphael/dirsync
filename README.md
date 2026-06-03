# dirsync

Directory synchronization over shared memory — a CLI tool that keeps two directories in sync using shared-memory IPC.

## Features

- **Shared memory transport** — events exchanged via a shared-memory ring buffer, no network overhead
- **Real-time monitoring** — uses OS-native file watchers (inotify / FSEvents / ReadDirectoryChangesW)
- **Incremental sync** — only transfers changed files, with blake3 content hashing
- **Large file support** — automatic chunked transfer for large files
- **Conflict detection** — detects simultaneous modifications and reports conflicts while preserving the local copy
- **Event debouncing** — merges rapid filesystem events within a configurable window
- **Heartbeat** — monitors peer liveness with periodic heartbeat events
- **Graceful shutdown** — handles Ctrl+C cleanly, no leaked shared memory segments

## Installation

```bash
cargo install --path .
```

Or build from source:

```bash
cargo build --release
```

## Usage

Open two terminals and point each at a directory to sync:

```bash
# Terminal 1
dirsync host -i /path/to/directory-a

# Terminal 2
dirsync join -i /path/to/directory-b
```

The host creates the shared memory segment; the join process connects to it. Changes in either directory are automatically synced to the other.

### Options

```
Usage: dirsync <COMMAND>

Commands:
  host  Start the first process in a sync pair
  join  Join an existing sync pair

Command options:
  -i, --input <INPUT>              Directory to monitor and sync
      --shm-name <SHM_NAME>        Shared memory segment name [default: dirsync_shm]
      --shm-size <SHM_SIZE>        Shared memory size in bytes [default: 67108864]
  -v, --verbose...                 Verbose output (-v, -vv, -vvv)
      --conflict <CONFLICT>        Conflict mode: last-write-wins | keep-both [default: last-write-wins]
      --debounce-ms <DEBOUNCE_MS>  Debounce interval in ms [default: 100]
      --ignore <IGNORE>            Directories to ignore (repeatable)
  -h, --help                       Print help
  -V, --version                    Print version
```

### Examples

```bash
# Sync with verbose logging
dirsync host -i ./project-a -v

# Custom shared memory name (for multiple sync pairs)
dirsync host -i ./docs-a --shm-name docs_sync
dirsync join -i ./docs-b --shm-name docs_sync

# Ignore node_modules and .git
dirsync host -i ./src-a --ignore node_modules --ignore .git
dirsync join -i ./src-b --ignore node_modules --ignore .git

# Choose a conflict mode
dirsync host -i ./work-a --conflict keep-both
dirsync join -i ./work-b --conflict keep-both
```

> Note: the current release detects conflicts and preserves the local copy. Full automatic `last-write-wins` / `keep-both` resolution is reserved for a follow-up.

## Architecture

```
┌─────────────────┐     Shared Memory      ┌─────────────────┐
│   Process A     │    ┌──────────────┐     │   Process B     │
│                 │    │  Ring Buffer  │     │                 │
│  ┌───────────┐  │    │  ┌────────┐  │     │  ┌───────────┐  │
│  │  Watcher  │──┼───>│  │ Events │  │<────┼──│  Applier  │  │
│  └───────────┘  │    │  └────────┘  │     │  └───────────┘  │
│       │         │    └──────────────┘     │       │         │
│  ┌───────────┐  │                         │  ┌───────────┐  │
│  │  Chunker  │  │                         │  │ Conflict  │  │
│  └───────────┘  │                         │  │ Detector  │  │
└─────────────────┘                         └─────────────────┘
```

### Module Structure

| Module | Purpose |
|--------|---------|
| `cli` | Command-line argument parsing (clap) |
| `event` | SyncEvent types and SHM header layout |
| `shm` | Shared memory transport with ring buffer |
| `watcher` | Filesystem monitoring with debounce and state tracking |
| `chunker` | Large file chunked transfer |
| `apply` | Apply remote events to local filesystem |
| `sync` | Main sync engine (heartbeat, conflict detection, event loop) |

### Shared Memory Layout

```
Offset   Size   Field
0x00     4      Magic "DSYN"
0x04     4      Protocol version
0x08     8      Sequence A / B
0x18     4      Spinlock A / B
0x20     4      Ring buffer write cursor
0x24     4      Ring buffer read cursor A
0x28     4      Ring buffer read cursor B
0x2C     4      Ring buffer capacity
0x38     ...    Ring buffer (circular, wrap-around sentinel)
```

### Sync Flow

1. **Initial scan** — scan directory, push FileCreated + FileContent events
2. **Watch loop** — monitor filesystem changes with debouncing
3. **Push** — serialize events into SHM ring buffer
4. **Pop** — read remote events, apply to local filesystem
5. **Heartbeat** — send every 5s, detect peer offline after 15s

## Development

```bash
# Check compilation
cargo clippy -- -D warnings

# Run all tests (unit + integration)
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -- host -i ./test-dir -v
```

## License

MIT
