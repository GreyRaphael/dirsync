# DirSync — Directory Sync over Shared Memory

## Overview

`dirsync` is a CLI tool that keeps two directories in sync using shared-memory IPC.
Two processes each monitor a directory and exchange file-change events through a
shared-memory ring buffer for near-real-time bidirectional synchronisation.

## Architecture

### High-level

```
┌─────────────────┐     Shared Memory      ┌─────────────────┐
│   Process A     │    ┌──────────────┐     │   Process B     │
│  (host -i dir1) │    │  Ring Buffer │     │  (join -i dir2) │
│                 │    │  ┌────────┐  │     │                 │
│  ┌───────────┐  │    │  │ Events │  │     │  ┌───────────┐  │
│  │  Watcher  │──┼───>│  └────────┘  │<────┼──│  Applier  │  │
│  └───────────┘  │    └──────────────┘     │  └───────────┘  │
│  ┌───────────┐  │                         │  ┌───────────┐  │
│  │  Chunker  │  │                         │  │ Conflict  │  │
│  └───────────┘  │                         │  │ Resolver  │  │
└─────────────────┘                         └─────────────────┘
```

### Module structure

| Module     | File          | Purpose                                                |
|------------|---------------|--------------------------------------------------------|
| `cli`      | `src/cli.rs`  | clap CLI — `host`/`join` subcommands, conflict strategy |
| `event`    | `src/event.rs`| `SyncEvent` enum, `EventEnvelope`, `ShmHeader` repr(C) |
| `shm`      | `src/shm.rs`  | Shared-memory transport — ring buffer, owner-encoded spinlock |
| `watcher`  | `src/watcher.rs` | notify-based FS watcher, debounce, blake3 hashing, initial scan |
| `chunker`  | `src/chunker.rs` | Large-file chunking helpers (used by tests & sync engine) |
| `apply`    | `src/apply.rs`| Applies remote events locally — conflict detection & resolution |
| `sync`     | `src/sync.rs` | SyncEngine — heartbeat, echo suppression, streaming push, event loop |

### Shared memory layout

```
Offset   Size   Field
0x00     4      Magic "DSYN"
0x04     4      Protocol version (currently 1)
0x08     8      Sequence counter A
0x10     8      Sequence counter B
0x18     4      Spinlock A (encodes holder instance_id + 1)
0x1C     4      Spinlock B (reserved)
0x20     4      Ring buffer write cursor
0x24     4      Ring buffer read cursor A
0x28     4      Ring buffer read cursor B
0x2C     4      Ring buffer capacity
0x30     4      active_mask (bitmask of live instances)
0x34     4      reserved
0x38     ...    Ring buffer data (circular, wrap-around sentinel)
```

## Sync flow

1. **Initial scan** — `watcher::initial_scan` produces `DirCreated` / `FileCreated` events for every entry.
2. **Streaming push** — For each file event, the engine does two streaming passes:
   - Pass 1: compute blake3 hash and size using a 1 MB read buffer (constant memory).
   - Pass 2: re-open the file and push `FileContent` chunks to SHM.
3. **Pop & apply** — The receiver drains the ring buffer, applies events to the local FS.
4. **Conflict detection** — On `FileModified`, the applier compares the local file hash against both the remote hash and the last-synced baseline.
5. **Conflict resolution** — Depends on `--conflict` strategy:
   - `last-write-wins` (default): compare remote event timestamp with local file mtime. Newer side wins.
   - `keep-both`: rename the local copy to `<name>.local.<ext>` and accept the remote version.
6. **Heartbeat** — Sent every 5 s. Peer declared offline after 15 s without a heartbeat.
7. **Peer crash recovery** — When the peer times out, the survivor force-clears the peer's `active_mask` bit so the ring buffer writer is no longer blocked by the dead reader.
8. **Graceful shutdown** — Ctrl+C triggers `Drop` on `ShmTransport`, which clears the instance's `active_mask` bit.

## Spinlock design

The ring buffer uses a single CAS-based spinlock (`lock_a`).  The lock value
encodes the holder's `instance_id + 1` (1 for instance 0, 2 for instance 1;
0 means unlocked).

After `MAX_SPIN` (2 000 000) iterations the contender checks the lock holder's
identity against `active_mask`:

- If the holder has **unregistered** → force-reclaim is safe.
- If the holder is **still active** → refuse reclaim to avoid concurrent
  read/write corruption; the caller receives an error.

## Event types

```rust
enum SyncEvent {
    FileCreated  { path, content_hash, size },
    FileModified { path, content_hash, size },
    FileDeleted  { path },
    DirCreated   { path },
    DirDeleted   { path },
    FileContent  { path, offset, data },   // chunked file transfer
    Heartbeat    { timestamp },             // process liveness
}
```

## Tech stack

| Component          | Crate                  | Purpose                        |
|--------------------|------------------------|--------------------------------|
| CLI parsing        | `clap` 4 (derive)      | Subcommands and options        |
| File watching      | `notify` 8             | Cross-platform FS events       |
| Shared memory      | `shared_memory` 0.12   | IPC shared-memory segments     |
| Directory walking  | `walkdir` 2            | Recursive initial scan         |
| Content hashing    | `blake3` 1             | Fast streaming file hashing    |
| Serialisation      | `bincode` 1 + `serde`  | Event encoding for the ring    |
| Logging            | `tracing` 0.1          | Structured diagnostics         |
| Clock              | `chrono` 0.4           | Wall-clock timestamps          |
| Shutdown           | `ctrlc` 3              | Ctrl+C signal handling         |
| Error handling     | `anyhow` 1             | Context-rich `Result`          |

## CLI usage

```
dirsync host -i <DIR> [OPTIONS]
dirsync join -i <DIR> [OPTIONS]

Options:
      --shm-name <NAME>        SHM segment name [default: dirsync_shm]
      --shm-size <BYTES>       SHM size in bytes [default: 67108864]
  -v, --verbose...             Verbosity (-v / -vv / -vvv)
      --conflict <STRATEGY>    last-write-wins | keep-both [default: last-write-wins]
      --debounce-ms <MS>       Debounce interval [default: 100]
      --ignore <DIR>           Directories to ignore (repeatable)
```

## Development

```bash
cargo clippy -- -D warnings   # lint
cargo test                     # unit + integration tests
RUST_LOG=debug cargo run -- host -i ./test-dir -v
```
