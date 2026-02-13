# Technical Approach

## Architecture Overview

```
[CAPTURE — maintainer only]
  Alchemy archive RPC
    └─ scripts/capture.py
         ├─ eth_getBlockByNumber      (free tier)
         ├─ eth_getBlockReceipts      (free tier)
         └─ debug_traceBlockByNumber  (Growth tier, skipped if unavailable)
         → capture.db (SQLite, intermediate)
         → blocks.parquet + transactions.parquet + logs.parquet
         → euler-finance.zip (distributed)

[REPLAY — every user, fully offline]
  euler-finance.zip
    └─ tools/offline-replay (Rust binary)
         → ethereum.db (SQLite)
         → sqlite3 queries
```

## Data Capture (scripts/capture.py)

### RPC Methods Used

| Method | Tier | Data |
|--------|------|------|
| `eth_getBlockByNumber` | Free | Block header + transaction list |
| `eth_getBlockReceipts` | Free | All receipts (gas used, status, logs) |
| `debug_traceBlockByNumber` | Growth ($) | Internal calls, storage diffs |

### Rate Limiting Strategy
- All RPC calls routed through `rpc_call()` wrapper
- 429 response → exponential backoff: 2s, 4s, 8s, 16s...
- 0.5s delay between blocks (`BLOCK_DELAY`)
- Other 4xx → raise immediately (bad request, not transient)

### Parquet Export
Explicit type handling required — SQLite stores everything as text/blob, pyarrow needs hints:

```python
STRING_COLS = {'hash', 'parent_hash', 'from_addr', 'to_addr', 'value',
               'gas_price', 'base_fee', 'tx_hash', 'address',
               'topic0', 'topic1', 'topic2', 'topic3'}
BINARY_COLS = {'input', 'data'}
# Everything else → int64
```

Compression: zstd via `pq.ParquetWriter(..., compression='zstd')`

### Resumability
- `sync_state` table in `capture.db` tracks `last_block`
- On restart, reads last_block and continues from there
- `--export-only` flag skips capture, re-exports existing `capture.db` to Parquet

## Offline Replay (tools/offline-replay)

Pure Rust, no network. Reads Parquet via `arrow` + `parquet` crates, writes SQLite via `rusqlite`.

### Arrow Column Helpers
```rust
fn col_str(batch, name) -> &StringArray   // text columns
fn col_i64(batch, name) -> &Int64Array    // integer columns
fn col_bin(batch, name) -> &BinaryArray   // blob columns
fn opt_str(arr, i) -> Option<&str>        // nullable text
fn opt_bin(arr, i) -> Option<&[u8]>       // nullable blob
```

### Performance
- WAL mode + `synchronous=NORMAL` + `cache_size=-65536` (64 MB)
- Each table loaded inside a single `BEGIN`/`COMMIT` transaction
- `INSERT OR IGNORE` for blocks and transactions (idempotent re-runs)
- Progress bars via `indicatif`

### Build
```bash
cd tools/offline-replay
cargo build --release
# Produces: target/release/offline-replay (~4 MB stripped)
```

Profile: `opt-level=3`, `lto=true`, `codegen-units=1`, `strip=true`

## reth-exex-indexer (future / alternative capture)

A Reth Execution Extension that runs **inside** the node process — zero-copy access to block data as it's processed. More efficient than RPC for large captures, but requires a full archive node (~2 TB).

**When to use:** When capturing 1000+ blocks or needing storage diffs without paying Growth tier RPC.

**Blocked on:** Disk space. Reth mainnet archive needs 2 TB+.

### Key Reth API Notes (reth main, Feb 2026)

These were hard-won through compiler errors — do not change without verifying against Cargo.lock:

```rust
// Required trait imports for block/tx access:
use alloy_consensus::{transaction::TxHashRef, BlockHeader, Sealable, Transaction, TxReceipt};
use reth_primitives_traits::{BlockBody, NodePrimitives, RecoveredBlock, SignedTransaction, SignerRecoverable};

// API changes from v1.9.0 → main:
// - reth_tracing::init_tracing() removed (CLI handles it)
// - ctx.notifications.next() → ctx.notifications.try_next().await? (TryStreamExt)
// - ExExNotification yields &Arc<Chain<P>>, not &Chain
// - ExExEvent::FinishedHeight takes BlockNumHash, not u64
// - receipt.success / .logs → .status() / .logs() via TxReceipt trait
// - block.number → block.header().number() via BlockHeader trait
// - block.body().transactions → block.body().transactions() via BlockBody trait
// - chain.blocks_and_receipts() yields RecoveredBlock<P::Block>
// - chain.fork_block() is a built-in helper
```

## Dataset Format (distributed zip)

```
euler-finance.zip
├── blocks.parquet
├── transactions.parquet
├── logs.parquet
├── offline-replay          # compiled binary (platform-specific)
├── metadata.json
└── queries.sql
```

Users unzip, run `./offline-replay --data . --out ethereum.db`, then query with `sqlite3`.
