# DeFi Replay Kit

> Pre-packaged blockchain datasets for offline DeFi security analysis. No RPC, no archive node, no ongoing costs.

## What is this?

Downloadable data packs covering real DeFi exploits and MEV attacks — blocks, transactions, and logs in Parquet format — plus an offline indexer binary that loads them into a queryable SQLite database.

**Problem:** Analyzing historical Ethereum data requires:
- Running archive nodes (~$50-200/month on a VPS, or 2 TB+ disk)
- Paying for RPC access to historical data ($50-200/month)
- Days of sync time and complex setup

**Solution:** Download one dataset (17 to 102 MB) covering the blocks around a real attack. Load it locally in seconds. Query with SQL. Zero ongoing costs.

## Quick Start

```bash
REL=https://github.com/yodablocks/defi-replay-kit/releases/download/v0.2.0

# 1. Pick a dataset and unpack it
#      euler-finance.zip    17 MB, one exploit, 301 blocks
#      curve-vyper.zip     102 MB, three exploits, 867 blocks
curl -LO $REL/euler-finance.zip
unzip euler-finance.zip -d euler-finance
cd euler-finance/

# 2. Download the indexer for your platform
#      offline-replay-linux-x86_64   statically linked, any distro
#      offline-replay-macos-arm64    Apple Silicon
#      offline-replay-macos-x86_64   Intel Mac
curl -Lo offline-replay $REL/offline-replay-linux-x86_64
chmod +x offline-replay
# The binaries are unsigned. If macOS refuses to run one:
#     xattr -d com.apple.quarantine offline-replay

# 3. Load into SQLite (takes ~5 seconds)
./offline-replay --data . --out ethereum.db

# 4. Query
sqlite3 ethereum.db "SELECT hash, gas_used FROM transactions WHERE block_number = 16817996"
```

Or run SQL interactively:
```bash
sqlite3 ethereum.db
```

## Datasets

### Euler Finance Exploit — $197M (March 2023)

- **Attack type:** Donation attack + flash loan
- **Block:** 16,817,996
- **Block range:** 16,817,896 – 16,818,196 (301 blocks)
- **Download:** 17 MB
- **Contents:** blocks, transactions, event logs (Parquet) plus metadata.json and queries.sql

Key transaction: `0xc310a0af...` — flash loan from Aave → donate to reserve → bad debt position → liquidate at profit.

### Curve Vyper Reentrancy — three pools, $37M (July 2023)

- **Attack type:** Reentrancy through a malfunctioning lock in Vyper 0.2.15, 0.2.16 and 0.3.0
- **Block range:** 17,806,006 – 17,806,872 (867 blocks)
- **Download:** 102 MB
- **Contents:** blocks, transactions, event logs (Parquet) plus metadata.json and queries.sql

| Block | Pool | Loss |
|---|---|---|
| 17,806,056 | JPEG'd pETH/ETH | ~$11M |
| 17,806,550 | Metronome msETH/ETH | ~$3.4M |
| 17,806,772 | Alchemix alETH/ETH | ~$22.6M |

One compiler bug, three pools, 716 blocks apart. Each attack flash loaned WETH
from the Balancer vault and re-entered the pool through `remove_liquidity` while
its balances were mid-update. The msETH transaction was sent by c0ffeebabe.eth
and is reported as a whitehat rescue rather than a theft. The Curve CRV/ETH pool
was hit in the same campaign at blocks 17,807,830 and 17,808,683, outside this
window.

## Repository Layout

```
defi-replay-kit/
├── tools/
│   └── offline-replay/      # Parquet → SQLite indexer (Rust, prebuilt in each release)
├── scripts/
│   └── capture.py           # RPC-based data capture script (Python, for maintainers)
├── examples/
│   ├── euler-finance/       # Euler Finance dataset + example queries
│   └── curve-vyper/         # Curve Vyper reentrancy dataset + example queries
└── README.md
```

## Building the Indexer

```bash
cd tools/offline-replay
cargo build --release
# Binary: target/release/offline-replay
```

**Requirements:** Rust 1.80+

```
offline-replay --data <dir>  --out <file.db>

Options:
  -d, --data <DIR>   Directory containing blocks.parquet, transactions.parquet, logs.parquet
  -o, --out <FILE>   Output SQLite database path [default: ethereum.db]
```

## SQLite Schema

```sql
blocks        (number, hash, parent_hash, timestamp, gas_used, gas_limit, base_fee, tx_count)
transactions  (hash, block_number, tx_index, from_addr, to_addr, value, gas_used, gas_price, input, status)
logs          (id, block_number, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data)
```

Indexes on `block_number`, `from_addr`, `to_addr`, `address`, `topic0`.

## Example Queries

See [`examples/euler-finance/queries.sql`](examples/euler-finance/queries.sql) and [`examples/curve-vyper/queries.sql`](examples/curve-vyper/queries.sql) for ready-to-run forensic queries, including:

- All events emitted by the exploit transaction
- ERC-20 Transfer events in the exploit block
- Most active contracts by log count
- Full attacker EOA activity

## Use Cases

- **Security students** — analyze real exploits without infrastructure costs
- **Smart contract auditors** — build pattern recognition from historical data
- **Researchers** — study MEV, flash loans, liquidation cascades offline
- **Tool builders** — prototype indexers and detectors against known-good data

## Comparison

| Feature | DeFiHackLabs | Phalcon Fork | DeFi Replay Kit |
|---------|:------------:|:------------:|:---------------:|
| Ongoing cost | Free (needs RPC) | Paid plans | One-time download |
| Works offline | No | No | **Yes** |
| Pre-packaged data | No | No | **Yes** |
| Setup time | 30 min | 10 min | ~2 min |

## License

MIT — see [LICENSE](LICENSE)

## Disclaimer

Educational purposes only. All data is sourced from the public Ethereum blockchain.
