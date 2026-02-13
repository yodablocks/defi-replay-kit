# DeFi Replay Kit

> Pre-packaged blockchain datasets for offline DeFi security analysis. No RPC, no archive node, no ongoing costs.

## What is this?

Downloadable data packs covering real DeFi exploits and MEV attacks — blocks, transactions, and logs in Parquet format — plus an offline indexer binary that loads them into a queryable SQLite database.

**Problem:** Analyzing historical Ethereum data requires:
- Running archive nodes (~$50-200/month on a VPS, or 2 TB+ disk)
- Paying for RPC access to historical data ($50-200/month)
- Days of sync time and complex setup

**Solution:** Download a small dataset (~100 MB) containing 300 blocks around a real attack. Load it locally in seconds. Query with SQL. Zero ongoing costs.

## Quick Start

```bash
# 1. Download a dataset
curl -LO https://github.com/yodablocks/defi-replay-kit/releases/download/v0.1.0/euler-finance.zip
unzip euler-finance.zip
cd euler-finance/

# 2. Load into SQLite (takes ~5 seconds)
./offline-replay --data . --out ethereum.db

# 3. Query
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
- **Block range:** 16,817,896 – 16,818,196 (300 blocks)
- **Contents:** blocks, transactions, event logs

Key transaction: `0xc310a0af...` — flash loan from Aave → donate to reserve → bad debt position → liquidate at profit.

## Repository Layout

```
defi-replay-kit/
├── tools/
│   ├── offline-replay/      # Parquet → SQLite indexer (Rust, ships in each zip)
│   └── reth-exex-indexer/   # Data capture tool (Rust, Reth ExEx — for maintainers)
├── scripts/
│   └── capture.py           # RPC-based data capture script (Python, for maintainers)
├── examples/
│   └── euler-finance/       # Euler Finance dataset + example queries
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

See [`examples/euler-finance/queries.sql`](examples/euler-finance/queries.sql) for 8 ready-to-run forensic queries, including:

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
| Ongoing cost | Free (needs RPC) | $500/team/mo | One-time download |
| Works offline | No | No | **Yes** |
| Pre-packaged data | No | No | **Yes** |
| Setup time | 30 min | 10 min | ~2 min |

## License

MIT — see [LICENSE](LICENSE)

## Disclaimer

Educational purposes only. All data is sourced from the public Ethereum blockchain.
