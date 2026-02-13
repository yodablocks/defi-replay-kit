#!/usr/bin/env python3
"""
capture.py — Pull a block range from an Alchemy archive RPC and write to SQLite + Parquet.

Usage:
    export RPC_URL="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
    python capture.py --start 16817896 --end 16818196 --out euler-finance

Output:
    euler-finance/capture.db      ← SQLite (blocks, transactions, logs, traces)
    euler-finance/blocks.parquet  ← Parquet export for distribution

Schema matches tools/reth-exex-indexer/src/db.rs exactly, plus a `traces` table.
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

# Load .env from the same directory as this script
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

RPC_URL = os.environ.get("RPC_URL", "")
CONCURRENCY = 1        # sequential — Alchemy free tier is ~10 req/s
RETRY_MAX = 8
RETRY_DELAY = 2.0      # base seconds between retries (doubles on 429)
BLOCK_DELAY = 0.5      # seconds between blocks to stay under rate limit


# ---------------------------------------------------------------------------
# RPC helpers
# ---------------------------------------------------------------------------

def connect(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 60}))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    if not w3.is_connected():
        print(f"ERROR: Cannot connect to {rpc_url}", file=sys.stderr)
        sys.exit(1)
    print(f"Connected — latest block: {w3.eth.block_number:,}")
    return w3


def rpc_call(w3: Web3, method: str, params: list, retries: int = RETRY_MAX):
    """Raw JSON-RPC call with retry on transient errors.
    - 429 Too Many Requests: retry with exponential backoff
    - 4xx other: raise immediately (unsupported method, bad params)
    - 5xx / network errors: retry
    """
    import requests
    for attempt in range(retries):
        try:
            response = w3.provider.make_request(method, params)
            if "error" in response:
                raise ValueError(response["error"])
            return response.get("result")
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 429:
                wait = RETRY_DELAY * (2 ** attempt)  # 2s, 4s, 8s, 16s, 32s
                if attempt < retries - 1:
                    time.sleep(wait)
                    continue
            if status not in (429, 500, 502, 503, 504):
                raise  # hard 4xx error, don't retry
            if attempt == retries - 1:
                raise
            time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(RETRY_DELAY * (attempt + 1))


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_block(w3: Web3, block_number: int) -> dict:
    """Fetch full block with transactions."""
    result = rpc_call(w3, "eth_getBlockByNumber", [hex(block_number), True])
    return result


def fetch_receipts(w3: Web3, block_number: int) -> list:
    """Fetch all receipts for a block in one call (eth_getBlockReceipts)."""
    result = rpc_call(w3, "eth_getBlockReceipts", [hex(block_number)])
    return result or []


def fetch_traces(w3: Web3, block_number: int) -> list:
    """
    Fetch call traces via debug_traceBlockByNumber (callTracer).
    Requires Alchemy Growth plan or higher. Returns [] silently if unavailable.
    """
    try:
        response = w3.provider.make_request(
            "debug_traceBlockByNumber",
            [hex(block_number), {"tracer": "callTracer", "tracerConfig": {"withLog": True}}],
        )
        if "error" in response:
            return []
        return response.get("result") or []
    except Exception:
        return []


# ---------------------------------------------------------------------------
# SQLite schema
# ---------------------------------------------------------------------------

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS blocks (
    number      INTEGER PRIMARY KEY,
    hash        TEXT    NOT NULL,
    parent_hash TEXT    NOT NULL,
    timestamp   INTEGER NOT NULL,
    gas_used    INTEGER NOT NULL,
    gas_limit   INTEGER NOT NULL,
    base_fee    TEXT,
    tx_count    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    hash         TEXT    PRIMARY KEY,
    block_number INTEGER NOT NULL REFERENCES blocks(number),
    tx_index     INTEGER NOT NULL,
    from_addr    TEXT    NOT NULL,
    to_addr      TEXT,
    value        TEXT    NOT NULL,
    gas_used     INTEGER NOT NULL,
    gas_price    TEXT    NOT NULL,
    input        BLOB    NOT NULL,
    status       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tx_block ON transactions(block_number);
CREATE INDEX IF NOT EXISTS idx_tx_from  ON transactions(from_addr);
CREATE INDEX IF NOT EXISTS idx_tx_to    ON transactions(to_addr);

CREATE TABLE IF NOT EXISTS logs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    block_number INTEGER NOT NULL REFERENCES blocks(number),
    tx_hash      TEXT    NOT NULL REFERENCES transactions(hash),
    log_index    INTEGER NOT NULL,
    address      TEXT    NOT NULL,
    topic0       TEXT,
    topic1       TEXT,
    topic2       TEXT,
    topic3       TEXT,
    data         BLOB
);
CREATE INDEX IF NOT EXISTS idx_log_block   ON logs(block_number);
CREATE INDEX IF NOT EXISTS idx_log_address ON logs(address);
CREATE INDEX IF NOT EXISTS idx_log_topic0  ON logs(topic0);

CREATE TABLE IF NOT EXISTS traces (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    block_number INTEGER NOT NULL REFERENCES blocks(number),
    tx_hash      TEXT    NOT NULL,
    tx_index     INTEGER NOT NULL,
    trace_json   TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trace_block  ON traces(block_number);
CREATE INDEX IF NOT EXISTS idx_trace_tx     ON traces(tx_hash);

CREATE TABLE IF NOT EXISTS sync_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    last_block INTEGER NOT NULL
);
"""


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


def get_last_block(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT last_block FROM sync_state WHERE id = 1").fetchone()
    return row[0] if row else None


def set_last_block(conn: sqlite3.Connection, block_number: int):
    conn.execute(
        "INSERT INTO sync_state (id, last_block) VALUES (1, ?) "
        "ON CONFLICT(id) DO UPDATE SET last_block = ?",
        (block_number, block_number),
    )


# ---------------------------------------------------------------------------
# Write one block
# ---------------------------------------------------------------------------

def to_hex(val) -> str:
    if isinstance(val, (bytes, bytearray)):
        return "0x" + val.hex()
    return str(val)


def hex_to_int(val, default=0) -> int:
    """Convert hex string or int to int."""
    if val is None:
        return default
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        return int(val, 16)
    return default


def write_block(conn: sqlite3.Connection, block, receipts: list, traces: list):
    block_number = hex_to_int(block["number"])
    base_fee = block.get("baseFeePerGas")

    # Receipt lookup by tx hash (normalised to lowercase hex string)
    receipt_by_hash = {}
    for r in receipts:
        key = r["transactionHash"]
        if isinstance(key, bytes):
            key = "0x" + key.hex()
        receipt_by_hash[key.lower()] = r

    with conn:
        # blocks
        conn.execute(
            "INSERT OR IGNORE INTO blocks "
            "(number, hash, parent_hash, timestamp, gas_used, gas_limit, base_fee, tx_count) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                block_number,
                to_hex(block["hash"]),
                to_hex(block["parentHash"]),
                hex_to_int(block["timestamp"]),
                hex_to_int(block["gasUsed"]),
                hex_to_int(block["gasLimit"]),
                str(hex_to_int(base_fee)) if base_fee is not None else None,
                len(block["transactions"]),
            ),
        )

        for tx_index, tx in enumerate(block["transactions"]):
            tx_hash = to_hex(tx["hash"]).lower()
            receipt = receipt_by_hash.get(tx_hash, {})

            gas_used = hex_to_int(receipt.get("gasUsed", 0))
            status   = hex_to_int(receipt.get("status", 1))

            gas_price = hex_to_int(tx.get("effectiveGasPrice") or tx.get("gasPrice") or 0)

            input_data = tx.get("input") or tx.get("data") or "0x"
            if isinstance(input_data, str):
                input_data = bytes.fromhex(input_data.removeprefix("0x"))
            elif isinstance(input_data, bytes):
                pass  # already bytes

            to_addr = tx.get("to")
            if to_addr:
                to_addr = to_addr.lower()

            from_addr = tx.get("from", "").lower()
            value = str(hex_to_int(tx.get("value", 0)))

            # transactions
            conn.execute(
                "INSERT OR IGNORE INTO transactions "
                "(hash, block_number, tx_index, from_addr, to_addr, value, "
                " gas_used, gas_price, input, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    tx_hash,
                    block_number,
                    tx_index,
                    from_addr,
                    to_addr,
                    value,
                    gas_used,
                    str(gas_price),
                    input_data,
                    status,
                ),
            )

            # logs
            logs = receipt.get("logs", [])
            for log in logs:
                topics = log.get("topics", [])
                conn.execute(
                    "INSERT INTO logs "
                    "(block_number, tx_hash, log_index, address, "
                    " topic0, topic1, topic2, topic3, data) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        block_number,
                        tx_hash,
                        hex_to_int(log.get("logIndex", 0)),
                        log["address"].lower(),
                        to_hex(topics[0]) if len(topics) > 0 else None,
                        to_hex(topics[1]) if len(topics) > 1 else None,
                        to_hex(topics[2]) if len(topics) > 2 else None,
                        to_hex(topics[3]) if len(topics) > 3 else None,
                        bytes.fromhex(log.get("data", "0x").removeprefix("0x")),
                    ),
                )

        # traces — one row per tx, full callTracer JSON
        for tx_index, trace in enumerate(traces):
            tx_hash_raw = trace.get("txHash") or ""
            conn.execute(
                "INSERT INTO traces (block_number, tx_hash, tx_index, trace_json) "
                "VALUES (?, ?, ?, ?)",
                (
                    block_number,
                    to_hex(tx_hash_raw) if isinstance(tx_hash_raw, bytes) else tx_hash_raw,
                    tx_index,
                    json.dumps(trace.get("result", trace)),
                ),
            )

        set_last_block(conn, block_number)


# ---------------------------------------------------------------------------
# Parquet export
# ---------------------------------------------------------------------------

def export_parquet(db_path: str, out_dir: Path):
    """Read SQLite and write Parquet files for blocks, transactions, logs, traces."""

    # Explicit column types — avoids misdetection when a column is stored as
    # TEXT in SQLite but contains large numeric strings (gas_price, value, etc.)
    STRING_COLS = {
        "hash", "parent_hash", "base_fee",
        "from_addr", "to_addr", "value", "gas_price",
        "address", "topic0", "topic1", "topic2", "topic3",
        "tx_hash", "trace_json",
    }
    BINARY_COLS = {"input", "data"}
    # Everything else is int64

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    for table, parquet_path in [
        ("blocks",       out_dir / "blocks.parquet"),
        ("transactions", out_dir / "transactions.parquet"),
        ("logs",         out_dir / "logs.parquet"),
        ("traces",       out_dir / "traces.parquet"),
    ]:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            print(f"  {table}: empty, skipping")
            continue

        col_names = rows[0].keys()
        cols = {k: [] for k in col_names}
        for row in rows:
            for k, v in zip(col_names, tuple(row)):
                cols[k].append(v)

        arrays = []
        fields = []
        for col_name, values in cols.items():
            if col_name in BINARY_COLS:
                arr = pa.array(
                    [v if isinstance(v, (bytes, bytearray)) else None for v in values],
                    type=pa.binary(),
                )
                fields.append(pa.field(col_name, pa.binary()))
            elif col_name in STRING_COLS:
                arr = pa.array(
                    [str(v) if v is not None else None for v in values],
                    type=pa.string(),
                )
                fields.append(pa.field(col_name, pa.string()))
            else:
                arr = pa.array(
                    [int(v) if v is not None else None for v in values],
                    type=pa.int64(),
                )
                fields.append(pa.field(col_name, pa.int64()))
            arrays.append(arr)

        table_pa = pa.table({f.name: arr for f, arr in zip(fields, arrays)})
        pq.write_table(table_pa, parquet_path, compression="zstd")
        print(f"  {table}: {len(rows):,} rows → {parquet_path}")

    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Capture Ethereum block range to SQLite + Parquet")
    parser.add_argument("--start", type=int, default=None, help="First block (inclusive)")
    parser.add_argument("--end",   type=int, default=None, help="Last block (inclusive)")
    parser.add_argument("--out",   type=str, default="capture", help="Output directory name")
    parser.add_argument("--rpc",   type=str, default=RPC_URL, help="RPC URL (or set RPC_URL env var)")
    parser.add_argument("--traces", action="store_true", default=True,
                        help="Fetch call traces via debug_traceBlockByNumber (requires archive+debug)")
    parser.add_argument("--no-traces", dest="traces", action="store_false",
                        help="Skip trace fetching (faster, smaller output)")
    parser.add_argument("--export-only", action="store_true",
                        help="Skip capture, just export existing capture.db to Parquet")
    args = parser.parse_args()

    if not args.export_only and (args.start is None or args.end is None):
        print("ERROR: --start and --end are required unless using --export-only", file=sys.stderr)
        sys.exit(1)

    if not args.export_only and not args.rpc:
        print("ERROR: Set RPC_URL env var or pass --rpc", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = str(out_dir / "capture.db")

    if args.export_only:
        print("Export-only mode — skipping capture.")
        print("\nExporting to Parquet...")
        export_parquet(db_path, out_dir)
        print(f"\nDone. Output in {out_dir}/")
        return

    w3 = connect(args.rpc)
    conn = open_db(db_path)

    # Resume from last captured block if interrupted
    last = get_last_block(conn)
    start = (last + 1) if last is not None else args.start
    if start > args.end:
        print(f"Already captured up to block {last}. Nothing to do.")
    else:
        if last is not None:
            print(f"Resuming from block {start} (last captured: {last})")

        total = args.end - start + 1
        print(f"Capturing blocks {start:,} → {args.end:,} ({total:,} blocks)")
        print(f"Output: {db_path}")

        for block_number in tqdm(range(start, args.end + 1), unit="block"):
            block    = fetch_block(w3, block_number)
            receipts = fetch_receipts(w3, block_number)
            traces   = fetch_traces(w3, block_number) if args.traces else []
            write_block(conn, block, receipts, traces)
            time.sleep(BLOCK_DELAY)

    conn.close()

    print("\nExporting to Parquet...")
    export_parquet(db_path, out_dir)

    print(f"\nDone. Output in {out_dir}/")
    print(f"  capture.db          — SQLite (query directly with sqlite3)")
    print(f"  blocks.parquet      — Parquet (for offline-replay distribution)")
    print(f"  transactions.parquet")
    print(f"  logs.parquet")
    print(f"  traces.parquet")


if __name__ == "__main__":
    main()
