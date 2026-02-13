//! offline-replay — Load a DeFi Replay Kit dataset into a queryable SQLite database.
//!
//! Usage:
//!   offline-replay --data ./euler-finance --out ethereum.db
//!
//! The --data directory must contain:
//!   blocks.parquet
//!   transactions.parquet
//!   logs.parquet
//!
//! Output: a SQLite database with the same schema, ready to query with sqlite3.

use std::path::{Path, PathBuf};

use arrow::array::{
    Array, BinaryArray, Int64Array, StringArray,
};
use clap::Parser;
use eyre::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rusqlite::{params, Connection};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(
    name = "offline-replay",
    about = "Load a DeFi Replay Kit Parquet dataset into a queryable SQLite database"
)]
struct Args {
    /// Directory containing blocks.parquet, transactions.parquet, logs.parquet
    #[arg(short, long, default_value = ".")]
    data: PathBuf,

    /// Output SQLite database path
    #[arg(short, long, default_value = "ethereum.db")]
    out: PathBuf,
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const SCHEMA: &str = "
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-65536;

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
";

// ---------------------------------------------------------------------------
// Helpers — extract typed columns from Arrow batches
// ---------------------------------------------------------------------------

fn col_str<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column: {name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("column {name} is not StringArray"))
}

fn col_i64<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column: {name}"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column {name} is not Int64Array"))
}

fn col_bin<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a BinaryArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column: {name}"))
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap_or_else(|| panic!("column {name} is not BinaryArray"))
}

fn opt_str(arr: &StringArray, i: usize) -> Option<&str> {
    if arr.is_null(i) { None } else { Some(arr.value(i)) }
}

fn opt_bin(arr: &BinaryArray, i: usize) -> Option<&[u8]> {
    if arr.is_null(i) { None } else { Some(arr.value(i)) }
}

// ---------------------------------------------------------------------------
// Progress bar
// ---------------------------------------------------------------------------

fn progress_bar(total: u64, msg: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(
            "{msg:20} [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
        )
        .unwrap()
        .progress_chars("=> "),
    );
    pb.set_message(msg.to_string());
    pb
}

// ---------------------------------------------------------------------------
// Load functions
// ---------------------------------------------------------------------------

fn load_blocks(conn: &Connection, path: &Path) -> Result<u64> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Cannot open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let total_rows = builder.metadata().file_metadata().num_rows() as u64;
    let reader = builder.build()?;

    let pb = progress_bar(total_rows, "blocks");
    let mut count = 0u64;

    let mut stmt = conn.prepare_cached(
        "INSERT OR IGNORE INTO blocks
         (number, hash, parent_hash, timestamp, gas_used, gas_limit, base_fee, tx_count)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
    )?;

    for batch in reader {
        let batch = batch?;
        let number      = col_i64(&batch, "number");
        let hash        = col_str(&batch, "hash");
        let parent_hash = col_str(&batch, "parent_hash");
        let timestamp   = col_i64(&batch, "timestamp");
        let gas_used    = col_i64(&batch, "gas_used");
        let gas_limit   = col_i64(&batch, "gas_limit");
        let base_fee    = col_str(&batch, "base_fee");
        let tx_count    = col_i64(&batch, "tx_count");

        for i in 0..batch.num_rows() {
            stmt.execute(params![
                number.value(i),
                hash.value(i),
                parent_hash.value(i),
                timestamp.value(i),
                gas_used.value(i),
                gas_limit.value(i),
                opt_str(base_fee, i),
                tx_count.value(i),
            ])?;
            count += 1;
        }
        pb.inc(batch.num_rows() as u64);
    }

    pb.finish_with_message(format!("blocks ✓ ({count})"));
    Ok(count)
}

fn load_transactions(conn: &Connection, path: &Path) -> Result<u64> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Cannot open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let total_rows = builder.metadata().file_metadata().num_rows() as u64;
    let reader = builder.build()?;

    let pb = progress_bar(total_rows, "transactions");
    let mut count = 0u64;

    let mut stmt = conn.prepare_cached(
        "INSERT OR IGNORE INTO transactions
         (hash, block_number, tx_index, from_addr, to_addr, value,
          gas_used, gas_price, input, status)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
    )?;

    for batch in reader {
        let batch = batch?;
        let hash         = col_str(&batch, "hash");
        let block_number = col_i64(&batch, "block_number");
        let tx_index     = col_i64(&batch, "tx_index");
        let from_addr    = col_str(&batch, "from_addr");
        let to_addr      = col_str(&batch, "to_addr");
        let value        = col_str(&batch, "value");
        let gas_used     = col_i64(&batch, "gas_used");
        let gas_price    = col_str(&batch, "gas_price");
        let input        = col_bin(&batch, "input");
        let status       = col_i64(&batch, "status");

        for i in 0..batch.num_rows() {
            stmt.execute(params![
                hash.value(i),
                block_number.value(i),
                tx_index.value(i),
                from_addr.value(i),
                opt_str(to_addr, i),
                value.value(i),
                gas_used.value(i),
                gas_price.value(i),
                opt_bin(input, i).unwrap_or(&[]),
                status.value(i),
            ])?;
            count += 1;
        }
        pb.inc(batch.num_rows() as u64);
    }

    pb.finish_with_message(format!("transactions ✓ ({count})"));
    Ok(count)
}

fn load_logs(conn: &Connection, path: &Path) -> Result<u64> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Cannot open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let total_rows = builder.metadata().file_metadata().num_rows() as u64;
    let reader = builder.build()?;

    let pb = progress_bar(total_rows, "logs");
    let mut count = 0u64;

    let mut stmt = conn.prepare_cached(
        "INSERT INTO logs
         (block_number, tx_hash, log_index, address,
          topic0, topic1, topic2, topic3, data)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
    )?;

    for batch in reader {
        let batch = batch?;
        let block_number = col_i64(&batch, "block_number");
        let tx_hash      = col_str(&batch, "tx_hash");
        let log_index    = col_i64(&batch, "log_index");
        let address      = col_str(&batch, "address");
        let topic0       = col_str(&batch, "topic0");
        let topic1       = col_str(&batch, "topic1");
        let topic2       = col_str(&batch, "topic2");
        let topic3       = col_str(&batch, "topic3");
        let data         = col_bin(&batch, "data");

        for i in 0..batch.num_rows() {
            stmt.execute(params![
                block_number.value(i),
                tx_hash.value(i),
                log_index.value(i),
                address.value(i),
                opt_str(topic0, i),
                opt_str(topic1, i),
                opt_str(topic2, i),
                opt_str(topic3, i),
                opt_bin(data, i),
            ])?;
            count += 1;
        }
        pb.inc(batch.num_rows() as u64);
    }

    pb.finish_with_message(format!("logs ✓ ({count})"));
    Ok(count)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let args = Args::parse();

    let blocks_path = args.data.join("blocks.parquet");
    let txs_path    = args.data.join("transactions.parquet");
    let logs_path   = args.data.join("logs.parquet");

    for p in [&blocks_path, &txs_path, &logs_path] {
        if !p.exists() {
            eyre::bail!("Missing file: {}", p.display());
        }
    }

    println!("Output: {}", args.out.display());

    let conn = Connection::open(&args.out)
        .with_context(|| format!("Cannot open {}", args.out.display()))?;
    conn.execute_batch(SCHEMA)?;

    // Wrap all inserts in a single transaction per table for speed
    conn.execute_batch("BEGIN;")?;
    let blocks = load_blocks(&conn, &blocks_path)?;
    conn.execute_batch("COMMIT;")?;

    conn.execute_batch("BEGIN;")?;
    let txs = load_transactions(&conn, &txs_path)?;
    conn.execute_batch("COMMIT;")?;

    conn.execute_batch("BEGIN;")?;
    let logs = load_logs(&conn, &logs_path)?;
    conn.execute_batch("COMMIT;")?;

    println!("\nDone.");
    println!("  {blocks} blocks");
    println!("  {txs} transactions");
    println!("  {logs} logs");
    println!("\nQuery with:  sqlite3 {}", args.out.display());

    Ok(())
}
