-- Euler Finance Exploit — Example SQL Queries
-- Run with: sqlite3 ethereum.db < queries.sql
-- Or interactively: sqlite3 ethereum.db

-- ============================================================
-- 1. Overview of the exploit block
-- ============================================================

SELECT
    number,
    hash,
    datetime(timestamp, 'unixepoch') AS block_time,
    tx_count,
    gas_used
FROM blocks
WHERE number = 16817996;


-- ============================================================
-- 2. The main exploit transaction
-- ============================================================

SELECT
    tx_index,
    hash,
    from_addr   AS attacker,
    to_addr     AS exploit_contract,
    gas_used,
    status
FROM transactions
WHERE hash = '0xc310a0affe2169d1f6feec1c63dbc7f7c62a887fa48795d327d4d2da2d6b111d';


-- ============================================================
-- 3. All events emitted by the exploit transaction
--    (shows flash loan, donation, liquidation events)
-- ============================================================

SELECT
    log_index,
    address     AS contract,
    topic0      AS event_signature,
    topic1,
    topic2,
    hex(data)   AS data_hex
FROM logs
WHERE tx_hash = '0xc310a0affe2169d1f6feec1c63dbc7f7c62a887fa48795d327d4d2da2d6b111d'
ORDER BY log_index;


-- ============================================================
-- 4. All transactions from the attacker EOA across the dataset
-- ============================================================

SELECT
    block_number,
    tx_index,
    hash,
    to_addr,
    gas_used,
    status
FROM transactions
WHERE from_addr = '0x5f259d0b76665c337c6104145894f4d1d2758b8c'
ORDER BY block_number, tx_index;


-- ============================================================
-- 5. ERC-20 Transfer events in the exploit block
--    topic0 = keccak256("Transfer(address,address,uint256)")
-- ============================================================

SELECT
    l.log_index,
    l.address   AS token_contract,
    l.topic1    AS from_addr,
    l.topic2    AS to_addr,
    hex(l.data) AS value_hex
FROM logs l
JOIN transactions t ON l.tx_hash = t.hash
WHERE t.block_number = 16817996
  AND l.topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
ORDER BY l.log_index
LIMIT 50;


-- ============================================================
-- 6. Most active contracts in the exploit block (by log count)
-- ============================================================

SELECT
    l.address   AS contract,
    COUNT(*)    AS log_count
FROM logs l
JOIN transactions t ON l.tx_hash = t.hash
WHERE t.block_number = 16817996
GROUP BY l.address
ORDER BY log_count DESC
LIMIT 20;


-- ============================================================
-- 7. Blocks before/during/after — transaction volume timeline
-- ============================================================

SELECT
    b.number,
    datetime(b.timestamp, 'unixepoch') AS block_time,
    b.tx_count,
    COUNT(DISTINCT t.hash) AS txs_in_db
FROM blocks b
LEFT JOIN transactions t ON t.block_number = b.number
GROUP BY b.number
ORDER BY b.number;


-- ============================================================
-- 8. Unique contracts interacted with during the exploit tx
-- ============================================================

SELECT DISTINCT address AS contract
FROM logs
WHERE tx_hash = '0xc310a0affe2169d1f6feec1c63dbc7f7c62a887fa48795d327d4d2da2d6b111d'
ORDER BY address;
