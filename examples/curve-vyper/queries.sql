-- Curve Vyper Reentrancy Exploits (2023-07-30) — Example SQL Queries
-- Run with: sqlite3 ethereum.db < queries.sql
-- Or interactively: sqlite3 ethereum.db
--
-- Three pools were drained in this window through the same Vyper reentrancy bug:
--   17,806,056  pETH/ETH   (JPEG'd)
--   17,806,550  msETH/ETH  (Metronome)
--   17,806,772  alETH/ETH  (Alchemix)


-- ============================================================
-- 1. The three exploit blocks side by side
-- ============================================================

SELECT
    number,
    datetime(timestamp, 'unixepoch') AS block_time,
    tx_count,
    gas_used
FROM blocks
WHERE number IN (17806056, 17806550, 17806772)
ORDER BY number;


-- ============================================================
-- 2. The three exploit transactions
-- ============================================================

SELECT
    block_number,
    tx_index,
    hash,
    from_addr AS sender,
    to_addr   AS exploit_contract,
    gas_used,
    status
FROM transactions
WHERE hash IN (
    '0xa84aa065ce61dbb1eb50ab6ae67fc31a9da50dd2c74eefd561661bfce2f1620c',
    '0xc93eb238ff42632525e990119d3edc7775299a70b56e54d83ec4f53736400964',
    '0xb676d789bb8b66a08105c844a49c2bcffb400e5c1cfabd4bc30cca4bff3c9801'
)
ORDER BY block_number;


-- ============================================================
-- 3. Every event emitted by the alETH exploit, in order
--    The largest single loss of the campaign, about $22.6M
-- ============================================================

SELECT
    log_index,
    address,
    topic0,
    substr(data, 1, 66) AS data_head
FROM logs
WHERE tx_hash = '0xb676d789bb8b66a08105c844a49c2bcffb400e5c1cfabd4bc30cca4bff3c9801'
ORDER BY log_index;


-- ============================================================
-- 4. Event signature breakdown per exploit
--    Same bug, so the three traces should rhyme
-- ============================================================

SELECT
    tx_hash,
    topic0,
    COUNT(*) AS occurrences
FROM logs
WHERE tx_hash IN (
    '0xa84aa065ce61dbb1eb50ab6ae67fc31a9da50dd2c74eefd561661bfce2f1620c',
    '0xc93eb238ff42632525e990119d3edc7775299a70b56e54d83ec4f53736400964',
    '0xb676d789bb8b66a08105c844a49c2bcffb400e5c1cfabd4bc30cca4bff3c9801'
)
GROUP BY tx_hash, topic0
ORDER BY tx_hash, occurrences DESC;


-- ============================================================
-- 5. Everything that touched the three vulnerable pools
--    across the whole 867-block window
-- ============================================================

SELECT
    address AS pool,
    COUNT(*)                AS log_count,
    COUNT(DISTINCT tx_hash) AS tx_count,
    MIN(block_number)       AS first_block,
    MAX(block_number)       AS last_block
FROM logs
WHERE address IN (
    '0x9848482da3ee3076165ce6497eda906e66bb85c5',  -- pETH/ETH
    '0xc897b98272aa23714464ea2a0bd5180f1b8c0025',  -- msETH/ETH
    '0xc4c319e2d4d66cca4464c0c2b32c9bd23ebe784e'   -- alETH/ETH
)
GROUP BY address;


-- ============================================================
-- 6. Balancer vault activity in the exploit blocks
--    All three attacks flash loaned WETH from here
-- ============================================================

SELECT
    block_number,
    tx_hash,
    log_index,
    topic0
FROM logs
WHERE address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
  AND block_number IN (17806056, 17806550, 17806772)
ORDER BY block_number, log_index;


-- ============================================================
-- 7. WETH transfers inside the exploit transactions
--    Follow the money in and back out
-- ============================================================

SELECT
    l.block_number,
    l.log_index,
    '0x' || substr(l.topic1, 27) AS from_addr,
    '0x' || substr(l.topic2, 27) AS to_addr,
    CAST(l.data AS TEXT)         AS raw_amount
FROM logs l
WHERE l.address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
  AND l.topic0  = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND l.tx_hash IN (
    '0xa84aa065ce61dbb1eb50ab6ae67fc31a9da50dd2c74eefd561661bfce2f1620c',
    '0xc93eb238ff42632525e990119d3edc7775299a70b56e54d83ec4f53736400964',
    '0xb676d789bb8b66a08105c844a49c2bcffb400e5c1cfabd4bc30cca4bff3c9801'
  )
ORDER BY l.block_number, l.log_index;


-- ============================================================
-- 8. Every transaction sent by the three senders in this window
-- ============================================================

SELECT
    block_number,
    tx_index,
    hash,
    to_addr,
    gas_used,
    status
FROM transactions
WHERE from_addr IN (
    '0x6ec21d1868743a44318c3c259a6d4953f9978538',  -- pETH attacker
    '0xc0ffeebabe5d496b2dde509f9fa189c25cf29671',  -- msETH sender, reported whitehat
    '0xdce5d6b41c32f578f875efffc0d422c57a75d7d8'   -- alETH attacker
)
ORDER BY block_number, tx_index;


-- ============================================================
-- 9. Busiest contracts in the window by log volume
-- ============================================================

SELECT
    address,
    COUNT(*) AS log_count
FROM logs
GROUP BY address
ORDER BY log_count DESC
LIMIT 20;
