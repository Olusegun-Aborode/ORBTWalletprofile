
/* 
   CIS Volume Metric - Web3 Team Definition
   - Includes ALL Transfers (In + Out)
   - Filters out Blacklisted Tokens (Scam/Inflationary)
   - Filters out Memecoin Name Patterns
   - Filters out dust (<$10) and errors (>$1B)
*/

WITH target_wallets AS (
    SELECT wallet FROM orbt_official.orbt_wallet_final_v2
),

all_transfers AS (
    -- 1. SENDS (Wallet is Sender)
    SELECT 
        t."from" as wallet,
        t.amount_usd,
        t.contract_address,
        tk.symbol,
        tk.name
    FROM erc20_ethereum.evt_Transfer t
    INNER JOIN target_wallets w ON t."from" = w.wallet
    LEFT JOIN tokens.erc20 tk ON t.contract_address = tk.contract_address AND tk.blockchain = 'ethereum'
    WHERE t.amount_usd > 10 
    AND t.amount_usd < 1000000000
    AND t.evt_block_time > NOW() - INTERVAL '3 years' -- Optimization: Focus on recent history? Or remove for full history. Let's keep full.

    UNION ALL

    -- 2. RECEIVES (Wallet is Receiver)
    SELECT 
        t."to" as wallet,
        t.amount_usd,
        t.contract_address,
        tk.symbol,
        tk.name
    FROM erc20_ethereum.evt_Transfer t
    INNER JOIN target_wallets w ON t."to" = w.wallet
    LEFT JOIN tokens.erc20 tk ON t.contract_address = tk.contract_address AND tk.blockchain = 'ethereum'
    WHERE t.amount_usd > 10 
    AND t.amount_usd < 1000000000
    AND t.evt_block_time > NOW() - INTERVAL '3 years'
),

filtered_transfers AS (
    SELECT * FROM all_transfers
    WHERE 
      -- Explicit Address Blacklist
      contract_address NOT IN ('0x087b81c5312bcb45179a05aff5aec5cdddc789b6', '0x40fd72257597aa14c7231a7b1aaa29fce868f677', '0x456d8f0d25a4e787ee60c401f8b963a465148f70', '0xb4357054c3da8d46ed642383f03139ac7f090343', '0x0f7dc5d02cc1e1f5ee47854d534d332a1081ccc8', '0x335f4e66b9b61cee5ceade4e727fcec20156b2f0')

      -- Symbol Blacklist
      AND LOWER(COALESCE(symbol, '')) NOT IN ('gre', 'xor', 'cava', 'ethm', 'zeus', 'elmo', 'gpu')

      -- Memecoin Name Patterns
      AND LOWER(COALESCE(name, '')) NOT LIKE '%pepe%'
      AND LOWER(COALESCE(name, '')) NOT LIKE '%doge%'
      AND LOWER(COALESCE(name, '')) NOT LIKE '%shib%'
      AND LOWER(COALESCE(name, '')) NOT LIKE '%inu%'
      AND LOWER(COALESCE(name, '')) NOT LIKE '%elon%'
      AND LOWER(COALESCE(name, '')) NOT LIKE '%moon%'
      AND LOWER(COALESCE(name, '')) NOT LIKE '%safe%'
      AND LOWER(COALESCE(name, '')) NOT LIKE '%baby%'
      AND LOWER(COALESCE(name, '')) NOT LIKE '%floki%'
)

SELECT 
    wallet,
    SUM(amount_usd) as total_cis_volume_usd
FROM filtered_transfers
GROUP BY 1
ORDER BY 2 DESC;
