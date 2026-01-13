
import pandas as pd
import requests
import os
import time

# Try to load env vars
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# UPDATE THIS TO MATCH YOUR UPLOADED TABLE NAME
DUNE_NAMESPACE = "dune.orbt_official"
DUNE_TABLE_NAME = "dataset_web3_wallets_55k" 

query_sql = f"""
/* 
   CIS Volume Metric - 55k Wallets
   - Source: {DUNE_NAMESPACE}.{DUNE_TABLE_NAME} (User uploaded CSV)
   - Scope: ONLY calculates volume for the 55k wallets in the list.
   - Method: Sums incoming and outgoing transfers (Native ETH + ERC20)
   - Deduplication: Uses unique events to avoid double-counting.
*/

WITH target_wallets AS (
    SELECT 
        -- Convert hex string to varbinary
        FROM_HEX(SUBSTR(wallet_address, 3)) as wallet 
    FROM {DUNE_NAMESPACE}.{DUNE_TABLE_NAME}
    WHERE wallet_address LIKE '0x%' 
      AND LENGTH(wallet_address) = 42
),

unique_transfers AS (
    -- 1. ERC20 Transfers
    SELECT 
        t.evt_tx_hash,
        t.evt_index,
        (t.value / POWER(10, COALESCE(tok.decimals, 18)) * p.price) as amount_usd
    FROM erc20_ethereum.evt_Transfer t
    LEFT JOIN prices.usd_latest p ON p.contract_address = t.contract_address AND p.blockchain = 'ethereum'
    LEFT JOIN tokens.erc20 tok ON tok.contract_address = t.contract_address AND tok.blockchain = 'ethereum'
    WHERE 
        (t."from" IN (SELECT wallet FROM target_wallets) OR t."to" IN (SELECT wallet FROM target_wallets))
        AND t.evt_block_time > NOW() - INTERVAL '3' YEAR
        AND p.price IS NOT NULL
        AND (t.value / POWER(10, COALESCE(tok.decimals, 18)) * p.price) BETWEEN 10 AND 1000000000
        -- Blacklist
        AND t.contract_address NOT IN (
              0x087b81c5312bcb45179a05aff5aec5cdddc789b6, -- GREENETH
              0x40fd72257597aa14c7231a7b1aaa29fce868f677, -- Sora
              0x456d8f0d25a4e787ee60c401f8b963a465148f70, -- Cavapoo
              0xb4357054c3da8d46ed642383f03139ac7f090343, -- Port3
              0x0f7dc5d02cc1e1f5ee47854d534d332a1081ccc8, -- Pepes Dog
              0x335f4e66b9b61cee5ceade4e727fcec20156b2f0   -- Elmo
        )

    UNION
    
    -- 2. Native ETH Transfers
    SELECT 
        t.hash as evt_tx_hash,
        0 as evt_index,
        (t.value / 1e18 * p.price) as amount_usd
    FROM ethereum.transactions t
    LEFT JOIN prices.usd_latest p ON p.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AND p.blockchain = 'ethereum'
    WHERE 
        (t."from" IN (SELECT wallet FROM target_wallets) OR t."to" IN (SELECT wallet FROM target_wallets))
        AND t.block_time > NOW() - INTERVAL '3' YEAR
        AND (t.value / 1e18 * p.price) BETWEEN 10 AND 1000000000
)

SELECT 
    SUM(amount_usd) as total_volume_usd
FROM unique_transfers;
"""

print("Generating SQL Query for Dune...")
print("-" * 50)
print(query_sql)
print("-" * 50)
