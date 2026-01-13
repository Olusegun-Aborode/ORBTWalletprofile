
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

DUNE_API_KEY = os.getenv("DUNE_API_KEY")
if not DUNE_API_KEY:
    raise ValueError("Please set DUNE_API_KEY in .env file")

# Use our new table name
DUNE_NAMESPACE = "orbt_official"
DUNE_TABLE_NAME = "orbt_wallet_final_v2"

# SQL to calculate Volume via Dune's Engine (Matching Web3 Team's Definition)
# Summing all outgoing ERC20/Native transfers
query_sql = f"""
/* 
   CIS Volume Metric - Web3 Team Definition (Revised)
   - Includes ALL Transfers (In + Out) for both Native ETH and ERC20
   - Filters out Blacklisted Tokens (Specific Addresses & Symbols)
   - DOES NOT filter out Memecoins (Pepe, Doge, etc. are INCLUDED)
   - Filters out dust (<$10) and errors (>$1B)
*/

WITH target_wallets AS (
    SELECT wallet FROM {DUNE_NAMESPACE}.{DUNE_TABLE_NAME}
),

all_transfers AS (
    -- 1. SENDS (Wallet is Sender) - ERC20
    SELECT 
        t."from" as wallet,
        t.amount_usd,
        t.contract_address,
        tk.symbol
    FROM erc20_ethereum.evt_Transfer t
    INNER JOIN target_wallets w ON t."from" = w.wallet
    LEFT JOIN tokens.erc20 tk ON t.contract_address = tk.contract_address AND tk.blockchain = 'ethereum'
    WHERE t.amount_usd > 10 
    AND t.amount_usd < 1000000000
    AND t.evt_block_time > NOW() - INTERVAL '3 years'

    UNION ALL

    -- 2. RECEIVES (Wallet is Receiver) - ERC20
    SELECT 
        t."to" as wallet,
        t.amount_usd,
        t.contract_address,
        tk.symbol
    FROM erc20_ethereum.evt_Transfer t
    INNER JOIN target_wallets w ON t."to" = w.wallet
    LEFT JOIN tokens.erc20 tk ON t.contract_address = tk.contract_address AND tk.blockchain = 'ethereum'
    WHERE t.amount_usd > 10 
    AND t.amount_usd < 1000000000
    AND t.evt_block_time > NOW() - INTERVAL '3 years'

    UNION ALL

    -- 3. NATIVE ETH SENDS
    SELECT 
        "from" as wallet,
        (value / 1e18 * (SELECT price FROM prices.usd_latest WHERE symbol='WETH')) as amount_usd,
        '0x0000000000000000000000000000000000000000' as contract_address,
        'ETH' as symbol
    FROM ethereum.core.fact_transactions
    WHERE "from" IN (SELECT wallet FROM target_wallets)
    AND (value / 1e18 * (SELECT price FROM prices.usd_latest WHERE symbol='WETH')) > 10
    AND (value / 1e18 * (SELECT price FROM prices.usd_latest WHERE symbol='WETH')) < 1000000000
    AND block_time > NOW() - INTERVAL '3 years'

    UNION ALL

    -- 4. NATIVE ETH RECEIVES
    SELECT 
        "to" as wallet,
        (value / 1e18 * (SELECT price FROM prices.usd_latest WHERE symbol='WETH')) as amount_usd,
        '0x0000000000000000000000000000000000000000' as contract_address,
        'ETH' as symbol
    FROM ethereum.core.fact_transactions
    WHERE "to" IN (SELECT wallet FROM target_wallets)
    AND (value / 1e18 * (SELECT price FROM prices.usd_latest WHERE symbol='WETH')) > 10
    AND (value / 1e18 * (SELECT price FROM prices.usd_latest WHERE symbol='WETH')) < 1000000000
    AND block_time > NOW() - INTERVAL '3 years'
),

filtered_transfers AS (
    SELECT * FROM all_transfers
    WHERE 
      -- Explicit Address Blacklist (Spam/Inflationary Tokens)
      contract_address NOT IN (
          '0x087b81c5312bcb45179a05aff5aec5cdddc789b6', -- GREENETH
          '0x40fd72257597aa14c7231a7b1aaa29fce868f677', -- Sora
          '0x456d8f0d25a4e787ee60c401f8b963a465148f70', -- Cavapoo
          '0xb4357054c3da8d46ed642383f03139ac7f090343', -- Port3
          '0x0f7dc5d02cc1e1f5ee47854d534d332a1081ccc8', -- Pepes Dog
          '0x335f4e66b9b61cee5ceade4e727fcec20156b2f0'  -- Elmo
      )

      -- Symbol Blacklist (Backup for above)
      AND LOWER(COALESCE(symbol, '')) NOT IN ('gre', 'xor', 'cava', 'ethm', 'zeus', 'elmo', 'gpu')
      
      -- NOTE: Memecoin filters (Pepe, Doge, etc.) have been REMOVED as requested.
)

SELECT 
    wallet,
    SUM(amount_usd) as total_cis_volume_usd
FROM filtered_transfers
GROUP BY 1
ORDER BY 2 DESC;
"""

print("Generating SQL Query for Dune...")
print("-" * 50)
print(query_sql)
print("-" * 50)
print("\nNOTE: You must run this in Dune to get the 'Billions' volume.")
print("This query sums ALL native and ERC20 transfers (priced), matching the CIS definition.")
