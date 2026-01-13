
# This script generates the EXACT Dune SQL query requested by the Web3 team.
# It implements:
# 1. CIS Volume Definition (In + Out Transfers)
# 2. Explicit Token Address Blacklist
# 3. Symbol/Name Pattern Filters
# 4. Value Sanity Checks ($10 - $1B)

DUNE_TABLE = "orbt_official.orbt_wallet_final_v2"

BLACKLIST_ADDRESSES = [
    '0x087b81c5312bcb45179a05aff5aec5cdddc789b6', # GREENETH
    '0x40fd72257597aa14c7231a7b1aaa29fce868f677', # Sora Token
    '0x456d8f0d25a4e787ee60c401f8b963a465148f70', # Cavapoo
    '0xb4357054c3da8d46ed642383f03139ac7f090343', # Port3 Network
    '0x0f7dc5d02cc1e1f5ee47854d534d332a1081ccc8', # Pepes Dog
    '0x335f4e66b9b61cee5ceade4e727fcec20156b2f0'  # Elmo
]

BLACKLIST_SYMBOLS = ['gre', 'xor', 'cava', 'ethm', 'zeus', 'elmo', 'gpu']

BLACKLIST_PATTERNS = [
    'pepe', 'doge', 'shib', 'inu', 'elon', 'moon', 'safe', 'baby', 'floki'
]

def generate_query():
    blacklist_addr_str = ", ".join([f"'{a}'" for a in BLACKLIST_ADDRESSES])
    blacklist_sym_str = ", ".join([f"'{s}'" for s in BLACKLIST_SYMBOLS])
    
    # Generate LIKE clauses
    like_clauses = "\n      AND ".join([f"LOWER(COALESCE(name, '')) NOT LIKE '%{p}%'" for p in BLACKLIST_PATTERNS])

    sql = f"""
/* 
   CIS Volume Metric - Web3 Team Definition
   - Includes ALL Transfers (In + Out)
   - Filters out Blacklisted Tokens (Scam/Inflationary)
   - Filters out Memecoin Name Patterns
   - Filters out dust (<$10) and errors (>$1B)
*/

WITH target_wallets AS (
    SELECT wallet FROM {DUNE_TABLE}
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
      contract_address NOT IN ({blacklist_addr_str})

      -- Symbol Blacklist
      AND LOWER(COALESCE(symbol, '')) NOT IN ({blacklist_sym_str})

      -- Memecoin Name Patterns
      AND {like_clauses}
)

SELECT 
    wallet,
    SUM(amount_usd) as total_cis_volume_usd
FROM filtered_transfers
GROUP BY 1
ORDER BY 2 DESC;
"""
    return sql

if __name__ == "__main__":
    query = generate_query()
    print(query)
    
    # Save to file for user
    with open("dune_cis_query.sql", "w") as f:
        f.write(query)
    print("\n✅ Query saved to 'dune_cis_query.sql'")
