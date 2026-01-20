import pandas as pd
import requests
import time
import os

# Try to load env vars
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

# CONFIG
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
if not DUNE_API_KEY:
    raise ValueError("Please set DUNE_API_KEY in .env file")

DUNE_NAMESPACE = "orbt_official"
DUNE_TABLE_NAME = "orbt_wallet_final_v2" # Shortened name for better visibility
UPLOAD_BATCH_SIZE = 5000

# 1. LOAD DATA
print("📖 Loading datasets...")

# Base List (Active Wallets)
df_base = pd.read_csv("delta_wallets.csv")
df_base['wallet'] = df_base['wallet'].astype(str).str.lower().str.strip()
print(f"   - Base Wallets: {len(df_base)}")

# Wallet Age
df_age = pd.read_csv("wallet_ages_delta.csv")
df_age['wallet'] = df_age['wallet'].astype(str).str.lower().str.strip()
print(f"   - Age Data: {len(df_age)}")

# Wallet Volumes
df_vols = pd.read_csv("wallet_volumes_delta.csv")
df_vols['wallet'] = df_vols['wallet'].astype(str).str.lower().str.strip()
print(f"   - Volume Data: {len(df_vols)}")

# Gas Fees
df_gas = pd.read_csv("wallet_gas_fees_delta.csv")
df_gas['wallet'] = df_gas['wallet'].astype(str).str.lower().str.strip()
print(f"   - Gas Data: {len(df_gas)}")

# Portfolio (SIM & Dune)
df_sim = pd.read_csv("wallet_portfolio_ath_delta.csv")
df_sim['wallet'] = df_sim['wallet'].astype(str).str.lower().str.strip()
# Deduplicate SIM data (keep last updated or just first)
df_sim = df_sim.drop_duplicates(subset=['wallet'])
print(f"   - Portfolio Data: {len(df_sim)}")

# Alchemy Balances
df_alc = pd.read_csv("alchemy_eth_balances_delta.csv")
df_alc['wallet'] = df_alc['wallet'].astype(str).str.lower().str.strip()
df_alc = df_alc.drop_duplicates(subset=['wallet'])
print(f"   - Alchemy Data: {len(df_alc)}")


# 2. MERGE DATA
print("🔄 Merging all datasets...")

# Start with Base
merged = df_base.copy()

# Merge Age
merged = pd.merge(merged, df_age[['wallet', 'wallet_age_days', 'first_tx_timestamp']], on='wallet', how='left')
merged.rename(columns={'first_tx_timestamp': 'first_seen_date'}, inplace=True)

# Merge Volumes
vol_cols = ['wallet', 'total_dex_volume_usd', 'total_cex_volume_usd', 'total_lending_volume_usd', 'total_volume_usd_cis']
merged = pd.merge(merged, df_vols[vol_cols], on='wallet', how='left')
# Rename for final upload to match user request "Total Volume"
merged.rename(columns={'total_volume_usd_cis': 'total_volume_usd'}, inplace=True)

# Merge Gas
merged = pd.merge(merged, df_gas[['wallet', 'gas_fees_usd']], on='wallet', how='left')

# Merge Portfolio
sim_cols = ['wallet', 'present_value_usd', 'ath_value_usd', 'top_tokens']
merged = pd.merge(merged, df_sim[sim_cols], on='wallet', how='left')

# Merge Alchemy
merged = pd.merge(merged, df_alc[['wallet', 'alchemy_eth_balance']], on='wallet', how='left')


# 3. CLEAN & CALCULATE
print("🧹 Cleaning data...")

# Fill NaNs for numeric columns with 0
numeric_cols = [
    'wallet_age_days', 
    'total_dex_volume_usd', 'total_cex_volume_usd', 'total_lending_volume_usd',
    'gas_fees_usd', 'present_value_usd', 'ath_value_usd', 'alchemy_eth_balance',
    'total_volume_usd'
]
merged[numeric_cols] = merged[numeric_cols].fillna(0)

# Cast to int
merged['wallet_age_days'] = merged['wallet_age_days'].astype(int)
merged['tx_count'] = merged['tx_count'].fillna(0).astype(int)

# Calculate Alchemy USD Value
ETH_PRICE = 3300 # Approx
merged['alchemy_current_wallet_value'] = merged['alchemy_eth_balance'] * ETH_PRICE

# Rename columns for final output
final_df = merged[[
    'wallet',
    'tx_count',
    'wallet_age_days',
    'first_seen_date',
    'gas_fees_usd',
    'total_dex_volume_usd',
    'total_cex_volume_usd',
    'total_lending_volume_usd',
    'alchemy_current_wallet_value',
    'present_value_usd',
    'ath_value_usd',
    'top_tokens'
]]

final_df.columns = [
    'wallet_address',
    'tx_count',
    'wallet_age_days',
    'first_seen_date',
    'gas_fees_usd',
    'total_dex_volume_usd',
    'total_cex_volume_usd',
    'total_lending_volume_usd',
    'alchemy_current_wallet_value',
    'sim_current_wallet_value',
    'sim_ath_wallet_value',
    'top_tokens_held'
]

# Round decimals
round_cols = [
    'gas_fees_usd', 'total_dex_volume_usd', 'total_cex_volume_usd', 
    'total_lending_volume_usd', 'alchemy_current_wallet_value',
    'sim_current_wallet_value', 'sim_ath_wallet_value'
]
final_df[round_cols] = final_df[round_cols].round(2)

# Save local CSV
final_df.to_csv("final_wallet_data_delta.csv", index=False)
print(f"✅ Saved final dataset: {len(final_df)} rows to final_wallet_data_delta.csv")



print('✅ Delta consolidation complete. Ready for upload.')
