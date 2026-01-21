import pandas as pd
import os

# Files
master_file = 'data/input/final_active_wallets.csv'
new_source_file = 'web3_wallets_updated_80k.csv' # The file we just generated with 80k wallets

print(f"Reading Master File: {master_file}...")
try:
    df_master = pd.read_csv(master_file)
    print(f"Current Master Count: {len(df_master)}")
    # Ensure wallet column is string and clean
    df_master['wallet'] = df_master['wallet'].astype(str).str.lower().str.strip()
except FileNotFoundError:
    print("Master file not found! Creating new one.")
    df_master = pd.DataFrame(columns=['wallet'])

print(f"Reading New Source: {new_source_file}...")
df_new = pd.read_csv(new_source_file)
# Rename column to match master
df_new = df_new.rename(columns={'wallet_address': 'wallet'})
df_new['wallet'] = df_new['wallet'].astype(str).str.lower().str.strip()
print(f"New Source Count: {len(df_new)}")

# Find strictly NEW wallets
existing_wallets = set(df_master['wallet'])
new_wallets_list = []

for wallet in df_new['wallet']:
    if wallet not in existing_wallets:
        new_wallets_list.append(wallet)
        existing_wallets.add(wallet) # Add to set to prevent internal dupes in new list

print(f"Found {len(new_wallets_list)} TRULY NEW wallets to add.")

if len(new_wallets_list) > 0:
    # Create DataFrame for new wallets
    df_to_add = pd.DataFrame({'wallet': new_wallets_list})
    
    # Append to master
    df_updated = pd.concat([df_master, df_to_add], ignore_index=True)
    
    # Save updated master
    df_updated.to_csv(master_file, index=False)
    print(f"Updated {master_file}. New Total: {len(df_updated)}")
    
    # Save ONLY the new ones to a temporary file for API upload
    # This is efficient: we only upload the delta
    df_to_add.to_csv('delta_upload.csv', index=False)
    print("Saved 'delta_upload.csv' for API upload.")
else:
    print("No new wallets to add. Master file is up to date.")
