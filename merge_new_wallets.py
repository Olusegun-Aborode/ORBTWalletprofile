import pandas as pd
import os

# Files
new_file_path = '/Users/olusegunaborode/Downloads/cis-users-export.csv'
existing_file_path = 'defiscorewallets'  # The 55k list we just made

print(f"Reading new data from {new_file_path}...")
df_new = pd.read_csv(new_file_path)

# Extract and Clean Wallets
# Handle multiple wallets separated by semicolon
wallets_series = df_new['Wallet Addresses'].astype(str).str.split(';')
# Explode the lists into separate rows
wallets_exploded = wallets_series.explode()
# Clean: strip whitespace and lowercase
wallets_clean = wallets_exploded.str.strip().str.lower()

# Filter valid addresses (simple length check for 0x... 42 chars)
wallets_clean = wallets_clean[wallets_clean.str.match(r'^0x[a-f0-9]{40}$')]

print(f"Found {len(wallets_clean)} valid wallet addresses in new file.")

# Read Existing File
print(f"Reading existing data from {existing_file_path}...")
try:
    with open(existing_file_path, 'r') as f:
        existing_wallets = [line.strip().lower() for line in f if line.strip()]
    print(f"Existing count: {len(existing_wallets)}")
except FileNotFoundError:
    print("Existing file not found, starting fresh.")
    existing_wallets = []

# Merge
combined_set = set(existing_wallets).union(set(wallets_clean))
print(f"Combined unique count: {len(combined_set)}")

# Write back to file
with open(existing_file_path, 'w') as f:
    for wallet in sorted(list(combined_set)):
        f.write(wallet + '\n')

print(f"Successfully updated {existing_file_path}")
