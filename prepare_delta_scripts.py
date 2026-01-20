import os

# Mappings: Original Script -> (Delta Script, Replacements)
scripts = {
    "fetch_wallet_age.py": ("fetch_wallet_age_delta.py", {
        "final_active_wallets.csv": "delta_wallets.csv",
        "wallet_ages.csv": "wallet_ages_delta.csv"
    }),
    "wallet_portfolio_ath_fetcher.py": ("wallet_portfolio_ath_fetcher_delta.py", {
        "final_active_wallets.csv": "delta_wallets.csv",
        "wallet_portfolio_ath_backup.csv": "wallet_portfolio_ath_delta.csv",
        "wallet_portfolio_backup.csv": "wallet_portfolio_backup_delta.csv",
        "upload_to_dune": "# upload_to_dune" # Disable intermediate uploads
    }),
    "fetch_volumes.py": ("fetch_volumes_delta.py", {
        "final_active_wallets.csv": "delta_wallets.csv",
        "wallet_volumes.csv": "wallet_volumes_delta.csv"
    }),
    "fetch_gas_fees.py": ("fetch_gas_fees_delta.py", {
        "final_active_wallets.csv": "delta_wallets.csv",
        "wallet_gas_fees.csv": "wallet_gas_fees_delta.csv"
    }),
    "fetch_alchemy_balances.py": ("fetch_alchemy_balances_delta.py", {
        "final_active_wallets.csv": "delta_wallets.csv",
        "alchemy_eth_balances.csv": "alchemy_eth_balances_delta.csv"
    }),
    "fetch_tx_counts.py": ("fetch_tx_counts_delta.py", {
        "wallet_portfolio_ath_backup.csv": "delta_wallets.csv",
        "wallet_tx_counts.csv": "wallet_tx_counts_delta.csv"
    }),
    "create_consolidated_table.py": ("create_consolidated_table_delta.py", {
        "final_active_wallets.csv": "delta_wallets.csv",
        "wallet_ages.csv": "wallet_ages_delta.csv",
        "wallet_volumes.csv": "wallet_volumes_delta.csv",
        "wallet_gas_fees.csv": "wallet_gas_fees_delta.csv",
        "wallet_portfolio_ath_backup.csv": "wallet_portfolio_ath_delta.csv",
        "alchemy_eth_balances.csv": "alchemy_eth_balances_delta.csv",
        "final_wallet_data.csv": "final_wallet_data_delta.csv"
    })
}

def create_delta_scripts():
    for original, (delta, replacements) in scripts.items():
        if not os.path.exists(original):
            print(f"Skipping {original} (not found)")
            continue
            
        with open(original, "r") as f:
            content = f.read()
            
        for old, new in replacements.items():
            content = content.replace(old, new)
            
        # Special handling for create_consolidated_table.py to stop before upload
        if original == "create_consolidated_table.py":
            # Find the upload section and cut it off
            if "# 4. UPLOAD TO DUNE" in content:
                content = content.split("# 4. UPLOAD TO DUNE")[0]
                content += "\nprint('✅ Delta consolidation complete. Ready for upload.')\n"
        
        with open(delta, "w") as f:
            f.write(content)
        print(f"Created {delta}")

if __name__ == "__main__":
    create_delta_scripts()
