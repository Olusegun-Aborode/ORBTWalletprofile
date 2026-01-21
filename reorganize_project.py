#!/usr/bin/env python3
"""
Project Reorganization Script
Reorganizes the ORBT Wallet Profile project into a clean directory structure
"""

import os
import shutil
from pathlib import Path

# Define new structure
NEW_STRUCTURE = {
    "scripts": {
        "fetchers": [
            "fetch_wallet_age.py",
            "fetch_wallet_age_delta.py",
            "fetch_volumes.py",
            "fetch_volumes_delta.py",
            "fetch_gas_fees.py",
            "fetch_gas_fees_delta.py",
            "fetch_alchemy_balances.py",
            "fetch_alchemy_balances_delta.py",
            "fetch_tx_counts.py",
            "fetch_tx_counts_delta.py",
            "wallet_portfolio_ath_fetcher.py",
            "wallet_portfolio_ath_fetcher_delta.py",
            "fetch_orbt_holders.py",
            "fetch_orbt_holders_rpc.py",
        ],
        "consolidation": [
            "create_consolidated_table.py",
            "create_consolidated_table_delta.py",
            "consolidate_wallets.py",
            "merge_new_wallets.py",
        ],
        "upload": [
            "upload_delta.py",
            "upload_enrichment_data.py",
            "upload_existing_progress.py",
            "upload_users_list.py",
            "upload_alchemy_table.py",
        ],
        "query_generation": [
            "generate_cis_query.py",
            "generate_dune_cis_query.py",
        ],
        "utilities": [
            "check_alchemy_coverage.py",
            "check_coverage.py",
            "check_duplicates.py",
            "analyze_filters.py",
            "get_token_addresses.py",
            "get_token_prices.py",
            "verify_data_subset.py",
        ],
        "pipeline": [
            "run_full_delta_pipeline.py",
            "prepare_delta_scripts.py",
            "process_new_wallets.py",
            "filter_and_upload.py",
        ]
    },
    "data": {
        "input": [
            "final_active_wallets.csv",
            "delta_wallets.csv",
            "all_wallets.csv",
            "orbt_base_minters.csv",
            "web3_wallets_55k.csv",
            "web3_wallets_updated_80k.csv",
            "defiscorewallets",
        ],
        "output": [
            "final_wallet_data.csv",
            "final_wallet_data_delta.csv",
        ],
        "intermediate": [
            "wallet_ages.csv",
            "wallet_ages_delta.csv",
            "wallet_volumes.csv",
            "wallet_volumes_delta.csv",
            "wallet_gas_fees.csv",
            "wallet_gas_fees_delta.csv",
            "alchemy_eth_balances.csv",
            "alchemy_eth_balances_delta.csv",
            "wallet_portfolio_ath_backup.csv",
            "wallet_portfolio_ath_delta.csv",
            "wallet_tx_counts.csv",
            "wallet_tx_counts_delta.csv",
        ],
        "backup": [
            "all_wallets.csv.bak",
        ],
        "verification": [
            "verification_results.csv",
        ]
    },
    "sql": [
        "dune_cis_query.sql",
        "dune_benchmark_query.sql",
    ],
    "docs": [
        "ORBT_Project_Technical_Report.md",
        "SIM_API_DOCS.md",
    ],
    "config": [
        ".env",
        ".gitignore",
        "requirements.txt",
    ]
}

def create_directory_structure(base_path):
    """Create the new directory structure"""
    print("📁 Creating directory structure...")
    
    for category, subcategories in NEW_STRUCTURE.items():
        if category in ["config"]:
            continue  # Config files stay in root
        
        category_path = base_path / category
        category_path.mkdir(exist_ok=True)
        print(f"  ✓ Created {category}/")
        
        if isinstance(subcategories, dict):
            for subcat in subcategories.keys():
                subcat_path = category_path / subcat
                subcat_path.mkdir(exist_ok=True)
                print(f"    ✓ Created {category}/{subcat}/")

def move_files(base_path, dry_run=True):
    """Move files to their new locations"""
    print(f"\n{'🔍 DRY RUN - ' if dry_run else '📦 '}Moving files...")
    
    moves = []
    
    for category, subcategories in NEW_STRUCTURE.items():
        if category == "config":
            # Config files stay in root
            continue
        
        if isinstance(subcategories, dict):
            for subcat, files in subcategories.items():
                for file in files:
                    src = base_path / file
                    dst = base_path / category / subcat / file
                    if src.exists():
                        moves.append((src, dst))
        else:
            # Direct list of files
            for file in subcategories:
                src = base_path / file
                dst = base_path / category / file
                if src.exists():
                    moves.append((src, dst))
    
    for src, dst in moves:
        if dry_run:
            print(f"  Would move: {src.name} → {dst.relative_to(base_path)}")
        else:
            shutil.move(str(src), str(dst))
            print(f"  ✓ Moved: {src.name} → {dst.relative_to(base_path)}")
    
    return len(moves)

def create_readme(base_path):
    """Create README for the new structure"""
    readme_content = """# ORBT Wallet Profiling System

## Project Structure

```
ORBTWalletprofile/
├── config/              # Configuration files
│   ├── .env            # API keys (not in git)
│   ├── .gitignore      # Git ignore rules
│   └── requirements.txt # Python dependencies
│
├── scripts/            # All Python scripts
│   ├── fetchers/       # Data fetching scripts (Alchemy, Sim API)
│   ├── consolidation/  # Data merging and consolidation
│   ├── upload/         # Dune upload scripts
│   ├── query_generation/ # SQL query generators
│   ├── utilities/      # Helper scripts (verification, checks)
│   └── pipeline/       # Pipeline orchestration scripts
│
├── data/               # All data files
│   ├── input/          # Source wallet lists
│   ├── output/         # Final consolidated datasets
│   ├── intermediate/   # Per-metric CSV files
│   ├── backup/         # Backup files
│   └── verification/   # Verification results
│
├── sql/                # SQL queries
│   ├── dune_cis_query.sql
│   └── dune_benchmark_query.sql
│
└── docs/               # Documentation
    ├── ORBT_Project_Technical_Report.md
    └── SIM_API_DOCS.md
```

## Quick Start

### Run Full Pipeline
```bash
cd scripts/pipeline
python3 run_full_delta_pipeline.py
```

### Run Individual Fetchers
```bash
cd scripts/fetchers
python3 fetch_wallet_age.py
```

### Verify Data Quality
```bash
cd scripts/utilities
python3 verify_data_subset.py
```

## Data Flow

1. **Input**: Wallet lists in `data/input/`
2. **Fetch**: Scripts in `scripts/fetchers/` pull data from APIs
3. **Intermediate**: Per-metric CSVs saved to `data/intermediate/`
4. **Consolidate**: Scripts in `scripts/consolidation/` merge all data
5. **Output**: Final dataset in `data/output/final_wallet_data.csv`
6. **Upload**: Scripts in `scripts/upload/` push to Dune

## Configuration

Copy `.env.example` to `.env` and add your API keys:
```bash
ALCHEMY_API_KEY=your_key_here
DUNE_API_KEY=your_key_here
SIM_API_KEY=your_key_here
```

## Documentation

See `docs/` folder for:
- Technical architecture report
- API documentation
- System analysis (in brain/ artifacts)
"""
    
    readme_path = base_path / "README.md"
    with open(readme_path, "w") as f:
        f.write(readme_content)
    
    print(f"\n✓ Created README.md")

def main():
    base_path = Path.cwd()
    
    print("="*60)
    print("ORBT Wallet Profile - Project Reorganization")
    print("="*60)
    print(f"\nBase directory: {base_path}\n")
    
    # Step 1: Dry run
    print("STEP 1: Dry Run (Preview)")
    print("-" * 60)
    create_directory_structure(base_path)
    num_files = move_files(base_path, dry_run=True)
    
    print(f"\n📊 Summary: {num_files} files will be moved")
    
    # Step 2: Confirm
    print("\n" + "="*60)
    response = input("Proceed with reorganization? (yes/no): ").strip().lower()
    
    if response != "yes":
        print("\n❌ Reorganization cancelled.")
        return
    
    # Step 3: Execute
    print("\nSTEP 2: Executing Reorganization")
    print("-" * 60)
    move_files(base_path, dry_run=False)
    create_readme(base_path)
    
    print("\n" + "="*60)
    print("✅ Reorganization Complete!")
    print("="*60)
    print("\nNext steps:")
    print("1. Review the new structure")
    print("2. Update any hardcoded paths in scripts if needed")
    print("3. Test running a script to ensure everything works")
    print("\n💡 Tip: Check README.md for the new structure overview")

if __name__ == "__main__":
    main()
