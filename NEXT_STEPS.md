# Next Steps: Process 19,579 New Wallets

## ✅ Completed

1. **Merged Datasets**
   - Created `master_wallets_all.csv` with **308,638 unique wallets**
   - Created `new_wallets_for_dune.csv` with **19,579 new wallets**

## 🎯 To Process the New Wallets

You need to run your existing delta pipeline on the 19,579 new wallets:

### Step 1: Prepare Delta Input File

```bash
cd /Users/olusegunaborode/Documents/trae_projects/ORBTWalletprofile
cp new_wallets_for_dune.csv data/input/delta_wallets.csv
```

### Step 2: Run Full Delta Pipeline

```bash
cd scripts/pipeline
python3 run_full_delta_pipeline.py
```

This will automatically:
1. ✅ Fetch transaction counts (`fetch_tx_counts_delta.py`)
2. ✅ Fetch wallet ages (`fetch_wallet_age_delta.py`)
3. ✅ Fetch Alchemy balances (`fetch_alchemy_balances_delta.py`)
4. ✅ Fetch volumes (`fetch_volumes_delta.py`)
5. ✅ Fetch gas fees (`fetch_gas_fees_delta.py`)
6. ✅ Fetch portfolio ATH (`wallet_portfolio_ath_fetcher_delta.py`)
7. ✅ Consolidate data (`create_consolidated_table_delta.py`)
8. ✅ Upload to Dune (`upload_delta.py`)

### Step 3: Merge with Existing Data

After the pipeline completes, merge the delta results with your existing `final_wallet_data.csv`:

```bash
cd scripts/consolidation
python3 merge_new_wallets.py
```

## 📊 Expected Output

- **Intermediate files** in `data/intermediate/`:
  - `wallet_tx_counts_delta.csv`
  - `wallet_ages_delta.csv`
  - `wallet_volumes_delta.csv`
  - `wallet_gas_fees_delta.csv`
  - `wallet_portfolio_ath_delta.csv`

- **Final output** in `data/output/`:
  - `final_wallet_data_delta.csv` (19,579 analyzed wallets)
  - Updated `final_wallet_data.csv` (merged with existing)

## ⏱️ Estimated Time

- **Transaction counts:** ~10-15 minutes
- **Wallet ages:** ~10-15 minutes
- **Alchemy balances:** ~30-45 minutes
- **Volumes:** ~60-90 minutes
- **Gas fees:** ~30-45 minutes
- **Portfolio ATH:** ~30-45 minutes
- **Total:** ~3-4 hours for 19,579 wallets

## 🚀 Quick Start Command

```bash
cd /Users/olusegunaborode/Documents/trae_projects/ORBTWalletprofile
cp new_wallets_for_dune.csv data/input/delta_wallets.csv
cd scripts/pipeline
python3 run_full_delta_pipeline.py
```
