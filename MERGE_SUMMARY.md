# Wallet Merge & Analysis Summary

**Date:** 2026-01-22  
**Action:** Merged wallet datasets and prepared new wallets for Dune analysis

---

## Files Created

### 1. **master_wallets_all.csv** (308,639 lines including header)
- **Total unique wallets:** 308,638
- **Source:** Merged from `final_active_wallets.csv` + `wallets_extracted.txt`
- **Format:** CSV with single column `wallet`
- **Purpose:** Complete master list of all known wallets

### 2. **new_wallets_for_dune.csv** (19,580 lines including header)
- **Total new wallets:** 19,579
- **Source:** Wallets in `wallets_extracted.txt` but NOT in `final_active_wallets.csv`
- **Format:** CSV with single column `wallet`
- **Purpose:** Ready for Dune upload and analysis
- **Status:** ⚠️ **NEEDS ANALYSIS** - These wallets need to be processed

### 3. **missing_wallets.txt** (11,860 wallets)
- Wallets from `final_active_wallets.csv` not in new extract
- Likely manually added wallets from other sources

---

## Next Steps for the 19,579 New Wallets

Based on your existing workflow, you need to:

1. **Upload to Dune** - Push `new_wallets_for_dune.csv` to Dune Analytics
2. **Run Analysis** - Execute wallet profiling scripts:
   - Transaction counts
   - Wallet volumes
   - Gas fees
   - Portfolio ATH
   - Wallet ages
3. **Merge Results** - Combine with existing `final_wallet_data.csv`
4. **Update Dashboard** - Refresh Dune dashboards with new data

---

## Data Breakdown

| Dataset | Count | Description |
|---------|-------|-------------|
| Original CSV records | 371,186 | Total user registrations |
| Blank wallet addresses | 74,408 | Users without wallet (20%) |
| Extracted wallets | 296,778 | Users with wallet addresses |
| Previous active wallets | 289,059 | From final_active_wallets.csv |
| **Merged master list** | **308,638** | **All unique wallets** |
| **New wallets to analyze** | **19,579** | **Need Dune processing** |
| Manually added wallets | 11,860 | In final_active but not in extract |

---

## File Locations

```
/Users/olusegunaborode/Documents/trae_projects/ORBTWalletprofile/
├── master_wallets_all.csv          # Master list (308,638 wallets)
├── new_wallets_for_dune.csv        # New wallets for analysis (19,579)
├── new_wallets_only.txt            # Same as above, no header
├── missing_wallets.txt             # Manually added wallets (11,860)
├── wallets_extracted.txt           # Original extract (296,778)
└── data/input/final_active_wallets.csv  # Previous dataset (289,059)
```
