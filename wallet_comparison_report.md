# Wallet Dataset Comparison Report

**Date:** 2026-01-21  
**Comparison:** `final_active_wallets.csv` vs `wallets_extracted.txt`

---

## Summary Statistics

| Metric | Count | Percentage |
|--------|-------|------------|
| **Wallets in final_active_wallets.csv** | 289,059 | 100% |
| **Wallets in wallets_extracted.txt** | 296,778 | 100% |
| **Common wallets (in both)** | 277,199 | 95.9% / 93.4% |
| **New wallets (only in extracted)** | 19,579 | 6.6% of extracted |
| **Missing wallets (only in final_active)** | 11,860 | 4.1% of final_active |

---

## Key Findings

### 1. **Overlap Analysis**
- **93.4%** of the newly extracted wallets already exist in `final_active_wallets.csv`
- **95.9%** of the wallets in `final_active_wallets.csv` are present in the new extract
- Strong overlap indicates consistent data sources

### 2. **New Wallets**
- **19,579 new wallets** found in `wallets_extracted.txt` that don't exist in `final_active_wallets.csv`
- These represent **6.6%** of the extracted dataset
- Saved to: `new_wallets_only.txt`

### 3. **Missing Wallets**
- **11,860 wallets** exist in `final_active_wallets.csv` but are NOT in `wallets_extracted.txt`
- These represent **4.1%** of the final_active dataset
- Saved to: `missing_wallets.txt`
- **Possible reasons:**
  - Users who didn't complete registration in the new system
  - Wallets removed/inactive
  - Data collection timing differences

---

## Output Files Created

1. **`new_wallets_only.txt`** - 19,579 wallets (new additions)
2. **`missing_wallets.txt`** - 11,860 wallets (not in new extract)

---

## Recommendations

1. **Merge Strategy:** Consider combining both datasets to create a comprehensive master list of 308,638 unique wallets
2. **Investigate Missing Wallets:** Review why 11,860 wallets from the active list are missing
3. **Validate New Wallets:** Verify the 19,579 new wallets are legitimate registrations
4. **Update Master List:** Use the combined dataset as the new baseline
