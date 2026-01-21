# ORBT Wallet Profiling System

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
