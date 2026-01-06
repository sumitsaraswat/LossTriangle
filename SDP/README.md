# Spark Declarative Pipeline (SDP) - Delta Live Tables

## Overview

This folder contains a **SQL-first** Delta Live Tables (DLT) pipeline for the Loss Triangle reserve estimation project. Python is used only where SQL cannot achieve the required functionality (NLP, matrix operations).

## Quick Start

### Option 1: Demo with Embedded Data (Recommended)

No external data files needed! The batch bronze file has sample data embedded.

1. **Create DLT Pipeline in Databricks UI:**
   - Go to **Workflows** → **Delta Live Tables** → **Create Pipeline**
   - Name: `LossTriangle_SDP_Pipeline`
   - Target: `unified_reserves`

2. **Add these notebooks (in order):**
   ```
   SDP/bronze/01_bronze_batch_embedded.sql    ← Has embedded sample data
   SDP/silver/01_silver_enriched_claims.sql
   SDP/silver/02_silver_loss_triangles.sql
   SDP/silver/03_silver_nlp_risk.py
   SDP/gold/01_gold_development_factors.sql
   SDP/gold/02_gold_chain_ladder.py
   SDP/gold/03_gold_views.sql
   ```

3. **Start the pipeline** - it will run with the embedded sample data.

### Option 2: Production with External Data

For production with real data from cloud files:

1. First run `SDP/data_prep/00_setup_volumes_and_data.py` to create volumes
2. Upload your CSV files to `/Volumes/unified_reserves/bronze/raw/claims/` and `/payments/`
3. Use `01_bronze_streaming_external.sql` instead of batch file

---

## Folder Structure

```
SDP/
├── data_prep/
│   └── 00_setup_volumes_and_data.py   # Volume setup (run first for production)
│
├── bronze/                             # Raw data ingestion
│   ├── 01_bronze_batch_embedded.sql   # ✅ USE THIS (has sample data)
│   └── 01_bronze_streaming_external.sql  # For production (needs Volumes)
│
├── silver/                             # Data enrichment & triangles
│   ├── 01_silver_enriched_claims.sql  # Claims with payments
│   ├── 02_silver_loss_triangles.sql   # Triangle construction
│   └── 03_silver_nlp_risk.py          # NLP risk detection (Python)
│
├── gold/                               # Analytics & reserves
│   ├── 01_gold_development_factors.sql # ATA/CDF factors
│   ├── 02_gold_chain_ladder.py        # Reserve calculation (Python)
│   └── 03_gold_views.sql              # Business views
│
├── pipeline_config.yml                 # Pipeline configuration
└── README.md                          # This file
```

---

## Pipeline DAG

```
                    ┌─────────────────────────────┐
                    │   BRONZE LAYER (SQL)        │
                    │   bronze_raw_claims_batch   │
                    │   bronze_raw_payments_batch │
                    └──────────────┬──────────────┘
                                   │
            ┌──────────────────────┼──────────────────────┐
            │                      │                      │
            ▼                      ▼                      ▼
┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐
│ silver_claims_    │  │ silver_triangle_  │  │ SILVER (SQL)      │
│ with_payments     │  │ base              │  │                   │
└────────┬──────────┘  └────────┬──────────┘  └───────────────────┘
         │                      │
         ▼                      ▼
┌───────────────────┐  ┌───────────────────┐
│ silver_claims_    │  │ silver_triangle_  │
│ basic_risk (SQL)  │  │ cells (SQL)       │
└────────┬──────────┘  └────────┬──────────┘
         │                      │
         ▼                      ▼
┌───────────────────┐  ┌───────────────────┐
│ silver_enriched_  │  │ silver_triangle_  │
│ claims_nlp (Py)   │  │ pivot (SQL)       │
└────────┬──────────┘  └────────┬──────────┘
         │                      │
         └──────────┬───────────┘
                    │
                    ▼
         ┌───────────────────┐
         │   GOLD LAYER      │
         │   ATA Factors     │ (SQL)
         │   CDF Factors     │ (SQL)
         │   Reserves        │ (Python)
         │   Views           │ (SQL)
         └───────────────────┘
```

---

## Tables Created

### Bronze Layer
| Table | Type | Description |
|-------|------|-------------|
| `bronze_raw_claims_batch` | SQL | Raw claims with embedded sample data |
| `bronze_raw_payments_batch` | SQL | Raw payments with embedded sample data |

### Silver Layer
| Table | Type | Description |
|-------|------|-------------|
| `silver_claims_with_payments` | SQL | Claims joined with payment aggregations |
| `silver_claims_basic_risk` | SQL | SQL-based risk flags |
| `silver_enriched_claims_nlp` | Python | NLP-based risk analysis |
| `silver_triangle_base` | SQL | Base data with development periods |
| `silver_triangle_cells` | SQL | Aggregated triangle cells |
| `silver_triangle_pivot` | SQL | Triangle in wide/pivot format |
| `silver_risk_summary` | SQL | Risk metrics by origin year |

### Gold Layer
| Table | Type | Description |
|-------|------|-------------|
| `gold_ata_factor_base` | SQL | Individual link ratios |
| `gold_ata_factors` | SQL | Volume-weighted ATA factors |
| `gold_cdf_factors` | SQL | Cumulative development factors |
| `gold_reserve_estimates` | Python | IBNR reserves by origin year |
| `gold_reserve_summary` | Python | Executive reserve summary |
| `gold_development_factors_python` | Python | Factors from Chain Ladder model |
| `gold_v_*` | SQL | Business-ready views |

---

## SQL vs Python Usage

| Component | Language | Reason |
|-----------|----------|--------|
| Data ingestion | SQL | Simple SELECT/FROM |
| Joins & aggregations | SQL | Standard SQL operations |
| Risk flags (pattern matching) | SQL | CASE/WHEN with LIKE |
| Triangle construction | SQL | GROUP BY, window functions |
| Development factors | SQL | Arithmetic operations |
| **NLP keyword extraction** | **Python** | Regex, UDFs, complex string ops |
| **Chain Ladder math** | **Python** | NumPy matrix operations |
| **Cumulative products** | **Python** | Array indexing, loops |

---

## Troubleshooting

### Error: "Failed to resolve flow"
- Make sure you're using `01_bronze_batch_embedded.sql` (NOT streaming)
- The silver/gold files expect `bronze_raw_claims_batch` and `bronze_raw_payments_batch` tables

### Error: Schema not found
- Run the setup notebook first: `SDP/data_prep/00_setup_volumes_and_data.py`
- Or create schemas manually:
  ```sql
  CREATE SCHEMA IF NOT EXISTS unified_reserves.bronze;
  CREATE SCHEMA IF NOT EXISTS unified_reserves.silver;
  CREATE SCHEMA IF NOT EXISTS unified_reserves.gold;
  ```

### Error: Volume not found (streaming mode only)
- Only applies if using `01_bronze_streaming_external.sql`
- Create volumes:
  ```sql
  CREATE VOLUME IF NOT EXISTS unified_reserves.bronze.raw;
  ```
- Upload data files to the volume
