# Architecture Documentation

## System Overview

The Long-Tail Storm is built on a Medallion Architecture using Azure Databricks and Delta Lake.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │   SOURCE     │     │   BRONZE     │     │   SILVER     │                │
│  │   SYSTEMS    │────▶│   LAYER      │────▶│   LAYER      │                │
│  │              │     │              │     │              │                │
│  │ • Claims DB  │     │ • Raw Data   │     │ • Triangles  │                │
│  │ • Payments   │     │ • Full Hist. │     │ • NLP Risk   │                │
│  │ • Reserves   │     │ • CDC Enabled│     │ • Enriched   │                │
│  └──────────────┘     └──────────────┘     └──────────────┘                │
│                                                   │                        │
│                                                   ▼                        │
│                       ┌──────────────┐     ┌──────────────┐                │
│                       │   AI/BI      │◀────│    GOLD      │                │
│                       │   GENIE      │     │    LAYER     │                │
│                       │              │     │              │                │
│                       │ • NL Queries │     │ • Reserves   │                │
│                       │ • Executive  │     │ • IBNR       │                │
│                       │   Dashboard  │     │ • Metrics    │                │
│                       └──────────────┘     └──────────────┘                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Layer Descriptions

### Bronze Layer (Raw)
- **Purpose**: Land raw data from source systems
- **Owner**: Marcus (Data Engineer)
- **Technology**: Auto Loader, Delta Lake
- **Tables**:
  - `bronze.raw_claims` - Raw claim records
  - `bronze.raw_payments` - Raw payment transactions

### Silver Layer (Transformed)
- **Purpose**: Business logic, transformations, enrichment
- **Owners**: Marcus (Triangles), Anya (NLP)
- **Technology**: PySpark, Delta Lake
- **Tables**:
  - `silver.loss_triangles` - Actuarial loss triangles
  - `silver.enriched_claims` - Claims with risk flags
  - `silver.risk_summary` - Aggregated risk metrics

### Gold Layer (Business)
- **Purpose**: Business-ready aggregations for reporting
- **Owner**: Sarah (Actuarial)
- **Technology**: Python, chainladder, Delta Lake
- **Tables**:
  - `gold.reserve_estimates` - IBNR by origin year
  - `gold.reserve_summary` - Executive summary
  - `gold.development_factors` - ATA/CDF factors

### Semantic Layer (AI/BI)
- **Purpose**: Natural language interface for executives
- **Owner**: David (Analytics)
- **Technology**: Databricks Genie
- **Views**:
  - `v_reserve_adequacy` - Reserve health metrics
  - `v_ibnr_by_origin` - IBNR detail view
  - `v_risk_exposure` - Risk analysis view

## Key Components

### Chain Ladder Model
The actuarial engine that projects ultimate losses:

```
Ultimate Loss = Latest Cumulative × CDF
IBNR = Ultimate Loss - Latest Cumulative
```

### NLP Risk Detector
AI-powered analysis of adjuster notes to flag:
- Water damage / Mold risks
- Litigation involvement
- Structural issues
- Fraud indicators

### Loss Triangles
2D matrices showing claim development:
- Rows: Origin years (when claims occurred)
- Columns: Development periods (claim age)
- Values: Cumulative paid or incurred losses

## Data Flow

1. **Ingestion**: Auto Loader streams claims/payments to Bronze
2. **Transformation**: Claims joined with payments, enriched with origin/dev periods
3. **Risk Analysis**: NLP scans adjuster notes for risk keywords
4. **Triangle Building**: Data pivoted to triangle format
5. **Reserving**: Chain Ladder calculates ATA factors and IBNR
6. **Reporting**: Metrics exposed via Genie for NL queries








