# Data Dictionary

**Catalog:** `unified_reserves`

## Bronze Layer Tables

### `unified_reserves.bronze.raw_claims`
Raw claims data from source systems.

| Column | Type | Description |
|--------|------|-------------|
| claim_id | STRING | Unique claim identifier (PK) |
| policy_id | STRING | Associated policy ID |
| loss_date | DATE | Date when loss occurred |
| report_date | DATE | Date claim was reported |
| claimant_name | STRING | Name of the claimant |
| loss_type | STRING | Type of loss (Water Damage, Wind, Fire, etc.) |
| loss_description | STRING | Detailed description of the loss |
| adjuster_notes | STRING | Notes from claims adjuster |
| claim_status | STRING | Current status (Open, Closed, Reopened) |
| county | STRING | County of loss location |
| state | STRING | State of loss location |
| zip_code | STRING | ZIP code of loss location |
| catastrophe_code | STRING | CAT event code if applicable |
| created_at | TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | Last update time |
| _ingested_at | TIMESTAMP | Ingestion timestamp |
| _source_file | STRING | Source file path |

### `unified_reserves.bronze.raw_payments`
Raw payment transactions.

| Column | Type | Description |
|--------|------|-------------|
| payment_id | STRING | Unique payment identifier (PK) |
| claim_id | STRING | Associated claim ID (FK) |
| payment_date | DATE | Date of payment |
| payment_type | STRING | Type (Indemnity, Expense) |
| payment_amount | DOUBLE | Payment amount |
| cumulative_paid | DOUBLE | Cumulative paid to date |
| reserve_amount | DOUBLE | Current reserve amount |
| payment_status | STRING | Status (Cleared, Pending) |
| check_number | STRING | Check number |
| payee_name | STRING | Payee name |
| expense_type | STRING | Expense category |
| created_at | TIMESTAMP | Record creation time |

---

## Silver Layer Tables

### `unified_reserves.silver.enriched_claims`
Claims enriched with risk analysis.

| Column | Type | Description |
|--------|------|-------------|
| claim_id | STRING | Unique claim identifier |
| origin_year | INT | Year of loss occurrence |
| origin_period | INT | Origin period for triangles |
| loss_type | STRING | Type of loss |
| total_paid | DOUBLE | Total paid to date |
| current_reserve | DOUBLE | Current reserve amount |
| incurred_amount | DOUBLE | Total incurred (paid + reserve) |
| risk_category | STRING | AI-detected risk category |
| risk_score | DOUBLE | Risk score (0-1) |
| risk_keywords | ARRAY<STRING> | Detected risk keywords |
| high_risk_flag | BOOLEAN | High risk indicator |

### `unified_reserves.silver.loss_triangles`
Loss triangles in long format.

| Column | Type | Description |
|--------|------|-------------|
| triangle_id | STRING | Unique triangle identifier |
| triangle_type | STRING | Type (paid, incurred) |
| loss_type | STRING | Loss type filter applied |
| origin_period | INT | Origin year |
| development_period | INT | Development period (age) |
| cumulative_value | DOUBLE | Cumulative loss value |
| claim_count | INT | Number of claims |
| period_type | STRING | Granularity (year, quarter) |
| as_of_date | DATE | Evaluation date |

### `unified_reserves.silver.risk_summary`
Aggregated risk metrics.

| Column | Type | Description |
|--------|------|-------------|
| origin_year | INT | Origin year |
| loss_type | STRING | Loss type |
| total_claims | BIGINT | Total claim count |
| high_risk_claims | BIGINT | High risk claim count |
| avg_risk_score | DOUBLE | Average risk score |
| total_incurred | DOUBLE | Total incurred amount |
| high_risk_incurred | DOUBLE | High risk incurred |
| high_risk_pct | DOUBLE | High risk percentage |

---

## Gold Layer Tables

### `unified_reserves.gold.reserve_estimates`
Chain Ladder reserve estimates by origin period.

| Column | Type | Description |
|--------|------|-------------|
| origin_period | INT | Origin year |
| latest_cumulative | DOUBLE | Latest cumulative loss |
| ultimate_loss | DOUBLE | Projected ultimate loss |
| ibnr | DOUBLE | IBNR reserve |
| percent_reported | DOUBLE | % of ultimate reported |
| triangle_type | STRING | Triangle type used |
| loss_type | STRING | Loss type |
| tail_factor | DOUBLE | Tail factor applied |

### `unified_reserves.gold.reserve_summary`
Executive summary of reserves.

| Column | Type | Description |
|--------|------|-------------|
| as_of_date | DATE | Evaluation date |
| triangle_type | STRING | Triangle type |
| loss_type | STRING | Loss type |
| total_paid | DOUBLE | Total paid losses |
| total_ibnr | DOUBLE | Total IBNR |
| total_ultimate | DOUBLE | Total ultimate |
| held_reserves | DOUBLE | Currently held reserves |
| reserve_adequacy | DOUBLE | Adequacy ratio |
| reserve_margin | DOUBLE | Margin over IBNR |
| ai_adjustment_factor | DOUBLE | AI risk adjustment |

### `unified_reserves.gold.development_factors`
Age-to-Age and CDF factors.

| Column | Type | Description |
|--------|------|-------------|
| triangle_type | STRING | Triangle type |
| loss_type | STRING | Loss type |
| development_period | INT | Development period |
| ata_factor | DOUBLE | Age-to-Age factor |
| cdf_to_ultimate | DOUBLE | CDF to ultimate |

---

## Key Metrics

| Metric | Formula | Description |
|--------|---------|-------------|
| **IBNR** | `Ultimate - Paid` | Incurred But Not Reported |
| **Reserve Adequacy** | `Held / Ultimate` | Solvency health indicator |
| **Loss Ratio** | `Incurred / Premium` | Underwriting profitability |
| **ATA Factor** | `Loss(t+1) / Loss(t)` | Development factor |
| **CDF** | `∏ ATA factors` | Cumulative development |

