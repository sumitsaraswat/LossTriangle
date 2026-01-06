# Loss Triangle Pipeline Documentation

## The Complete Guide to Understanding Your Actuarial Data Pipeline

This document walks you through every step of the Loss Triangle pipeline, explaining what each table does with simple, real-world examples.

---

# 🥉 BRONZE LAYER: Raw Data Ingestion

The Bronze layer is where data enters the system in its rawest form. Think of it as the "loading dock" of your data warehouse—everything comes in as-is, exactly how the source systems provide it.

---

## Step 1: `bronze_raw_claims_batch`

### What it does
Loads raw insurance claim records into the system. Each row represents one claim filed by a policyholder after an incident (hurricane, fire, theft, etc.).

### The Data
A typical claim record looks like this:

| Field | Example Value |
|-------|---------------|
| claim_id | CLM-2018-001 |
| policy_id | POL-12345 |
| loss_date | 2018-09-15 |
| claimant_name | John Smith |
| loss_type | Water Damage |
| adjuster_notes | "Hurricane caused roof damage. Water intrusion in multiple rooms." |
| claim_status | Open |
| catastrophe_code | CAT-2018-MICHAEL |

### Why it matters
This is the foundation of everything. Every downstream calculation—reserves, risk scores, ultimate losses—starts from these claim records.

### Simple Example
> 🏠 **John Smith's Claim**
> 
> On September 15, 2018, Hurricane Michael damaged John's roof. He filed claim CLM-2018-001 the next day. The adjuster noted water intrusion in multiple rooms.
> 
> This single record enters the Bronze layer and will eventually flow through Silver (where we calculate how much we've paid) and Gold (where we predict how much more we'll pay).

---

## Step 2: `bronze_raw_payments_batch`

### What it does
Loads raw payment transactions for claims. Each row represents one payment made on a claim—either to the claimant (indemnity) or to service providers (expenses like legal fees or remediation).

### The Data

| Field | Example Value |
|-------|---------------|
| payment_id | PAY-001 |
| claim_id | CLM-2018-001 |
| payment_date | 2018-10-15 |
| payment_type | Indemnity |
| payment_amount | $15,000 |
| cumulative_paid | $15,000 |
| reserve_amount | $50,000 |

### The Payment Journey
For John Smith's claim (CLM-2018-001), payments happened over time:

```
October 2018:   $15,000  (initial repair payment)
December 2018:  $20,000  (additional damage found)
June 2019:      $10,000  (final settlement)
─────────────────────────
Total Paid:     $45,000
```

### Why it matters
Payments are the "facts on the ground"—they tell us how much we've actually spent. But claims develop over time, and early payments rarely tell the whole story.

---

# 🥈 SILVER LAYER: Data Enrichment & Transformation

The Silver layer cleans, enriches, and transforms raw data into business-ready formats. This is where the magic of actuarial science begins.

---

## Step 3: `silver_claims_with_payments`

### What it does
Joins claims with their payment history to create an enriched view showing the current financial state of each claim.

### The Calculation

For each claim, we calculate:

$$\text{Incurred Amount} = \text{Total Paid} + \text{Current Reserve}$$

### Simple Example

> **John Smith's Claim Summary**
> 
> | Metric | Value |
> |--------|-------|
> | Total Paid to Date | $45,000 |
> | Current Reserve | $25,000 |
> | **Incurred Amount** | **$70,000** |
> 
> The incurred amount ($70,000) is our current best estimate of what this claim will ultimately cost. It includes what we've paid ($45,000) plus what we're holding in reserve for future payments ($25,000).

### Why it matters
This table gives claims handlers and managers a single view of each claim's financial position.

---

## Step 4: `silver_claims_basic_risk`

### What it does
Scans adjuster notes for keywords that indicate elevated risk, using simple SQL pattern matching.

### The Risk Keywords

| Risk Category | Keywords Detected |
|---------------|-------------------|
| 🦠 Water/Mold | "mold", "seepage", "moisture", "damp", "black mold" |
| ⚖️ Litigation | "attorney", "lawyer", "lawsuit", "litigation" |
| 🏗️ Structural | "structural", "foundation", "collapse" |

### Simple Example

> **Analyzing John's Claim**
> 
> Adjuster notes: *"Water intrusion in multiple rooms. Early signs of moisture accumulation."*
> 
> | Check | Result |
> |-------|--------|
> | Contains "mold"? | ❌ No |
> | Contains "moisture"? | ✅ Yes |
> | Contains "attorney"? | ❌ No |
> | **High Risk Flag** | **✅ TRUE** |
> 
> Even though "mold" isn't mentioned yet, the word "moisture" triggers a water/mold risk flag. This claim needs watching!

### Why it matters
Early risk detection helps claims managers prioritize reviews before small problems become expensive lawsuits.

---

## Step 5: `silver_enriched_claims_nlp`

### What it does
Uses **AI-powered analysis** (Databricks AI Functions) to intelligently classify claims—going beyond simple keyword matching to understand context and nuance.

### The AI Functions Used

| Function | Purpose |
|----------|---------|
| `ai_classify()` | Classifies claims as high/medium/low risk |
| `ai_extract()` | Extracts specific entities (mold, litigation, fraud indicators) |
| `ai_analyze_sentiment()` | Detects negative sentiment indicating problematic claims |

### Simple Example

> **AI Analysis of a Complex Claim**
> 
> Adjuster notes: *"Customer discovered black mold in walls two months after storm. Extensive remediation required. Attorney representation confirmed."*
> 
> **AI Results:**
> 
> | Analysis | Result |
> |----------|--------|
> | Risk Category | 🔴 HIGH_RISK |
> | Risk Score | 0.92 |
> | Sentiment | Negative |
> | Extracted Entities | mold_damage: Yes, attorney_involvement: Yes |
> 
> The AI understands that "black mold" + "attorney" = a claim likely to escalate significantly.

### Why it matters
AI catches risks that simple keyword matching misses. "The client's legal team is reviewing options" means the same thing as "lawsuit" but wouldn't trigger keyword detection.

---

## Step 6: `silver_triangle_base`

### What it does
Calculates the **development period** for each payment—how many years after the loss date was this payment made?

### The Calculation

$$\text{Development Period} = \text{FLOOR}\left(\frac{\text{Payment Date} - \text{Loss Date}}{12 \text{ months}}\right)$$

### Simple Example

> **John's Payment Timeline**
> 
> Loss Date: September 15, 2018
> 
> | Payment Date | Months Since Loss | Development Period |
> |--------------|-------------------|-------------------|
> | October 2018 | 1 month | **0** (same year) |
> | December 2018 | 3 months | **0** (same year) |
> | June 2019 | 9 months | **0** (same year) |
> | January 2020 | 16 months | **1** (1 year later) |
> | March 2021 | 30 months | **2** (2 years later) |
> 
> Development Period 0 = payments made within the first 12 months of the loss.

### Why it matters
This is the foundation of loss triangles—we need to know WHEN payments happened relative to when losses occurred.

---

## Step 7: `silver_triangle_cells`

### What it does
Aggregates all payments into the classic **Loss Triangle** format—a matrix showing cumulative payments by origin year and development period.

### The Calculation

**Incremental Value (X_ij):** Sum of all payments for origin year i in development period j

**Cumulative Value (C_ij):** Running total of incremental values

$$C_{ij} = X_{i0} + X_{i1} + ... + X_{ij}$$

### Simple Example

> **Building the Triangle**
> 
> For Origin Year 2018:
> 
> | Development Period | Incremental (X) | Cumulative (C) |
> |-------------------|-----------------|----------------|
> | 0 | $160,000 | $160,000 |
> | 1 | $80,000 | $240,000 |
> | 2 | $25,000 | $265,000 |
> | 3 | $35,000 | $300,000 |
> 
> By Development Period 3, we've cumulatively paid $300,000 for all 2018 claims.

### Why it matters
The triangle shape emerges because newer years have fewer development periods observed. We need to "complete" the triangle to estimate ultimate losses.

---

## Step 8: `silver_triangle_pivot`

### What it does
Pivots the triangle cells into the traditional actuarial format—rows are origin years, columns are development periods.

### The Result

```
              Development Period
           0        1        2        3
2018   $160,000  $240,000  $265,000  $300,000
2019   $68,000      ?         ?         ?
2020   $27,000      ?         ?         ?
2021   $25,000      ?         ?         ?
```

### Simple Example

> **Reading the Triangle**
> 
> - **2018 row:** We can see the full development history (4 data points)
> - **2021 row:** We only have 1 data point (payments in the first year)
> - **The "?"s:** These are the future payments we need to predict!
> 
> The goal of the Chain Ladder method is to fill in those question marks.

### Why it matters
This is the exact format actuaries have used for over 100 years. It's the input to all reserve estimation methods.

---

## Step 9: `silver_risk_summary_nlp`

### What it does
Aggregates AI risk scores by origin year to understand risk exposure trends over time.

### Simple Example

> **Risk Summary by Year**
> 
> | Origin Year | Total Claims | High Risk Claims | High Risk % | High Risk Incurred |
> |-------------|--------------|------------------|-------------|-------------------|
> | 2018 | 5 | 3 | 60% | $355,000 |
> | 2019 | 2 | 0 | 0% | $0 |
> | 2020 | 2 | 1 | 50% | $165,000 |
> | 2021 | 1 | 1 | 100% | $100,000 |
> 
> 2018 was a hurricane year—lots of high-risk claims with attorney involvement!

### Why it matters
Helps actuaries adjust reserve estimates upward for years with high litigation exposure.

---

# 🥇 GOLD LAYER: Actuarial Analytics & Business Metrics

The Gold layer contains the crown jewels—the actuarial calculations that power reserve estimates and financial reporting.

---

## Step 10: `gold_ata_factor_base` (The "Link" Ratio)

### What it does
Calculates how a single origin year changed from one development period to the next.

### The Data
- At 12 Months (Period 0): We had paid **$160,000** for Year 2018 claims
- At 24 Months (Period 1): We had paid **$240,000** for those same claims

### The Calculation

We simply divide the new value by the old value:

$$\frac{\$240,000}{\$160,000} = 1.50$$

### The Result
The "Individual ATA" for this specific point is **1.50** (meaning payments grew by 50%).

---

## Step 11: `gold_ata_factors` (The "Group" Average)

### What it does
Combines multiple years to find a standard average development pattern.

### The Data

| Origin Year | Period 0 → Period 1 |
|-------------|---------------------|
| Year 2018 | $160,000 → $240,000 |
| Year 2019 | $68,000 → (not observed) |

For a better example with multiple data points:
- Year A (Period 1 → 2): $1,000 → $1,200
- Year B (Period 1 → 2): $5,000 → $5,500

### The Calculation (Volume Weighted)

Instead of averaging the ratios, we sum the money first (giving more weight to larger years):

$$\text{Total Start} = \$1,000 + \$5,000 = \$6,000$$
$$\text{Total End} = \$1,200 + \$5,500 = \$6,700$$
$$\text{Factor} = \frac{\$6,700}{\$6,000} = 1.116$$

### The Result
The selected ATA Factor for this development period is **1.116**.

### Why Volume Weighted?
If we simple-averaged the ratios:
- Year A ratio: 1.200
- Year B ratio: 1.100
- Simple average: 1.150

But Year B has 5x more money at stake! Volume weighting gives its more stable ratio more influence, resulting in 1.116 (closer to Year B's 1.100).

---

## Step 12: `gold_cdf_factors` (The Cumulative Factor)

### What it does
Calculates how much more development is expected from each point to ultimate settlement.

### The Calculation

The CDF is the product of all remaining ATA factors:

$$CDF_j = ATA_j \times ATA_{j+1} \times ... \times ATA_{final} \times TailFactor$$

### Simple Example

Given these ATA factors:

| Transition | ATA Factor |
|------------|------------|
| Period 0 → 1 | 1.500 |
| Period 1 → 2 | 1.104 |
| Period 2 → 3 | 1.132 |
| Tail | 1.000 |

Calculate CDFs (working backwards):

```
CDF(3) = 1.000 (fully developed)
CDF(2) = 1.132 × 1.000 = 1.132
CDF(1) = 1.104 × 1.132 = 1.250
CDF(0) = 1.500 × 1.250 = 1.875
```

### The Result

| Development Period | CDF to Ultimate | % Developed |
|--------------------|-----------------|-------------|
| 0 | 1.875 | 53.3% |
| 1 | 1.250 | 80.0% |
| 2 | 1.132 | 88.3% |
| 3 | 1.000 | 100.0% |

### Reading the CDF

> If a claim is at Period 0 with $100,000 cumulative paid:
> 
> $$\text{Ultimate} = \$100,000 \times 1.875 = \$187,500$$
> 
> We expect to pay an additional $87,500 before the claim is fully settled!

---

## Step 13: `gold_reserve_estimates` (The IBNR Calculation)

### What it does
Calculates **IBNR (Incurred But Not Reported)** reserves—the amount we need to hold for future payments on existing claims.

### The Calculationa

$$\text{Ultimate Loss} = \text{Latest Cumulative} \times CDF$$
$$\text{IBNR} = \text{Ultimate Loss} - \text{Latest Cumulative}$$

### Simple Example

> **Reserve Estimates by Origin Year**
> 
> | Origin Year | Latest Paid | CDF | Ultimate Loss | IBNR Reserve |
> |-------------|-------------|-----|---------------|--------------|
> | 2018 | $300,000 | 1.000 | $300,000 | $0 |
> | 2019 | $68,000 | 1.875 | $127,500 | $59,500 |
> | 2020 | $27,000 | 1.875 | $50,625 | $23,625 |
> | 2021 | $25,000 | 1.875 | $46,875 | $21,875 |
> | **Total** | **$420,000** | - | **$525,000** | **$105,000** |
> 
> We need to hold **$105,000** in reserves for future payments!

### Why it matters
This is THE number that goes on the balance sheet. Underestimate it, and the company is insolvent. Overestimate it, and you're tying up capital unnecessarily.

---

## Step 14: `gold_reserve_summary` (Executive Dashboard)

### What it does
Combines Chain Ladder reserves with AI risk adjustments to create an executive-level summary.

### The AI Adjustment

High-risk claims (attorney involvement, litigation) tend to develop worse than average. We apply an uplift:

$$\text{AI Adjustment} = 1.0 + (\text{High Risk Ratio} \times 0.50)$$

### Simple Example

> **Reserve Summary**
> 
> | Metric | Value |
> |--------|-------|
> | Total Paid to Date | $420,000 |
> | Chain Ladder IBNR | $105,000 |
> | AI Adjustment Factor | 1.15 |
> | **Risk-Adjusted IBNR** | **$120,750** |
> | Reserve Margin | $15,750 |
> 
> The AI detected that 30% of incurred is in high-risk claims, so we add a 15% margin ($15,750) for adverse development.

---

## Step 15: `gold_v_*` Views (Semantic Layer)

### What they do
Pre-built views optimized for dashboards, reports, and AI/BI Genie queries.

| View | Purpose |
|------|---------|
| `gold_v_reserve_adequacy` | Solvency monitoring |
| `gold_v_ibnr_by_origin` | Detailed reserve breakdown |
| `gold_v_risk_exposure` | AI risk metrics |
| `gold_v_executive_dashboard` | Boardroom reporting |
| `gold_v_claims_watchlist` | High-risk claims for review |

---

# 📊 The Complete Picture

```
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                             │
│  bronze_raw_claims_batch    bronze_raw_payments_batch           │
│         ↓                           ↓                           │
├─────────────────────────────────────────────────────────────────┤
│                        SILVER LAYER                             │
│  silver_claims_with_payments → silver_claims_basic_risk         │
│         ↓                           ↓                           │
│  silver_enriched_claims_nlp → silver_risk_summary_nlp           │
│         ↓                                                       │
│  silver_triangle_base → silver_triangle_cells                   │
│         ↓                           ↓                           │
│  silver_triangle_pivot → silver_triangle_by_loss_type           │
├─────────────────────────────────────────────────────────────────┤
│                         GOLD LAYER                              │
│  gold_ata_factor_base → gold_ata_factors → gold_cdf_factors     │
│         ↓                                                       │
│  gold_reserve_estimates → gold_reserve_summary                  │
│         ↓                                                       │
│  gold_v_executive_dashboard (Semantic Views)                    │
└─────────────────────────────────────────────────────────────────┘
```

---

# 🔑 Key Takeaways

1. **Bronze = Raw Data**: Claims and payments enter exactly as source systems provide them

2. **Silver = Enriched Data**: We add risk scores, calculate development periods, and build the triangle

3. **Gold = Business Metrics**: Chain Ladder factors, IBNR reserves, and executive dashboards

4. **The Chain Ladder Method**: Uses historical development patterns to predict future payments

5. **AI Enhancement**: NLP risk detection helps identify claims likely to develop worse than average

---

*Document generated for the Loss Triangle Actuarial Analytics Platform*



