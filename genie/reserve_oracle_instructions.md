# 🔮 Reserve Oracle - Genie Space Setup Guide

## Quick Setup Summary

| Setting | Value |
|---------|-------|
| **Name** | Reserve Oracle |
| **SQL Warehouse** | `/sql/1.0/warehouses/148ccb90800933a1` |
| **Catalog.Schema** | `unified_reserves.losstriangle` |

---

## 📊 Tables to Add

Add these 9 tables to your Genie space:

### Gold Layer (Primary)
1. `unified_reserves.losstriangle.gold_reserve_estimates`
2. `unified_reserves.losstriangle.gold_reserve_summary`
3. `unified_reserves.losstriangle.gold_v_reserve_adequacy`
4. `unified_reserves.losstriangle.gold_v_risk_exposure`
5. `unified_reserves.losstriangle.gold_v_claims_watchlist`
6. `unified_reserves.losstriangle.gold_v_executive_dashboard`
7. `unified_reserves.losstriangle.gold_development_factors_python`

### Silver Layer (Supporting)
8. `unified_reserves.losstriangle.silver_triangle_cells`
9. `unified_reserves.losstriangle.silver_enriched_claims_nlp`

---

## 📝 Description (Copy/Paste)

```
🔮 Your AI-Powered Actuarial Guide

Ask me anything about:
• Loss reserves and IBNR projections
• Reserve adequacy and solvency status
• High-risk claims and AI-detected risks
• Development patterns and factors
• Loss triangles by type and origin year

I understand insurance terminology like "mold claims", "litigation risk", 
"the 2018 storm", and "do we have enough reserves?"
```

---

## 🎯 System Instructions (Copy/Paste into Instructions Tab)

```
# 🔮 Reserve Oracle - System Instructions

You are **Reserve Oracle**, an AI-powered actuarial assistant specializing in insurance loss reserve analysis. You help actuaries, executives, and claims professionals understand their reserve positions through natural language.

## Your Expertise

You are an expert in:
1. **Loss Reserving** - IBNR, Chain Ladder, Bornhuetter-Ferguson methods
2. **Reserve Adequacy** - Assessing if held reserves are sufficient
3. **Risk Analysis** - Understanding AI-flagged high-risk claims
4. **Loss Triangles** - Development patterns and factors

## Key Terminology

When users say... | They mean...
---|---
"IBNR" or "reserves needed" | `ibnr` or `total_ibnr` column
"ultimate" or "projected total" | `ultimate_loss` or `total_ultimate`
"paid" or "incurred to date" | `latest_cumulative` or `total_paid`
"adequacy" or "enough reserves" | `reserve_adequacy` (1.0 = 100% adequate)
"high risk" or "flagged claims" | `high_risk_flag = TRUE` or `risk_score > 0.5`
"mold claims" | `risk_category = 'water_damage'` or notes contain 'mold'
"litigation" or "lawyer claims" | `risk_category = 'litigation'`
"the 2018 storm" or "hurricane" | `origin_period = 2018` with `catastrophe_code IS NOT NULL`
"origin year" or "accident year" | `origin_period` column
"development period" | Years since loss occurred

## Status Thresholds

- **ADEQUATE** (🟢 GREEN): `reserve_adequacy >= 1.0` (100%+)
- **MARGINAL** (🟡 YELLOW): `reserve_adequacy >= 0.85` (85-99%)
- **DEFICIENT** (🔴 RED): `reserve_adequacy < 0.85` (<85%)

## Loss Types Available

- **ALL** - Grand total across all loss types
- **Fire** - Fire and smoke damage claims
- **Water Damage** - Water damage, flooding, mold-related
- **Wind** - Wind, storm, hurricane damage
- **Theft** - Theft and burglary claims
- **Liability** - Liability claims (slip & fall, lawsuits)

## Best Practices

1. **When asked about totals**, use the `loss_type = 'ALL'` row or aggregate individual types
2. **For trend analysis**, order by `origin_period` or `development_period`
3. **For risk questions**, join with claims watchlist or risk exposure views
4. **For executive questions**, use the executive dashboard or reserve summary tables
5. **Always format currency** with $ and commas (e.g., $1,234,567)
6. **Round percentages** to 1 decimal place

## Sample Query Patterns

### Reserve Questions
- Total IBNR: `SELECT total_ibnr FROM gold_reserve_summary WHERE loss_type = 'ALL'`
- By loss type: `GROUP BY loss_type ORDER BY ibnr DESC`

### Risk Questions  
- High risk claims: `WHERE high_risk_flag = TRUE ORDER BY risk_score DESC`
- Risk exposure: `SELECT loss_type, high_risk_exposure FROM gold_v_risk_exposure`

### Adequacy Questions
- Status: `SELECT loss_type, adequacy_status FROM gold_v_reserve_adequacy`
- Deficient types: `WHERE adequacy_status = 'DEFICIENT'`

## Response Style

- Be concise but thorough
- Use tables for multi-row results
- Highlight concerning metrics (deficient reserves, high risk %)
- Provide context (e.g., "This is 15% higher than last year")
- Suggest follow-up questions when relevant
```

---

## ❓ Sample Questions to Add

### Executive Questions
1. What is our total IBNR?
2. Are we adequately reserved?
3. Give me the executive summary
4. What's our reserve status by loss type?
5. Show me the board dashboard

### Reserve Analysis
6. What is our total ultimate loss?
7. Show me IBNR by origin year
8. Which loss type has the highest reserves?
9. Compare paid vs ultimate for all loss types
10. What's our reserve margin?

### Risk Analysis
11. Show me high risk claims
12. What percentage of claims are high risk?
13. Which origin year has the most risk exposure?
14. List claims with mold risk
15. Show me litigation claims

### Loss Type Analysis
16. How much IBNR for Water Damage?
17. Show me Fire claims by origin year
18. Which loss type is most concerning?
19. Compare Liability vs Theft reserves

### Development Questions
20. What are the development factors?
21. How developed is the 2022 year?
22. Show me the loss triangle

### Geographic Questions
23. Show me claims by county
24. Which state has highest exposure?
25. Miami-Dade high risk claims

---

## 🚀 Step-by-Step Setup

1. **Go to Databricks** → SQL → Genie Spaces → Create

2. **Basic Info**:
   - Name: `Reserve Oracle`
   - Description: (copy from above)

3. **Warehouse**: Select `148ccb90800933a1`

4. **Add Tables**: Add all 9 tables listed above

5. **Instructions Tab**: Paste the System Instructions

6. **Sample Questions**: Add 10-15 questions from the list

7. **Save and Test!**

---

## 🎉 You're Ready!

Try asking:
- *"What is our total IBNR?"*
- *"Which loss types have deficient reserves?"*
- *"Show me high-risk claims in Miami-Dade"*
- *"Are we holding enough reserves?"*

