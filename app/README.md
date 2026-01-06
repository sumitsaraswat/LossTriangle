# ClaimWise Actuarial Analytics Platform

<p align="center">
  <img src="logo.svg" alt="ClaimWise Insurance Logo" width="150"/>
</p>

<p align="center">
  <strong>A Databricks App for Loss Triangle Analysis & Reserve Estimation</strong>
</p>

---

## 🎯 Overview

The **ClaimWise Actuarial Analytics Platform** is an interactive dashboard application built with Streamlit, designed for actuarial teams to analyze loss development patterns, compare reserving methods, and assess reserve variability.

This app is inspired by the [chainladder-python gallery](https://chainladder-python.readthedocs.io/en/latest/gallery/index.html) and translates those visualizations into an enterprise-ready Databricks App.

## 📊 Dashboard Pages

| Page | Description | Chainladder Inspiration |
|------|-------------|------------------------|
| **Executive Summary** | High-level KPIs and reserve overview | - |
| **Loss Development** | Development curves and ATA factors | [Loss Development Patterns](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_development.html) |
| **Method Comparison** | Chain Ladder vs BF vs Benktander | [BF vs Chainladder](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_bf_cl.html) |
| **Reserve Variability** | Confidence intervals and tail sensitivity | [Value at Risk](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_value_at_risk.html) |
| **Diagnostics** | Actual vs Expected, residuals | [Actual vs Expected](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_ave.html) |
| **Development Exhibit** | Standard actuarial exhibit format | [MackChainladder](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_mack.html) |

## 🚀 Deployment

### Deploy to Databricks Apps

1. **Via Databricks UI:**
   ```
   Workspace → Apps → Create App → Upload from folder
   ```

2. **Via Databricks CLI:**
   ```bash
   databricks apps deploy ./app --name claimwise-actuarial-analytics
   ```

3. **Via API:**
   ```python
   from databricks.sdk import WorkspaceClient
   
   w = WorkspaceClient()
   w.apps.deploy(
       name="claimwise-actuarial-analytics",
       source_code_path="/Workspace/Users/you/LossTriangle/app"
   )
   ```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
streamlit run app.py
```

## 📁 Project Structure

```
app/
├── app.py              # Main Streamlit application
├── app.yaml            # Databricks App configuration
├── requirements.txt    # Python dependencies
├── logo.svg            # ClaimWise Insurance logo
└── README.md           # This file
```

## 🔗 Data Sources

The app connects to Unity Catalog tables in the `unified_reserves` catalog:

| Layer | Tables |
|-------|--------|
| **Gold** | `gold_loss_development_curves`, `gold_method_comparison`, `gold_reserve_variability`, `gold_ata_factor_diagnostics` |
| **Silver** | `silver_triangle_cells`, `silver_enriched_claims_nlp` |

## ⚙️ Configuration

Environment variables (set in `app.yaml` or Databricks secrets):

| Variable | Description | Default |
|----------|-------------|---------|
| `CATALOG` | Unity Catalog name | `unified_reserves` |
| `SCHEMA` | Default schema | `gold` |

## 📈 Key Features

- **Interactive Filters**: Filter by origin year, loss type
- **Multiple Visualization Types**: Line charts, bar charts, scatter plots, area charts
- **Export Capability**: Download exhibits as CSV
- **Responsive Design**: Works on desktop and tablet
- **Demo Mode**: Runs with sample data when Databricks connection unavailable

## 🎨 Branding

- **Company**: ClaimWise Insurance
- **Colors**: Navy Blue (#1e3a5f), Medium Blue (#2d5a87), Light Blue (#3d7ab5)
- **Logo**: Shield with triangle (representing loss triangles) and checkmark

## 📝 License

Proprietary - ClaimWise Insurance © 2024

---

<p align="center">
  Built with ❤️ using Streamlit and Databricks
</p>
