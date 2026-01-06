# 🌀 The Long-Tail Storm

**Automated Loss Triangle Reserving & AI-Driven Solvency on Azure Databricks**

*Theme: From Chaos to Solvency*

---

## 📖 The Story

> *"The Storm That Lasted Five Years"*

It is 2018. A massive hurricane strikes the Florida coast. For policyholders, the event is terrifying but finite—the wind stops blowing after 24 hours. For the insurance company, however, the storm is just beginning.

In the first month, thousands of claims pour in for broken windows. The company pays out $50M and breathes a sigh of relief. Their legacy dashboards show the storm is "fully paid."

**They are wrong.**

Two years later, claims arrive for toxic mold. Three years later, complex lawsuits emerge. If they treated their 2018 books as "closed," they face **insolvency**.

---

## 🎯 Mission

Build a **"Time Machine"**—an Automated Reserving Engine on Azure Databricks that:

| Capability | Description |
|------------|-------------|
| **Ingest & Organize** | Transform raw transactions into actuarial Loss Triangles |
| **Predict** | Use Chain Ladder Method to estimate IBNR reserves |
| **Detect** | Use NLP to scan claim notes for hidden risks |
| **Explain** | Empower executives with AI/BI Genie |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         AZURE DATABRICKS                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐  │
│  │  BRONZE  │───▶│  SILVER  │───▶│   GOLD   │───▶│  AI/BI GENIE │  │
│  │          │    │          │    │          │    │              │  │
│  │ Raw Data │    │ Triangle │    │ Reserves │    │  Executive   │  │
│  │ Ingestion│    │ + NLP    │    │ & IBNR   │    │  Interface   │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────────┘  │
│       ▲              ▲               ▲                  ▲          │
│       │              │               │                  │          │
│    Marcus         Marcus           Sarah             David         │
│  (Engineer)      + Anya          (Actuary)        (Analytics)     │
│                  (AI/NLP)                                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 👥 The Squad

| Persona | Role | Focus | Responsibility |
|---------|------|-------|----------------|
| **Marcus** | Data Engineer | Speed & Structure | Medallion Architecture, Triangle Transformation |
| **Anya** | AI Engineer | Unstructured Data | NLP Risk Detection ("mold", "attorney") |
| **Sarah** | Actuarial Data Scientist | Math & Models | Chain Ladder, IBNR Calculation |
| **David** | Analytics Lead | Semantic Layer | AI/BI Genie, Executive Storytelling |

---

## 📁 Project Structure

```
LossTriangle/
├── .github/workflows/          # CI/CD pipelines
├── data/
│   ├── raw/                    # Raw synthetic data
│   ├── samples/                # Sample datasets
│   └── schemas/                # Data schemas
├── docs/
│   ├── architecture/           # Technical diagrams
│   └── narrative/              # Story documentation
├── notebooks/
│   ├── 00_setup/               # Environment setup
│   ├── 01_bronze/              # Bronze layer notebooks
│   ├── 02_silver/              # Silver layer notebooks
│   ├── 03_gold/                # Gold layer notebooks
│   └── 04_analytics/           # Genie & dashboards
├── src/
│   ├── data_engineering/       # Bronze/Silver ETL
│   ├── ai_risk/                # NLP risk detection
│   ├── actuarial/              # Chain Ladder models
│   └── semantic/               # Genie metrics
├── tests/                      # Unit & integration tests
├── config/                     # Environment configs
├── workflows/                  # Databricks jobs
└── databricks.yml              # Asset Bundle config
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- Azure Databricks workspace
- Databricks CLI configured

### Setup

```bash
# Clone the repository
git clone https://github.com/your-org/LossTriangle.git
cd LossTriangle

# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Deploy to Databricks
databricks bundle deploy --target dev
```

---

## 📊 Key Metrics

| Metric | Formula | Description |
|--------|---------|-------------|
| **IBNR** | `Ultimate Loss - Cumulative Paid` | Incurred But Not Reported reserves |
| **Reserve Adequacy** | `Held Reserves / Ultimate Loss` | Solvency health indicator |
| **Loss Ratio** | `Incurred Loss / Earned Premium` | Underwriting performance |
| **ATA Factor** | `Loss(t) / Loss(t-1)` | Age-to-Age development factor |

---

## 🎬 The Demo Moment

> **CFO asks:** *"What is our total exposure for Water Damage claims in Miami-Dade county older than 24 months?"*

**Genie responds:** 
> *"The Reserve Adequacy for 2018 is 85%. This is below target because the AI model detected a surge in mold-related claims in the last quarter."*

---

## 📚 Documentation

- [Architecture Deep Dive](docs/architecture/README.md)
- [Data Dictionary](docs/architecture/data_dictionary.md)
- [Chain Ladder Methodology](docs/architecture/chain_ladder.md)
- [NLP Risk Detection](docs/architecture/nlp_risk.md)

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| **Storage** | Delta Lake (Unity Catalog) |
| **Compute** | Databricks Runtime 17.3 |
| **Orchestration** | Databricks Workflows |
| **ML/NLP** | Spark MLlib, Hugging Face |
| **Actuarial** | chainladder-python |
| **BI** | AI/BI Genie |

---

## 📄 License

MIT License - See [LICENSE](LICENSE)

---

<p align="center">
  <i>Built with ❤️ for insurance solvency</i>
</p>








