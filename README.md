# 🏙 Taiwan Real Estate AI Terminal — 六都趨勢監測系統

> **GitHub Repository:** https://github.com/eli20261124/howard-tiawan-house-trend-monitor  
> **Local Web App:** http://127.0.0.1:8000 *(start the server first — see Quick Start below)*

> For commercial licensing inquiries, please contact: **spoky119@gmail.com**

---

## What Is This?

An AI-driven real estate intelligence dashboard covering **Taiwan's six major cities** — Taipei, New Taipei, Taoyuan, Taichung, Tainan, and Kaohsiung. It pulls live data from the Ministry of the Interior (MOI) 實價登錄 open data feed, processes it through a Python pipeline, and presents the results in a browser-based terminal UI.

The goal is to turn raw government transaction records into something a researcher, investor, or analyst can actually use: ranked districts, price trend charts, presale vs. existing-home comparisons, and early-warning signals — all in one view.

---

## Project Overview

The system is split into three layers that work together:

**1. Data Pipeline (`main.py`)**  
Downloads the latest CSV exports from the MOI server for all six cities, normalizes column names, cleans prices, computes derived metrics (price-per-ping, building age, floor range), and writes the results as compressed Parquet snapshots plus JSON summaries into `data/processed/`.

**2. Analysis Engine (built into `main.py`)**  
On top of the raw cleaned data, the pipeline runs a second pass that:
- Computes quarterly medians, QoQ/YoY growth rates, and transaction volume per district.
- Estimates a forward-looking Oracle price signal for each district.
- Tags short-term turnover patterns, cluster anomalies, and market temperature.
- Joins presale project metadata (developer, building scale, zoning) from local CSV files in `data/project_info/`.
- Writes `v2_insights.json` and `summary.csv` for each city.

**3. Frontend Dashboard (`index.html` + `api.py`)**  
A single-page terminal-style UI served by a FastAPI backend. No framework or build step — pure JavaScript reads the API and renders everything client-side. Three main zones:

| Zone | Purpose |
|------|---------|
| **Zone 1 — Oracle Intelligence** | City-level leaderboard ranked by resilience, price, forecast, or leverage. Shows district-level Oracle predictions and market temperature. |
| **Zone 2 — District Health Matrix** | Color-coded health tiles for every district. Green = normal, amber = watch, red = alert. Signals include short-term turnover rate, leverage gap, DOM pressure, and price clusters. |
| **Zone 3 — Transaction Explorer** | Full row-level transaction table for existing homes and presale units. Supports filtering, sorting, and switching between the two tracks. |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data download | Python `requests`, direct MOI CSV endpoints |
| Data processing | `pandas` >= 2.0, custom cleaning & normalization logic |
| Storage format | Apache Parquet (compressed with Zstandard) via `pyarrow` |
| API server | FastAPI + Uvicorn |
| Frontend | Vanilla JavaScript, HTML, CSS — no build step |
| Automation | GitHub Actions (daily cron `0 2 * * *` = 10:00 AM Taipei Time) |
| License | CC BY-NC-SA 4.0 |

---

## Deep Observation Indicators

### Market Heat Signals

Each district is scored across five risk dimensions every time the pipeline runs:

1. **Presale Premium Gap** — how far presale median prices are above the existing-home median. A gap above 20% flags speculative pressure.
2. **Absorption Rate** — presale transaction volume as a percentage of existing-home volume. A ratio above 80% signals the presale market is dominating and the actual market may be thinning.
3. **City Momentum** — the balance of districts with positive vs. negative YoY growth. More rising districts = heating signal.
4. **Hot District YoY** — if the single hottest district's year-on-year growth exceeds 15%, it raises the heat score.
5. **Low-confidence Districts** — districts with fewer than 5 transactions in the latest quarter. Their signals are marked as unreliable.

These five dimensions feed a 0–5 risk gauge displayed in the top bar. 0–1 = Stable, 2–3 = Caution, 4–5 = Elevated/Hot.

### Oracle Price Prediction

The Oracle signal estimates the next quarter's district median price using a weighted blend:

- **60% weight** — slope of the actual (existing-home) price trend over the most recent available quarters.
- **40% weight** — presale price level, which typically leads the existing-home market by one to two quarters.

The result is a directional label (↑ / ↓ / →) and a projected median value. It is a screening tool, not a formal appraisal.

### Data Fusion (Pre-sale Enrichment)

Raw MOI presale records (`_b.csv`) carry the contract price and date but often lack readable project context. The Data Fusion step enriches every presale row by:

1. **Joining on `建案名稱`** (project name) against a local project info CSV in `data/project_info/{City}.csv`.
2. **Deriving Developer** — reads the `起造人` (builder) field, strips legal suffixes, and classifies individual builders separately from companies.
3. **Extracting Scale** — parses `層棟戶數` (floors/buildings/units) to pull out the unit count (e.g. `2棟/96戶` → `96`).
4. **Tagging Zoning** — reads the MOI native `都市土地使用分區` column first. Falls back to the project info CSV. Renders as a colored badge: cyan `住` for residential, yellow `商` for commercial, gray for others.
5. **Smart Address** — strips the district prefix from addresses (e.g. removes `新北市鶯歌區` prefix), uses a smaller font, and shows `—` when the field is empty.

---

## Repository Layout

```
.
├── main.py                     # Full data pipeline (download → clean → enrich → export)
├── api.py                      # FastAPI server — serves index.html + all /api/* endpoints
├── index.html                  # Single-page dashboard UI
├── requirements.txt            # Python dependencies
├── .gitignore                  # Excludes .env, .venv, raw CSVs, Parquet files, OS files
├── LICENSE                     # CC BY-NC-SA 4.0
├── data/
│   ├── project_info/           # Per-city enrichment CSVs (建案名稱, 起造人, 層棟戶數, 使用分區)
│   │   ├── Taipei.csv
│   │   ├── New_Taipei.csv
│   │   ├── Taoyuan.csv
│   │   ├── Taichung.csv
│   │   ├── Tainan.csv
│   │   └── Kaohsiung.csv
│   └── processed/              # Generated outputs (not committed — see .gitignore)
│       ├── manifest.json
│       ├── last_updated.json
│       └── {City}/
│           ├── summary.csv
│           ├── timeseries.json
│           ├── presale_timeseries.json
│           ├── v2_insights.json
│           ├── snapshot_meta.json
│           └── {District}_v3.parquet
└── .github/
    └── workflows/
        └── update_data.yml     # Daily scheduled pipeline run + auto-commit
```

---

## Quick Start (Local)

```bash
# 1. Clone
git clone https://github.com/eli20261124/howard-tiawan-house-trend-monitor.git
cd howard-tiawan-house-trend-monitor

# 2. Create environment
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the data pipeline (downloads live MOI data)
python main.py

# 5. Start the API server
uvicorn api:app --host 127.0.0.1 --port 8000

# 6. Open in browser
open http://127.0.0.1:8000
```

The pipeline downloads ~6 city CSV files from the MOI server, processes them, and writes outputs to `data/processed/`. First run takes 1–3 minutes depending on network speed.

---

## GitHub Actions — Automated Daily Refresh

The workflow at `.github/workflows/update_data.yml` runs automatically every day at **02:00 UTC (10:00 AM Taipei Time)**. It:

1. Checks out the repository.
2. Installs Python 3.11 and dependencies.
3. Runs `python main.py` to fetch the latest MOI data.
4. Commits and pushes the updated `v2_insights.json` files back to the repository.

You can also trigger it manually from the **Actions** tab in GitHub.

---

## Data Source

All transaction data is sourced from the **Ministry of the Interior (內政部) 不動產交易實價登錄** open data platform:

- Endpoint: `https://plvr.land.moi.gov.tw`
- Update frequency: Batches released on the 1st, 11th, and 21st of each month.
- Coverage: Existing-home sales (`_a.csv`) and presale contracts (`_b.csv`) for all six major cities.

No data is scraped. All downloads use the official open API endpoints.

---

## API Reference

The local server exposes these endpoints after `uvicorn api:app` is running:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Serves `index.html` |
| `GET` | `/api/health` | Returns server status |
| `GET` | `/api/manifest` | Returns available cities and last-updated timestamps |
| `GET` | `/api/{city}/leaderboard` | Oracle rankings and heat signals for a city |
| `GET` | `/api/{city}/summary` | District summary stats (median price, QoQ, YoY, volume) |
| `GET` | `/api/{city}/districts/{district}/rows` | Full transaction rows for a district (existing + presale) |

City codes used in URLs: `Taipei`, `New_Taipei`, `Taoyuan`, `Taichung`, `Tainan`, `Kaohsiung`.

---

## Security & Privacy

- `.gitignore` excludes `.env`, private keys, raw CSV files, and Parquet snapshots.
- No hardcoded credentials or personal paths exist in any script.
- The GitHub Actions workflow uses only the built-in `GITHUB_TOKEN` — no secrets need to be configured manually.

---

## License

This project is licensed under the **Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA 4.0)** license.

**Commercial use is strictly prohibited without prior written authorization.**

For commercial licensing inquiries: **spoky119@gmail.com**
