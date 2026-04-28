# AI-Driven Real Estate Insight Dashboard

For commercial licensing inquiries, please contact: spoky119@gmail.com

An AI-driven real estate intelligence dashboard for Taiwan’s six major cities, built to transform MOI open data into a fast, browsable decision layer for market monitoring, pricing context, and presale enrichment.

## Project Overview

This repository combines a Python data pipeline, a lightweight analysis engine, and a browser-based terminal UI. It ingests MOI open data, cleans and normalizes it, builds city and district snapshots, calculates indicator layers, and serves the result through a local API for interactive exploration.

The product is organized around three visible zones:

1. Market ranking and leaderboards for city-level comparison.
2. District health tiles for quick anomaly spotting.
3. A transaction explorer for detailed existing-home and presale review.

## Tech Stack

- Data pipeline: Python, pandas, requests, pyarrow, Parquet, JSON, CSV.
- Analysis engine: trend extraction, forecast heuristics, DOM filtering, clustering, and presale enrichment.
- Backend/API: FastAPI and Uvicorn.
- Frontend: JavaScript, HTML, CSS, and Tailwind-style utility structure for a compact operator dashboard.

## Deep Observation Indicators

### Market Heat Signals

Market Heat Signals summarize how active and stressed a district looks relative to its own recent history. The dashboard uses transaction cadence, pricing movement, short-term turnover, and resilience-style comparisons to surface districts that are accelerating, stabilizing, or cooling. The intent is not a single-point forecast, but an operator-friendly read on whether a district is trading hot, normal, or under pressure.

### Oracle Price Prediction

Oracle Price Prediction is the forward-looking layer. It estimates the next median price trend from recent summary structure, seasonal movement, and smoothed district behavior rather than from raw point noise. The goal is to produce a directional signal that is useful for screening, not a substitute for a formal valuation model.

### Data Fusion

Data Fusion is the presale enrichment layer that joins MOI presale transactions with project-level metadata on `建案名稱`.

- Existing-home rows remain the baseline market record.
- Presale rows are enriched with project info such as developer, scale, and zoning.
- When both sources are available, the dashboard keeps the transaction record and adds project context instead of replacing the market data.

This makes the presale explorer easier to read because each project can be viewed with its developer identity, building scale, and zoning tag in the same table.

## What the Dashboard Shows

- Smart Address handling that strips district prefixes and compresses empty values cleanly.
- Zoning tags derived from MOI presale zoning fields and project info fallbacks.
- Developer and scale enrichment for presale projects.
- Separate existing-home and presale views for each district.
- City-level leaderboards, district health tiles, and detailed transaction tables.

## Repository Layout

- `main.py` - data ingestion, cleansing, enrichment, and export pipeline.
- `api.py` - FastAPI service that serves the processed outputs.
- `index.html` - single-page dashboard UI.
- `data/processed/` - generated city and district outputs.
- `data/project_info/` - project-level join tables used for presale fusion.
- `.github/workflows/update_data.yml` - scheduled refresh automation.

## Run Locally

1. Create and activate a Python environment.
2. Install dependencies with `pip install -r requirements.txt`.
3. Run the pipeline with `python main.py`.
4. Start the API with `uvicorn api:app --host 127.0.0.1 --port 8000`.
5. Open `http://127.0.0.1:8000` in a browser.

## Security & Privacy

- Local secrets, private keys, `.env` files, and raw generated data files are excluded by `.gitignore`.
- No personal tokens or hardcoded credentials should be committed to the repository.
- The repository is intended for local analysis and non-commercial use only.

## Commercial Use Notice

Commercial use is strictly prohibited without prior authorization.

For commercial licensing inquiries, please contact: spoky119@gmail.com