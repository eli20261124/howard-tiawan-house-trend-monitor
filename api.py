#!/usr/bin/env python3

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data" / "processed"
DATA_ROOT = ROOT / "data"

app = FastAPI(title="Taipei Real Estate AI Terminal v3")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)
app.mount("/data", StaticFiles(directory=str(DATA_ROOT)), name="data")


def _safe_name(value: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fff]", "_", str(value)).strip("_") or "unknown"


def _jsonable(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _df_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {key: _jsonable(val) for key, val in row.items()}
        for row in df.to_dict(orient="records")
    ]


def _city_folder(city: str) -> str:
    manifest = load_manifest()
    cities = manifest.get("cities", {})
    if city in cities:
        return cities[city]["folder"]
    for payload in cities.values():
        if payload.get("folder") == city:
            return city
    raise HTTPException(status_code=404, detail=f"Unknown city: {city}")


def _city_dir(city: str) -> Path:
    return DATA_DIR / _city_folder(city)


def _summary_path(city: str) -> Path:
    return _city_dir(city) / "summary.csv"


def _timeseries_path(city: str) -> Path:
    return _city_dir(city) / "timeseries.json"


def _snapshot_meta_path(city: str) -> Path:
    return _city_dir(city) / "snapshot_meta.json"


def _district_snapshot_path(city: str, district: str) -> Path:
    return _city_dir(city) / f"{_safe_name(district)}_v3.parquet"


@lru_cache(maxsize=1)
def load_manifest() -> dict:
    path = DATA_DIR / "manifest.json"
    if not path.exists():
        return {"cities": {}}
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=16)
def load_timeseries(city: str) -> dict:
    path = _timeseries_path(city)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=16)
def load_snapshot_meta(city: str) -> dict:
    path = _snapshot_meta_path(city)
    if not path.exists():
        return {"districts": []}
    return json.loads(path.read_text(encoding="utf-8"))


def _v2_insights_path(city: str) -> Path:
    return _city_dir(city) / "v2_insights.json"


@lru_cache(maxsize=16)
def load_v2_insights(city: str) -> dict:
    path = _v2_insights_path(city)
    if not path.exists():
        return {"districts": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_summary(city: str) -> pd.DataFrame:
    path = _summary_path(city)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Missing summary.csv for {city}")
    return pd.read_csv(path)


def _load_snapshot(city: str, district: str) -> pd.DataFrame:
    path = _district_snapshot_path(city, district)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Missing snapshot for {city}/{district}")
    return pd.read_parquet(path)


def _leaderboard_metrics(city: str) -> pd.DataFrame:
    summary = _load_summary(city)
    if summary.empty:
        return summary

    if "Quarter" in summary.columns:
        summary = summary.sort_values(["District", "Quarter"], ascending=[True, True])
    latest = summary.groupby("District", as_index=False).tail(1).copy()

    ts = load_timeseries(city)
    district_series = ts.get("districts", {}) if isinstance(ts, dict) else {}
    snapshot_meta = {item.get("district"): item for item in load_snapshot_meta(city).get("districts", [])}
    v2_insights = load_v2_insights(city).get("districts", {})

    rows = []
    for _, row in latest.iterrows():
        district = str(row.get("District", ""))
        series = district_series.get(district, [])
        series_vals = [float(v) for v in series if v is not None and pd.notna(v)]
        peak = max(series_vals) if series_vals else None
        latest_val = series_vals[-1] if series_vals else None
        resilience_score = None
        drawdown_pct = None
        if peak and latest_val is not None and peak > 0:
            drawdown_pct = (latest_val / peak - 1.0) * 100.0
            resilience_score = 100.0 + drawdown_pct

        district_meta = snapshot_meta.get(district, {})
        short_term_ratio = None
        cluster_count = None
        if district_meta:
            try:
                snap = _load_snapshot(city, district)
                if not snap.empty and "DOM_Tag" in snap.columns:
                    short_term_ratio = float(
                        snap["DOM_Tag"].astype(str).str.contains("Short-term|短期|⚠️", na=False).mean() * 100.0
                    )
                if not snap.empty and {"Address", "Date"}.issubset(snap.columns):
                    dup = snap.groupby(["Address", "Date"]).size()
                    cluster_count = int((dup > 1).sum())
            except Exception:
                pass

        rows.append(
            {
                "District": district,
                "Quarter": row.get("Quarter"),
                "MedianPricePerPing": row.get("MedianPricePerPing"),
                "OracleNextMedian": row.get("OracleNextMedian"),
                "OracleTrend": row.get("OracleTrend"),
                "ResilienceScore": resilience_score,
                "DrawdownPct": drawdown_pct,
                "ConfidenceLeveragePct": row.get("PremiumGap_pct"),
                "MarketTemp": row.get("MarketTemp"),
                "ShortTermRatioPct": row.get("ShortTermRatioPct", short_term_ratio),
                "ClusterCount": cluster_count,
                "TransactionCount": row.get("TransactionCount"),
                # V3 fields from v2_insights
                "PriceToRentRatio": v2_insights.get(district, {}).get("price_to_rent"),
                "RentalAnchorLabel": v2_insights.get(district, {}).get("rental_anchor_label", "⚪ N/A"),
                "LeverageFlag": v2_insights.get(district, {}).get("leverage_flag", "⚪ N/A"),
                "RippleTag": v2_insights.get(district, {}).get("ripple_tag", ""),
            }
        )

    return pd.DataFrame(rows)


@app.get("/")
def root() -> FileResponse:
    return FileResponse(ROOT / "index.html")


@app.get("/api/health")
def health() -> dict:
    return {
        "ok": True,
        "cities": list(load_manifest().get("cities", {}).keys()),
        "processed_dir_exists": DATA_DIR.exists(),
    }


@app.get("/api/manifest")
def manifest() -> dict:
    return load_manifest()


@app.get("/api/cities")
def cities() -> list[dict[str, Any]]:
    manifest = load_manifest()
    rows = []
    for code, payload in manifest.get("cities", {}).items():
        rows.append({"code": code, **payload})
    return rows


@app.get("/api/{city}/leaderboard")
def leaderboard(
    city: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    sort_by: str = Query("ResilienceScore"),
    sort_dir: str = Query("desc"),
) -> dict:
    df = _leaderboard_metrics(city)
    if df.empty:
        return {"page": page, "page_size": page_size, "records_total": 0, "items": []}

    if sort_by in df.columns:
        ascending = sort_dir.lower() != "desc"
        df = df.sort_values(sort_by, ascending=ascending, na_position="last")

    total = len(df)
    start = (page - 1) * page_size
    end = start + page_size
    items = df.iloc[start:end].reset_index(drop=True)
    return {
        "page": page,
        "page_size": page_size,
        "records_total": total,
        "items": _df_records(items),
    }


@app.get("/api/{city}/summary")
def summary_rows(
    city: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    sort_by: str = Query("Quarter"),
    sort_dir: str = Query("desc"),
) -> dict:
    df = _load_summary(city)
    if df.empty:
        return {"page": page, "page_size": page_size, "records_total": 0, "items": []}

    if sort_by in df.columns:
        ascending = sort_dir.lower() != "desc"
        df = df.sort_values(sort_by, ascending=ascending, na_position="last")

    total = len(df)
    start = (page - 1) * page_size
    end = start + page_size
    items = df.iloc[start:end].reset_index(drop=True)
    return {
        "page": page,
        "page_size": page_size,
        "records_total": total,
        "items": _df_records(items),
    }


@app.get("/api/{city}/districts")
def districts(city: str) -> dict:
    meta = load_snapshot_meta(city)
    return {
        "city": _city_folder(city),
        "districts": meta.get("districts", []),
    }


@app.get("/api/{city}/districts/{district}/rows")
def district_rows(
    city: str,
    district: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    track: Optional[str] = None,
    sort_by: str = Query("Date"),
    sort_dir: str = Query("desc"),
) -> dict:
    df = _load_snapshot(city, district)

    if track:
        df = df[df["Track"].astype(str).str.lower() == track.lower()] if "Track" in df.columns else df

    if sort_by in df.columns:
        ascending = sort_dir.lower() != "desc"
        df = df.sort_values(sort_by, ascending=ascending, na_position="last")

    total = len(df)
    start = (page - 1) * page_size
    end = start + page_size
    items = df.iloc[start:end].reset_index(drop=True)

    # Infer date range from this snapshot
    date_min: Optional[str] = None
    date_max: Optional[str] = None
    if "Date" in df.columns:
        dates = df["Date"].dropna()
        if not dates.empty:
            date_min = str(dates.min())
            date_max = str(dates.max())

    return {
        "city": _city_folder(city),
        "district": district,
        "page": page,
        "page_size": page_size,
        "records_total": total,
        "date_min": date_min,
        "date_max": date_max,
        "items": _df_records(items),
    }


@app.get("/api/{city}/districts/{district}/projects")
def district_projects(
    city: str,
    district: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    sort_by: str = Query("Date"),
    sort_dir: str = Query("desc"),
) -> dict:
    """Return one representative row per presale project for the Pre-sale Explorer tab.

    Groups presale transactions by ProjectName, keeps the most recent row per project,
    and returns project-level fields suitable for the LVR-style project explorer UI.
    """
    PROJECT_FIELDS = [
        "ProjectName", "Road", "Developer", "ProjectScale", "ZoningTag",
        "TotalFloors", "FloorLevel", "AddressFloor", "Address",
        "PricePerPing", "TotalPrice_10kTWD", "MainBuildingRatioPct", "Date", "District",
    ]
    df = _load_snapshot(city, district)

    # Filter to presale track only
    if "Track" in df.columns:
        df = df[df["Track"].astype(str).str.lower() == "presale"].copy()

    if df.empty:
        return {
            "city": _city_folder(city), "district": district,
            "page": page, "page_size": page_size, "records_total": 0,
            "items": [],
        }

    # Sort so most recent transaction per project ends up as the representative row
    if sort_by in df.columns:
        ascending = sort_dir.lower() != "desc"
        df = df.sort_values(sort_by, ascending=ascending, na_position="last")

    # Deduplicate by ProjectName, keeping the most-recently-sorted row
    if "ProjectName" in df.columns:
        df = df.drop_duplicates(subset=["ProjectName"], keep="first")

    # Restrict to fields the explorer UI needs
    keep = [f for f in PROJECT_FIELDS if f in df.columns]
    df = df[keep].reset_index(drop=True)

    total = len(df)
    start = (page - 1) * page_size
    items = df.iloc[start: start + page_size].reset_index(drop=True)

    return {
        "city": _city_folder(city),
        "district": district,
        "page": page,
        "page_size": page_size,
        "records_total": total,
        "items": _df_records(items),
    }


@app.get("/api/{city}/health-tiles")
def health_tiles(city: str) -> dict:
    """
    Compact health summary per district for Zone 2 Health Tiles.
    Returns one row per district with light-traffic-light indicators.
    """
    df = _leaderboard_metrics(city)
    if df.empty:
        return {"city": _city_folder(city), "tiles": []}

    tiles = []
    for _, row in df.iterrows():
        leverage_pct = row.get("ConfidenceLeveragePct")
        dom_pct      = row.get("ShortTermRatioPct")
        cluster_n    = row.get("ClusterCount") or 0
        try:
            leverage_light = "red" if float(leverage_pct or 0) > 25 else (
                "yellow" if float(leverage_pct or 0) > 10 else "green"
            )
        except (TypeError, ValueError):
            leverage_light = "dim"
        try:
            dom_light = "red" if float(dom_pct or 0) > 30 else (
                "yellow" if float(dom_pct or 0) > 15 else "green"
            )
        except (TypeError, ValueError):
            dom_light = "dim"
        cluster_light = "red" if int(cluster_n) > 0 else "green"

        tiles.append({
            "district":       row.get("District"),
            "market_temp":    row.get("MarketTemp", "⚪ N/A"),
            "leverage_light": leverage_light,
            "dom_light":      dom_light,
            "cluster_light":  cluster_light,
            "ripple_tag":     row.get("RippleTag", ""),
            "rental_label":   row.get("RentalAnchorLabel", "⚪ N/A"),
            "resilience":     row.get("ResilienceScore"),
        })

    return {"city": _city_folder(city), "tiles": tiles}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=False)
