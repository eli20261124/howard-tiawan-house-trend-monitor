#!/usr/bin/env python3
"""
Taiwan Real Estate Dashboard — Data Pipeline
MOI 不動產交易實價登錄 Downloader & Processor

Usage:
  python main.py                   # live download from MOI
  python main.py /path/to/dir/     # use a local dir of pre-downloaded CSVs
"""

import io
import json
import logging
import math
import re
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# ── Configuration ──────────────────────────────────────────────────────────────

BASE_URL = "https://plvr.land.moi.gov.tw"
LANDING  = f"{BASE_URL}/DownloadOpenData"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Referer": LANDING,
    "Accept":  "*/*",
}

# City code → (folder name, Chinese name)
CITY_MAP = {
    "A": ("Taipei",     "台北市"),
    "F": ("New_Taipei", "新北市"),
    "H": ("Taoyuan",    "桃園市"),
    "B": ("Taichung",   "台中市"),
    "D": ("Tainan",     "台南市"),
    "E": ("Kaohsiung",  "高雄市"),
}

def city_csv_url(code: str) -> str:
    """Direct per-city CSV download URL (no national ZIP needed)."""
    # The MOI endpoint uses lowercase code in the filename
    return f"{BASE_URL}/Download?fileName={code.lower()}_lvr_land_a.csv"

def city_presale_url(code: str) -> str:
    """Per-city presale transaction CSV (_b.csv)."""
    return f"{BASE_URL}/Download?fileName={code.lower()}_lvr_land_b.csv"

OUTPUT_DIR      = Path("data")
CHUNK_SIZE      = 20_000   # rows per read_csv chunk
MIN_SAMPLE      = 5        # min transactions for reliable growth-rate display
TRIM_PERCENTILE = 0.05     # clip bottom/top 5 % of price samples per group

# Historical seasonal download — national ZIP (~80–120 MB each)
HIST_URL = f"{BASE_URL}/DownloadHistory?type=season&fileName="

# Seasons to backfill: Minguo 112 = 2023 CE, 113 = 2024, 114 = 2025
BACKFILL_SEASONS = [
    "112S1", "112S2", "112S3", "112S4",   # 2023
    "113S1", "113S2", "113S3", "113S4",   # 2024
    "114S1", "114S2", "114S3", "114S4",   # 2025
]

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Column mapping ─────────────────────────────────────────────────────────────

COL = {
    "鄉鎮市區":               "District",
    "交易標的":               "TransactionType",
    "土地區段位置或建物門牌":  "Address",
    "交易年月日":             "DateMinguo",
    "移轉層次":               "Floor",
    "總樓層數":               "TotalFloors",
    "建物型態":               "BuildingTypeRaw",
    "總價元":                 "TotalPriceTWD",
    "單價元平方公尺":          "UnitPricePerSqm",
    "備註":                   "Remarks",
    "電梯":                   "HasElevator",
    "主建物面積":              "MainBuildingArea",
}

BUILDING_NORM = {
    "公寓(5樓含以下非電梯)":      "Apartment",
    "華廈(10層含以下有電梯)":     "Elevator Building",
    "住宅大樓(11層含以上有電梯)": "Mansion",
    "套房(1房1廳1衛)":           "Studio",
    "透天厝":                    "Townhouse",
    "店面(店鋪)":                "Shop",
    "辦公商業大樓":               "Office",
    "廠辦":                      "Industrial Office",
    "倉庫":                      "Warehouse",
    "工廠":                      "Factory",
}

CHINESE_NUMS = {
    "一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9, "十": 10,
    "十一": 11, "十二": 12, "十三": 13, "十四": 14, "十五": 15,
    "十六": 16, "十七": 17, "十八": 18, "十九": 19, "二十": 20,
    "二十一": 21, "二十二": 22, "二十三": 23, "二十四": 24, "二十五": 25,
    "二十六": 26, "二十七": 27, "二十八": 28, "二十九": 29, "三十": 30,
    "三十一": 31, "三十二": 32, "三十三": 33, "三十四": 34, "三十五": 35,
}

ROOFTOP_KW = ("違建", "增建", "頂樓加蓋", "屋頂", "鐵皮")

DETAIL_COLS = [
    "District", "Road", "Floor", "FloorCategory", "BuildingType",
    "TotalPrice_10kTWD", "PricePerPing", "Quarter", "Status",
]

# ── Pure helper functions ──────────────────────────────────────────────────────

def _chinese_to_int(s: str) -> Optional[int]:
    """Convert a Chinese floor label (e.g. '三層') to an integer."""
    cleaned = s.strip().replace("層", "").replace("F", "").replace("f", "")
    if cleaned in CHINESE_NUMS:
        return CHINESE_NUMS[cleaned]
    if cleaned.isdigit():
        return int(cleaned)
    return None


def minguo_to_quarter(val) -> Optional[str]:
    """Convert Minguo date YYYMMDD → Western quarter label (e.g. 2024Q4)."""
    try:
        s = str(int(float(str(val)))).zfill(7)
        yyy = int(s[:3])
        mm  = int(s[3:5])
        return f"{yyy + 1911}Q{(mm - 1) // 3 + 1}"
    except (ValueError, TypeError):
        return None


def to_ping_price(val) -> Optional[float]:
    """Convert TWD/sqm → 10k TWD per Ping.  Formula: (val / 10000) * 3.3058"""
    try:
        return round(float(val) / 10_000 * 3.3058, 2)
    except (TypeError, ValueError):
        return None


def categorize_floor(val) -> str:
    """Map raw floor text to a clean tier label."""
    if pd.isna(val) or str(val).strip() in ("", "全", "見使用執照"):
        return "Unknown"
    s = str(val).strip()
    if "地下" in s:
        return "Basement"
    n = _chinese_to_int(s)
    if n is None:
        m = re.search(r"\d+", s)
        n = int(m.group()) if m else None
    if n is None:
        return s
    if n <= 5:
        return "Low (1-5F)"
    if n <= 10:
        return "Mid (6-10F)"
    if n <= 20:
        return "High (11-20F)"
    return "Super High (21F+)"


def extract_road(addr) -> str:
    """Best-effort extraction of the road name from a Taiwanese address."""
    if pd.isna(addr):
        return ""
    s = str(addr)
    # Match: Chinese characters + road/street keyword + optional section
    m = re.search(r"[\u4e00-\u9fff\w]+(路|街|道|大道)([\u4e00-\u9fff\w段]*)", s)
    if m:
        return m.group(0)
    # Fallback: everything before the first house number
    parts = re.split(r"\d+號", s)
    return parts[0].strip() if parts else s[:20]


def _tag_status(row) -> str:
    """Assign a status emoji tag if the transaction has notable features."""
    text = str(row.get("Address", "")) + str(row.get("Remarks", ""))
    if any(k in text for k in ROOFTOP_KW):
        return "🏗️ Rooftop Addition"
    return ""

# ── Download ───────────────────────────────────────────────────────────────────

def download_city_csv(code: str) -> bytes:
    """Download a single city’s real estate CSV directly from the MOI server."""
    url = city_csv_url(code)
    log.info("  GET %s", url)
    resp = requests.get(url, headers=HEADERS, timeout=120)
    resp.raise_for_status()
    ct = resp.headers.get("Content-Type", "")
    if "text/html" in ct:
        raise RuntimeError(
            f"Server returned HTML for city {code}. "
            "The MOI endpoint may have changed. "
            f"Check {LANDING}"
        )
    return resp.content
def download_presale_csv(code: str) -> bytes:
    """Download a single city's presale transaction CSV (_b.csv) from MOI."""
    url = city_presale_url(code)
    log.info("  GET %s", url)
    resp = requests.get(url, headers=HEADERS, timeout=120)
    resp.raise_for_status()
    ct = resp.headers.get("Content-Type", "")
    if "text/html" in ct:
        raise RuntimeError(f"Server returned HTML for presale city {code}.")
    return resp.content
# ── CSV reading ────────────────────────────────────────────────────────────────

def _early_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the minimum-necessary filters inside each read_csv chunk
    to keep peak memory usage low on the GitHub Actions runner.
    """
    rn = {
        k: v for k, v in {
            "交易標的": "TransactionType",
            "備註":     "Remarks",
        }.items()
        if k in df.columns
    }
    df = df.rename(columns=rn)

    # Keep only 'Building + Land' transactions
    if "TransactionType" in df.columns:
        df = df[df["TransactionType"].str.contains("房地", na=False)]

    # Discard transactions flagged with remarks (family trades, special relationships)
    if "Remarks" in df.columns:
        df = df[
            df["Remarks"].isna()
            | (df["Remarks"].astype(str).str.strip() == "")
        ]
    return df


def _early_filter_presale(df: pd.DataFrame) -> pd.DataFrame:
    """Early filter for presale _b.csv: drop cancelled/terminated contracts."""
    rn = {k: v for k, v in {"解約情形": "Termination", "備註": "Remarks"}.items()
          if k in df.columns}
    df = df.rename(columns=rn)
    if "Termination" in df.columns:
        df = df[
            df["Termination"].isna()
            | (df["Termination"].astype(str).str.strip() == "")
        ]
    return df


def read_csv_bytes(raw: bytes) -> pd.DataFrame:
    """
    Read a CSV from raw bytes using chunked I/O and encoding fallback.
    Tries utf-8-sig → utf-8 → big5 → cp950.
    """
    for enc in ("utf-8-sig", "utf-8", "big5", "cp950"):
        try:
            frames = []
            bio = io.BytesIO(raw)
            for chunk in pd.read_csv(
                bio,
                encoding=enc,
                chunksize=CHUNK_SIZE,
                low_memory=False,
                on_bad_lines="skip",
            ):
                filtered = _early_filter(chunk)
                if not filtered.empty:
                    frames.append(filtered)
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        except (UnicodeDecodeError, LookupError):
            frames = []
            continue
    raise ValueError("CSV unreadable with utf-8, big5, or cp950 encoding")


def read_presale_csv_bytes(raw: bytes) -> pd.DataFrame:
    """Read presale _b.csv bytes with encoding fallback; drops cancelled contracts."""
    for enc in ("utf-8-sig", "utf-8", "big5", "cp950"):
        try:
            frames = []
            bio = io.BytesIO(raw)
            for chunk in pd.read_csv(
                bio,
                encoding=enc,
                chunksize=CHUNK_SIZE,
                low_memory=False,
                on_bad_lines="skip",
            ):
                filtered = _early_filter_presale(chunk)
                if not filtered.empty:
                    frames.append(filtered)
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        except (UnicodeDecodeError, LookupError):
            frames = []
            continue
    raise ValueError("Presale CSV unreadable with utf-8, big5, or cp950 encoding")

# ── Cleaning & enrichment ──────────────────────────────────────────────────────

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all cleaning, normalization, and enrichment rules."""
    # Rename known columns
    df = df.rename(columns={k: v for k, v in COL.items() if k in df.columns})

    # Date → Western quarter label
    if "DateMinguo" in df.columns:
        df["Quarter"] = df["DateMinguo"].apply(minguo_to_quarter)

    # Unit price TWD/sqm → 10k TWD/Ping
    if "UnitPricePerSqm" in df.columns:
        df["PricePerPing"] = (
            pd.to_numeric(df["UnitPricePerSqm"], errors="coerce")
            .apply(to_ping_price)
        )

    # Total price TWD → 10k TWD
    if "TotalPriceTWD" in df.columns:
        df["TotalPrice_10kTWD"] = (
            pd.to_numeric(df["TotalPriceTWD"], errors="coerce") / 10_000
        ).round(1)

    # Building type normalization
    if "BuildingTypeRaw" in df.columns:
        df["BuildingType"] = (
            df["BuildingTypeRaw"]
            .map(BUILDING_NORM)
            .fillna(df["BuildingTypeRaw"])
        )

    # Floor tier
    if "Floor" in df.columns:
        df["FloorCategory"] = df["Floor"].apply(categorize_floor)

    # Road extraction
    if "Address" in df.columns:
        df["Road"] = df["Address"].apply(extract_road)

    # Special status tag
    df["Status"] = df.apply(_tag_status, axis=1)

    return df


def clean_presale(df: pd.DataFrame) -> pd.DataFrame:
    """Clean presale transactions: same pipeline as clean(), also maps ProjectName."""
    if "建案名稱" in df.columns:
        df = df.rename(columns={"建案名稱": "ProjectName"})
    return clean(df)

# ── Trend computation ──────────────────────────────────────────────────────────

def _trim_outliers(s: pd.Series) -> pd.Series:
    """Clip bottom/top TRIM_PERCENTILE fraction of a price series (per-group call)."""
    if len(s) < 4:
        return s
    lo = s.quantile(TRIM_PERCENTILE)
    hi = s.quantile(1.0 - TRIM_PERCENTILE)
    return s[(s >= lo) & (s <= hi)]


def compute_trend(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate cleaned transactions into a quarterly district-level trend table.
    Statistical rules:
      - Trimmed median (outliers clipped at TRIM_PERCENTILE) replaces raw mean
      - QoQ/YoY suppressed to NaN when TransactionCount < MIN_SAMPLE
    Computes:
      - MedianPricePerPing  (trimmed median price per Ping in 10k TWD)
      - TransactionCount    (post-trim sample size)
      - QoQ_pct             (quarter-on-quarter % — NaN if N < MIN_SAMPLE)
      - YoY_pct             (year-on-year %       — NaN if N < MIN_SAMPLE)
    """
    required = {"District", "Quarter", "PricePerPing"}
    if not required.issubset(df.columns):
        log.warning("Missing columns for trend: %s", required - set(df.columns))
        return pd.DataFrame()

    clean_df = df.dropna(subset=list(required))

    def _grp_median(x: pd.Series) -> float:
        t = _trim_outliers(x)
        return t.median() if len(t) > 0 else float("nan")

    def _grp_count(x: pd.Series) -> int:
        return len(_trim_outliers(x))

    agg = (
        clean_df
        .groupby(["District", "Quarter"], sort=True)
        .agg(
            MedianPricePerPing=("PricePerPing", _grp_median),
            TransactionCount  =("PricePerPing", _grp_count),
        )
        .reset_index()
    )
    agg["MedianPricePerPing"] = agg["MedianPricePerPing"].round(2)

    # QoQ: immediately preceding quarter for the same district
    agg["QoQ_pct"] = (
        agg.groupby("District")["MedianPricePerPing"]
        .pct_change(periods=1)
        .mul(100)
        .round(2)
    )

    # YoY: same quarter 4 periods back
    agg["YoY_pct"] = (
        agg.groupby("District")["MedianPricePerPing"]
        .pct_change(periods=4)
        .mul(100)
        .round(2)
    )

    # Suppress growth rates where sample size is below minimum threshold
    low_n = agg["TransactionCount"] < MIN_SAMPLE
    agg.loc[low_n, ["QoQ_pct", "YoY_pct"]] = float("nan")

    return agg


def export_timeseries(city_name: str, df: pd.DataFrame) -> None:
    """
    Write data/{City}/timeseries.json for Chart.js.
    Shape:
      {
        "quarters":    ["2023Q1", ...],          # sorted chronologically
        "city_median": [52.3, null, 54.1, ...],  # city-wide trimmed median
        "city_volume": [1200, null, 980,  ...],  # city-wide transaction count
        "districts":   {"中山區": [55.1, null, ...], ...}  # aligned to quarters
      }
    All value arrays are 1-to-1 with "quarters"; null = no data.
    """
    required = {"Quarter", "PricePerPing"}
    if not required.issubset(df.columns):
        return
    clean_df = df.dropna(subset=["Quarter", "PricePerPing"])
    if clean_df.empty:
        return

    def _nan_to_none(v) -> Optional[float]:
        return None if (v is None or (isinstance(v, float) and math.isnan(v))) else round(float(v), 2)

    def _grp_median(x: pd.Series) -> float:
        t = _trim_outliers(x)
        return t.median() if len(t) > 0 else float("nan")

    # ── City-level ────────────────────────────────────────────────────────────
    city_agg = (
        clean_df
        .groupby("Quarter", sort=True)
        .agg(
            city_median=("PricePerPing", _grp_median),
            city_volume=("PricePerPing", "count"),
        )
        .reset_index()
        .sort_values("Quarter")
    )
    all_quarters: list = sorted(city_agg["Quarter"].tolist())

    ts: dict = {
        "quarters":    all_quarters,
        "city_median": [_nan_to_none(v) for v in city_agg["city_median"].tolist()],
        "city_volume": city_agg["city_volume"].tolist(),
        "districts":   {},
    }

    # ── Per-district arrays (aligned to all_quarters) ─────────────────────────
    if "District" in clean_df.columns:
        for dist, grp in clean_df.groupby("District"):
            dist_agg = (
                grp.groupby("Quarter", sort=True)
                .agg(median=("PricePerPing", _grp_median))
                .reindex(all_quarters)   # fill missing quarters with NaN
            )
            ts["districts"][str(dist)] = [
                _nan_to_none(v) for v in dist_agg["median"].tolist()
            ]

    # ── Merge with any existing timeseries.json (preserves live-pipeline data) ─
    city_dir = OUTPUT_DIR / city_name
    existing_path = city_dir / "timeseries.json"
    if existing_path.exists():
        try:
            existing = json.loads(existing_path.read_text(encoding="utf-8"))
            ex_quarters = existing.get("quarters", [])
            # Build a unified quarter list
            all_merged = sorted(set(all_quarters) | set(ex_quarters))
            def _align(series, quarters, merged):
                q_map = {q: v for q, v in zip(quarters, series)}
                return [q_map.get(q) for q in merged]
            merged_ts = {
                "quarters":    all_merged,
                "city_median": _align(ts["city_median"], all_quarters, all_merged),
                "city_volume": _align(ts["city_volume"], all_quarters, all_merged),
                "districts":   {},
            }
            # Fill any newly-added quarter gaps from existing data
            ex_median_map = {q: v for q, v in zip(ex_quarters, existing.get("city_median", []))}
            ex_volume_map = {q: v for q, v in zip(ex_quarters, existing.get("city_volume", []))}
            for i, q in enumerate(all_merged):
                if merged_ts["city_median"][i] is None and ex_median_map.get(q) is not None:
                    merged_ts["city_median"][i] = ex_median_map[q]
                if merged_ts["city_volume"][i] is None and ex_volume_map.get(q) is not None:
                    merged_ts["city_volume"][i] = ex_volume_map[q]
            # Merge district series
            all_districts = set(ts["districts"]) | set(existing.get("districts", {}))
            for dist in all_districts:
                new_series = _align(ts["districts"].get(dist, [None] * len(all_quarters)),
                                    all_quarters, all_merged)
                ex_dist = existing.get("districts", {}).get(dist, [])
                ex_dist_map = {q: v for q, v in zip(ex_quarters, ex_dist)}
                for i, q in enumerate(all_merged):
                    if new_series[i] is None and ex_dist_map.get(q) is not None:
                        new_series[i] = ex_dist_map[q]
                merged_ts["districts"][dist] = new_series
            ts = merged_ts
            all_quarters = all_merged
        except Exception as exc:
            log.warning("  Could not merge existing timeseries.json: %s", exc)

    (city_dir / "timeseries.json").write_text(
        json.dumps(ts, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info(
        "  Wrote timeseries.json (%d quarters, %d districts)",
        len(all_quarters), len(ts["districts"]),
    )


def export_presale_timeseries(city_name: str, df: pd.DataFrame) -> None:
    """
    Write data/{City}/presale_timeseries.json.
    Same structure as timeseries.json; source field = "presale".
    """
    required = {"Quarter", "PricePerPing"}
    if not required.issubset(df.columns):
        return
    clean_df = df.dropna(subset=["Quarter", "PricePerPing"])
    if clean_df.empty:
        return

    def _nan_to_none(v) -> Optional[float]:
        return None if (v is None or (isinstance(v, float) and math.isnan(v))) else round(float(v), 2)

    def _grp_median(x: pd.Series) -> float:
        t = _trim_outliers(x)
        return t.median() if len(t) > 0 else float("nan")

    city_agg = (
        clean_df
        .groupby("Quarter", sort=True)
        .agg(
            city_median=("PricePerPing", _grp_median),
            city_volume=("PricePerPing", "count"),
        )
        .reset_index()
        .sort_values("Quarter")
    )
    all_quarters: list = sorted(city_agg["Quarter"].tolist())

    ts: dict = {
        "source":      "presale",
        "quarters":    all_quarters,
        "city_median": [_nan_to_none(v) for v in city_agg["city_median"].tolist()],
        "city_volume": city_agg["city_volume"].tolist(),
        "districts":   {},
    }

    if "District" in clean_df.columns:
        for dist, grp in clean_df.groupby("District"):
            dist_agg = (
                grp.groupby("Quarter", sort=True)
                .agg(median=("PricePerPing", _grp_median))
                .reindex(all_quarters)
            )
            ts["districts"][str(dist)] = [
                _nan_to_none(v) for v in dist_agg["median"].tolist()
            ]

    city_dir = OUTPUT_DIR / city_name
    (city_dir / "presale_timeseries.json").write_text(
        json.dumps(ts, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info(
        "  Wrote presale_timeseries.json (%d quarters, %d districts)",
        len(all_quarters), len(ts["districts"]),
    )


# ── Export ─────────────────────────────────────────────────────────────────────

def _safe_name(s: str) -> str:
    """Convert a district name to a filesystem-safe filename."""
    return re.sub(r"[^\w\u4e00-\u9fff]", "_", str(s)).strip("_") or "unknown"


def export_city(city_name: str, df: pd.DataFrame) -> list:
    """
    Write two output layers for one city:
      data/{City}/{District}.csv  — cleaned transaction detail rows
      data/{City}/summary.csv     — quarterly QoQ/YoY trend summary
    Returns the list of district names written.
    """
    city_dir = OUTPUT_DIR / city_name
    city_dir.mkdir(parents=True, exist_ok=True)

    # ── Detail layer ──────────────────────────────────────────────────────────
    detail_cols = [c for c in DETAIL_COLS if c in df.columns]
    districts: list[str] = []

    if "District" in df.columns:
        for dist, grp in df.groupby("District"):
            safe = _safe_name(str(dist))
            grp[detail_cols].to_csv(
                city_dir / f"{safe}.csv",
                index=False,
                encoding="utf-8-sig",
            )
            districts.append(safe)

    # ── Summary layer ─────────────────────────────────────────────────────────
    summary = compute_trend(df)
    if not summary.empty:
        summary.to_csv(
            city_dir / "summary.csv",
            index=False,
            encoding="utf-8-sig",
        )
        log.info(
            "  %s: %d districts, %d summary rows",
            city_name, len(districts), len(summary),
        )
    else:
        log.warning("  %s: no summary data produced", city_name)

    export_timeseries(city_name, df)
    return districts


def export_manifest(city_districts: dict) -> None:
    """Write data/manifest.json so the dashboard knows what files exist."""
    manifest = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "cities": {
            code: {
                "folder":    CITY_MAP[code][0],
                "name":      CITY_MAP[code][1],
                "districts": city_districts.get(CITY_MAP[code][0], []),
            }
            for code in CITY_MAP
        },
    }
    (OUTPUT_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info("Wrote data/manifest.json")

# ── Main pipeline ──────────────────────────────────────────────────────────────

def run(local_dir: Optional[Path] = None) -> None:
    """
    local_dir: if provided, reads pre-downloaded CSVs from that directory
               (filenames: a_lvr_land_a.csv, f_lvr_land_a.csv, …)
               instead of fetching from the MOI server.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    city_districts: dict = {}

    for code, (city_name, _) in CITY_MAP.items():
        log.info("Processing city %s (%s) …", code, city_name)

        if local_dir:
            csv_path = local_dir / f"{code.lower()}_lvr_land_a.csv"
            if not csv_path.exists():
                log.warning("  Local file not found: %s — skipping", csv_path)
                continue
            raw = csv_path.read_bytes()
        else:
            try:
                raw = download_city_csv(code)
            except Exception as exc:
                log.error("  Download failed for city %s: %s", code, exc)
                continue

        try:
            df = read_csv_bytes(raw)
        except ValueError as exc:
            log.error("  Encoding failed for city %s: %s", code, exc)
            continue

        log.info("  Loaded %d rows after early filter", len(df))
        df = clean(df)
        log.info("  Cleaned: %d rows", len(df))

        districts = export_city(city_name, df)
        city_districts[city_name] = districts

        # Presale transactions (_b.csv)
        try:
            presale_raw = download_presale_csv(code)
            presale_df = read_presale_csv_bytes(presale_raw)
            log.info("  Presale: %d rows after early filter", len(presale_df))
            presale_df = clean_presale(presale_df)
            export_presale_timeseries(city_name, presale_df)
        except Exception as exc:
            log.warning("  Presale skipped for %s: %s", code, exc)

    export_manifest(city_districts)
    log.info("Pipeline complete. Output: %s/", OUTPUT_DIR)


# ── Historical backfill ───────────────────────────────────────────────────────

def download_season_zip(season_code: str) -> bytes:
    """Download a full national seasonal ZIP from the MOI DownloadHistory endpoint.
    Each ZIP is ~80–120 MB and contains per-city CSVs for an entire quarter.
    """
    url = HIST_URL + season_code
    log.info("  GET %s  (~80–120 MB, may take several minutes) …", url)
    resp = requests.get(url, headers=HEADERS, timeout=600, stream=True)
    resp.raise_for_status()
    ct = resp.headers.get("Content-Type", "")
    if "text/html" in ct:
        raise RuntimeError(
            f"Server returned HTML for season {season_code}. "
            "Endpoint may have changed."
        )
    chunks = []
    for chunk in resp.iter_content(chunk_size=131_072):   # 128 KB chunks
        chunks.append(chunk)
    return b"".join(chunks)


def _extract_city_csv_from_zip(zip_bytes: bytes, city_code: str,
                               suffix: str = "a") -> Optional[bytes]:
    """
    Extract one city's CSV from a season ZIP blob held in memory.
    suffix: 'a' for actual price registration, 'b' for presale transactions.
    """
    target = f"{city_code.lower()}_lvr_land_{suffix}.csv"
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(target):
                    return zf.read(name)
    except zipfile.BadZipFile as exc:
        log.warning("  BadZipFile: %s", exc)
    return None


def run_backfill(seasons: Optional[list] = None) -> None:
    """
    Download historical seasonal ZIPs and rebuild summary + timeseries data
    for all six cities.  Per-district detail CSVs are NOT written (too large).

    Season notation: Minguo year + S + quarter number
      112S1 = Jan–Mar 2023,  113S1 = Jan–Mar 2024,  114S4 = Oct–Dec 2025

    Tip: run via GitHub Actions for fast download speeds.
    """
    seasons = seasons or BACKFILL_SEASONS
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    city_frames: dict = {CITY_MAP[c][0]: [] for c in CITY_MAP}
    city_presale_frames: dict = {CITY_MAP[c][0]: [] for c in CITY_MAP}

    for season in seasons:
        log.info("Backfilling season %s …", season)
        try:
            zip_bytes = download_season_zip(season)
        except Exception as exc:
            log.error("  Download failed for %s: %s", season, exc)
            continue

        for code, (city_name, _) in CITY_MAP.items():
            # Actual price (_a.csv)
            csv_bytes = _extract_city_csv_from_zip(zip_bytes, code, "a")
            if csv_bytes is None:
                log.warning("  %s _a not found in season %s", city_name, season)
            else:
                try:
                    df = read_csv_bytes(csv_bytes)
                    df = clean(df)
                    city_frames[city_name].append(df)
                    log.info("  %s %s actual: %d rows", city_name, season, len(df))
                except ValueError as exc:
                    log.error("  Encoding error %s %s _a: %s", city_name, season, exc)
            # Presale (_b.csv)
            pb_bytes = _extract_city_csv_from_zip(zip_bytes, code, "b")
            if pb_bytes is not None:
                try:
                    pf = read_presale_csv_bytes(pb_bytes)
                    pf = _early_filter_presale(pf)
                    pf = clean_presale(pf)
                    city_presale_frames[city_name].append(pf)
                    log.info("  %s %s presale: %d rows", city_name, season, len(pf))
                except Exception as exc:
                    log.warning("  Presale parse error %s %s: %s", city_name, season, exc)

    city_districts: dict = {}
    for city_name, frames in city_frames.items():
        if not frames:
            log.warning("No backfill data for %s — skipping", city_name)
            continue
        combined = pd.concat(frames, ignore_index=True).drop_duplicates()
        city_dir = OUTPUT_DIR / city_name
        city_dir.mkdir(parents=True, exist_ok=True)

        summary = compute_trend(combined)
        if not summary.empty:
            summary.to_csv(city_dir / "summary.csv", index=False, encoding="utf-8-sig")
            log.info("  %s: %d summary rows written", city_name, len(summary))
        export_timeseries(city_name, combined)

        # Presale
        pframes = city_presale_frames.get(city_name, [])
        if pframes:
            pcombined = pd.concat(pframes, ignore_index=True).drop_duplicates()
            export_presale_timeseries(city_name, pcombined)

        city_districts[city_name] = []

    export_manifest(city_districts)
    log.info("Backfill complete — %d seasons processed.", len(seasons))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Taiwan Real Estate Pipeline — MOI 不動產實價登錄"
    )
    parser.add_argument(
        "local_dir", nargs="?", type=Path,
        help="Path to local pre-downloaded CSV directory (skips live download)",
    )
    parser.add_argument(
        "--backfill", action="store_true",
        help="Download historical seasonal ZIPs for 2023–2025 (Minguo 112–114)",
    )
    parser.add_argument(
        "--seasons", nargs="+", metavar="SEASON",
        help="Specific seasons to backfill, e.g. --seasons 112S1 113S2 114S4",
    )
    args = parser.parse_args()

    if args.backfill or args.seasons:
        run_backfill(seasons=args.seasons)
    else:
        run(local_dir=args.local_dir)
