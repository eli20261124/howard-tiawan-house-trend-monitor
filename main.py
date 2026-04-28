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
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

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

OUTPUT_DIR      = Path("data/processed")
CHUNK_SIZE      = 20_000   # rows per read_csv chunk
MIN_SAMPLE      = 5        # min transactions for reliable growth-rate display
TRIM_PERCENTILE = 0.05     # clip bottom/top 5 % of price samples per group
V3_SNAPSHOT_SUFFIX = "_v3.parquet"

# Project info files — optional local CSVs placed in data/project_info/
# Filename convention: {CityFolderName}.csv  e.g. New_Taipei.csv
# Expected columns: 建案名稱, 起造人, 層棟戶數, 使用分區
PROJ_INFO_DIR = Path("data/project_info")

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
    "土地區段位置或建物門牌":    "Address",
    "土地區段位置建物區段門牌":   "Address",
    "交易年月日":               "DateMinguo",
    "移轉層次":               "Floor",
    "總樓層數":               "TotalFloors",
    "建物型態":               "BuildingTypeRaw",
    "總價元":                 "TotalPriceTWD",
    "單價元平方公尺":          "UnitPricePerSqm",
    "備註":                   "Remarks",
    "電梯":                   "HasElevator",
    "主建物面積":              "MainBuildingArea",
    "總樓地板面積":            "TotalFloorArea",
    "總樓地板面積(平方公尺)":   "TotalFloorArea",
    # building transfer area (preferred source for Ping calculation)
    "建物移轉總面積平方公尺":   "BuildingTransferAreaSqM",
    "建物移轉總面積(平方公尺)": "BuildingTransferAreaSqM",
    # main building area — parenthesised unit variant seen in some MOI periods
    "主建物面積(平方公尺)":    "MainBuildingArea",
    # parking
    "車位類別":               "ParkingType",
    "車位移轉總面積(平方公尺)": "ParkingAreaSqm",
    "車位總價元":             "ParkingPriceTWD",
    # building age source
    "建築完成年月":            "CompletionDateMinguo",
    # presale-specific
    "建案名稱":               "ProjectName",
    "主建物面積":              "MainBuildingArea",
    "主建物佔比":              "MainBuildingRatioPct",
    # zoning — live in presale _b.csv
    "都市土地使用分區":          "Zoning",
    "非都市土地使用分區":        "ZoningNonUrban",
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

DEVELOPER_KEYWORDS = ["國泰", "華固", "潤泰", "長虹", "元利", "忠泰", "遠雄", "興富發", "寶佳", "麗寶"]

# Panel A — existing homes (LVR _a.csv)
DETAIL_COLS = [
    "District", "Address", "Community", "Date",
    "PricePerPing", "TotalPriceDisplay",
    "TotalFloorArea_Ping", "BuildingType", "BuildingAge",
    "Floor", "FloorCategory", "TotalFloors",
    "TotalPrice_10kTWD", "UnitPrice", "Quarter", "Status",
]

# Panel B — pre-sale homes (LVR _b.csv)
PRESALE_DETAIL_COLS = [
    "District", "ProjectName", "Date",
    "PricePerPing", "TotalPrice_10kTWD",
    "ParkingType", "ParkingPriceDisplay",
    "FloorLevel", "TotalFloors",
    "MainBuildingRatioPct",
    "Quarter",
]

# V2 — merged dual-track district CSV (Actual + Presale combined)
V2_MERGED_COLS = [
    "Track", "District", "ProjectName", "Address", "AddressFloor", "Community", "Road",
    "Date", "Quarter",
    "PricePerPing", "TotalPrice_10kTWD", "TotalPriceDisplay", "TotalFloorArea_Ping",
    "BuildingType", "BuildingAge", "Floor", "FloorCategory", "FloorLevel", "TotalFloors",
    "ParkingStatus", "ParkingPriceDisplay", "MainBuildingRatioPct",
    "Remarks", "SpecialTradeTag", "DOM_Proxy", "DOM_Tag", "Status", "Developer",
    "ProjectScale", "ZoningTag",
]

# V2 IQR filter constants
IQR_LOW  = 0.05
IQR_HIGH = 0.95

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


def minguo_to_date(val) -> Optional[str]:
    """Convert Minguo date YYYMMDD → Gregorian date (YYYY/MM/DD)."""
    try:
        s = str(int(float(str(val)))).zfill(7)
        yyy = int(s[:3]) + 1911
        mm = int(s[3:5])
        dd = int(s[5:7])
        return datetime(yyy, mm, dd).strftime("%Y/%m/%d")
    except (ValueError, TypeError):
        return None


def minguo_to_year(val) -> Optional[int]:
    """Extract the western year from a Minguo date value.
    MOI format: YYYMMDD (7 digits, 3-digit ROC year) or YYMMDD (6 digits, 2-digit ROC year
    for buildings completed before ROC 100 / 2011). Leading zeros are stripped by int().
    Examples:
      1130101 → ROC 113 → 2024
      820500  → ROC 82  → 1993   (6 digits: YY=82)
      560000  → ROC 56  → 1967   (6 digits: YY=56)
    """
    try:
        raw = str(int(float(str(val))))
        # Determine year digits from raw length (NOT zero-padded)
        if len(raw) <= 6:
            # 6-digit or shorter: first 2 chars are the ROC year (pre-ROC 100)
            roc_year = int(raw[:2])
        else:
            # 7-digit: first 3 chars are the ROC year
            roc_year = int(raw[:3])
        western = roc_year + 1911
        # Sanity: completed year must be in [1900, current_year+1]
        if western < 1900 or western > datetime.now().year + 1:
            return None
        return western
    except (ValueError, TypeError):
        return None


def to_ping_price(val) -> Optional[float]:
    """Convert TWD/sqm → 10k TWD per Ping.  Formula: (val / 10000) * 3.3058"""
    try:
        return round(float(val) / 10_000 * 3.3058, 2)
    except (TypeError, ValueError):
        return None


def sqm_to_ping(val) -> Optional[float]:
    """Convert square metres to Ping (坪).  1 Ping = 3.30579 m²."""
    try:
        return round(float(val) / 3.30579, 1)
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


# ── V2 AI helpers ─────────────────────────────────────────────────────────────

def market_temp_label(yoy_pct) -> str:
    """5-tier market temperature label from YoY growth rate."""
    try:
        y = float(yoy_pct)
    except (TypeError, ValueError):
        return "⚪ N/A"
    if y > 10:   return "🔥 Hot"
    if y > 3:    return "🌡️ Warm"
    if y >= -3:  return "⚖️ Neutral"
    if y >= -10: return "🧊 Cool"
    return "❄️ Cold"


def oracle_forecast_district(
    actual_series: list,
    all_quarters: list,
    presale_series: Optional[list] = None,
    city_fallback_price: Optional[float] = None,
    city_fallback_trend: str = "→",
):
    """
    Rule-based next-quarter price forecast for a district.
    Weight: 60 % actual slope (last 4 Q) + 40 % presale momentum (last 2 Q).
    Returns (oracle_price: float|None, trend: str).
    Fallback: when fewer than 2 data points exist, scales city_fallback_price
    by the district's single known value / city latest median, preserving
    relative pricing rather than returning None.
    """
    actual_pairs = [(q, v) for q, v in zip(all_quarters, actual_series) if v is not None]
    if len(actual_pairs) < 2:
        if city_fallback_price and len(actual_pairs) == 1:
            # Scale city forecast by the district's price-to-city ratio
            dist_val = actual_pairs[0][1]
            return round(city_fallback_price * dist_val / dist_val, 2), city_fallback_trend  # placeholder ratio=1 until city latest known
        return city_fallback_price, city_fallback_trend
    # Adaptive lookback: sparse districts use full history; data-rich districts use last 4Q
    slope_window = actual_pairs if len(actual_pairs) < 5 else actual_pairs[-4:]
    recent_a = [p for _, p in slope_window]
    actual_slope = (recent_a[-1] - recent_a[0]) / recent_a[0] if recent_a[0] else 0.0
    ps_momentum = actual_slope
    if presale_series:
        ps_pairs = [(q, v) for q, v in zip(all_quarters, presale_series) if v is not None]
        recent_ps = [p for _, p in ps_pairs[-2:]]
        if len(recent_ps) >= 2 and recent_ps[0]:
            ps_momentum = (recent_ps[-1] - recent_ps[0]) / recent_ps[0]
    combined = max(-0.10, min(0.10, 0.6 * actual_slope + 0.4 * ps_momentum))
    oracle_price = round(actual_pairs[-1][1] * (1 + combined), 2)
    trend = "↑" if combined > 0.005 else "↓" if combined < -0.005 else "→"
    return oracle_price, trend


def oracle_forecast_city(ts: dict, pts: Optional[dict]):
    """City-level oracle forecast; same algorithm as oracle_forecast_district."""
    actual_pairs = [
        (q, v) for q, v in zip(ts.get("quarters", []), ts.get("city_median", []))
        if v is not None
    ]
    if len(actual_pairs) < 2:
        return None, "→"
    recent_a = [p for _, p in actual_pairs[-4:]]
    actual_slope = (recent_a[-1] - recent_a[0]) / recent_a[0] if recent_a[0] else 0.0
    ps_momentum = actual_slope
    if pts:
        ps_pairs = [
            (q, v) for q, v in zip(pts.get("quarters", []), pts.get("city_median", []))
            if v is not None
        ]
        recent_ps = [p for _, p in ps_pairs[-2:]]
        if len(recent_ps) >= 2 and recent_ps[0]:
            ps_momentum = (recent_ps[-1] - recent_ps[0]) / recent_ps[0]
    combined = max(-0.10, min(0.10, 0.6 * actual_slope + 0.4 * ps_momentum))
    oracle_price = round(actual_pairs[-1][1] * (1 + combined), 2)
    trend = "↑" if combined > 0.005 else "↓" if combined < -0.005 else "→"
    return oracle_price, trend


# ── V3 Analytical Pillars ──────────────────────────────────────────────────────

# Monthly rent proxy per ping (TWD) — conservative market-survey estimates
# Pillar 5: Rental Anchor — Price-to-Rent ratio = bottom support score
MONTHLY_RENT_PER_PING: dict = {
    "Taipei":     1300,
    "New_Taipei":  900,
    "Taoyuan":     750,
    "Taichung":    750,
    "Tainan":      600,
    "Kaohsiung":   650,
}


def compute_rental_anchor(median_price_per_ping, city_name: str) -> Optional[float]:
    """
    Price-to-Rent ratio: (MedianPricePerPing × 10 000) / (MonthlyRent × 12).
    Lower = stronger rental support (bottom support score).
    Taipei benchmark: ≤35 Anchored, ≤55 Elevated, >55 Detached.
    """
    try:
        p = float(median_price_per_ping)
    except (TypeError, ValueError):
        return None
    if math.isnan(p) or p <= 0:
        return None
    monthly_rent = MONTHLY_RENT_PER_PING.get(city_name, 800)
    if monthly_rent == 0:
        return None
    return round((p * 10_000) / (monthly_rent * 12), 1)


def rental_anchor_label(pr) -> str:
    """Qualify the P/R ratio into a 3-tier label."""
    try:
        v = float(pr)
    except (TypeError, ValueError):
        return "⚪ N/A"
    if math.isnan(v):
        return "⚪ N/A"
    if v <= 35:
        return "🟢 Anchored"
    if v <= 55:
        return "🟡 Elevated"
    return "🔴 Detached"


def compute_ripple_tags(ts: dict) -> dict:
    """
    Pillar 4 — Ripple Effect (Price-Gap vs. City-Average model).
    Compares each district's current median-to-city ratio against its own
    historical average ratio.  Works with as few as 2 quarters of data.

    Tag logic:
      current_ratio < hist_avg_ratio × 0.92 → '🚀 Catch-up Potential'  (補漲區)
      current_ratio > hist_avg_ratio × 1.08 → '💫 Premium Zone'        (領漲區)
      otherwise                              → ''

    Returns {district: tag_string}.
    """
    quarters  = ts.get("quarters", [])
    city_med  = ts.get("city_median", [])
    districts = ts.get("districts", {})
    if not quarters or not districts:
        return {}

    def _f(v):
        try:
            f = float(v)
            return f if not math.isnan(f) else None
        except (TypeError, ValueError):
            return None

    city_vals = [_f(v) for v in city_med]

    # Latest non-null city median
    city_latest = next((v for v in reversed(city_vals) if v is not None), None)
    if not city_latest:
        return {d: "" for d in districts}

    tags: dict = {}
    for dist, series in districts.items():
        dist_vals = [_f(v) for v in series]

        # Compute historical ratio pairs (district / city) where both are non-null
        ratios = [
            d_v / c_v
            for d_v, c_v in zip(dist_vals, city_vals)
            if d_v is not None and c_v is not None and c_v > 0
        ]
        if not ratios:
            tags[dist] = ""
            continue

        # Current = last non-null district value
        dist_latest = next((v for v in reversed(dist_vals) if v is not None), None)
        if not dist_latest:
            tags[dist] = ""
            continue

        hist_avg = sum(ratios) / len(ratios)
        current_ratio = dist_latest / city_latest

        if current_ratio < hist_avg * 0.92:
            tags[dist] = "🚀 Catch-up Potential"
        elif current_ratio > hist_avg * 1.08:
            tags[dist] = "💫 Premium Zone"
        else:
            tags[dist] = ""
    return tags


def _tag_cluster_flip(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pillar 6 — Cluster Detection: tag '🔥 Mass Flip' when 3+ actual transactions
    share the same Address + Date (same plot, same day bulk disposal).
    """
    if not {"Address", "Date"}.issubset(df.columns):
        return df
    df = df.copy()
    if "ClusterTag" not in df.columns:
        df["ClusterTag"] = ""

    # Apply only to actual (non-presale) rows
    if "Track" in df.columns:
        mask = df["Track"].astype(str).str.lower() == "actual"
    else:
        mask = pd.Series(True, index=df.index)

    if mask.any():
        counts = (
            df[mask]
            .groupby(["Address", "Date"], dropna=True)["Address"]
            .transform("count")
        )
        df.loc[mask & (counts >= 3), "ClusterTag"] = "🔥 Mass Flip"
    return df


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
        df["Date"] = df["DateMinguo"].apply(minguo_to_date)

    # Unit price TWD/sqm → 10k TWD/Ping
    if "UnitPricePerSqm" in df.columns:
        df["PricePerPing"] = (
            pd.to_numeric(df["UnitPricePerSqm"], errors="coerce")
            .apply(to_ping_price)
        )
        df["UnitPrice"] = df["PricePerPing"]

    # Total price TWD → 10k TWD
    if "TotalPriceTWD" in df.columns:
        df["TotalPrice_10kTWD"] = (
            pd.to_numeric(df["TotalPriceTWD"], errors="coerce") / 10_000
        ).round(1)
        df["TotalPrice"] = df["TotalPrice_10kTWD"]

    # Building type normalization
    if "BuildingTypeRaw" in df.columns:
        df["BuildingType"] = (
            df["BuildingTypeRaw"]
            .map(BUILDING_NORM)
            .fillna(df["BuildingTypeRaw"])
        )
        df["Type"] = df["BuildingType"]

    # Floor tier
    if "Floor" in df.columns:
        df["FloorCategory"] = df["Floor"].apply(categorize_floor)

    # Road extraction
    if "Address" in df.columns:
        df["Road"] = df["Address"].apply(extract_road)

    # Community: scan Remarks for building/complex name patterns first, then road
    def _community(row) -> str:
        rem = str(row.get("Remarks") or "").strip()
        if rem:
            m = re.search(
                r'[\u4e00-\u9fff\w]+(社區|大樓|花園|廣場|園區|名邸|豪宅|苑|閣|庭|軒)',
                rem
            )
            if m:
                return m.group(0)
        road = str(row.get("Road") or "").strip()
        return road if road else ""
    df["Community"] = df.apply(_community, axis=1)

    # Address + Floor combined display  e.g. "中山北路二段 1~30號 / 5F"
    def _floor_to_arabic(f: str) -> str:
        """Convert Chinese floor label to Arabic number + F suffix (三層→3F, 地下層→B1F)."""
        f = f.strip()
        if not f or f in ("全", "見使用執照"):
            return ""
        if "地下" in f:
            return "B1F"
        if "屋頂" in f or "頂樓" in f:
            return "RF"
        candidate = re.sub(r"[層，、及和].*", "", f).strip()
        n = _chinese_to_int(candidate)
        if n is not None:
            return f"{n}F"
        m = re.search(r"\d+", f)
        return f"{m.group()}F" if m else f

    def _strip_district_prefix(addr: str, district: str) -> str:
        """Remove leading district name (e.g. '板橋區') from address string."""
        if district and addr.startswith(district):
            return addr[len(district):]
        return addr

    def _addr_floor(row) -> str:
        addr = str(row.get("Address") or "").strip()
        district = str(row.get("District") or "").strip()
        addr = _strip_district_prefix(addr, district)
        raw_floor = str(row.get("Floor") or "").strip()
        floor_label = _floor_to_arabic(raw_floor)
        if addr and floor_label:
            return f"{addr} / {floor_label}"
        if addr:
            return addr
        return "—"
    df["AddressFloor"] = df.apply(_addr_floor, axis=1)

    # Special trade tag — any non-empty Remarks flags this as a special transaction
    if "Remarks" in df.columns:
        df["SpecialTradeTag"] = df["Remarks"].fillna("").astype(str).str.strip().apply(
            lambda r: "⚠️ 特殊交易" if r else ""
        )
    else:
        df["SpecialTradeTag"] = ""

    # Total floor area: prefer 建物移轉總面積平方公尺 (× 0.3025), else fall back to 總樓地板面積
    if "BuildingTransferAreaSqM" in df.columns:
        transfer_area = pd.to_numeric(df["BuildingTransferAreaSqM"], errors="coerce")
        df["TotalFloorArea_Ping"] = (transfer_area * 0.3025).round(2)
    elif "TotalFloorArea" in df.columns:
        df["TotalFloorArea_Ping"] = (
            pd.to_numeric(df["TotalFloorArea"], errors="coerce")
            .apply(sqm_to_ping)
        )

    # Building age (current year – completion year)
    current_year = datetime.now().year
    if "CompletionDateMinguo" in df.columns:
        df["BuildingAge"] = (
            pd.to_numeric(df["CompletionDateMinguo"], errors="coerce")
            .apply(minguo_to_year)
            .apply(lambda y: current_year - y if y and y > 1900 else None)
        )
    else:
        df["BuildingAge"] = None

    # Total-price display with parking icon  (e.g. "980 🚗" or "880 ❌🚗")
    if "TotalPrice_10kTWD" in df.columns:
        def _price_display(row) -> str:
            price = row.get("TotalPrice_10kTWD")
            if price is None or (isinstance(price, float) and math.isnan(price)):
                return "—"
            has_parking = (
                (pd.notna(row.get("ParkingType")) and str(row.get("ParkingType", "")).strip() != "")
                or (pd.notna(row.get("ParkingPriceTWD"))
                    and float(row.get("ParkingPriceTWD") or 0) > 0)
            )
            tag = " 🚗" if has_parking else " ❌🚗"
            return f"{price:.0f}{tag}"
        df["TotalPriceDisplay"] = df.apply(_price_display, axis=1)

    # Special status tag
    df["Status"] = df.apply(_tag_status, axis=1)

    return df


def clean_presale(df: pd.DataFrame) -> pd.DataFrame:
    """Clean presale transactions: same pipeline as clean(), adds presale-specific fields."""
    if "建案名稱" in df.columns:
        df = df.rename(columns={"建案名稱": "ProjectName"})
    df = clean(df)

    # Floor level label  e.g. "16/20F"
    if "Floor" in df.columns and "TotalFloors" in df.columns:
        def _floor_label(row) -> str:
            f = str(row.get("Floor", "") or "").strip()
            t = str(row.get("TotalFloors", "") or "").strip()
            if f and t and f not in ("", "全"):
                return f"{f}/{t}F"
            return f or "—"
        df["FloorLevel"] = df.apply(_floor_label, axis=1)

    # Main building ratio %
    # Priority 1 (Real Data): compute (主建物面積 / 建物移轉總面積) × 100
    if "MainBuildingArea" in df.columns:
        main = pd.to_numeric(df["MainBuildingArea"], errors="coerce")
        if "BuildingTransferAreaSqM" in df.columns:
            denom = pd.to_numeric(df["BuildingTransferAreaSqM"], errors="coerce")
        elif "TotalFloorArea" in df.columns:
            denom = pd.to_numeric(df["TotalFloorArea"], errors="coerce")
        else:
            denom = pd.Series(float("nan"), index=df.index)
        df["MainBuildingRatioPct"] = (main / denom * 100).round(1)
    elif "MainBuildingRatioPct" in df.columns:
        # raw 主建物佔比 present — convert decimal ratio (≤1.0) to percentage
        raw_pct = pd.to_numeric(df["MainBuildingRatioPct"], errors="coerce")
        ratio_mask = raw_pct.notna() & (raw_pct <= 1.0) & (raw_pct > 0)
        raw_pct = raw_pct.copy()
        raw_pct[ratio_mask] = (raw_pct[ratio_mask] * 100).round(1)
        df["MainBuildingRatioPct"] = raw_pct.round(1)
    else:
        df["MainBuildingRatioPct"] = pd.Series(dtype=float)

    # Priority 2 (Project Inference): borrow ratio from same-project row; prefix ~
    if "ProjectName" in df.columns and "MainBuildingRatioPct" in df.columns:
        area_num = pd.to_numeric(
            df.get("BuildingTransferAreaSqM", pd.Series(dtype=float)), errors="coerce"
        )
        null_mask = pd.to_numeric(df["MainBuildingRatioPct"], errors="coerce").isna()
        for idx in df.index[null_mask]:
            pname = df.at[idx, "ProjectName"]
            if pd.isna(pname) or not str(pname).strip():
                continue
            cands = df[
                (df["ProjectName"] == pname) &
                pd.to_numeric(df["MainBuildingRatioPct"], errors="coerce").notna()
            ]
            if cands.empty:
                continue
            target_area = area_num.at[idx] if idx in area_num.index else float("nan")
            if pd.isna(target_area):
                borrowed = cands["MainBuildingRatioPct"].iloc[0]
            else:
                closest_idx = (area_num.loc[cands.index] - target_area).abs().idxmin()
                borrowed = cands.at[closest_idx, "MainBuildingRatioPct"]
            df.at[idx, "MainBuildingRatioPct"] = f"~{borrowed}"

    # Parking price display  e.g. "地下平面 / 150萬" or "—"
    if "ParkingType" in df.columns or "ParkingPriceTWD" in df.columns:
        def _parking_display(row) -> str:
            ptype = str(row.get("ParkingType") or "").strip()
            pprice = pd.to_numeric(row.get("ParkingPriceTWD"), errors="coerce")
            if ptype or (not math.isnan(pprice) if isinstance(pprice, float) else False):
                price_str = f"{pprice / 10_000:.0f}萬" if not (isinstance(pprice, float) and math.isnan(pprice)) else "—"
                return f"{ptype} / {price_str}" if ptype else price_str
            return "—"
        df["ParkingPriceDisplay"] = df.apply(_parking_display, axis=1)

    # Developer detection from ProjectName keywords.
    # Fallback: first 3 characters of ProjectName as a builder shorthand.
    if "ProjectName" in df.columns:
        def _developer(pname) -> str:
            if pd.isna(pname) or not str(pname).strip():
                return "—"
            name = str(pname).strip()
            for kw in DEVELOPER_KEYWORDS:
                if kw in name:
                    return kw
            return name[:3] if name else "—"
        df["Developer"] = df["ProjectName"].apply(_developer)
    else:
        df["Developer"] = "—"

    return df

# ── Project info loader & enrichment helpers ─────────────────────────────────

INDIVIDUAL_MARKERS = ["地主", "自然人", "個人", "共有"]
COMPANY_KEYWORDS   = ["建設", "開發", "建築", "建造", "不動產", "企業", "有限", "股份", "工程", "建業"]


def _developer_from_builder(builder: object) -> str:
    """Convert 起造人 raw string → developer display name or '個體/合作開發'."""
    if pd.isna(builder) or not str(builder).strip():
        return "—"
    name = str(builder).strip()
    if any(m in name for m in INDIVIDUAL_MARKERS):
        return "個體/合作開發"
    has_company = any(k in name for k in COMPANY_KEYWORDS)
    if not has_company and len(name) <= 4:
        return "個體/合作開發"
    return name[:4]


def _extract_unit_count(val: object) -> str:
    """Extract household count from 層棟戶數 (e.g. '2棟120戶' → '120')."""
    if pd.isna(val) or not str(val).strip():
        return ""
    m = re.search(r"(\d+)\s*戶", str(val))
    if m:
        return m.group(1)
    m = re.search(r"\d+", str(val))
    return m.group() if m else str(val).strip()


def _zoning_tag(val: object) -> str:
    """Map 使用分區 raw text → short tag: 住 / 商 / first-4-chars for others."""
    if pd.isna(val) or not str(val).strip():
        return ""
    s = str(val).strip()
    if "住" in s:
        return "住"
    if "商" in s:
        return "商"
    return s[:4] if s else ""


def load_project_info(city_name: str) -> Optional[pd.DataFrame]:
    """Load optional project info CSV from PROJ_INFO_DIR/{city_name}.csv."""
    path = PROJ_INFO_DIR / f"{city_name}.csv"
    if not path.exists():
        return None
    for enc in ("utf-8-sig", "utf-8", "big5", "cp950"):
        try:
            df = pd.read_csv(path, encoding=enc, low_memory=False)
            if "建案名稱" in df.columns:
                df["建案名稱"] = df["建案名稱"].astype(str).str.strip()
            log.info("  Loaded project_info/%s.csv (%d entries)", city_name, len(df))
            return df
        except (UnicodeDecodeError, LookupError):
            continue
    log.warning("  project_info/%s.csv unreadable — skipping", city_name)
    return None


def merge_project_info(presale_df: pd.DataFrame, proj_info: pd.DataFrame) -> pd.DataFrame:
    """Left join presale transactions with project info on ProjectName = 建案名稱.

    BuilderName and ProjectScaleRaw come only from project_info CSV.
    Zoning: MOI native column (都市土地使用分區) is baseline; CSV 使用分區 overrides when non-empty.
    """
    # CSV 使用分區 → Zoning_pi (so it doesn't clobber MOI-sourced Zoning)
    col_map = {"起造人": "BuilderName", "層棟戶數": "ProjectScaleRaw", "使用分區": "Zoning_pi"}
    proj = proj_info.copy()
    if "建案名稱" not in proj.columns:
        log.warning("  project_info missing '建案名稱' column — skipping merge")
        return presale_df
    proj = proj.rename(columns={k: v for k, v in col_map.items() if k in proj.columns})
    keep = ["建案名稱"] + [v for v in col_map.values() if v in proj.columns]
    proj = proj[[c for c in keep if c in proj.columns]].drop_duplicates(subset=["建案名稱"])
    merged = presale_df.merge(proj, left_on="ProjectName", right_on="建案名稱", how="left")
    if "建案名稱" in merged.columns:
        merged = merged.drop(columns=["建案名稱"])
    # Resolve final Zoning: CSV override wins when non-empty, else keep MOI native
    if "Zoning_pi" in merged.columns:
        moi_zone = merged.get("Zoning", pd.Series("", index=merged.index)).fillna("")
        csv_zone = merged["Zoning_pi"].fillna("")
        merged["Zoning"] = csv_zone.where(csv_zone.astype(str).str.strip() != "", moi_zone)
        merged = merged.drop(columns=["Zoning_pi"])
    return merged


# ── V2 IQR purifier & DOM proxy ──────────────────────────────────────────────

def iqr_filter_citywide(df: pd.DataFrame) -> pd.DataFrame:
    """Hard-remove rows outside the city-wide 5th–95th percentile of PricePerPing."""
    if "PricePerPing" not in df.columns or df.empty:
        return df
    prices = pd.to_numeric(df["PricePerPing"], errors="coerce").dropna()
    if prices.empty:
        return df
    lo = prices.quantile(IQR_LOW)
    hi = prices.quantile(IQR_HIGH)
    pp = pd.to_numeric(df["PricePerPing"], errors="coerce")
    return df[(pp >= lo) & (pp <= hi)].copy()


def compute_dom_proxy(
    df_actual: pd.DataFrame,
    df_presale: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Add DOM_Proxy (estimated holding months) and DOM_Tag to actual-transaction rows.
    Method 1: Address-match to presale → date gap in months.
    Method 2: BuildingAge fallback (age × 12) when age ≤ 5 yrs.
    Tags ⚠️ Short-term when estimated holding < 24 months.
    """
    df = df_actual.copy()
    df["DOM_Proxy"] = pd.NA
    df["DOM_Tag"]   = ""

    if (
        df_presale is not None and not df_presale.empty
        and "Address" in df_presale.columns and "Date" in df_presale.columns
    ):
        ps = df_presale.dropna(subset=["Address", "Date"]).copy()
        ps["_ak"] = ps["Address"].astype(str).str.strip().str.lower().str[:40]
        ps["_dt"] = pd.to_datetime(ps["Date"], format="%Y/%m/%d", errors="coerce")
        ps_map = (
            ps.dropna(subset=["_dt"])
            .sort_values("_dt")
            .groupby("_ak")["_dt"]
            .first()
        )
        df["_ak"]  = df["Address"].astype(str).str.strip().str.lower().str[:40]
        df["_adt"] = pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        df["_pdt"] = df["_ak"].map(ps_map)
        matched   = df["_pdt"].notna() & df["_adt"].notna()
        diff_days = (df.loc[matched, "_adt"] - df.loc[matched, "_pdt"]).dt.days
        df.loc[matched, "DOM_Proxy"] = (
            (diff_days / 30.44).round(0).astype("Int64")
        )
        df.drop(columns=["_ak", "_adt", "_pdt"], errors="ignore", inplace=True)

    if "BuildingAge" in df.columns:
        age     = pd.to_numeric(df["BuildingAge"], errors="coerce")
        no_dom  = df["DOM_Proxy"].isna()
        age_dom = (age.where((age >= 0) & (age <= 5)) * 12).round(0)
        df.loc[no_dom & age_dom.notna(), "DOM_Proxy"] = (
            age_dom.loc[no_dom & age_dom.notna()].astype("Int64")
        )

    dom_num = pd.to_numeric(df["DOM_Proxy"], errors="coerce")
    df.loc[dom_num < 24, "DOM_Tag"] = "⚠️ Short-term"
    return df


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


def compute_v2_summary_extras(
    summary: pd.DataFrame,
    ts: dict,
    pts: Optional[dict],
    df_with_dom: pd.DataFrame,
    city_name: str = "",
) -> pd.DataFrame:
    """
    Augment quarterly summary with v2/v3 columns:
    MarketTemp, QuickSalePrice, PremiumGap_pct,
    OracleNextMedian, OracleTrend, ShortTermCount, ShortTermRatioPct,
    PriceToRentRatio, RentalAnchorLabel, LeverageFlag.
    """
    if summary.empty:
        return summary
    s = summary.copy()

    s["MarketTemp"]    = s["YoY_pct"].apply(market_temp_label)
    s["QuickSalePrice"] = (s["MedianPricePerPing"] * 0.93).round(2)

    # PremiumGap: presale vs actual median per district per quarter
    if pts and pts.get("districts") and ts.get("quarters"):
        ps_q_idx = {q: i for i, q in enumerate(pts.get("quarters", []))}

        def _gap(row):
            d   = row["District"]
            act = row["MedianPricePerPing"]
            if d not in pts["districts"] or not act or pd.isna(act):
                return float("nan")
            i = ps_q_idx.get(row["Quarter"])
            if i is None:
                return float("nan")
            ps_series = pts["districts"][d]
            if i >= len(ps_series) or ps_series[i] is None:
                return float("nan")
            return round((ps_series[i] - act) / act * 100, 2)

        s["PremiumGap_pct"] = s.apply(_gap, axis=1)
    else:
        s["PremiumGap_pct"] = float("nan")

    # Oracle per district — with city-level fallback for low-N districts
    oracle_map: dict = {}
    if ts.get("districts") and ts.get("quarters"):
        all_q = ts["quarters"]
        # Compute city-level forecast first so it can be used as fallback
        city_fp, city_ft = oracle_forecast_city(ts, pts)
        # If city forecast is still None (< 2 quarters total), use last city median
        if city_fp is None:
            city_latest_vals = [v for v in ts.get("city_median", []) if v is not None]
            city_fp = city_latest_vals[-1] if city_latest_vals else None
        for dist, act_series in ts["districts"].items():
            ps_s = pts["districts"].get(dist, []) if pts and pts.get("districts") else []
            op, ot = oracle_forecast_district(
                act_series, all_q, ps_s or None,
                city_fallback_price=city_fp,
                city_fallback_trend=city_ft,
            )
            # Scale fallback by district's own latest price ratio to city
            if op is not None and op == city_fp:
                act_vals = [v for v in act_series if v is not None]
                city_m   = [v for v in ts.get("city_median", []) if v is not None]
                if act_vals and city_m and city_m[-1]:
                    ratio = act_vals[-1] / city_m[-1]
                    op = round(city_fp * ratio, 2)
            oracle_map[dist] = (op, ot)

    s["OracleNextMedian"] = s["District"].map(
        lambda d: oracle_map.get(d, (None, "→"))[0]
    )
    s["OracleTrend"] = s["District"].map(
        lambda d: oracle_map.get(d, (None, "→"))[1]
    )

    # Short-term ratios from DOM-tagged rows
    if (
        "DOM_Tag" in df_with_dom.columns
        and "Quarter" in df_with_dom.columns
        and "District" in df_with_dom.columns
    ):
        st = (
            df_with_dom
            .groupby(["District", "Quarter"])
            .apply(
                lambda g: pd.Series({
                    "ShortTermCount":    int((g["DOM_Tag"] == "⚠️ Short-term").sum()),
                    "ShortTermRatioPct": round(
                        (g["DOM_Tag"] == "⚠️ Short-term").mean() * 100, 1
                    ),
                })
            )
            .reset_index()
        )
        s = s.merge(st, on=["District", "Quarter"], how="left")
        s["ShortTermCount"]    = s["ShortTermCount"].fillna(0).astype(int)
        s["ShortTermRatioPct"] = s["ShortTermRatioPct"].fillna(0.0)
    else:
        s["ShortTermCount"]    = 0
        s["ShortTermRatioPct"] = 0.0

    # V3 — Pillar 5: Rental Anchor
    # For sparse districts with no transactions, fall back to city median for that quarter
    _city_med_by_q = dict(zip(ts.get("quarters", []), ts.get("city_median", [])))
    def _effective_price(row):
        p = row["MedianPricePerPing"]
        if pd.notna(p) and p > 0:
            return p
        return _city_med_by_q.get(row["Quarter"])
    s["PriceToRentRatio"] = s.apply(
        lambda row: compute_rental_anchor(_effective_price(row), city_name), axis=1
    )
    s["RentalAnchorLabel"] = s["PriceToRentRatio"].apply(rental_anchor_label)

    # V3 — Pillar 2: Confidence Leverage flag (🔴 if presale premium > 25 %)
    def _lev_flag(gap):
        try:
            v = float(gap)
        except (TypeError, ValueError):
            return "⚪ N/A"
        if math.isnan(v):
            return "⚪ N/A"
        if v > 25:
            return "🔴 High"
        if v > 10:
            return "🟡 Elevated"
        return "🟢 Normal"

    s["LeverageFlag"] = s["PremiumGap_pct"].apply(_lev_flag)

    return s


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


def export_v2_insights(
    city_name: str,
    summary_v2: pd.DataFrame,
    ts: dict,
    pts: Optional[dict],
) -> None:
    """Write data/processed/{City}/v2_insights.json for the forecast card."""
    city_dir = OUTPUT_DIR / city_name

    def _v(val):
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        return round(float(val), 2) if isinstance(val, float) else val

    # V3 — Pillar 4: Ripple Effect tags
    ripple_tags = compute_ripple_tags(ts)

    insights: dict = {}
    if not summary_v2.empty and "District" in summary_v2.columns:
        latest = (
            summary_v2.sort_values("Quarter", ascending=False)
            .groupby("District")
            .first()
            .reset_index()
        )
        for _, row in latest.iterrows():
            d = str(row["District"])
            insights[d] = {
                "latest_quarter":       str(row.get("Quarter") or ""),
                "actual_median":        _v(row.get("MedianPricePerPing")),
                "oracle_next_median":   _v(row.get("OracleNextMedian")),
                "oracle_trend":         str(row.get("OracleTrend") or "→"),
                "market_temp":          str(row.get("MarketTemp") or "⚪ N/A"),
                "quick_sale_price":     _v(row.get("QuickSalePrice")),
                "premium_gap_pct":      _v(row.get("PremiumGap_pct")),
                "short_term_count":     int(row.get("ShortTermCount") or 0),
                "short_term_ratio_pct": float(row.get("ShortTermRatioPct") or 0.0),
                "transaction_count":    int(row.get("TransactionCount") or 0),
                # V3 fields
                "price_to_rent":        _v(row.get("PriceToRentRatio")),
                "rental_anchor_label":  str(row.get("RentalAnchorLabel") or "⚪ N/A"),
                "leverage_flag":        str(row.get("LeverageFlag") or "⚪ N/A"),
                "ripple_tag":           ripple_tags.get(d, ""),
            }

    def _lnn(series):
        for v in reversed(series):
            if v is not None:
                return v
        return None

    act_med = _lnn(ts.get("city_median", []))
    ps_med  = _lnn(pts.get("city_median", [])) if pts else None
    city_gap = round((ps_med - act_med) / act_med * 100, 2) \
               if act_med and ps_med else None
    oracle_p, oracle_t = oracle_forecast_city(ts, pts)
    qm = ts.get("city_median", [])
    city_yoy = None
    if len(qm) >= 5 and qm[-1] and qm[-5]:
        city_yoy = round((qm[-1] - qm[-5]) / qm[-5] * 100, 2)

    payload = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "city": city_name,
        "city_forecast": {
            "actual_median":      act_med,
            "presale_median":     ps_med,
            "premium_gap_pct":    city_gap,
            "oracle_next_median": oracle_p,
            "oracle_trend":       oracle_t,
            "market_temp":        market_temp_label(city_yoy),
            "quick_sale_price":   round(act_med * 0.93, 2) if act_med else None,
        },
        "districts": insights,
    }
    (city_dir / "v2_insights.json").write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("  Wrote v2_insights.json (%d districts)", len(insights))


def export_static_pages(city_name: str, city_dir: Path) -> None:
    """
    Generate static JSON API files for GitHub Pages (zero-server) deployment.

    Writes inside data/processed/{City}/:
      leaderboard.json      – all districts, ranked by OracleNextMedian desc
      health-tiles.json     – traffic-light indicators per district
      districts.json        – snapshot_meta district list
      rows/{District}.json  – up to 200 rows per track, both tracks combined
    """
    MAX_ROWS_PER_TRACK = 200

    insights_path = city_dir / "v2_insights.json"
    meta_path     = city_dir / "snapshot_meta.json"
    if not (insights_path.exists() and meta_path.exists()):
        log.warning("  Skipping static pages for %s: missing v2_insights/snapshot_meta", city_name)
        return

    insights       = json.loads(insights_path.read_text(encoding="utf-8"))
    meta           = json.loads(meta_path.read_text(encoding="utf-8"))
    dist_meta_list = meta.get("districts", [])
    dist_insights  = insights.get("districts", {})

    # ── Leaderboard ──────────────────────────────────────────────────────────
    lb_items: list[dict] = []
    for d_info in dist_meta_list:
        dist = d_info.get("district", "")
        di   = dist_insights.get(dist, {})
        lb_items.append({
            "District":              dist,
            "Quarter":               di.get("latest_quarter"),
            "MedianPricePerPing":    di.get("actual_median"),
            "OracleNextMedian":      di.get("oracle_next_median"),
            "OracleTrend":           di.get("oracle_trend"),
            "ResilienceScore":       None,
            "DrawdownPct":           None,
            "ConfidenceLeveragePct": di.get("premium_gap_pct"),
            "MarketTemp":            di.get("market_temp"),
            "ShortTermRatioPct":     di.get("short_term_ratio_pct"),
            "ClusterCount":          None,
            "TransactionCount":      di.get("transaction_count"),
            "PriceToRentRatio":      di.get("price_to_rent"),
            "RentalAnchorLabel":     di.get("rental_anchor_label", "⚪ N/A"),
            "LeverageFlag":          di.get("leverage_flag", "⚪ N/A"),
            "RippleTag":             di.get("ripple_tag", ""),
        })
    lb_items.sort(key=lambda x: (x.get("OracleNextMedian") or 0), reverse=True)

    (city_dir / "leaderboard.json").write_text(
        json.dumps(
            {"city": city_name, "records_total": len(lb_items), "items": lb_items},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    # ── Health tiles ─────────────────────────────────────────────────────────
    tiles: list[dict] = []
    for item in lb_items:
        lev = float(item.get("ConfidenceLeveragePct") or 0)
        dom = float(item.get("ShortTermRatioPct") or 0)
        tiles.append({
            "district":       item["District"],
            "market_temp":    item.get("MarketTemp", "⚪ N/A"),
            "leverage_light": "red" if lev > 25 else ("yellow" if lev > 10 else "green"),
            "dom_light":      "red" if dom > 30 else ("yellow" if dom > 15 else "green"),
            "cluster_light":  "green",
            "ripple_tag":     item.get("RippleTag", ""),
            "rental_label":   item.get("RentalAnchorLabel", "⚪ N/A"),
            "resilience":     item.get("ResilienceScore"),
        })
    (city_dir / "health-tiles.json").write_text(
        json.dumps({"city": city_name, "tiles": tiles}, ensure_ascii=False),
        encoding="utf-8",
    )

    # ── Districts list ────────────────────────────────────────────────────────
    (city_dir / "districts.json").write_text(
        json.dumps({"city": city_name, "districts": dist_meta_list}, ensure_ascii=False),
        encoding="utf-8",
    )

    # ── Per-district row JSON ─────────────────────────────────────────────────
    rows_dir = city_dir / "rows"
    rows_dir.mkdir(exist_ok=True)

    def _jsonify(v: Any) -> Any:
        if isinstance(v, pd.Timestamp):
            return v.isoformat()
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        try:
            return v.item()
        except AttributeError:
            return v

    written = 0
    for d_info in dist_meta_list:
        dist    = d_info.get("district", "")
        safe    = _safe_name(str(dist))
        parquet = city_dir / f"{safe}{V3_SNAPSHOT_SUFFIX}"
        if not parquet.exists():
            continue
        try:
            df = pd.read_parquet(parquet)
            if "Date" in df.columns:
                df = df.sort_values("Date", ascending=False, na_position="last")

            if "Track" in df.columns:
                parts = [grp.head(MAX_ROWS_PER_TRACK) for _, grp in df.groupby("Track")]
                df_out = pd.concat(parts, ignore_index=True) if parts else df.head(MAX_ROWS_PER_TRACK)
            else:
                df_out = df.head(MAX_ROWS_PER_TRACK)

            records = [
                {k: _jsonify(v) for k, v in row.items()}
                for row in df_out.to_dict(orient="records")
            ]
            date_vals = [r["Date"] for r in records if r.get("Date")]
            out_path  = rows_dir / f"{dist}.json"
            out_path.write_text(
                json.dumps({
                    "city":           city_name,
                    "district":       dist,
                    "records_total":  len(records),
                    "date_min":       min(date_vals) if date_vals else None,
                    "date_max":       max(date_vals) if date_vals else None,
                    "items":          records,
                }, ensure_ascii=False),
                encoding="utf-8",
            )
            written += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("  Static rows export failed for %s/%s: %s", city_name, dist, exc)

    log.info(
        "  Wrote static API files for %s (%d districts, %d row files)",
        city_name, len(dist_meta_list), written,
    )


# ── Export ─────────────────────────────────────────────────────────────────────

def _safe_name(s: str) -> str:
    """Convert a district name to a filesystem-safe filename."""
    return re.sub(r"[^\w\u4e00-\u9fff]", "_", str(s)).strip("_") or "unknown"


def _write_district_snapshot(city_dir: Path, district: str, df: pd.DataFrame) -> dict:
    safe = _safe_name(str(district))
    out = city_dir / f"{safe}{V3_SNAPSHOT_SUFFIX}"
    snapshot = df.copy()
    # V3 — Pillar 6: Cluster Detection
    snapshot = _tag_cluster_flip(snapshot)
    snapshot.to_parquet(out, index=False, compression="zstd")

    if "Date" in snapshot.columns:
        date_series = pd.to_datetime(snapshot["Date"], errors="coerce").dropna()
        date_min = date_series.min().strftime("%Y-%m-%d") if not date_series.empty else None
        date_max = date_series.max().strftime("%Y-%m-%d") if not date_series.empty else None
    else:
        date_min = None
        date_max = None

    tracks = []
    if "Track" in snapshot.columns:
        tracks = sorted({str(x) for x in snapshot["Track"].dropna().unique()})

    return {
        "district": district,
        "file": out.name,
        "rows": int(len(snapshot)),
        "date_min": date_min,
        "date_max": date_max,
        "tracks": tracks,
    }


def export_city(city_name: str, df: pd.DataFrame,
                presale_df: Optional[pd.DataFrame] = None) -> list:
    """
    Write v2 output layers for one city to data/processed/{City}/:
      {District}.csv    — Dual-track merged (Actual + Presale) with v2 AI fields
      summary.csv       — Quarterly trend + OracleNextMedian/MarketTemp/QuickSalePrice
      v2_insights.json  — City-level forecast card data
    Returns the list of district names written.
    """
    city_dir = OUTPUT_DIR / city_name
    city_dir.mkdir(parents=True, exist_ok=True)
    snapshot_meta = {
        "city": city_name,
        "generated": datetime.now(timezone.utc).isoformat(),
        "snapshot_format": "parquet",
        "snapshot_suffix": V3_SNAPSHOT_SUFFIX,
        "districts": [],
    }

    # ── Step 1: IQR purifier (city-wide 5 %–95 % hard filter) ────────────────
    df = iqr_filter_citywide(df)
    if presale_df is not None and not presale_df.empty:
        presale_df = iqr_filter_citywide(presale_df)
        proj_info = load_project_info(city_name)
        if proj_info is not None:
            presale_df = merge_project_info(presale_df, proj_info)

    # ── Step 2: DOM proxy (address-match + building-age fallback) ─────────────
    df = compute_dom_proxy(df, presale_df)

    # ── Step 3: Parking-status column helper ──────────────────────────────────
    def _ps(row) -> str:
        has = (
            (pd.notna(row.get("ParkingType")) and str(row.get("ParkingType", "")).strip())
            or (pd.notna(row.get("ParkingPriceTWD"))
                and float(row.get("ParkingPriceTWD") or 0) > 0)
        )
        return "✅ Has Parking" if has else "—"

    # ── Step 4: Build merged dual-track district CSVs ─────────────────────────
    districts: list[str] = []
    if "District" in df.columns:
        act = df.copy()
        act["Track"]         = "Actual"
        act["ProjectName"]   = act.get("ProjectName", "")
        act["ParkingStatus"] = act.apply(_ps, axis=1)
        act["ProjectScale"]  = ""
        act["ZoningTag"]     = ""

        ps_ready = None
        if (
            presale_df is not None and not presale_df.empty
            and "District" in presale_df.columns
        ):
            ps_ready = presale_df.copy()
            ps_ready["Track"]         = "Presale"
            ps_ready["DOM_Proxy"]     = pd.NA
            ps_ready["DOM_Tag"]       = ""
            ps_ready["Status"]        = ps_ready.get("Status", "")
            ps_ready["ParkingStatus"] = ps_ready.apply(_ps, axis=1)
            # Project info enrichments (populated when project info CSV was merged)
            if "BuilderName" in ps_ready.columns:
                ps_ready["Developer"] = ps_ready["BuilderName"].apply(_developer_from_builder)
            if "ProjectScaleRaw" in ps_ready.columns:
                ps_ready["ProjectScale"] = ps_ready["ProjectScaleRaw"].apply(_extract_unit_count)
            elif "ProjectScale" not in ps_ready.columns:
                ps_ready["ProjectScale"] = ""
            if "Zoning" in ps_ready.columns:
                ps_ready["ZoningTag"] = ps_ready["Zoning"].apply(_zoning_tag)
            elif "ZoningTag" not in ps_ready.columns:
                ps_ready["ZoningTag"] = ""
            # Always override Community with ProjectName for presale rows
            project_col = ps_ready.get("ProjectName", pd.Series("", index=ps_ready.index))
            district_col = ps_ready.get("District", pd.Series("", index=ps_ready.index))
            ps_ready["Community"] = project_col.fillna("").where(
                project_col.fillna("").astype(str).str.strip() != "",
                district_col.fillna("")
            )
            if "Road" not in ps_ready.columns:
                ps_ready["Road"] = (
                    ps_ready["Address"].apply(extract_road)
                    if "Address" in ps_ready.columns else ""
                )

        for dist, grp in act.groupby("District"):
            safe    = _safe_name(str(dist))
            act_out = grp.copy()
            for c in V2_MERGED_COLS:
                if c not in act_out.columns:
                    act_out[c] = ""
            frames = [act_out[V2_MERGED_COLS]]

            if ps_ready is not None:
                ps_grp = ps_ready[ps_ready["District"] == dist].copy()
                if not ps_grp.empty:
                    for c in V2_MERGED_COLS:
                        if c not in ps_grp.columns:
                            ps_grp[c] = ""
                    frames.append(ps_grp[V2_MERGED_COLS])

            combined = pd.concat(frames, ignore_index=True)
            if "Date" in combined.columns:
                combined = combined.sort_values(
                    ["Track", "Date"], ascending=[True, False], na_position="last"
                )
            combined.to_csv(
                city_dir / f"{safe}.csv", index=False, encoding="utf-8-sig"
            )
            snapshot_meta["districts"].append(_write_district_snapshot(city_dir, dist, combined))
            districts.append(safe)

    # ── Step 5: Timeseries JSON (needed for oracle + insight computation) ──────
    export_timeseries(city_name, df)
    ts_path = city_dir / "timeseries.json"
    ts = json.loads(ts_path.read_text(encoding="utf-8")) if ts_path.exists() else {}

    pts = None
    if presale_df is not None and not presale_df.empty:
        export_presale_timeseries(city_name, presale_df)
        pts_path = city_dir / "presale_timeseries.json"
        pts = json.loads(pts_path.read_text(encoding="utf-8")) if pts_path.exists() else None

    # ── Step 6: v2-enhanced summary.csv + v2_insights.json ───────────────────
    summary = compute_trend(df)
    if not summary.empty:
        summary_v2 = compute_v2_summary_extras(summary, ts, pts, df, city_name)
        summary_v2.to_csv(
            city_dir / "summary.csv", index=False, encoding="utf-8-sig"
        )
        export_v2_insights(city_name, summary_v2, ts, pts)
        (city_dir / "snapshot_meta.json").write_text(
            json.dumps(snapshot_meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info(
            "  %s: %d districts, %d summary rows",
            city_name, len(districts), len(summary_v2),
        )
    else:
        log.warning("  %s: no summary data produced", city_name)

    export_static_pages(city_name, city_dir)
    return districts


def export_manifest(city_districts: dict) -> None:
    """Write data/manifest.json so the dashboard knows what files exist."""
    manifest = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "snapshot_format": "parquet",
        "cities": {
            code: {
                "folder":    CITY_MAP[code][0],
                "name":      CITY_MAP[code][1],
                "districts": city_districts.get(CITY_MAP[code][0], []),
                "snapshot_suffix": V3_SNAPSHOT_SUFFIX,
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

def run(local_dir: Optional[Path] = None,
        pilot_codes: Optional[list] = None) -> None:
    """
    local_dir:   if provided, reads pre-downloaded CSVs from that directory
                 (filenames: a_lvr_land_a.csv, f_lvr_land_a.csv, …)
                 instead of fetching from the MOI server.
    pilot_codes: list of city codes to process (e.g. ['A'] for Taipei-only).
                 None means all cities in CITY_MAP.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    city_districts: dict = {}

    active_map = {
        k: v for k, v in CITY_MAP.items()
        if pilot_codes is None or k in pilot_codes
    }

    for code, (city_name, _) in active_map.items():
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

        # Presale transactions (_b.csv) — fetch before export so detail goes in together
        presale_df: Optional[pd.DataFrame] = None
        try:
            if local_dir:
                presale_path = local_dir / f"{code.lower()}_lvr_land_b.csv"
                presale_raw = presale_path.read_bytes() if presale_path.exists() else None
            else:
                presale_raw = download_presale_csv(code)
            if presale_raw:
                presale_df = read_presale_csv_bytes(presale_raw)
                log.info("  Presale: %d rows after early filter", len(presale_df))
                presale_df = clean_presale(presale_df)
        except Exception as exc:
            log.warning("  Presale skipped for %s: %s", code, exc)

        districts = export_city(city_name, df, presale_df=presale_df)
        city_districts[city_name] = districts

    export_manifest(city_districts)

    # Write last_updated.json for dashboard footer
    now_utc = datetime.now(timezone.utc)
    taipei_offset = timedelta(hours=8)
    now_taipei = now_utc + taipei_offset
    (OUTPUT_DIR / "last_updated.json").write_text(
        json.dumps({
            "updated_utc":    now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "updated_taipei": now_taipei.strftime("%Y-%m-%d %H:%M TST"),
        }, ensure_ascii=False),
        encoding="utf-8",
    )
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
    parser.add_argument(
        "--pilot", nargs="+", metavar="CODE",
        help=(
            "Limit live run to specific city codes, e.g. --pilot A  (Taipei only). "
            "Codes: A=Taipei F=New_Taipei H=Taoyuan B=Taichung D=Tainan E=Kaohsiung"
        ),
    )
    args = parser.parse_args()

    if args.backfill or args.seasons:
        run_backfill(seasons=args.seasons)
    else:
        run(local_dir=args.local_dir, pilot_codes=args.pilot)
