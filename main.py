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
import re
import sys
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

OUTPUT_DIR = Path("data")
CHUNK_SIZE = 20_000  # rows per read_csv chunk

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

# ── Trend computation ──────────────────────────────────────────────────────────

def compute_trend(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate cleaned transactions into a quarterly district-level trend table.
    Computes:
      - AvgPricePerPing  (mean price per Ping in 10k TWD)
      - TransactionCount (sample size — important for reliability check)
      - QoQ_pct          (quarter-on-quarter % change vs. prior quarter)
      - YoY_pct          (year-on-year % change vs. same quarter last year)
    Missing prior periods are left as NaN (not synthesized as 0).
    """
    required = {"District", "Quarter", "PricePerPing"}
    if not required.issubset(df.columns):
        log.warning("Missing columns for trend: %s", required - set(df.columns))
        return pd.DataFrame()

    agg = (
        df.dropna(subset=list(required))
        .groupby(["District", "Quarter"], sort=True)
        .agg(
            AvgPricePerPing =("PricePerPing", "mean"),
            TransactionCount=("PricePerPing", "count"),
        )
        .reset_index()
    )
    agg["AvgPricePerPing"] = agg["AvgPricePerPing"].round(2)

    # QoQ: immediately preceding quarter for the same district
    agg["QoQ_pct"] = (
        agg.groupby("District")["AvgPricePerPing"]
        .pct_change()
        .mul(100)
        .round(2)
    )

    # YoY: same quarter 4 periods back (works correctly only when data covers ≥5 quarters)
    agg["YoY_pct"] = (
        agg.groupby("District")["AvgPricePerPing"]
        .pct_change(periods=4)
        .mul(100)
        .round(2)
    )

    return agg

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

    export_manifest(city_districts)
    log.info("Pipeline complete. Output: %s/", OUTPUT_DIR)


if __name__ == "__main__":
    arg = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    run(arg)
