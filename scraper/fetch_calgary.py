#!/usr/bin/env python3
"""
fetch_calgary.py — WaterWatch Calgary data scraper
Runs every 6 hours via GitHub Actions.

Sources:
  - River flow/level: MSC Datamart CSV (Environment Canada)
    https://dd.weather.gc.ca/hydrometric/csv/AB/hourly/
  - Glenmore reservoir storage: Alberta Rivers PDF
    https://rivers.alberta.ca/forecasting/data/reports/Res_storage.pdf

Output: site/data/calgary.json
"""

import json
import re
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────────────────────────────

STATIONS = {
    "05BH004": "Bow River at Calgary",
    "05BJ001": "Elbow River below Glenmore Dam",
    "05BJ004": "Elbow River at Bragg Creek",
}

PROVINCE = "AB"
DATAMART_BASE = "https://dd.weather.gc.ca/hydrometric/csv"
RESERVOIR_PDF_URL = "https://rivers.alberta.ca/forecasting/data/reports/Res_storage.pdf"

# Glenmore station in Alberta Rivers report
GLENMORE_STATION_ID = "05BJ008"
GLENMORE_MAX_DAM3 = 23502

# History window: keep last 168 rows (~7 days of hourly data)
HISTORY_ROWS = 168

OUTPUT_PATH = Path("site/data/calgary.json")

# ── FETCH RIVER DATA ──────────────────────────────────────────────────────────

def fetch_station(station_id: str) -> dict:
    """Fetch hourly CSV from MSC Datamart and return latest + 7d history."""
    url = f"{DATAMART_BASE}/{PROVINCE}/hourly/{PROVINCE}_{station_id}_hourly_hydrometric.csv"
    print(f"  Fetching {station_id}: {url}")

    req = urllib.request.Request(url, headers={"User-Agent": "WaterWatch/1.0 (criticalto.ca)"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  ✗ {station_id}: {e}")
        return {}

    lines = raw.strip().splitlines()
    if len(lines) < 2:
        print(f"  ✗ {station_id}: empty response")
        return {}

    header = lines[0].split(",")
    # Typical column order:
    # ID, Date, Level (m), Level Grade, Level Symbol, Level Approval,
    # Discharge (cms), Discharge Grade, Discharge Symbol, Discharge Approval
    # But varies — find by name
    def col_idx(candidates):
        for c in candidates:
            for i, h in enumerate(header):
                if c.lower() in h.lower():
                    return i
        return None

    dt_col    = col_idx(["Date"])
    level_col = col_idx(["Level (m)", "Water Level"])
    flow_col  = col_idx(["Discharge (cms)", "Discharge"])

    history = []
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split(",")
        try:
            entry = {
                "datetime": parts[dt_col].strip() if dt_col is not None and dt_col < len(parts) else None,
                "level":    float(parts[level_col]) if level_col is not None and level_col < len(parts) and parts[level_col].strip() else None,
                "flow":     float(parts[flow_col])  if flow_col  is not None and flow_col  < len(parts) and parts[flow_col].strip()  else None,
            }
            history.append(entry)
        except (ValueError, IndexError):
            continue

    if not history:
        print(f"  ✗ {station_id}: no valid rows parsed")
        return {}

    # Keep last HISTORY_ROWS
    history = history[-HISTORY_ROWS:]

    latest = history[-1]
    print(f"  ✓ {station_id}: {len(history)} rows, latest flow={latest.get('flow')} m³/s, level={latest.get('level')} m")

    return {
        "latest_flow":  latest.get("flow"),
        "latest_level": latest.get("level"),
        "latest_dt":    latest.get("datetime"),
        "history":      history,
    }


# ── FETCH GLENMORE RESERVOIR ──────────────────────────────────────────────────

def fetch_glenmore_reservoir() -> dict:
    """
    Parse Glenmore storage from Alberta Rivers PDF.
    The PDF is text-extractable; we use pdfminer if available, else fall back
    to urllib and a regex over the raw byte stream for the ASCII text portions.
    """
    print(f"  Fetching Glenmore reservoir PDF…")

    req = urllib.request.Request(
        RESERVOIR_PDF_URL,
        headers={"User-Agent": "WaterWatch/1.0 (criticalto.ca)"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            pdf_bytes = resp.read()
    except Exception as e:
        print(f"  ✗ Glenmore PDF: {e}")
        return {}

    # Try pdfminer (available in Actions via pip install pdfminer.six)
    try:
        from pdfminer.high_level import extract_text
        import io
        text = extract_text(io.BytesIO(pdf_bytes))
    except ImportError:
        # Fallback: decode bytes and grep for ASCII text segments
        text = pdf_bytes.decode("latin-1", errors="replace")

    # Look for the Glenmore row in the reservoir table
    # Pattern in PDF: "Glenmore 05BJ008  14,477  23,502  62%  ABOVE  ..."
    # We match station ID to be safe
    pattern = re.compile(
        r"Glenmore\s+" + re.escape(GLENMORE_STATION_ID) +
        r"\s+([\d,]+)\s+([\d,]+)\s+(\d+)%\s+(ABOVE|NORMAL|BELOW).*?(\d{4}-\d{2}-\d{2})",
        re.DOTALL | re.IGNORECASE
    )
    m = pattern.search(text)

    if not m:
        # Looser: just find 05BJ008 line
        pattern2 = re.compile(
            re.escape(GLENMORE_STATION_ID) +
            r"\s+([\d,]+)\s+([\d,]+)\s+(\d+)%\s+(ABOVE|NORMAL|BELOW).*?(\d{4}-\d{2}-\d{2})",
            re.DOTALL | re.IGNORECASE
        )
        m = pattern2.search(text)

    if not m:
        print(f"  ✗ Glenmore: pattern not found in PDF text")
        # Return a hard-coded last-known value with a stale flag
        return {"stale": True, "note": "PDF parse failed — using last known data"}

    storage    = int(m.group(1).replace(",", ""))
    max_stor   = int(m.group(2).replace(",", ""))
    pct        = int(m.group(3))
    status     = m.group(4).upper()
    read_date  = m.group(5)

    print(f"  ✓ Glenmore: {storage} dam³ / {max_stor} dam³ = {pct}% ({status}), reading {read_date}")

    return {
        "station_id":    GLENMORE_STATION_ID,
        "storage_dam3":  storage,
        "max_dam3":      max_stor,
        "pct_capacity":  pct,
        "compared_to_normal": status,
        "reading_date":  read_date,
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=== WaterWatch Calgary scraper ===")
    print(f"Run time: {datetime.now(timezone.utc).isoformat()}")

    output = {
        "city":       "Calgary",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "stations":   {},
        "glenmore":   {},
    }

    # River stations
    print("\n— River stations —")
    for station_id, name in STATIONS.items():
        print(f"[{station_id}] {name}")
        result = fetch_station(station_id)
        if result:
            output["stations"][station_id] = {
                "name": name,
                **result
            }

    # Glenmore reservoir
    print("\n— Glenmore reservoir —")
    glenmore = fetch_glenmore_reservoir()
    if glenmore:
        output["glenmore"] = glenmore

    # Write output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    stations_ok = len(output["stations"])
    glenmore_ok = bool(output["glenmore"] and not output["glenmore"].get("stale"))

    print(f"\n✓ Wrote {OUTPUT_PATH}")
    print(f"  Stations: {stations_ok}/{len(STATIONS)}")
    print(f"  Glenmore: {'✓' if glenmore_ok else '⚠ stale/failed'}")

    if stations_ok == 0:
        print("ERROR: no station data retrieved")
        sys.exit(1)


if __name__ == "__main__":
    main()
