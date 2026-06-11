#!/usr/bin/env python3
"""
fetch_calgary.py — WaterWatch Calgary data scraper
Runs every 6 hours via GitHub Actions.

Sources:
  - River flow/level: MSC OGC API GeoJSON (Environment Canada)
    https://api.weather.gc.ca/collections/hydrometric-realtime/items
  - Glenmore reservoir storage: Alberta Rivers PDF
    https://rivers.alberta.ca/forecasting/data/reports/Res_storage.pdf

Output: site/data/calgary.json
"""

import json
import re
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────────────────────────────

STATIONS = {
    "05BH004": "Bow River at Calgary",
    "05BJ001": "Elbow River below Glenmore Dam",
    "05BJ004": "Elbow River at Bragg Creek",
}

OGC_API_BASE = "https://api.weather.gc.ca/collections/hydrometric-realtime/items"
RESERVOIR_PDF_URL = "https://rivers.alberta.ca/forecasting/data/reports/Res_storage.pdf"

# Glenmore station in Alberta Rivers report
GLENMORE_STATION_ID = "05BJ008"
GLENMORE_MAX_DAM3 = 23502

# History window: keep last 168 rows (~7 days of hourly data)
HISTORY_ROWS = 168

OUTPUT_PATH = Path("site/data/calgary.json")

# ── FETCH RIVER DATA ──────────────────────────────────────────────────────────

def fetch_station(station_id: str) -> dict:
    """Fetch hourly data from MSC OGC API using per-station path endpoint."""
    # The OGC API ignores query params for station filtering;
    # correct pattern is /items/{STATION_NUMBER} for latest reading,
    # then /items?station_number=X for history (with client-side filter).
    # Approach: fetch latest via path, then bulk fetch and filter for history.

    # Step 1: latest observation via path lookup
    latest_url = f"{OGC_API_BASE}/{station_id}?f=json"
    print(f"  Fetching {station_id} latest: {latest_url}")

    latest_flow = latest_level = latest_dt = None
    req = urllib.request.Request(latest_url, headers={
        "User-Agent": "WaterWatch/1.0 (criticalto.ca)",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            item = json.loads(resp.read())
        props = item.get("properties", {})
        latest_flow  = props.get("DISCHARGE")
        latest_level = props.get("LEVEL")
        latest_dt    = props.get("DATETIME")
        print(f"  ✓ {station_id} latest: flow={latest_flow} m³/s, level={latest_level} m")
    except Exception as e:
        print(f"  ✗ {station_id} latest: {e}")
        return {}

    # Step 2: history — fetch large batch, filter to this station
    history_url = (
        f"{OGC_API_BASE}?f=json&limit={HISTORY_ROWS * 6}"
        f"&sortby=DATETIME&PROV_TERR_STATE_LOC=AB"
    )
    history = []
    try:
        req2 = urllib.request.Request(history_url, headers={
            "User-Agent": "WaterWatch/1.0 (criticalto.ca)",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req2, timeout=45) as resp:
            data = json.loads(resp.read())
        for f in data.get("features", []):
            p = f.get("properties", {})
            if p.get("STATION_NUMBER") == station_id and p.get("DISCHARGE") is not None:
                history.append({
                    "datetime": p.get("DATETIME"),
                    "level":    p.get("LEVEL"),
                    "flow":     p.get("DISCHARGE"),
                })
        history = history[-HISTORY_ROWS:]
        print(f"  ✓ {station_id} history: {len(history)} rows")
    except Exception as e:
        print(f"  ⚠ {station_id} history fetch failed ({e}), using latest only")
        history = [{"datetime": latest_dt, "level": latest_level, "flow": latest_flow}]

    return {
        "latest_flow":  latest_flow,
        "latest_level": latest_level,
        "latest_dt":    latest_dt,
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
