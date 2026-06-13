#!/usr/bin/env python3
"""
waterwatch.criticalto.ca — data scraper
Multi-city water supply intelligence for Canadian communities.

Cities:
  vancouver — Metro Vancouver (BC) Stage 3 restrictions, June–Oct 2026
  canmore   — Canmore, Alberta (Bow River watershed)

Outputs:
  site/data/vancouver.json
  site/data/canmore.json

Run manually or via GitHub Actions on a schedule.
Licence: Open Government Licence - Canada (Environment and Climate Change Canada)
         Open Government Licence - BC (BC River Forecast Centre)
"""

import json
import csv
import io
import re
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

DATAMART_BASE = "https://dd.weather.gc.ca/today/hydrometric/csv"

# ── VANCOUVER config ──────────────────────────────────────────────────────────
VANCOUVER = {
    "meta": {
        "city": "Metro Vancouver",
        "province": "BC",
        "dashboard": "waterwatch.criticalto.ca/vancouver",
    },
    "stations": {
        "capilano":  {"id": "08GA010", "province": "BC", "name": "Capilano River above Intake"},
        "seymour":   {"id": "08GA030", "province": "BC", "name": "Seymour River at Seymour Falls"},
        "coquitlam": {"id": "08MH141", "province": "BC", "name": "Coquitlam River near Port Coquitlam"},
    },
    "restriction": {
        "stage": 3,
        "start_date": "2026-06-08",
        "end_date": "2026-10-15",
        "fine_cad": 500,
        "lawn_watering": False,
        "tree_shrub_watering": False,
        "vegetable_garden": True,
        "vehicle_washing": False,
        "pressure_washing": False,
        "pools_hot_tubs": False,
        "stanley_park_tunnel": True,
        "source_url": "https://metrovancouver.org/services/water/water-restrictions",
        "last_verified": "2026-06-08",
    },
    "snowpack": {
        "pct_of_normal": 53,
        "reference": "April 1 peak, South Coast region",
        "source": "BC River Forecast Centre",
        "note": "2026 reading — one of the lowest on record",
        "last_updated": "2026-05-01",
    },
    "reservoirs": {
        "capilano":  {"name": "Capilano",  "pct_of_target": 68, "note": "Entered May at ~68% of seasonal target"},
        "seymour":   {"name": "Seymour",   "pct_of_target": 65, "note": "Entered May at ~65% of seasonal target"},
        "coquitlam": {"name": "Coquitlam", "pct_of_target": 72, "note": "Slightly higher due to larger volumetric capacity"},
        "last_updated": "2026-06-01",
        "source": "Metro Vancouver Reservoir Levels and Water Use",
        "source_url": "https://metrovancouver.org/services/water/reservoir-levels-water-use",
        "note": "Values shown are % of seasonal storage target, not % of maximum capacity.",
    },
}

# ── CANMORE config ────────────────────────────────────────────────────────────
CANMORE = {
    "meta": {
        "city": "Canmore",
        "province": "AB",
        "dashboard": "waterwatch.criticalto.ca/canmore",
    },
    "stations": {
        "bow_banff":   {"id": "05BB001", "province": "AB", "name": "Bow River at Banff (25km upstream)"},
        "spray_banff": {"id": "05BC001", "province": "AB", "name": "Spray River at Banff"},
        "waiparous":   {"id": "05BG006", "province": "AB", "name": "Waiparous Creek near Cochrane"},
        "bow_cochrane": {"id": "05BH005", "province": "AB", "name": "Bow River near Cochrane (downstream indicator)"},
    },
    "restriction": {
        # Canmore uses a staged drought response — no mandatory restrictions currently active
        # Source: https://www.canmore.ca/your-community/public-safety/hazard-monitoring/drought-monitoring
        "stage": 0,
        "stage_label": "No active restrictions",
        "start_date": None,
        "end_date": None,
        "fine_cad": None,
        "operator": "EPCOR Water Services (on behalf of Town of Canmore)",
        "source_url": "https://www.canmore.ca/your-community/public-safety/hazard-monitoring/drought-monitoring",
        "last_verified": "2026-06-08",
        "note": "Canmore draws water from two sources: Bow River aquifer (groundwater) and Rundle Forebay (surface water via Spray Lakes/TransAlta canal system). EPCOR operates under a utility management agreement with the Town until 2030.",
    },
    "snowpack": {
        "pct_of_normal": 68,
        "reference": "April 1 peak, Bow River Basin",
        "source": "Alberta River Forecast Centre",
        "note": "2026 reading — below average but not crisis level",
        "last_updated": "2026-05-01",
    },
    "watershed_context": {
        "primary_source": "Rundle Forebay (surface water, ~50% of supply)",
        "secondary_source": "Bow River aquifer (groundwater, ~50% of supply)",
        "forebay_fed_by": "Spray Lakes reservoir via TransAlta canal system (Kananaskis Country)",
        "operator": "EPCOR Water Services Inc.",
        "agreement_expires": "2030",
        "note": "No public API for Rundle Forebay levels. Bow River at Banff (25km upstream) is the best available public indicator of watershed health.",
        "source": "EPCOR Canada / Town of Canmore",
        "source_url": "https://www.epcor.com/ca/en/about/our-company/where-we-operate/canmore.html",
    },
}

CITIES = {
    "vancouver": VANCOUVER,
    "canmore": CANMORE,
}


def fetch_station(station: dict) -> dict:
    """Fetch latest hourly reading from Environment Canada Datamart."""
    sid = station["id"]
    prov = station["province"]
    url = f"{DATAMART_BASE}/{prov}/hourly/{prov}_{sid}_hourly_hydrometric.csv"
    result = {
        "station_id": sid,
        "station_name": station["name"],
        "url": url,
        "level_m": None,
        "discharge_cms": None,
        "timestamp": None,
        "status": "unknown",
    }
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "waterwatch-criticalto/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        rows = [r for r in csv.reader(io.StringIO(raw)) if r and r[0].strip()]
        data_rows = [r for r in rows[1:] if len(r) >= 7]
        if not data_rows:
            result["status"] = "no_data"
            return result
        latest = data_rows[-1]
        result["timestamp"] = latest[1].strip()
        result["level_m"] = float(latest[2]) if latest[2].strip() else None
        result["discharge_cms"] = float(latest[6]) if latest[6].strip() else None
        result["status"] = "ok"
    except urllib.error.HTTPError as e:
        result["status"] = f"http_error_{e.code}"
    except Exception as e:
        result["status"] = f"error: {str(e)[:80]}"
    return result


def build_city_payload(city_key: str, config: dict) -> dict:
    """Build the full JSON payload for one city."""
    now_utc = datetime.now(timezone.utc).isoformat()

    station_data = {}
    for key, station in config["stations"].items():
        print(f"    {station['name']} ({station['id']})...")
        station_data[key] = fetch_station(station)

    # Days remaining under restriction (Vancouver only for now)
    restriction = dict(config["restriction"])
    if restriction.get("end_date"):
        end = datetime.strptime(restriction["end_date"], "%Y-%m-%d")
        days_remaining = max(0, (end - datetime.now()).days)
        restriction["days_remaining"] = days_remaining
    else:
        restriction["days_remaining"] = 0

    payload = {
        "meta": {
            **config["meta"],
            "generated_utc": now_utc,
            "data_licences": [
                "Open Government Licence – Canada (Environment and Climate Change Canada)",
            ],
        },
        "restriction": restriction,
        "snowpack": config["snowpack"],
        "watershed_inflow": station_data,
    }

    # City-specific extra sections
    if "reservoirs" in config:
        payload["reservoirs"] = config["reservoirs"]
    if "watershed_context" in config:
        payload["watershed_context"] = config["watershed_context"]

    return payload


def main():
    print(f"waterwatch.criticalto.ca — data fetch starting")
    print(f"  Time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}")

    out_dir = Path(__file__).parent.parent / "site" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Vancouver + Canmore
    for city_key, config in CITIES.items():
        print(f"\n  [{config['meta']['city']}]")
        payload = build_city_payload(city_key, config)

        out_path = out_dir / f"{city_key}.json"
        with open(out_path, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"  Written → {out_path}")

        print(f"  Station status:")
        for key, s in payload["watershed_inflow"].items():
            level = f"{s['level_m']}m" if s["level_m"] is not None else "n/a"
            flow = f"{s['discharge_cms']} cms" if s["discharge_cms"] is not None else "n/a"
            print(f"    {key:14} [{s['status']:8}]  level={level}  discharge={flow}")

        stage = payload["restriction"].get("stage_label") or f"Stage {payload['restriction']['stage']}"
        days = payload["restriction"].get("days_remaining", 0)
        print(f"  Restriction: {stage}  ({days} days remaining)")

    # Calgary (separate JSON format)
    print(f"\n  [Calgary]")
    calgary_payload = build_calgary_payload()
    out_path = out_dir / "calgary.json"
    with open(out_path, "w") as f:
        json.dump(calgary_payload, f, indent=2)
    print(f"  Written → {out_path}")
    print(f"  Station status:")
    for sid, s in calgary_payload["stations"].items():
        flow = f"{s['latest_flow']} cms" if s["latest_flow"] is not None else "n/a"
        print(f"    {sid}  [{s['status']:8}]  discharge={flow}")
    g = calgary_payload.get("glenmore", {})
    print(f"  Glenmore: {g.get('pct_capacity','—')}% ({g.get('compared_to_normal','—')})  [{g.get('status','—')}]")

    print("\n  Done.")


if __name__ == "__main__":
    main()

# ── CALGARY config ────────────────────────────────────────────────────────────
CALGARY = {
    "meta": {
        "city": "Calgary",
        "province": "AB",
        "dashboard": "waterwatch.criticalto.ca/calgary",
    },
    "stations": {
        "bow_calgary":    {"id": "05BH004", "province": "AB", "name": "Bow River at Calgary"},
        "elbow_glenmore": {"id": "05BJ001", "province": "AB", "name": "Elbow River below Glenmore Dam"},
        "elbow_bragg":    {"id": "05BJ004", "province": "AB", "name": "Elbow River at Bragg Creek"},
    },
}

GLENMORE_PDF_URL  = "https://rivers.alberta.ca/forecasting/data/reports/Res_storage.pdf"
GLENMORE_STATION  = "05BJ008"
GLENMORE_MAX_DAM3 = 23502


def fetch_glenmore() -> dict:
    """Parse Glenmore Reservoir storage from Alberta Rivers PDF."""
    try:
        req = urllib.request.Request(
            GLENMORE_PDF_URL,
            headers={"User-Agent": "waterwatch-criticalto/1.0"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            pdf_bytes = resp.read()
    except Exception as e:
        return {"status": f"fetch_error: {str(e)[:80]}"}

    try:
        from pdfminer.high_level import extract_text
        text = extract_text(io.BytesIO(pdf_bytes))
    except ImportError:
        text = pdf_bytes.decode("latin-1", errors="replace")

    # Match: Glenmore  05BJ008  14,477  23,502  62%  ABOVE  ...  2026-04-26
    pattern = re.compile(
        r"Glenmore\s+" + re.escape(GLENMORE_STATION) +
        r"\s+([\d,]+)\s+([\d,]+)\s+(\d+)%\s+(ABOVE|NORMAL|BELOW).*?(\d{4}-\d{2}-\d{2})",
        re.DOTALL | re.IGNORECASE
    )
    m = pattern.search(text)
    if not m:
        # looser: just station ID
        pattern2 = re.compile(
            re.escape(GLENMORE_STATION) +
            r"\s+([\d,]+)\s+([\d,]+)\s+(\d+)%\s+(ABOVE|NORMAL|BELOW).*?(\d{4}-\d{2}-\d{2})",
            re.DOTALL | re.IGNORECASE
        )
        m = pattern2.search(text)

    if not m:
        return {"status": "pattern_not_found"}

    return {
        "station_id":         GLENMORE_STATION,
        "storage_dam3":       int(m.group(1).replace(",", "")),
        "max_dam3":           int(m.group(2).replace(",", "")),
        "pct_capacity":       int(m.group(3)),
        "compared_to_normal": m.group(4).upper(),
        "reading_date":       m.group(5),
        "status":             "ok",
    }


def build_calgary_payload() -> dict:
    """Build calgary.json — stations + Glenmore reservoir."""
    now_utc = datetime.now(timezone.utc).isoformat()

    station_data = {}
    for key, station in CALGARY["stations"].items():
        print(f"    {station['name']} ({station['id']})...")
        raw = fetch_station(station)
        # Remap to the format calgary.html expects
        station_data[station["id"]] = {
            "name":         raw["station_name"],
            "latest_flow":  raw["discharge_cms"],
            "latest_level": raw["level_m"],
            "latest_dt":    raw["timestamp"],
            "history":      [],   # today-only endpoint; no history array
            "status":       raw["status"],
        }

    print("    Glenmore Reservoir (05BJ008)...")
    glenmore = fetch_glenmore()

    return {
        "city":       "Calgary",
        "fetched_at": now_utc,
        "stations":   station_data,
        "glenmore":   glenmore,
        "infrastructure": CALGARY.get("infrastructure", {}),
    }
