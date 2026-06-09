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

    print("\n  Done.")


if __name__ == "__main__":
    main()
