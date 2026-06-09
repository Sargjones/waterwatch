#!/usr/bin/env python3
"""
waterwatch.criticalto.ca — data scraper
Fetches watershed inflow data from Environment Canada Datamart
and reservoir/restriction context from Metro Vancouver.
Outputs: data/waterwatch.json (consumed by the dashboard)

Run manually or via GitHub Actions on a schedule.
Licence: Open Government Licence - Canada (EC data)
         Open Government Licence - BC (BCRFC data)
"""

import json
import csv
import io
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Station IDs ──────────────────────────────────────────────────────────────
# All three Metro Vancouver source watersheds
STATIONS = {
    "capilano":  {"id": "08GA010", "name": "Capilano River above Intake"},
    "seymour": {"id": "08GA030", "name": "Seymour River at Seymour Falls"},
    "coquitlam": {"id": "08MH141", "name": "Coquitlam River near Port Coquitlam"},
}

DATAMART_BASE = "https://dd.weather.gc.ca/today/hydrometric/csv/BC/hourly"

# ── Restriction stage config (update manually when Metro Van changes stage) ──
# Source: https://vancouver.ca/home-property-development/understanding-watering-restrictions.aspx
# Stage 3 confirmed active June 8 – October 15, 2026
RESTRICTION = {
    "stage": 3,
    "start_date": "2026-06-08",
    "end_date": "2026-10-15",
    "fine_cad": 500,
    "lawn_watering": False,
    "tree_shrub_watering": False,   # Stage 3: prohibited
    "vegetable_garden": True,        # Allowed by hand/drip at any time
    "vehicle_washing": False,
    "pressure_washing": False,
    "pools_hot_tubs": False,         # No filling or topping up
    "stanley_park_tunnel": True,     # Construction factor contributing to Stage 3
    "source_url": "https://metrovancouver.org/services/water/water-restrictions",
    "last_verified": "2026-06-08",
}

# ── Snowpack context (update from BCRFC each week May–Oct) ──────────────────
# Source: BC River Forecast Centre — Orchid Lake station, South Coast region
# https://bcrfc.env.gov.bc.ca
SNOWPACK = {
    "pct_of_normal": 53,
    "reference": "April 1 peak, South Coast region",
    "source": "BC River Forecast Centre",
    "note": "2026 reading — one of the lowest on record",
    "last_updated": "2026-05-01",
}

# ── Reservoir levels (update weekly from Metro Vancouver PDF/page) ───────────
# Source: https://metrovancouver.org/services/water/reservoir-levels-water-use
# Published every Monday during high-demand season (May–Oct)
# These values should be refreshed by the GitHub Actions scrape each Monday.
# Format: pct_of_seasonal_target (not % full — Metro Van reports vs seasonal target)
RESERVOIRS_STATIC = {
    "capilano": {
        "name": "Capilano",
        "pct_of_target": 68,
        "historical_note": "Entered May at ~68% of seasonal target",
    },
    "seymour": {
        "name": "Seymour",
        "pct_of_target": 65,
        "historical_note": "Entered May at ~65% of seasonal target",
    },
    "coquitlam": {
        "name": "Coquitlam",
        "pct_of_target": 72,
        "historical_note": "Slightly higher due to larger volumetric capacity",
    },
    "last_updated": "2026-06-01",
    "source": "Metro Vancouver Reservoir Levels and Water Use",
    "source_url": "https://metrovancouver.org/services/water/reservoir-levels-water-use",
    "note": "Metro Vancouver publishes weekly updates May–Oct. Values shown are % of seasonal target, not % of total capacity.",
}


def fetch_station_data(station_key: str, station: dict) -> dict:
    """Fetch the latest hourly CSV from Environment Canada Datamart."""
    station_id = station["id"]
    url = f"{DATAMART_BASE}/BC_{station_id}_hourly_hydrometric.csv"
    result = {
        "station_id": station_id,
        "station_name": station["name"],
        "url": url,
        "level_m": None,
        "discharge_cms": None,
        "timestamp": None,
        "status": "unknown",
    }

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "waterwatch-criticalto/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="replace")

        rows = list(csv.reader(io.StringIO(raw)))
        # Header row: ID, Date, Water Level (m), Grade, Symbol, QA/QC, Discharge (cms), ...
        data_rows = [r for r in rows[1:] if len(r) >= 7 and r[0].strip()]
        if not data_rows:
            result["status"] = "no_data"
            return result

        # Most recent row is last
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


def build_payload() -> dict:
    """Assemble the full JSON payload for the dashboard."""
    now_utc = datetime.now(timezone.utc).isoformat()

    station_data = {}
    for key, station in STATIONS.items():
        print(f"  Fetching {station['name']} ({station['id']})...")
        station_data[key] = fetch_station_data(key, station)

    # Days remaining under current restriction
    end = datetime.strptime(RESTRICTION["end_date"], "%Y-%m-%d")
    today = datetime.utcnow()
    days_remaining = max(0, (end - today).days)

    payload = {
        "meta": {
            "generated_utc": now_utc,
            "dashboard": "waterwatch.criticalto.ca",
            "city": "Metro Vancouver",
            "province": "BC",
            "data_licences": [
                "Open Government Licence – Canada (Environment and Climate Change Canada)",
                "Open Government Licence – BC (BC River Forecast Centre)",
            ],
        },
        "restriction": {
            **RESTRICTION,
            "days_remaining": days_remaining,
            "active": True,
        },
        "snowpack": SNOWPACK,
        "reservoirs": RESERVOIRS_STATIC,
        "watershed_inflow": station_data,
    }

    return payload


def main():
    print("waterwatch.criticalto.ca — data fetch starting")
    print(f"  Time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}")

    payload = build_payload()

    # Write to /data/waterwatch.json relative to script location
    out_dir = Path(__file__).parent.parent / "site" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "waterwatch.json"

    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"\n  Written → {out_path}")

    # Print summary
    print("\n  Station status:")
    for key, s in payload["watershed_inflow"].items():
        level = f"{s['level_m']}m" if s["level_m"] is not None else "n/a"
        flow  = f"{s['discharge_cms']} cms" if s["discharge_cms"] is not None else "n/a"
        print(f"    {key:12} [{s['status']:8}]  level={level}  discharge={flow}")

    print(f"\n  Restriction: Stage {payload['restriction']['stage']}  "
          f"({payload['restriction']['days_remaining']} days remaining)")
    print("  Done.")


if __name__ == "__main__":
    main()
