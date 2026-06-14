#!/usr/bin/env python3
"""
waterwatch.criticalto.ca — national data scraper
Water supply intelligence for Canadian provincial and territorial capitals.

ONE script. ONE workflow. All cities.

Cities (Phase 1):
  vancouver    — Metro Vancouver, BC
  victoria     — Greater Victoria, BC
  calgary      — Calgary, AB
  edmonton     — Edmonton, AB
  canmore      — Canmore, AB (existing)

Outputs: site/data/{city_key}.json for each city

Licence: Open Government Licence – Canada (Environment and Climate Change Canada)
"""

import csv
import io
import json
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Primary: HPFX mirror — more reliable from automated clients during high-demand periods
# Fallback: dd.weather.gc.ca — same data, less reliable from GitHub Actions IPs
DATAMART_BASE = "https://hpfx.collab.science.gc.ca/today/hydrometric/csv"
DATAMART_FALLBACK = "https://dd.weather.gc.ca/today/hydrometric/csv"

# ── CITY CONFIGS ──────────────────────────────────────────────────────────────

VANCOUVER = {
    "meta": {
        "city": "Metro Vancouver",
        "province": "BC",
        "operator": "Metro Vancouver Regional District",
        "population_served": 2700000,
        "per_capita_lpd": 490,
        "per_capita_lpd_source": "Metro Vancouver 2023 Annual Water Report",
        "dashboard": "waterwatch.criticalto.ca/vancouver",
    },
    "stations": {
        "capilano":  {"id": "08GA010", "province": "BC", "name": "Capilano River above Intake",          "role": "primary_source"},
        "seymour":   {"id": "08GA030", "province": "BC", "name": "Seymour River at Seymour Falls",       "role": "primary_source"},
        "coquitlam": {"id": "08MH141", "province": "BC", "name": "Coquitlam River near Port Coquitlam", "role": "primary_source"},
    },
    "source": {
        "type": "surface",
        "groundwater_pct": 0,
        "primary": "Capilano, Seymour, and Coquitlam watersheds (closed)",
        "watershed_protected": True,
        "wfi_risk": "low_moderate",
        "wfi_note": "Closed watersheds with active management; no public access",
    },
    "storage": {
        "reservoirs": {
            "capilano":  {"name": "Capilano Reservoir",  "max_dam3": None, "pct_of_target": 68, "data_note": "Max capacity not publicly published; target % from Metro Van reports"},
            "seymour":   {"name": "Seymour Reservoir",   "max_dam3": None, "pct_of_target": 65, "data_note": "Max capacity not publicly published"},
            "coquitlam": {"name": "Coquitlam Reservoir", "max_dam3": None, "pct_of_target": 72, "data_note": "Max capacity not publicly published"},
        },
        "per_capita_dam3": None,
        "data_note": "Metro Vancouver does not publish reservoir volumes — only % of seasonal target",
        "last_updated": "2026-06-01",
        "source_url": "https://metrovancouver.org/services/water/reservoir-levels-water-use",
    },
    "treatment": {
        "plants": [
            {"name": "Seymour-Capilano Filtration Plant", "capacity_ml_day": 1800, "process": "membrane filtration, UV, chlorination"},
            {"name": "Coquitlam WTP",                     "capacity_ml_day": 600,  "process": "conventional, UV, chlorination"},
        ],
        "fluoridation": False,
        "fluoride_note": "Fluoride never added to Metro Vancouver water supply",
        "data_note": None,
    },
    "restriction": {
        "system": "formal",
        "system_url": "https://metrovancouver.org/services/water/water-restrictions",
        "stage": 3,
        "stage_label": "Stage 3",
        "start_date": "2026-06-08",
        "end_date": "2026-10-15",
        "fine_cad": 500,
        "prohibitions": ["lawn watering", "tree/shrub irrigation", "vehicle washing", "pressure washing", "pools/hot tubs"],
        "permitted": ["vegetable gardens", "hand watering"],
        "last_verified": "2026-06-08",
    },
    "snowpack": {
        "pct_of_normal": 53,
        "reference": "April 1 peak, South Coast region",
        "source": "BC River Forecast Centre",
        "note": "2026 reading — one of the lowest on record",
        "last_updated": "2026-05-01",
    },
    "risk": {
        "wfi_risk": "low_moderate",
        "infrastructure_risk": "low",
        "single_point_of_failure": None,
        "groundwater_dependency": False,
        "climate_note": "Low snowpack (53% of normal) driving Stage 3 restrictions; drought risk elevated",
    },
    "use": {
        "per_capita_lpd": 490,
        "residential_pct": 55,
        "industrial_pct": 15,
        "commercial_pct": 30,
        "major_industrial_users": None,
        "data_note": "Sector split approximate from Metro Vancouver annual report",
    },
    "egress": {
        "treatment_type": "secondary",
        "plants": ["Annacis Island WWTP", "Iona Island WWTP", "Lulu Island WWTP", "Northwest Langley WWTP"],
        "discharge_point": "Fraser River / Strait of Georgia",
        "cso_risk": "moderate",
        "cso_note": "Combined sewer overflow risk during heavy rain in older municipal areas",
        "data_note": None,
    },
    # Legacy
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

VICTORIA = {
    "meta": {
        "city": "Greater Victoria",
        "province": "BC",
        "operator": "Capital Regional District (CRD) Integrated Water Services",
        "population_served": 400000,
        "per_capita_lpd": 330,
        "per_capita_lpd_source": "CRD 2023 Annual Water Report",
        "dashboard": "waterwatch.criticalto.ca/victoria",
    },
    "stations": {
        "sooke_river": {"id": "08HA010", "province": "BC", "name": "Sooke River below Millar Creek",          "role": "watershed_inflow"},
        "sooke_upper": {"id": "08HA059", "province": "BC", "name": "Sooke River upstream of Charters Creek",  "role": "upstream_context"},
    },
    "source": {
        "type": "surface",
        "groundwater_pct": 0,
        "primary": "Sooke Lake Reservoir (primary), Goldstream River (secondary), Thetis Lake (tertiary)",
        "watershed_protected": True,
        "wfi_risk": "moderate",
        "wfi_note": "Sooke watershed adjacent to fire-prone terrain on Vancouver Island; CRD owns entire watershed",
    },
    "storage": {
        "reservoirs": {
            "sooke_lake": {
                "name": "Sooke Lake Reservoir",
                "max_dam3": 160320,
                "usable_dam3": 92700,
                "data_note": "~2-year supply at current demand; CRD owns entire watershed",
            },
        },
        "per_capita_dam3": 0.401,
        "per_capita_dam3_note": "Based on usable capacity / population served",
        "data_note": "CRD does not publish real-time reservoir levels; values from CRD annual report",
        "last_updated": "2026-01-01",
        "source_url": "https://www.crd.bc.ca/service/drinking-water",
    },
    "treatment": {
        "plants": [
            {"name": "CRD Drinking Water Treatment Plant", "capacity_ml_day": 330, "process": "UV disinfection, chlorination (no filtration — closed watershed)"},
        ],
        "fluoridation": False,
        "fluoride_note": "Fluoride removed in 2011 by CRD board vote",
        "data_note": None,
    },
    "restriction": {
        "system": "formal",
        "system_url": "https://www.crd.bc.ca/service/drinking-water/water-conservation/watering-restrictions",
        "stage": 0,
        "stage_label": "No active restrictions",
        "start_date": None,
        "end_date": None,
        "fine_cad": None,
        "prohibitions": [],
        "permitted": [],
        "last_verified": "2026-06-08",
        "hypothesized_triggers": "CRD activates Stage 1 when reservoir approaches ~70% of capacity heading into summer",
    },
    "snowpack": {
        "pct_of_normal": 58,
        "reference": "April 1 peak, Vancouver Island",
        "source": "BC River Forecast Centre",
        "note": "2026 reading — below average",
        "last_updated": "2026-05-01",
    },
    "risk": {
        "wfi_risk": "moderate",
        "infrastructure_risk": "low_moderate",
        "single_point_of_failure": "Sooke Flowline (44km concrete aqueduct, built 1915) — aging primary conveyance",
        "groundwater_dependency": False,
        "climate_note": "Below-average snowpack; 2-year reservoir buffer provides resilience. Sooke Flowline age (~110 years) is a growing concern.",
    },
    "use": {
        "per_capita_lpd": 330,
        "residential_pct": 62,
        "industrial_pct": 8,
        "commercial_pct": 30,
        "major_industrial_users": None,
        "data_note": "Sector split from CRD annual report",
    },
    "egress": {
        "treatment_type": "secondary",
        "plants": ["McLoughlin Point WWTP"],
        "discharge_point": "Strait of Juan de Fuca",
        "cso_risk": "low",
        "cso_note": "Victoria controversially discharged raw sewage until 2020; McLoughlin Point upgrade completed that year",
        "data_note": None,
    },
}

CALGARY = {
    "meta": {
        "city": "Calgary",
        "province": "AB",
        "operator": "City of Calgary Water Services",
        "population_served": 1400000,
        "per_capita_lpd": 215,
        "per_capita_lpd_source": "City of Calgary 2023 Water Use Report",
        "dashboard": "waterwatch.criticalto.ca/calgary",
    },
    "stations": {
        "bow_calgary":    {"id": "05BH004", "province": "AB", "name": "Bow River at Calgary",           "role": "primary_source"},
        "elbow_glenmore": {"id": "05BJ001", "province": "AB", "name": "Elbow River below Glenmore Dam", "role": "primary_source"},
        "elbow_bragg":    {"id": "05BJ004", "province": "AB", "name": "Elbow River at Bragg Creek",     "role": "upstream_context"},
    },
    "source": {
        "type": "surface",
        "groundwater_pct": 0,
        "primary": "Bow River (Bearspaw WTP, ~60%) + Elbow River (Glenmore WTP, ~40%)",
        "watershed_protected": False,
        "wfi_risk": "low",
        "wfi_note": "Prairie city; upstream Bow/Elbow watersheds have low fire risk",
    },
    "storage": {
        "reservoirs": {
            "bearspaw": {
                "name": "Bearspaw Reservoir",
                "max_dam3": None,
                "data_note": "Run-of-river — negligible storage. Real-time data not publicly available.",
            },
            "glenmore": {
                "name": "Glenmore Reservoir",
                "max_dam3": 23502,
                "station_id": "05BJ008",
                "data_note": "Live data from Alberta Rivers PDF",
            },
        },
        "per_capita_dam3": 0.017,
        "per_capita_dam3_note": "Glenmore only; Bearspaw has negligible storage",
        "data_note": "Calgary has very limited reservoir storage — supply depends on continuous river inflow",
        "last_updated": None,
    },
    "treatment": {
        "plants": [
            {"name": "Bearspaw WTP", "capacity_ml_day": 800, "process": "conventional coagulation/flocculation, filtration, UV, chlorination"},
            {"name": "Glenmore WTP", "capacity_ml_day": 400, "process": "conventional coagulation/flocculation, filtration, UV, chlorination"},
        ],
        "fluoridation": True,
        "fluoride_note": None,
        "data_note": None,
    },
    "restriction": {
        "system": "informal",
        "system_url": "https://www.calgary.ca/water/water-conservation.html",
        "stage": 0,
        "stage_label": "No active restrictions",
        "start_date": None,
        "end_date": None,
        "fine_cad": None,
        "prohibitions": [],
        "permitted": [],
        "last_verified": "2026-06-08",
        "hypothesized_triggers": "Calgary has no formal staged restriction bylaw. Conservation advisories issued at operator discretion during drought or infrastructure events.",
    },
    "snowpack": {
        "pct_of_normal": 72,
        "reference": "April 1 peak, Bow River Basin",
        "source": "Alberta River Forecast Centre",
        "note": "2026 reading — below average",
        "last_updated": "2026-05-01",
    },
    "risk": {
        "wfi_risk": "low",
        "infrastructure_risk": "high",
        "single_point_of_failure": "Bearspaw South Feeder Main (PCCP 1970s) — failures June 2024 and Dec 30 2025. Reinforced April 2026; parallel main under construction.",
        "groundwater_dependency": False,
        "climate_note": "Infrastructure risk dominates. Feeder main failure Dec 2025 exposed single-point-of-failure vulnerability.",
    },
    "use": {
        "per_capita_lpd": 215,
        "residential_pct": 58,
        "industrial_pct": 22,
        "commercial_pct": 20,
        "major_industrial_users": None,
        "data_note": "Sector split approximate from City of Calgary annual report",
    },
    "egress": {
        "treatment_type": "secondary",
        "plants": ["Bonnybrook WWTP", "Fish Creek WWTP", "Pine Creek WWTP"],
        "discharge_point": "Bow River (downstream of city)",
        "cso_risk": "low",
        "cso_note": None,
        "data_note": None,
    },
    "_fetch_glenmore": True,
}

EDMONTON = {
    "meta": {
        "city": "Edmonton",
        "province": "AB",
        "operator": "EPCOR Water Services Inc.",
        "population_served": 1100000,
        "per_capita_lpd": 225,
        "per_capita_lpd_source": "EPCOR 2023 Annual Water Quality Report",
        "dashboard": "waterwatch.criticalto.ca/edmonton",
    },
    "stations": {
        "nsr_edmonton": {"id": "05DF001", "province": "AB", "name": "North Saskatchewan River at Edmonton",        "role": "primary_source"},
        "nsr_upstream": {"id": "05DA006", "province": "AB", "name": "North Saskatchewan River at Whirlpool Point", "role": "upstream_context"},
    },
    "source": {
        "type": "surface",
        "groundwater_pct": 0,
        "primary": "North Saskatchewan River (sole source — no reservoir backup)",
        "watershed_protected": False,
        "wfi_risk": "moderate_high",
        "wfi_note": "NSR headwaters in Rocky Mountain foothills. 2023 upstream fires caused turbidity spikes requiring operational response.",
    },
    "storage": {
        "reservoirs": {
            "distribution_only": {
                "name": "Distribution reservoirs only (~72-hr storage)",
                "max_dam3": None,
                "data_note": "No source water reservoir. Edmonton draws directly from NSR and holds ~72 hours of treated water in distribution storage.",
            },
        },
        "per_capita_dam3": 0.001,
        "per_capita_dam3_note": "Effectively zero source storage — distribution reservoirs only",
        "data_note": "Edmonton's no-reservoir design is its defining water vulnerability. Any NSR disruption is immediately critical.",
        "last_updated": None,
    },
    "treatment": {
        "plants": [
            {"name": "E.L. Smith WTP", "capacity_ml_day": 800, "process": "conventional coagulation/sedimentation/filtration, UV, chlorination, fluoride"},
            {"name": "Rossdale WTP",   "capacity_ml_day": 270, "process": "conventional coagulation/sedimentation/filtration, UV, chlorination, fluoride"},
        ],
        "fluoridation": True,
        "fluoride_note": None,
        "data_note": "Both plants draw directly from NSR. No alternative source if NSR is compromised.",
    },
    "restriction": {
        "system": "informal",
        "system_url": "https://www.edmonton.ca/programs_services/water/water-conservation",
        "stage": 0,
        "stage_label": "No active restrictions",
        "start_date": None,
        "end_date": None,
        "fine_cad": None,
        "prohibitions": [],
        "permitted": [],
        "last_verified": "2026-06-08",
        "hypothesized_triggers": "EPCOR issues conservation advisories during low-flow events or upstream contamination. No formal staged bylaw.",
    },
    "snowpack": {
        "pct_of_normal": 78,
        "reference": "April 1 peak, North Saskatchewan Basin",
        "source": "Alberta River Forecast Centre",
        "note": "2026 reading — near normal",
        "last_updated": "2026-05-01",
    },
    "risk": {
        "wfi_risk": "moderate_high",
        "infrastructure_risk": "moderate",
        "single_point_of_failure": "North Saskatchewan River is sole source — no reservoir, no alternative intake. Contamination or prolonged low-flow = immediate supply crisis.",
        "groundwater_dependency": False,
        "climate_note": "NSR carries agricultural runoff and upstream oil sands drainage. Wildfire turbidity events increasing. No storage buffer.",
    },
    "use": {
        "per_capita_lpd": 225,
        "residential_pct": 55,
        "industrial_pct": 25,
        "commercial_pct": 20,
        "major_industrial_users": None,
        "data_note": "Sector split approximate from EPCOR annual report",
    },
    "egress": {
        "treatment_type": "secondary",
        "plants": ["Gold Bar WWTP"],
        "discharge_point": "North Saskatchewan River (downstream of city)",
        "cso_risk": "low_moderate",
        "cso_note": "Older combined sewer areas in river valley neighbourhoods have CSO risk during heavy rain",
        "data_note": None,
    },
}

CANMORE = {
    "meta": {
        "city": "Canmore",
        "province": "AB",
        "operator": "EPCOR Water Services (on behalf of Town of Canmore)",
        "population_served": 16000,
        "per_capita_lpd": None,
        "per_capita_lpd_source": None,
        "per_capita_lpd_data_note": "Not publicly reported by EPCOR for Canmore separately",
        "dashboard": "waterwatch.criticalto.ca/canmore",
    },
    "stations": {
        "bow_banff":    {"id": "05BB001", "province": "AB", "name": "Bow River at Banff (25km upstream)",             "role": "upstream_context"},
        "spray_banff":  {"id": "05BC001", "province": "AB", "name": "Spray River at Banff",                           "role": "upstream_context"},
        "waiparous":    {"id": "05BG006", "province": "AB", "name": "Waiparous Creek near Cochrane",                  "role": "tributary"},
        "bow_cochrane": {"id": "05BH005", "province": "AB", "name": "Bow River near Cochrane (downstream indicator)", "role": "downstream_context"},
    },
    "source": {
        "type": "blended",
        "groundwater_pct": 50,
        "primary": "Rundle Forebay (surface, ~50%) + Bow River aquifer (groundwater, ~50%)",
        "watershed_protected": False,
        "wfi_risk": "moderate",
        "wfi_note": "Kananaskis watershed has active wildfire history",
    },
    "storage": {
        "reservoirs": {
            "rundle_forebay": {
                "name": "Rundle Forebay",
                "max_dam3": None,
                "data_note": "No public API or real-time data. Fed by Spray Lakes reservoir via TransAlta canal system.",
            },
        },
        "per_capita_dam3": None,
        "data_note": "No public reservoir storage data available for Canmore",
        "last_updated": None,
    },
    "treatment": {
        "plants": [
            {"name": "Canmore WTP (EPCOR operated)", "capacity_ml_day": None, "process": "Not publicly detailed"},
        ],
        "fluoridation": None,
        "fluoride_note": None,
        "data_note": "Treatment details not publicly available from EPCOR for Canmore",
    },
    "restriction": {
        "system": "informal",
        "system_url": "https://www.canmore.ca/your-community/public-safety/hazard-monitoring/drought-monitoring",
        "stage": 0,
        "stage_label": "No active restrictions",
        "start_date": None,
        "end_date": None,
        "fine_cad": None,
        "prohibitions": [],
        "permitted": [],
        "last_verified": "2026-06-08",
        "note": "EPCOR manages under utility agreement with the Town until 2030.",
    },
    "snowpack": {
        "pct_of_normal": 68,
        "reference": "April 1 peak, Bow River Basin",
        "source": "Alberta River Forecast Centre",
        "note": "2026 reading — below average but not crisis level",
        "last_updated": "2026-05-01",
    },
    "risk": {
        "wfi_risk": "moderate",
        "infrastructure_risk": "low",
        "single_point_of_failure": None,
        "groundwater_dependency": True,
        "climate_note": "Below-average snowpack affects both surface and groundwater recharge",
    },
    "use": {
        "per_capita_lpd": None,
        "residential_pct": None,
        "industrial_pct": None,
        "commercial_pct": None,
        "major_industrial_users": None,
        "data_note": "Usage data not publicly reported for Canmore by EPCOR",
    },
    "egress": {
        "treatment_type": "secondary",
        "plants": ["Canmore WWTP"],
        "discharge_point": "Bow River",
        "cso_risk": "low",
        "cso_note": None,
        "data_note": "Capacity details not publicly available",
    },
    # Legacy canmore.html compatibility
    "watershed_context": {
        "primary_source": "Rundle Forebay (surface water, ~50% of supply)",
        "secondary_source": "Bow River aquifer (groundwater, ~50% of supply)",
        "forebay_fed_by": "Spray Lakes reservoir via TransAlta canal system (Kananaskis Country)",
        "operator": "EPCOR Water Services Inc.",
        "agreement_expires": "2030",
        "note": "No public API for Rundle Forebay levels. Bow River at Banff is the best available public indicator of watershed health.",
        "source": "EPCOR Canada / Town of Canmore",
        "source_url": "https://www.epcor.com/ca/en/about/our-company/where-we-operate/canmore.html",
    },
}


# ── REGINA config ─────────────────────────────────────────────────────────────
REGINA = {
    "meta": {
        "city": "Regina",
        "province": "SK",
        "operator": "Buffalo Pound Water Treatment Plant (City of Regina + City of Saskatoon)",
        "population_served": 250000,
        "per_capita_lpd": 330,
        "per_capita_lpd_source": "City of Regina 2023 Water Quality Report",
        "dashboard": "waterwatch.criticalto.ca/regina",
    },
    "stations": {
        "south_sask_saskatoon": {"id": "05HG001", "province": "SK", "name": "South Saskatchewan River at Saskatoon (upstream regional indicator)", "role": "upstream_context"},
        "qu_appelle_lumsden":   {"id": "05JF006", "province": "SK", "name": "Qu'Appelle River near Lumsden",                                      "role": "downstream_context"},
    },
    "source": {
        "type": "surface",
        "groundwater_pct": 0,
        "primary": "Buffalo Pound Lake (Qu'Appelle River system, Last Mountain Lake drainage)",
        "watershed_protected": False,
        "wfi_risk": "low",
        "wfi_note": "Prairie watershed — negligible wildfire risk",
    },
    "storage": {
        "reservoirs": {
            "buffalo_pound": {
                "name": "Buffalo Pound Lake",
                "max_dam3": 112000,
                "data_note": "Natural lake used as source reservoir. Levels not published in WSC real-time network.",
            },
        },
        "per_capita_dam3": 0.448,
        "per_capita_dam3_note": "Buffalo Pound Lake max capacity / Regina share of population served",
        "data_note": "Buffalo Pound WTP serves both Regina and Saskatoon (~500,000 combined). Capacity shown is total lake volume.",
        "last_updated": None,
    },
    "treatment": {
        "plants": [
            {"name": "Buffalo Pound WTP", "capacity_ml_day": 230, "process": "conventional coagulation/sedimentation/filtration, UV, chlorination, fluoride, PAC for taste/odour"},
        ],
        "fluoridation": True,
        "fluoride_note": None,
        "data_note": "Buffalo Pound WTP is a shared provincial asset co-owned by Regina and Saskatoon. Single plant serving two major cities is a shared vulnerability.",
    },
    "restriction": {
        "system": "informal",
        "system_url": "https://www.regina.ca/home-property/water-sewer/water-conservation/",
        "stage": 0,
        "stage_label": "No active restrictions",
        "start_date": None,
        "end_date": None,
        "fine_cad": None,
        "prohibitions": [],
        "permitted": [],
        "last_verified": "2026-06-14",
        "hypothesized_triggers": "Regina does not have a formal staged restriction bylaw. Seasonal lawn watering advisories issued during dry periods.",
    },
    "snowpack": {
        "pct_of_normal": 82,
        "reference": "April 1 peak, Qu'Appelle Basin",
        "source": "Saskatchewan Water Security Agency",
        "note": "2026 reading — near normal",
        "last_updated": "2026-05-01",
    },
    "risk": {
        "wfi_risk": "low",
        "infrastructure_risk": "moderate",
        "single_point_of_failure": "Buffalo Pound WTP is the sole treatment plant for both Regina and Saskatoon — a single facility serving ~500,000 people with no backup treatment capacity.",
        "groundwater_dependency": False,
        "climate_note": "Buffalo Pound Lake experiences recurring blue-green algae (cyanobacteria) blooms driven by agricultural nutrient loading from the Qu'Appelle watershed. Blooms require activated carbon treatment and can stress plant capacity.",
    },
    "use": {
        "per_capita_lpd": 330,
        "residential_pct": 60,
        "industrial_pct": 20,
        "commercial_pct": 20,
        "major_industrial_users": None,
        "data_note": "Sector split approximate from City of Regina annual report",
    },
    "egress": {
        "treatment_type": "secondary",
        "plants": ["Regina WWTP"],
        "discharge_point": "Wascana Creek / Qu'Appelle River system",
        "cso_risk": "low",
        "cso_note": None,
        "data_note": None,
    },
}

# ── WINNIPEG config ────────────────────────────────────────────────────────────
WINNIPEG = {
    "meta": {
        "city": "Winnipeg",
        "province": "MB",
        "operator": "City of Winnipeg Water and Waste Department",
        "population_served": 800000,
        "per_capita_lpd": 280,
        "per_capita_lpd_source": "City of Winnipeg 2023 Water Quality Report",
        "dashboard": "waterwatch.criticalto.ca/winnipeg",
    },
    "stations": {
        "red_river":   {"id": "05OJ001", "province": "MB", "name": "Red River at Winnipeg",              "role": "regional_context"},
        "assiniboine": {"id": "05QB004", "province": "MB", "name": "Assiniboine River at Headingley",    "role": "regional_context"},
    },
    "source": {
        "type": "surface",
        "groundwater_pct": 0,
        "primary": "Shoal Lake (Lake of the Woods), Ontario/Manitoba border — conveyed 154km via Greater Winnipeg Water District Aqueduct",
        "watershed_protected": True,
        "wfi_risk": "low",
        "wfi_note": "Boreal Shield source watershed; low wildfire transmission risk to intake",
    },
    "storage": {
        "reservoirs": {
            "shoal_lake": {
                "name": "Shoal Lake (natural lake)",
                "max_dam3": None,
                "data_note": "Shoal Lake is a large natural lake — capacity effectively unlimited relative to Winnipeg's demand. Real-time levels not in WSC network.",
            },
        },
        "per_capita_dam3": None,
        "data_note": "Shoal Lake provides effectively unlimited storage. The constraint is aqueduct capacity (390 ML/day), not lake volume.",
        "last_updated": None,
    },
    "treatment": {
        "plants": [
            {"name": "Deacon WTP", "capacity_ml_day": 390, "process": "UV, chlorination, fluoride, orthophosphate (corrosion control), opened 2009"},
        ],
        "fluoridation": True,
        "fluoride_note": None,
        "data_note": "The 154km gravity-fed aqueduct from Shoal Lake was built in 1919. Deacon WTP (2009) replaced the original treatment infrastructure. Orthophosphate added for lead pipe corrosion control.",
    },
    "restriction": {
        "system": "formal",
        "system_url": "https://www.winnipeg.ca/waterandwaste/water/conservation/",
        "stage": 0,
        "stage_label": "No active restrictions",
        "start_date": None,
        "end_date": None,
        "fine_cad": None,
        "prohibitions": [],
        "permitted": [],
        "last_verified": "2026-06-14",
        "hypothesized_triggers": "Winnipeg has a seasonal odd/even outdoor watering bylaw. Full restrictions uncommon given large source volume.",
    },
    "snowpack": {
        "pct_of_normal": 91,
        "reference": "April 1 peak, Lake of the Woods Basin",
        "source": "Manitoba Hydrological Forecast Centre",
        "note": "2026 reading — near normal",
        "last_updated": "2026-05-01",
    },
    "risk": {
        "wfi_risk": "low",
        "infrastructure_risk": "moderate",
        "single_point_of_failure": "The Greater Winnipeg Water District Aqueduct (154km, built 1919) is the sole conveyance from Shoal Lake. A major aqueduct failure would cut supply with no immediate alternative.",
        "groundwater_dependency": False,
        "climate_note": "Source water security is excellent. The primary risk is the aging aqueduct and the justice implications of the Shoal Lake #40 First Nation situation.",
    },
    "use": {
        "per_capita_lpd": 280,
        "residential_pct": 58,
        "industrial_pct": 22,
        "commercial_pct": 20,
        "major_industrial_users": None,
        "data_note": "Sector split approximate from City of Winnipeg annual report",
    },
    "egress": {
        "treatment_type": "secondary",
        "plants": ["North End Water Pollution Control Centre", "South End Water Pollution Control Centre"],
        "discharge_point": "Red River",
        "cso_risk": "moderate",
        "cso_note": "Winnipeg has combined sewers in older neighbourhoods. North End WPCC upgrades ongoing to reduce CSO events.",
        "data_note": None,
    },
}

# ── TORONTO config ─────────────────────────────────────────────────────────────
TORONTO = {
    "meta": {
        "city": "Toronto",
        "province": "ON",
        "operator": "City of Toronto (Toronto Water)",
        "population_served": 3600000,
        "per_capita_lpd": 200,
        "per_capita_lpd_source": "Toronto Water 2023 Annual Report",
        "dashboard": "waterwatch.criticalto.ca/toronto",
    },
    "stations": {
        "don_river":    {"id": "02HC003", "province": "ON", "name": "Don River at Todmorden (watershed indicator)", "role": "watershed_indicator"},
        "humber_river": {"id": "02HC009", "province": "ON", "name": "Humber River at Raymore Drive",               "role": "watershed_indicator"},
    },
    "source": {
        "type": "surface",
        "groundwater_pct": 0,
        "primary": "Lake Ontario (sole source) — intake depth 6–9m, 1–3km offshore",
        "watershed_protected": False,
        "wfi_risk": "negligible",
        "wfi_note": "Great Lakes source — no wildfire interface risk",
        "lake_ontario_note": "Lake Ontario levels managed by IJC Plan 2014. Daily mean level updated by IJC on business days. Current level: 75.37m IGLD 1985.",
    },
    "storage": {
        "reservoirs": {
            "lake_ontario": {
                "name": "Lake Ontario (de facto reservoir)",
                "max_dam3": 1639000000,
                "data_note": "Lake Ontario has ~1,639 km³ of water — effectively unlimited storage. Intake conditions (turbidity, temperature, algae) are the operational variable, not volume.",
            },
        },
        "per_capita_dam3": None,
        "data_note": "Lake Ontario provides essentially unlimited supply. Toronto Water's constraint is treatment capacity and distribution infrastructure, not source volume.",
        "last_updated": None,
        "source_url": "https://ijc.org/en/loslrb/watershed/water-levels",
    },
    "treatment": {
        "plants": [
            {"name": "R.C. Harris WTP",    "capacity_ml_day": 950,  "process": "conventional coagulation/sedimentation/filtration, UV, chlorination, fluoride. Built 1941 — Art Deco landmark."},
            {"name": "F.J. Horgan WTP",    "capacity_ml_day": 950,  "process": "conventional + UV + chlorination + fluoride"},
            {"name": "R.L. Clark WTP",     "capacity_ml_day": 600,  "process": "conventional + UV + chlorination + fluoride"},
            {"name": "Island WTP",         "capacity_ml_day": 100,  "process": "conventional + UV + chlorination + fluoride. Serves Toronto Island."},
        ],
        "fluoridation": True,
        "fluoride_note": None,
        "data_note": "Total treatment capacity ~2,600 ML/day for a city using ~700 ML/day — significant redundancy. R.C. Harris WTP (1941) is a designated heritage building.",
    },
    "restriction": {
        "system": "formal",
        "system_url": "https://www.toronto.ca/services-payments/water-environment/tap-water-in-toronto/",
        "stage": 0,
        "stage_label": "No active restrictions",
        "start_date": None,
        "end_date": None,
        "fine_cad": None,
        "prohibitions": [],
        "permitted": [],
        "last_verified": "2026-06-14",
        "hypothesized_triggers": "Toronto has a seasonal outdoor watering bylaw (odd/even by address). Full restrictions rare given Great Lakes source.",
    },
    "snowpack": {
        "pct_of_normal": None,
        "reference": "Not applicable — Lake Ontario source",
        "source": None,
        "note": "Toronto's supply is from Lake Ontario, not snowmelt-dependent watersheds. IJC manages lake levels under Plan 2014.",
        "last_updated": None,
    },
    "risk": {
        "wfi_risk": "negligible",
        "infrastructure_risk": "low_moderate",
        "single_point_of_failure": "None at source level — Lake Ontario is effectively unlimited. Distribution network age (many pipes pre-1950) is the primary infrastructure risk.",
        "groundwater_dependency": False,
        "climate_note": "Lake Ontario levels are above long-term average (75.37m vs historical mean). Climate risk is increasing algae and turbidity events near intakes, not supply volume.",
    },
    "use": {
        "per_capita_lpd": 200,
        "residential_pct": 55,
        "industrial_pct": 20,
        "commercial_pct": 25,
        "major_industrial_users": None,
        "data_note": "Toronto has one of the lowest per-capita uses of any major Canadian city — result of decades of conservation programming and metering.",
    },
    "egress": {
        "treatment_type": "tertiary",
        "plants": ["Humber WWTP", "Highland Creek WWTP", "North Toronto WWTP", "Ashbridges Bay WWTP"],
        "discharge_point": "Lake Ontario",
        "cso_risk": "moderate",
        "cso_note": "Combined sewers in older (pre-1950) neighbourhoods. Wet weather flow management program ongoing. Ashbridges Bay WTP receives the largest volume.",
        "data_note": "Toronto is one of few Canadian cities with tertiary wastewater treatment — phosphorus removal before Lake Ontario discharge.",
    },
}

# ── OTTAWA config ──────────────────────────────────────────────────────────────
OTTAWA = {
    "meta": {
        "city": "Ottawa",
        "province": "ON",
        "operator": "City of Ottawa (Ottawa Water Services)",
        "population_served": 1100000,
        "per_capita_lpd": 230,
        "per_capita_lpd_source": "City of Ottawa 2023 Drinking Water Quality Report",
        "dashboard": "waterwatch.criticalto.ca/ottawa",
    },
    "stations": {
        "ottawa_river":  {"id": "02KF005", "province": "ON", "name": "Ottawa River at Ottawa (Britannia)",  "role": "primary_source"},
        "rideau_river":  {"id": "02LA004", "province": "ON", "name": "Rideau River at Ottawa",              "role": "secondary_source"},
    },
    "source": {
        "type": "surface",
        "groundwater_pct": 0,
        "primary": "Ottawa River (primary, ~90% of supply via Britannia WTP) + Rideau River (secondary, ~10% via Lemieux Island WTP)",
        "watershed_protected": False,
        "wfi_risk": "low",
        "wfi_note": "Ottawa and Rideau River watersheds have low wildfire interface risk",
    },
    "storage": {
        "reservoirs": {
            "direct_intake": {
                "name": "No reservoir — direct river intakes",
                "max_dam3": None,
                "data_note": "Ottawa draws directly from the Ottawa and Rideau Rivers with no storage reservoir. Distribution reservoirs provide limited buffer.",
            },
        },
        "per_capita_dam3": None,
        "data_note": "No source storage — similar vulnerability to Edmonton but mitigated by dual-river sourcing and higher baseline flows.",
        "last_updated": None,
    },
    "treatment": {
        "plants": [
            {"name": "Britannia WTP",     "capacity_ml_day": 680, "process": "conventional + UV + chlorination + fluoride. Draws from Ottawa River."},
            {"name": "Lemieux Island WTP","capacity_ml_day": 220, "process": "conventional + UV + chlorination + fluoride. Draws from Rideau River."},
        ],
        "fluoridation": True,
        "fluoride_note": None,
        "data_note": "Dual-source design provides operational redundancy. Ottawa River carries significant runoff from the Quebec highlands; Rideau River has chronic cyanobacteria issues in late summer.",
    },
    "restriction": {
        "system": "formal",
        "system_url": "https://ottawa.ca/en/residents/water-and-environment/water-services/water-conservation",
        "stage": 0,
        "stage_label": "No active restrictions",
        "start_date": None,
        "end_date": None,
        "fine_cad": None,
        "prohibitions": [],
        "permitted": [],
        "last_verified": "2026-06-14",
        "hypothesized_triggers": "Ottawa uses seasonal watering bylaws (odd/even). Full restrictions historically rare.",
    },
    "snowpack": {
        "pct_of_normal": 88,
        "reference": "April 1 peak, Ottawa River Basin",
        "source": "Ottawa River Regulation Planning Board",
        "note": "2026 reading — near normal",
        "last_updated": "2026-05-01",
    },
    "risk": {
        "wfi_risk": "low",
        "infrastructure_risk": "low_moderate",
        "single_point_of_failure": "Ottawa River is primary source for ~90% of supply. A contamination event upstream (Quebec industrial corridor) could force full reliance on Rideau River, which has capacity for only ~25% of demand.",
        "groundwater_dependency": False,
        "climate_note": "Rideau River cyanobacteria blooms are increasing in frequency with warming summers. Ottawa River carries agricultural runoff from the Quebec highlands upstream.",
    },
    "use": {
        "per_capita_lpd": 230,
        "residential_pct": 58,
        "industrial_pct": 20,
        "commercial_pct": 22,
        "major_industrial_users": None,
        "data_note": "Sector split approximate from City of Ottawa annual report",
    },
    "egress": {
        "treatment_type": "secondary",
        "plants": ["Robert O. Pickard Environmental Centre (ROPEC)"],
        "discharge_point": "Ottawa River",
        "cso_risk": "moderate",
        "cso_note": "Combined sewers in older urban areas. ROPEC handles all Ottawa wastewater.",
        "data_note": None,
    },
}

# ── CITIES REGISTRY ───────────────────────────────────────────────────────────
CITIES = {
    "vancouver": VANCOUVER,
    "victoria":  VICTORIA,
    "calgary":   CALGARY,
    "edmonton":  EDMONTON,
    "canmore":   CANMORE,
    "regina":    REGINA,
    "winnipeg":  WINNIPEG,
    "toronto":   TORONTO,
    "ottawa":    OTTAWA,
}

# ── GLENMORE RESERVOIR (Calgary-specific) ────────────────────────────────────
GLENMORE_PDF_URL = "https://rivers.alberta.ca/forecasting/data/reports/Res_storage.pdf"
GLENMORE_STATION = "05BJ008"


def fetch_glenmore() -> dict:
    """Parse Glenmore Reservoir storage from Alberta Rivers PDF.

    pdfminer extracts PDF columns as separate vertical blocks.
    We find 05BJ008's ordinal position in the station ID block,
    then extract the same position from storage, percentage, and status blocks.
    """
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
    except Exception as e:
        return {"status": f"pdfminer_error: {str(e)[:80]}"}

    id_pattern = re.compile(
        r'((?:[\w-]+\n){1,10}' + re.escape(GLENMORE_STATION) + r'\n(?:[\w-]+\n){0,10}Total)',
        re.MULTILINE
    )
    id_match = id_pattern.search(text)
    if not id_match:
        return {"status": "id_block_not_found"}

    ids = [l.strip() for l in id_match.group(0).strip().split('\n') if l.strip()]
    try:
        pos = ids.index(GLENMORE_STATION)
    except ValueError:
        return {"status": "station_not_in_block"}

    n         = len(ids)
    after_ids = text[id_match.end():]

    def get_nth(block_text, i):
        vals = [l.strip() for l in block_text.strip().split('\n') if l.strip()]
        return vals[i] if i < len(vals) else None

    num_pat    = re.compile(r'(?:(?:[\d,]+|-)\n){%d}' % n, re.MULTILINE)
    num_blocks = list(num_pat.finditer(after_ids))
    if len(num_blocks) < 2:
        return {"status": f"number_blocks_insufficient ({len(num_blocks)})"}

    storage_raw = get_nth(num_blocks[0].group(), pos)
    max_raw     = get_nth(num_blocks[1].group(), pos)
    storage = int(storage_raw.replace(',', '')) if storage_raw and storage_raw != '-' else None
    max_s   = int(max_raw.replace(',', ''))     if max_raw     and max_raw     != '-' else None

    pct_pat    = re.compile(r'(?:(?:\d+%|-)\n){%d}' % n, re.MULTILINE)
    pct_blocks = list(pct_pat.finditer(after_ids))
    pct_raw    = get_nth(pct_blocks[0].group(), pos) if pct_blocks else None
    pct        = int(pct_raw.replace('%', '')) if pct_raw and pct_raw != '-' else None

    status_pat    = re.compile(r'(?:(?:ABOVE|NORMAL|BELOW|-)\n){%d}' % n, re.MULTILINE)
    status_blocks = list(status_pat.finditer(after_ids))
    status        = get_nth(status_blocks[0].group(), pos).strip() if status_blocks else "UNKNOWN"

    dates = re.findall(r'\d{4}-\d{2}-\d{2}', text)
    date  = dates[0] if dates else None

    print(f"    [Glenmore] {storage} dam3 / {max_s} = {pct}% ({status})  date={date}")
    return {
        "station_id":         GLENMORE_STATION,
        "storage_dam3":       storage,
        "max_dam3":           max_s,
        "pct_capacity":       pct,
        "compared_to_normal": status,
        "reading_date":       date,
        "status":             "ok",
    }


# ── STATION FETCH ─────────────────────────────────────────────────────────────

def fetch_station(station: dict) -> dict:
    """Fetch latest hourly reading from Environment Canada Datamart."""
    sid  = station["id"]
    prov = station["province"]
    url  = f"{DATAMART_BASE}/{prov}/hourly/{prov}_{sid}_hourly_hydrometric.csv"
    result = {
        "station_id":    sid,
        "station_name":  station["name"],
        "role":          station.get("role", "unknown"),
        "url":           url,
        "level_m":       None,
        "discharge_cms": None,
        "timestamp":     None,
        "status":        "unknown",
    }
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "waterwatch-criticalto/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except Exception:
            # Fallback to dd.weather.gc.ca mirror
            fallback_url = url.replace(DATAMART_BASE, DATAMART_FALLBACK)
            req2 = urllib.request.Request(fallback_url, headers={"User-Agent": "waterwatch-criticalto/1.0"})
            with urllib.request.urlopen(req2, timeout=20) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        rows      = [r for r in csv.reader(io.StringIO(raw)) if r and r[0].strip()]
        data_rows = [r for r in rows[1:] if len(r) >= 7]
        if not data_rows:
            result["status"] = "no_data"
            return result
        latest = data_rows[-1]
        result["timestamp"]     = latest[1].strip()
        result["level_m"]       = float(latest[2]) if latest[2].strip() else None
        result["discharge_cms"] = float(latest[6]) if latest[6].strip() else None
        result["status"]        = "ok"
    except urllib.error.HTTPError as e:
        result["status"] = f"http_error_{e.code}"
    except Exception as e:
        result["status"] = f"error: {str(e)[:80]}"
    return result


# ── PAYLOAD BUILDER ───────────────────────────────────────────────────────────

def build_city_payload(city_key: str, config: dict) -> dict:
    """Build the full JSON payload for one city."""
    now_utc = datetime.now(timezone.utc).isoformat()

    station_data = {}
    for key, station in config["stations"].items():
        print(f"    {station['name']} ({station['id']})...")
        station_data[key] = fetch_station(station)

    restriction = dict(config["restriction"])
    if restriction.get("end_date"):
        end = datetime.strptime(restriction["end_date"], "%Y-%m-%d")
        restriction["days_remaining"] = max(0, (end - datetime.now()).days)
    else:
        restriction["days_remaining"] = 0

    payload = {
        "meta":            {**config["meta"], "generated_utc": now_utc,
                            "data_licences": ["Open Government Licence \u2013 Canada (Environment and Climate Change Canada)"]},
        "source":          config.get("source", {}),
        "storage":         config.get("storage", {}),
        "treatment":       config.get("treatment", {}),
        "restriction":     restriction,
        "snowpack":        config.get("snowpack", {}),
        "risk":            config.get("risk", {}),
        "use":             config.get("use", {}),
        "egress":          config.get("egress", {}),
        "watershed_inflow": station_data,
    }

    if config.get("_fetch_glenmore"):
        print(f"    Glenmore Reservoir ({GLENMORE_STATION})...")
        payload["glenmore"] = fetch_glenmore()

    # Legacy compatibility keys
    if "watershed_context" in config:
        payload["watershed_context"] = config["watershed_context"]
    if "reservoirs" in config:
        payload["reservoirs"] = config["reservoirs"]

    return payload


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("waterwatch.criticalto.ca — national data fetch")
    print(f"  Time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}")
    print(f"  Cities ({len(CITIES)}): {', '.join(CITIES.keys())}")

    out_dir = Path(__file__).parent.parent / "site" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    results = {}
    for city_key, config in CITIES.items():
        print(f"\n  [{config['meta']['city']}]")
        try:
            payload  = build_city_payload(city_key, config)
            out_path = out_dir / f"{city_key}.json"
            with open(out_path, "w") as f:
                json.dump(payload, f, indent=2)
            print(f"  Written -> {out_path}")
            ok    = sum(1 for s in payload["watershed_inflow"].values() if s["status"] == "ok")
            total = len(payload["watershed_inflow"])
            print(f"  Stations: {ok}/{total} ok")
            for key, s in payload["watershed_inflow"].items():
                level = f"{s['level_m']}m"          if s["level_m"]       is not None else "n/a"
                flow  = f"{s['discharge_cms']} cms"  if s["discharge_cms"] is not None else "n/a"
                print(f"    {key:18} [{s['status']:32}]  level={level}  flow={flow}")
            stage = payload["restriction"].get("stage_label") or f"Stage {payload['restriction']['stage']}"
            days  = payload["restriction"].get("days_remaining", 0)
            print(f"  Restriction: {stage}  ({days} days remaining)")
            results[city_key] = f"ok ({ok}/{total} stations)"
        except Exception as e:
            print(f"  ERROR: {e}")
            results[city_key] = f"error: {e}"

    print(f"\n  Summary:")
    for city, status in results.items():
        print(f"    {city:16} {status}")
    print("\n  Done.")


if __name__ == "__main__":
    main()
