[README.md](https://github.com/user-attachments/files/28727245/README.md)
# WaterWatch — waterwatch.criticalto.ca

Public water supply intelligence dashboard for Metro Vancouver.
Built by [CriticalTO](https://criticalto.ca).

## What it does

- Shows current Metro Vancouver water restriction stage in plain language
- Tracks reservoir levels (Capilano, Seymour, Coquitlam) vs seasonal target
- Displays real-time watershed inflow from three Environment Canada gauging stations
- Provides snowpack context explaining why the current year is severe
- Lists exactly what is and isn't allowed under the current stage
- Counts down days until restrictions are expected to lift

## Data sources

| Layer | Source | Licence | Update freq |
|---|---|---|---|
| Watershed inflow | Environment Canada Datamart (`dd.weather.gc.ca`) | Open Govt Licence – Canada | Hourly |
| Reservoir levels | Metro Vancouver weekly release | Public | Weekly (Mon, May–Oct) |
| Snowpack % of normal | BC River Forecast Centre | Open Govt Licence – BC | Weekly (peak season) |
| Restriction stage | Metro Vancouver Drinking Water Conservation Plan | Public | Manual (changes a few times/season) |

## Station IDs

- Capilano: `08GA010` — Capilano River above Intake
- Seymour: `08GA074` — Seymour River above Orchid Creek
- Coquitlam: `08MH141` — Coquitlam River near Port Coquitlam

## Architecture

```
waterwatch/
├── scraper/
│   └── fetch_data.py        # Fetches all sources → site/data/waterwatch.json
├── site/
│   ├── index.html           # Dashboard (reads waterwatch.json at runtime)
│   └── data/
│       └── waterwatch.json  # Output — committed by GitHub Actions
├── .github/
│   └── workflows/
│       └── refresh.yml      # Runs scraper on schedule; commits JSON
├── netlify.toml             # Deploys site/ to Netlify
└── README.md
```

## Deployment

1. Push repo to GitHub (private or public)
2. Connect to Netlify → set publish directory to `site`
3. Add `waterwatch.criticalto.ca` as custom domain in Netlify
4. Add CNAME record in Rebel DNS: `waterwatch` → `[your-netlify-subdomain].netlify.app`
5. GitHub Actions runs on schedule and commits updated `waterwatch.json`
6. Netlify auto-deploys on each commit

## Manual updates required

**When Metro Vancouver changes the restriction stage**, update `RESTRICTION` in `fetch_data.py`:
- `stage` — new stage number (1–4)
- `start_date` / `end_date`
- The boolean fields for what's allowed/prohibited

**Weekly reservoir levels** — currently hardcoded with static values in `RESERVOIRS_STATIC`.
Phase 2 work: automate scraping of Metro Vancouver's weekly PDF release using `pdfplumber`.

**Snowpack** — update `SNOWPACK.pct_of_normal` from BC River Forecast Centre each spring (April measurement).

## Future phases

- **Phase 2**: Household profiler + personalized conservation calculator (no backend required — client-side JS)
- **Phase 2**: Automated Metro Vancouver PDF scraper for weekly reservoir updates
- **Phase 3**: Neighbourhood comparison layer (requires Metro Vancouver data partnership)
- **Phase 3**: Pacific DataStream CoSMo creek monitoring data integration

## Licence

Dashboard code: MIT  
Data: Open Government Licence – Canada / Open Government Licence – BC  
Attribution required for Environment Canada data.
