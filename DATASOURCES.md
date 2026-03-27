# FishLark Data Sources & Architecture

## Overview

This document catalogs all data sources identified for the FishLark fishing intelligence platform,
covering Southern California and Northern Baja offshore fishing. Data is organized into three tiers:
primary (Fishdope scraped data), secondary (NOAA/NASA environmental APIs), and tertiary (derived/computed layers).

---

## 1. Primary Data: Fishdope Fish Reports

### Source Details
- **URL**: https://www.fishdope.com/category/fish-reports/
- **Format**: WordPress blog posts (HTML)
- **Date Range**: February 2009 – Present (17+ years)
- **Volume**: ~2,087 paginated listing pages, ~6,000+ individual reports
- **Update Frequency**: Daily during fishing season (roughly March–November), reduced in winter
- **Access**: Requires paid Fishdope membership + WordPress session cookie
- **Individual Report URL Pattern**: `fishdope.com/{YYYY}/{MM}/{post-slug}/`

### Data Fields Extracted Per Report
| Field | Description | Availability |
|-------|-------------|-------------|
| date | Publication date | All reports |
| title | Report title/headline | All reports |
| zones | Geographic fishing zones mentioned | All reports |
| species | Fish species with catch quality scores (0-5) | All reports |
| depths | Fishing depths (fathoms/feet) | Most reports |
| water_temp | Sea surface temperature readings | Most reports |
| bait_reports | Bait availability (squid, sardine, anchovy) | Most reports |
| gps_coordinates | Specific lat/lon fishing spots | 2009-2017 era reports (embedded in text) |
| weather_conditions | Wind, swell, visibility | Many reports |
| squid_report | Squid location and availability | Seasonal |
| user_reports | Angler-submitted catch reports | Many reports |

### Zone Definitions (15 Zones)
| Zone ID | Zone Name | Coverage |
|---------|-----------|----------|
| dana_point | Dana Point | Coastal |
| san_clemente | San Clemente Island | Offshore Island |
| catalina | Catalina Island | Offshore Island |
| la_jolla | La Jolla / San Diego | Coastal |
| 209_277 | 209/277 Bank | Offshore Bank |
| coronado_islands | Coronado Islands | Cross-border |
| ensenada | Ensenada | Baja MX |
| san_quintin | San Quintin | Baja MX |
| colonet | Colonet | Baja MX |
| tanner_cortez | Tanner/Cortez Bank | Deep Offshore |
| hidden_bank | Hidden Bank | Offshore Bank |
| butterfly_bank | Butterfly Bank | Offshore Bank |
| san_nicolas | San Nicolas Island | Offshore Island |
| santa_barbara_island | Santa Barbara Island | Offshore Island |
| newport_huntington | Newport / Huntington | Coastal |

### Species Tracked (30+ Species with Aliases)
**Pelagics**: Yellowfin Tuna, Bluefin Tuna, Yellowtail, Dorado (Mahi-Mahi), Wahoo, White Seabass, Barracuda, Bonito
**Billfish**: Striped Marlin, Blue Marlin, Swordfish, Sailfish
**Bottom Fish**: Lingcod, Rockfish, Calico Bass, Sand Bass, Halibut, Sheephead, Whitefish
**Sharks**: Mako, Thresher, Blue Shark
**Bait Species**: Squid (Market/Humboldt), Sardine, Anchovy, Mackerel

### Catch Quality Scoring
| Score | Label | Description |
|-------|-------|-------------|
| 5 | Wide Open | Limits for everyone, epic fishing |
| 4 | Very Good | Consistent catches, most boats scoring |
| 3 | Good | Decent action, some boats do well |
| 2 | Fair | Hit or miss, scattered catches |
| 1 | Slow | Few fish caught, tough conditions |
| 0 | None | No reports of catches |

### HTML Scraping Selectors
```
Listing page container: div.the_content_wrapper div.post[id^='post-']
Post title:            h3.post-title a
Post date:             div.post-updated
Post content:          .desc (listing) or .the_content (individual page)
Pagination:            Page URL pattern /page/{N}/
Individual post link:  h3.post-title a[href]
```

### Report Format Evolution
| Era | Date Range | Key Characteristics |
|-----|-----------|---------------------|
| Early | 2009-2012 | Shorter reports, basic zone/species info, some GPS |
| Middle | 2013-2017 | Detailed reports with embedded GPS coordinates as text, specific spot names |
| Modern | 2018-Present | Structured prose format, zone-based organization, no inline GPS, references chart app |

---

## 2. Fishdope Chart App Layers

### Source Details
- **URL**: https://app.fishdope.com/regions/1 (SoCal/Mexico Overview)
- **Format**: Interactive web map (Mapbox GL JS)
- **Access**: Requires paid Fishdope membership

### Available Layers (12 Total)
| Layer | Data Type | Update Frequency | Notes |
|-------|-----------|-----------------|-------|
| Water Temperature (SST) | Satellite imagery overlay | Daily | Multi-source composite, 1km resolution |
| Water Color (Chlorophyll) | Satellite imagery overlay | Daily | Chlorophyll-a concentration from ocean color satellites |
| Altimetry / Currents | Vector overlay | Daily/Weekly | Sea surface height anomalies + derived current vectors |
| Wind | Vector overlay | Multiple daily | Wind speed and direction |
| Imagery | Satellite base imagery | Daily | Cloud-free composite |
| Closures | Polygon overlay | Static + seasonal | MPAs, seasonal closures, restricted zones |
| Map Layers | POI/reference | Static | Kelp beds, landmarks, ports |
| Base Map | Tile layer | Static | Nautical chart or satellite base |
| Bottom Contours | Bathymetry lines | Static | Depth contour lines in fathoms |
| Lat/Lon Grid | Reference grid | Static | Geographic coordinate grid |
| Labels | Text labels | Static | Place names, feature names |
| Buoys | Point data | Real-time | NOAA buoy stations with live conditions |

---

## 3. Fishdope Oceanographic Intelligence (from SST/Chlorophyll Guides)

### SST Patterns for Fish Finding (Dr. Jeff Barr)
| Pattern | Description | Fishing Significance |
|---------|-------------|---------------------|
| Temperature Breaks | Sharp gradients between warm and cool water masses | Primary fish-finding indicator; gamefish concentrate along breaks |
| Warm Eddies | Clockwise-spinning warm water masses | Attract and hold pelagic species; create current edges |
| Cool Upwelling Zones | Cold, nutrient-rich water rising to surface | Drive bait aggregation; attract predators to edges |
| Current Boundaries | Where different water masses meet | Concentrate bait and create feeding lanes |
| Temperature Persistence | Same break appearing for multiple days | More reliable fishing; fish have time to aggregate |

### Chlorophyll Patterns
| Pattern | Description | Fishing Significance |
|---------|-------------|---------------------|
| Chlorophyll Edges | Boundaries between high and low chlorophyll | Indicate productive water meeting clear water; bait concentrates here |
| Upwelling Plumes | High-chlorophyll tongues extending offshore | Mark nutrient-rich water that drives the entire food chain |
| Clear Water Intrusions | Low-chlorophyll warm water pushing inshore | Can bring offshore pelagics closer to coast |
| Sustained Blooms | Persistent high-chlorophyll areas | Long-term bait holding areas |

### Key SST Ranges for Target Species (SoCal)
| Species | Preferred SST (°F) | Peak Season |
|---------|-------------------|-------------|
| Bluefin Tuna | 60-68 | May-October |
| Yellowfin Tuna | 65-75 | July-November |
| Yellowtail | 62-72 | March-October |
| Dorado | 68-78 | July-November |
| White Seabass | 58-66 | March-July |
| Striped Marlin | 66-74 | August-November |
| Wahoo | 70-80 | August-November |

---

## 4. Fishdope GPS Spots

### Source Details
- **URL**: https://www.fishdope.com/protected/gps-s/
- **Format**: IMAGE-BASED (JPG/PNG screenshots of GPS coordinates) — NOT scrapeable as text
- **Regions**: 9 sub-pages covering Dana Point to Mexico + offshore banks

### Regional Pages
1. Dana Point to Del Mar
2. La Jolla to Mexico
3. Offshore SoCal / Mexico
4. San Clemente Island
5. Catalina Island
6. Santa Barbara Island
7. San Nicolas Island
8. Coronado Islands
9. Ensenada / San Quintin

### Alternative GPS Source
GPS coordinates ARE embedded as text in the 2013-2017 era daily fish reports.
The parser extracts these using regex patterns:
```
Decimal: 32.1234°N, 117.5678°W
DMS: 32°12'34"N, 117°56'78"W
Numeric: 32-12.34 x 117-56.78
```

---

## 5. Fishdope Closures / MPA Layer

### Data Included
- Marine Protected Areas (MPAs) — California state MPAs per MLPA
- Federal closures (National Marine Sanctuaries)
- Seasonal fishing closures (rockfish conservation areas, cowcod zones)
- International boundary (US/Mexico)
- Military restricted areas

---

## 6. Additional Fishdope Content (Not Yet Integrated)

| Feature | URL Pattern | Data Type | Priority |
|---------|------------|-----------|----------|
| Fish Spotter Plane Reports | TBD | Aerial fish sighting reports | High |
| Hot Bite Report Map | TBD | Interactive map of recent catches | High |
| Bait Barge Reports | TBD | Live bait availability by location | Medium |
| Forums | TBD | Community discussion threads | Low |
| Tackle/Rigging Guides | TBD | Educational content | Low |

---

## 7. External API Data Sources (Planned)

### NOAA APIs
| Dataset | API/Service | Endpoint | Coverage | Resolution |
|---------|------------|----------|----------|------------|
| SST (Sea Surface Temp) | ERDDAP / CoastWatch | See APIS.md | Global / SoCal regional | 1-5 km |
| Ocean Currents | OSCAR / HyCOM | See APIS.md | Global / SoCal regional | 1/12° to 1/3° |
| Wind | NDBC Buoys / GFS | See APIS.md | Point (buoy) + Gridded | Hourly / 0.25° |
| Tides | CO-OPS | See APIS.md | Coastal tide stations | 6-minute intervals |
| Wave/Swell | NDBC / WaveWatch III | See APIS.md | Buoy + Gridded | Hourly |
| Bathymetry | ETOPO / GEBCO | See APIS.md | Global | 15 arc-second |

### NASA/Satellite APIs
| Dataset | API/Service | Endpoint | Coverage | Resolution |
|---------|------------|----------|----------|------------|
| Chlorophyll-a | NASA OceanColor / ERDDAP | See APIS.md | Global | 4 km / 1 km |
| Ocean Color (RGB) | MODIS/VIIRS | See APIS.md | Global | 1 km |
| Sea Surface Height | Altimetry (Jason/Sentinel) | See APIS.md | Global | ~7 km along-track |

### Astronomical/Tidal APIs
| Dataset | API/Service | Notes |
|---------|------------|-------|
| Moon Phase | USNO API / Computed | Solunar feeding theory |
| Sunrise/Sunset | USNO API / Computed | Fishing time windows |
| Solunar Tables | Computed from ephemeris | Major/minor feeding periods |

---

## 8. Data Architecture

### Storage Layers
```
data/
├── raw/                    # Raw HTML from Fishdope scraping
│   ├── listings/           # Paginated listing pages
│   └── reports/            # Individual report HTML
├── parsed/                 # Extracted structured data
│   ├── reports.jsonl       # One JSON object per report
│   └── reports_summary.json # Statistics and metadata
├── environmental/          # External API data cache
│   ├── sst/               # Sea surface temperature grids
│   ├── chlorophyll/        # Chlorophyll-a grids
│   ├── currents/           # Ocean current vectors
│   ├── wind/               # Wind data
│   ├── tides/              # Tide predictions
│   └── moon/               # Moon phase / solunar data
├── bathymetry/             # Static depth/contour data
│   ├── etopo/              # ETOPO global relief
│   └── contours/           # Derived contour lines
└── training/               # ML-ready feature matrices
    ├── features.parquet    # Combined feature set per date/zone
    └── labels.parquet      # Catch quality scores per date/zone/species
```

### Feature Matrix Concept
Each training row represents: **(date, zone, species)** → **catch_quality_score**

Features for each row:
- SST at zone centroid (°F)
- SST gradient magnitude (°F/km)
- Chlorophyll-a concentration (mg/m³)
- Chlorophyll gradient
- Current speed and direction
- Wind speed and direction
- Moon phase (0-1 illumination fraction)
- Solunar rating (major/minor periods)
- Tide state (rising/falling/slack, spring/neap)
- Day of year (seasonality)
- Water depth at zone centroid
- Days since last reported catch at zone
- Historical catch rate for zone/species/month

---

## 9. Pipeline Status

| Component | File | Status |
|-----------|------|--------|
| Config & Zone Definitions | fishdope_pipeline/config.py | ✅ Complete |
| Data Schema (Dataclasses) | fishdope_pipeline/schema.py | ✅ Complete |
| Web Scraper | fishdope_pipeline/scraper.py | ✅ Complete |
| Rule-Based Parser | fishdope_pipeline/parser_rules.py | ✅ Complete |
| CLI Orchestrator | fishdope_pipeline/pipeline.py | ✅ Complete |
| LLM-Enhanced Parser | fishdope_pipeline/parser_llm.py | 🔲 Planned |
| Environmental Data Fetcher | fishdope_pipeline/env_data.py | 🔲 Planned |
| Feature Matrix Builder | fishdope_pipeline/features.py | 🔲 Planned |
| ML Model Training | fishdope_pipeline/model.py | 🔲 Planned |
| Chart/Map Visualization | TBD | 🔲 Planned |
