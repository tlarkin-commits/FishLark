"""
FishLark Chlorophyll Enrichment

Fetches NASA ERDDAP satellite chlorophyll-a data for the SoCal Bight
and correlates it with fishing outcomes.

Chlorophyll-a is a proxy for phytoplankton → zooplankton → baitfish → gamefish.
High chl-a in an area typically precedes gamefish arrival by 7-21 days.

Data source: NOAA CoastWatch ERDDAP (erdMH1chla8day — MODIS Aqua 8-day composite)
  - Product: chlorophyll-a (mg/m³)
  - Resolution: ~4km
  - Coverage: 2002–present
  - Free, no API key

Bounding box (SoCal): lat 29.5–34.5 N, lon -121–-116.5 W

Usage:
    python chlorophyll.py --fetch               # fetch + cache all data
    python chlorophyll.py --fetch --year 2024   # single year
    python chlorophyll.py --correlate           # run correlation analysis
    python chlorophyll.py --fetch --correlate   # full pipeline
"""
import argparse
import json
import re
import time
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean

CHLORO_CACHE_DIR   = Path("data/chlorophyll_cache")
ENRICHED_DATASET   = "data/enriched_master.jsonl"
CHLORO_DATASET     = "data/chlorophyll_index.json"   # date → {zone → chl_value}
ANALYSIS_DIR       = Path("data/analysis")

# NOAA CoastWatch ERDDAP — MODIS Aqua 8-day chlorophyll composite
# griddap endpoint returns CSV for a lat/lon point
ERDDAP_BASE = (
    "https://coastwatch.pfeg.noaa.gov/erddap/griddap/erdMH1chla8day.csv"
    "?chlorophyll[({time}):1:({time})][({lat}):1:({lat})][({lon}):1:({lon})]"
)

# Representative sample points per zone (lat, lon)
ZONE_SAMPLE_POINTS = {
    "san_miguel_santa_rosa":  (34.05, -120.1),
    "santa_cruz_anacapa":     (34.0,  -119.5),
    "san_nicolas":            (33.25, -119.5),
    "santa_barbara_island":   (33.47, -119.03),
    "santa_monica_bay":       (33.9,  -118.6),
    "palos_verdes_newport":   (33.7,  -118.3),
    "catalina":               (33.45, -118.5),
    "san_clemente_island":    (32.9,  -118.5),
    "dana_point_oceanside":   (33.2,  -117.5),
    "la_jolla_point_loma":    (32.8,  -117.35),
    "lower_9_mile":           (32.5,  -117.5),
    "coronado_islands":       (32.6,  -117.28),
    "14_mile_bank":           (32.2,  -117.8),
    "lower_500_colonet":      (30.5,  -116.5),
    "deep_offshore":          (31.5,  -118.5),
    "ensenada_south":         (31.8,  -116.6),
}

# Trophic lag: days chlorophyll spike precedes good fishing (by species)
# Based on trophic chain: phyto → zoo → bait → gamefish
TROPHIC_LAG = {
    "yellowtail":      14,
    "bluefin_tuna":    21,
    "yellowfin_tuna":  21,
    "dorado":          14,
    "white_seabass":   10,
    "halibut":          7,
    "barracuda":       10,
    "bonito":          10,
    "calico_bass":      7,
    "rockfish":        14,
    "squid":            7,
    "lingcod":         10,
}

# Chlorophyll thresholds for SoCal
# < 0.1  : oligotrophic (blue water, poor productivity)
# 0.1-0.3: mesotrophic (transitional)
# 0.3-1.0: eutrophic (green water, productive)
# > 1.0  : very high (upwelling or runoff)
CHLORO_BINS = [
    (0.0,  0.1,  "oligotrophic",   "blue water"),
    (0.1,  0.3,  "mesotrophic",    "transitional"),
    (0.3,  1.0,  "eutrophic",      "productive"),
    (1.0,  99.0, "hyper_eutrophic","very high"),
]


def classify_chloro(chl):
    if chl is None:
        return "unknown"
    for lo, hi, key, _ in CHLORO_BINS:
        if lo <= chl < hi:
            return key
    return "hyper_eutrophic"


# ---------------------------------------------------------------------------
# ERDDAP fetch
# ---------------------------------------------------------------------------

def _erddap_url(zone, sample_date):
    """Build ERDDAP URL for a zone's sample point on a given date."""
    lat, lon = ZONE_SAMPLE_POINTS[zone]
    # ERDDAP wants ISO 8601 with Z
    time_str = f"{sample_date}T00:00:00Z"
    return (
        "https://coastwatch.pfeg.noaa.gov/erddap/griddap/erdMH1chla8day.csv"
        f"?chlorophyll[({time_str}):1:({time_str})][({lat}):1:({lat})][({lon}):1:({lon})]"
    )


def _parse_erddap_csv(csv_text):
    """Extract chlorophyll value from ERDDAP CSV response."""
    lines = csv_text.strip().split("\n")
    if len(lines) < 3:
        return None
    # Line 0: header, Line 1: units, Line 2+: data
    try:
        val = lines[2].strip().split(",")[-1]
        if val.lower() in ("nan", "null", ""):
            return None
        return float(val)
    except (IndexError, ValueError):
        return None


def fetch_chlorophyll_for_date(sample_date: str, zones=None, delay=0.5):
    """
    Fetch chlorophyll values for all zones on a given 8-day composite date.
    Returns dict: zone → chl_value (mg/m³) or None
    """
    if zones is None:
        zones = list(ZONE_SAMPLE_POINTS.keys())

    results = {}
    for zone in zones:
        url = _erddap_url(zone, sample_date)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "FishLark/1.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                csv_text = resp.read().decode("utf-8")
            chl = _parse_erddap_csv(csv_text)
            results[zone] = chl
            time.sleep(delay)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                results[zone] = None  # date out of range for this composite
            else:
                print(f"    HTTP {e.code} for {zone} {sample_date}")
                results[zone] = None
        except Exception as e:
            print(f"    Error fetching {zone} {sample_date}: {e}")
            results[zone] = None
    return results


def _erddap_composite_dates(year: int):
    """
    MODIS 8-day composites start Jan 1 each year.
    Returns list of date strings for ~46 composites per year.
    """
    dates = []
    d = date(year, 1, 1)
    end = date(year, 12, 31)
    while d <= end:
        dates.append(d.isoformat())
        d += timedelta(days=8)
    return dates


# ---------------------------------------------------------------------------
# Fetch and cache full dataset
# ---------------------------------------------------------------------------

def fetch_all(start_year=2014, end_year=None, zones=None):
    """
    Fetch chlorophyll composites for all years and cache by date.
    MODIS Aqua data: 2002-07-04 to present.
    """
    if end_year is None:
        end_year = date.today().year

    CHLORO_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if zones is None:
        zones = list(ZONE_SAMPLE_POINTS.keys())

    all_data = {}  # date_str → {zone → value}

    # Load existing cache
    if Path(CHLORO_DATASET).exists():
        all_data = json.loads(Path(CHLORO_DATASET).read_text())
        print(f"Loaded {len(all_data)} cached dates")

    for year in range(start_year, end_year + 1):
        year_cache = CHLORO_CACHE_DIR / f"chl_{year}.json"

        if year_cache.exists():
            year_data = json.loads(year_cache.read_text())
            all_data.update(year_data)
            print(f"  {year}: loaded {len(year_data)} dates from cache")
            continue

        year_data = {}
        composite_dates = _erddap_composite_dates(year)
        print(f"  {year}: fetching {len(composite_dates)} composites for {len(zones)} zones...")

        for i, d_str in enumerate(composite_dates):
            values = fetch_chlorophyll_for_date(d_str, zones)
            year_data[d_str] = values
            if (i + 1) % 10 == 0:
                print(f"    {i+1}/{len(composite_dates)} composites done")

        year_cache.write_text(json.dumps(year_data, indent=2))
        all_data.update(year_data)
        print(f"  {year}: saved {len(year_data)} dates")

    Path(CHLORO_DATASET).write_text(json.dumps(all_data, indent=2))
    print(f"\nChlorophyll index: {len(all_data)} dates → {CHLORO_DATASET}")
    return all_data


# ---------------------------------------------------------------------------
# Lookup chlorophyll for a given date (interpolate to nearest composite)
# ---------------------------------------------------------------------------

def get_chloro_for_date(date_str: str, chl_index: dict, zone: str = None, lag_days: int = 0):
    """
    Return chlorophyll value(s) for a date, optionally looking back by lag_days.
    If zone is None, returns average across all zones.
    """
    target = date_str
    if lag_days > 0:
        try:
            d = date.fromisoformat(date_str)
            target = (d - timedelta(days=lag_days)).isoformat()
        except ValueError:
            pass

    # Find nearest composite date (within ±4 days)
    best_date = None
    best_diff = 5
    for d_str in chl_index:
        try:
            diff = abs((date.fromisoformat(d_str) - date.fromisoformat(target)).days)
            if diff < best_diff:
                best_diff = diff
                best_date = d_str
        except ValueError:
            pass

    if best_date is None:
        return None

    record = chl_index.get(best_date, {})
    if zone:
        return record.get(zone)

    vals = [v for v in record.values() if v is not None]
    return round(mean(vals), 4) if vals else None


# ---------------------------------------------------------------------------
# Correlation analysis
# ---------------------------------------------------------------------------

def correlate(chl_index: dict):
    """
    For each species, correlate chlorophyll (at lag) with catch quality.
    Also looks for daily/weekly/monthly patterns in chlorophyll vs fishing outcomes.
    """
    if not Path(ENRICHED_DATASET).exists():
        print(f"Missing {ENRICHED_DATASET} — run enrich.py first")
        return

    # Load observations
    from collections import defaultdict
    sp_obs = defaultdict(list)  # species → list of (date, zone, quality, chl_now, chl_lagged)

    with open(ENRICHED_DATASET, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                report = json.loads(line)
            except json.JSONDecodeError:
                continue

            date_str = report.get("date", "")
            if not date_str:
                continue

            for zk in ["inshore_zones", "offshore_zones", "mexican_zones"]:
                for zone in report.get(zk, []):
                    zone_id = zone.get("zone_id", "")
                    for sp in zone.get("species_reports", []):
                        species = sp.get("species", "")
                        quality = sp.get("catch_quality_score", 0)

                        # Current chlorophyll
                        chl_now = get_chloro_for_date(date_str, chl_index, zone=zone_id)
                        # Lagged chlorophyll
                        lag = TROPHIC_LAG.get(species, 14)
                        chl_lag = get_chloro_for_date(date_str, chl_index, zone=zone_id, lag_days=lag)

                        sp_obs[species].append({
                            "date": date_str,
                            "zone": zone_id,
                            "quality": quality,
                            "chl_now": chl_now,
                            "chl_lag": chl_lag,
                            "chl_class_now": classify_chloro(chl_now),
                            "chl_class_lag": classify_chloro(chl_lag),
                        })

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    lines = ["CHLOROPHYLL CORRELATION ANALYSIS\n"]
    print("\n" + "="*70)
    print("  CHLOROPHYLL CORRELATION ANALYSIS")
    print("="*70)

    gamefish = ["yellowtail", "bluefin_tuna", "yellowfin_tuna", "dorado",
                "white_seabass", "halibut", "barracuda", "bonito",
                "calico_bass", "rockfish", "white_seabass", "lingcod"]

    for species in gamefish:
        obs = sp_obs.get(species, [])
        obs_with_chl = [o for o in obs if o["chl_lag"] is not None]
        if len(obs_with_chl) < 20:
            continue

        lag = TROPHIC_LAG.get(species, 14)

        # Quality by chlorophyll class at lag
        by_class = defaultdict(list)
        for o in obs_with_chl:
            by_class[o["chl_class_lag"]].append(o["quality"])

        hdr = f"\n{species.upper().replace('_',' ')} (lag={lag} days, n={len(obs_with_chl)})"
        print(hdr)
        lines.append(hdr)

        class_order = ["oligotrophic", "mesotrophic", "eutrophic", "hyper_eutrophic"]
        row_hdr = f"  {'Chlorophyll Class':<20} {'Avg Quality':>12} {'n':>6}  Bar"
        print(row_hdr)
        lines.append(row_hdr)
        print(f"  {'-'*60}")
        lines.append(f"  {'-'*60}")

        for cls in class_order:
            scores = by_class.get(cls, [])
            if not scores:
                continue
            avg = round(mean(scores), 2)
            bar = "█" * int(avg / 5 * 20) + "░" * (20 - int(avg / 5 * 20))
            row = f"  {cls:<20} {avg:>12.2f} {len(scores):>6}  {bar}"
            print(row)
            lines.append(row)

        # Monthly chlorophyll trend
        monthly_chl = defaultdict(list)
        monthly_q   = defaultdict(list)
        for o in obs_with_chl:
            try:
                m = int(o["date"][5:7])
                monthly_chl[m].append(o["chl_lag"])
                monthly_q[m].append(o["quality"])
            except (ValueError, IndexError):
                pass

        month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        row_hdr2 = f"\n  Monthly avg chlorophyll (at -{lag}d lag) vs catch quality:"
        row_hdr3 = f"  {'Month':<6} {'Avg Chl':>10} {'Chl Class':>14} {'Avg Quality':>12}"
        print(row_hdr2)
        print(row_hdr3)
        lines.extend([row_hdr2, row_hdr3])
        print(f"  {'-'*48}")
        lines.append(f"  {'-'*48}")

        for m in range(1, 13):
            chls = monthly_chl.get(m, [])
            qs   = monthly_q.get(m, [])
            if not chls:
                continue
            avg_chl = round(mean(chls), 3)
            avg_q   = round(mean(qs), 2)
            cls     = classify_chloro(avg_chl)
            row = f"  {month_names[m]:<6} {avg_chl:>10.3f} {cls:>14} {avg_q:>12.2f}"
            print(row)
            lines.append(row)

    # Daily/weekly pattern: does high chl today → good fishing within 1 week?
    print("\n\nSHORT-TERM CHL → FISHING LAG ANALYSIS (all species combined)")
    lines.append("\n\nSHORT-TERM CHL → FISHING LAG ANALYSIS")
    print(f"  {'Lag (days)':<12} {'Avg Quality (high chl)':>22} {'Avg Quality (low chl)':>22} {'Lift':>8}")
    print(f"  {'-'*70}")
    lines.append(f"  {'Lag (days)':<12} {'Avg Quality (high chl)':>22} {'Avg Quality (low chl)':>22} {'Lift':>8}")
    lines.append(f"  {'-'*70}")

    all_obs = []
    with open(ENRICHED_DATASET, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            date_str = r.get("date", "")
            if not date_str:
                continue
            for zk in ["inshore_zones", "offshore_zones", "mexican_zones"]:
                for z in r.get(zk, []):
                    for sp in z.get("species_reports", []):
                        all_obs.append({
                            "date": date_str,
                            "zone": z.get("zone_id", ""),
                            "quality": sp.get("catch_quality_score", 0),
                        })

    for lag_test in [0, 3, 7, 10, 14, 21]:
        high_q = []
        low_q  = []
        for o in all_obs:
            chl = get_chloro_for_date(o["date"], chl_index, zone=o["zone"], lag_days=lag_test)
            if chl is None:
                continue
            if chl >= 0.3:
                high_q.append(o["quality"])
            else:
                low_q.append(o["quality"])

        if not high_q or not low_q:
            continue
        high_avg = round(mean(high_q), 3)
        low_avg  = round(mean(low_q), 3)
        lift     = round(high_avg - low_avg, 3)
        row = f"  {lag_test:<12} {high_avg:>22.3f} {low_avg:>22.3f} {lift:>+8.3f}"
        print(row)
        lines.append(row)

    out = ANALYSIS_DIR / "chlorophyll.txt"
    out.write_text("\n".join(lines))
    print(f"\n  → saved to {out}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="FishLark Chlorophyll Analysis")
    parser.add_argument("--fetch", action="store_true",
                        help="Fetch chlorophyll data from NOAA ERDDAP")
    parser.add_argument("--correlate", action="store_true",
                        help="Run correlation analysis")
    parser.add_argument("--year", type=int,
                        help="Fetch a specific year only")
    parser.add_argument("--start-year", type=int, default=2014)
    args = parser.parse_args()

    chl_index = {}

    if args.fetch:
        if args.year:
            chl_index = fetch_all(start_year=args.year, end_year=args.year)
        else:
            chl_index = fetch_all(start_year=args.start_year)
    elif args.correlate:
        if Path(CHLORO_DATASET).exists():
            print(f"Loading cached chlorophyll data...")
            chl_index = json.loads(Path(CHLORO_DATASET).read_text())
            print(f"  {len(chl_index)} composite dates loaded")
        else:
            print("No chlorophyll data found. Run with --fetch first.")
            return

    if args.correlate and chl_index:
        correlate(chl_index)


if __name__ == "__main__":
    main()
