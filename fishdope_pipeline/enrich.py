"""
FishLark Enrichment Engine

Adds environmental context to every report in the master JSONL:
  - Moon phase (calculated, no API)
  - Improved marine forecast extraction (wind dir/speed/gusts, swell height/period)
  - NOAA CO-OPS tide predictions (high/low times, tide state at dawn)

Outputs: data/enriched_master.jsonl
         data/tide_cache/  (cached NOAA responses by year+station)

Usage:
    python enrich.py                  # full enrichment
    python enrich.py --no-tides       # skip NOAA fetch (offline mode)
"""
import argparse
import json
import math
import re
import time
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from config import MASTER_DATASET

ENRICHED_DATASET = "data/enriched_master.jsonl"
TIDE_CACHE_DIR   = Path("data/tide_cache")

# NOAA CO-OPS stations covering SoCal
TIDE_STATIONS = {
    "north":  "9411340",   # Santa Barbara
    "central": "9410660",  # LA Harbor (Wilmington)
    "south":  "9410170",   # San Diego
}

# Zone → station mapping
ZONE_STATION = {
    "san_miguel_santa_rosa":  "north",
    "santa_cruz_anacapa":     "north",
    "san_nicolas":            "central",
    "santa_barbara_island":   "north",
    "santa_monica_bay":       "central",
    "palos_verdes_newport":   "central",
    "catalina":               "central",
    "san_clemente_island":    "central",
    "dana_point_oceanside":   "south",
    "la_jolla_point_loma":    "south",
    "lower_9_mile":           "south",
    "coronado_islands":       "south",
    "ensenada_south":         "south",
    "14_mile_bank":           "south",
    "lower_500_colonet":      "south",
    "deep_offshore":          "south",
}

# ---------------------------------------------------------------------------
# Moon phase
# ---------------------------------------------------------------------------
# Reference new moon: Jan 6, 2000 (JD 2451550.1)
_KNOWN_NEW_MOON_JD = 2451550.1
_LUNAR_CYCLE = 29.53058867


def _date_to_jd(d: date) -> float:
    """Julian Day Number for a calendar date."""
    a = (14 - d.month) // 12
    y = d.year + 4800 - a
    m = d.month + 12 * a - 3
    return (d.day
            + (153 * m + 2) // 5
            + 365 * y
            + y // 4
            - y // 100
            + y // 400
            - 32045)


def moon_phase(d: date) -> dict:
    """Return moon phase details for a date."""
    jd = _date_to_jd(d)
    cycle_pos = ((jd - _KNOWN_NEW_MOON_JD) % _LUNAR_CYCLE) / _LUNAR_CYCLE
    angle_deg = cycle_pos * 360

    if cycle_pos < 0.0625:
        name, major = "New Moon", "new"
    elif cycle_pos < 0.1875:
        name, major = "Waxing Crescent", "waxing_crescent"
    elif cycle_pos < 0.3125:
        name, major = "First Quarter", "first_quarter"
    elif cycle_pos < 0.4375:
        name, major = "Waxing Gibbous", "waxing_gibbous"
    elif cycle_pos < 0.5625:
        name, major = "Full Moon", "full"
    elif cycle_pos < 0.6875:
        name, major = "Waning Gibbous", "waning_gibbous"
    elif cycle_pos < 0.8125:
        name, major = "Last Quarter", "last_quarter"
    elif cycle_pos < 0.9375:
        name, major = "Waning Crescent", "waning_crescent"
    else:
        name, major = "New Moon", "new"

    # Days to next full moon and new moon
    days_since_new = cycle_pos * _LUNAR_CYCLE
    days_to_full   = (_LUNAR_CYCLE * 0.5 - days_since_new) % _LUNAR_CYCLE
    days_to_new    = _LUNAR_CYCLE - days_since_new

    return {
        "phase_name": name,
        "phase_key": major,
        "cycle_fraction": round(cycle_pos, 4),   # 0=new, 0.5=full
        "illumination_pct": round(50 * (1 - math.cos(math.radians(angle_deg))), 1),
        "days_to_full": round(days_to_full, 1),
        "days_to_new": round(days_to_new, 1),
        "is_major": major in ("new", "full", "first_quarter", "last_quarter"),
    }


# ---------------------------------------------------------------------------
# Marine forecast extraction from zone raw_text
# ---------------------------------------------------------------------------
_WIND_PAT    = re.compile(
    r'\b([NSEW]{1,3})\s+winds?\s+(\d+)\s+to\s+(\d+)\s+kt', re.I)
_GUST_PAT    = re.compile(r'gusts?\s+(?:up\s+to\s+|to\s+)?(\d+)\s+kt', re.I)
_SEAS_PAT    = re.compile(
    r'[Cc]ombined\s+seas?\s+(?:of\s+)?(\d+(?:\.\d+)?)\s+to\s+(\d+(?:\.\d+)?)\s+ft', re.I)
_PERIOD_PAT  = re.compile(r'dominant\s+period\s+(\d+)\s+sec', re.I)
_SWELL_PAT   = re.compile(r'\bswell[s]?\s+(\d+)\s+to\s+(\d+)\s+ft', re.I)
_SCA_PAT     = re.compile(r'small\s+craft\s+advisory', re.I)
_GALE_PAT    = re.compile(r'gale\s+(?:force\s+)?warning', re.I)
_STORM_PAT   = re.compile(r'storm\s+warning', re.I)


def extract_marine_forecast(zone_raw_text: str) -> dict:
    """Extract structured marine forecast from zone raw_text."""
    txt = zone_raw_text or ""
    result = {}

    # Use the FIRST occurrence (today's forecast, not tonight's)
    wind_m = _WIND_PAT.search(txt)
    if wind_m:
        result["wind_direction"] = wind_m.group(1).upper()
        result["wind_speed_kt_min"] = int(wind_m.group(2))
        result["wind_speed_kt_max"] = int(wind_m.group(3))
        result["wind_speed_kt_avg"] = (int(wind_m.group(2)) + int(wind_m.group(3))) // 2

    gust_m = _GUST_PAT.search(txt)
    if gust_m:
        result["wind_gusts_kt"] = int(gust_m.group(1))

    seas_m = _SEAS_PAT.search(txt)
    if seas_m:
        result["seas_ft_min"] = float(seas_m.group(1))
        result["seas_ft_max"] = float(seas_m.group(2))
        result["seas_ft_avg"] = (float(seas_m.group(1)) + float(seas_m.group(2))) / 2

    period_m = _PERIOD_PAT.search(txt)
    if period_m:
        result["swell_period_sec"] = int(period_m.group(1))

    swell_m = _SWELL_PAT.search(txt)
    if swell_m:
        result["swell_ft_min"] = float(swell_m.group(1))
        result["swell_ft_max"] = float(swell_m.group(2))

    result["has_sca"]          = bool(_SCA_PAT.search(txt))
    result["has_gale_warning"] = bool(_GALE_PAT.search(txt))
    result["has_storm_warning"] = bool(_STORM_PAT.search(txt))

    # Severity score 0-4
    severity = 0
    if result.get("seas_ft_max", 0) > 10:
        severity = 4
    elif result.get("seas_ft_max", 0) > 6:
        severity = 3
    elif result.get("has_sca"):
        severity = 2
    elif result.get("seas_ft_max", 0) > 3:
        severity = 1
    result["sea_severity"] = severity

    return result


def extract_report_level_forecast(raw_text: str) -> dict:
    """Extract report-level weather summary for reports without zone forecasts."""
    return extract_marine_forecast(raw_text or "")


# ---------------------------------------------------------------------------
# NOAA CO-OPS tide data
# ---------------------------------------------------------------------------
NOAA_API = (
    "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    "?begin_date={begin}&end_date={end}&station={station}"
    "&product=predictions&datum=MLLW&interval=hilo"
    "&time_zone=lst_ldt&units=english&application=fishlark&format=json"
)


def _fetch_noaa_year(year: int, station_id: str) -> list:
    """Fetch one year of high/low tide predictions from NOAA."""
    begin = f"{year}0101"
    end   = f"{year}1231"
    url   = NOAA_API.format(begin=begin, end=end, station=station_id)
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read())
        if "predictions" in data:
            return data["predictions"]
        print(f"  NOAA {station_id} {year}: {data.get('error', {}).get('message', 'no predictions')}")
    except Exception as e:
        print(f"  NOAA fetch error {station_id} {year}: {e}")
    return []


def load_tide_cache(station_key: str, year: int, fetch_missing: bool = True) -> list:
    """Load cached tide data, fetching from NOAA if missing."""
    TIDE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    station_id = TIDE_STATIONS[station_key]
    cache_file = TIDE_CACHE_DIR / f"{station_key}_{year}.json"

    if cache_file.exists():
        return json.loads(cache_file.read_text())

    if not fetch_missing:
        return []

    print(f"  Fetching NOAA tides: {station_key} ({station_id}) {year}...")
    predictions = _fetch_noaa_year(year, station_id)
    if predictions:
        cache_file.write_text(json.dumps(predictions))
        time.sleep(0.5)  # be polite to NOAA
    return predictions


def build_tide_index(predictions: list) -> dict:
    """
    Index tide predictions by date string YYYY-MM-DD.
    Each date → list of {time, type, height_ft}
    """
    index = defaultdict(list)
    for p in predictions:
        dt_str = p.get("t", "")  # "2024-01-15 06:43"
        if dt_str:
            day = dt_str[:10]
            index[day].append({
                "time": dt_str[11:],
                "type": p.get("type", ""),     # "H" or "L"
                "height_ft": float(p.get("v", 0)),
            })
    return dict(index)


def tide_state_for_date(date_str: str, tide_index: dict) -> dict:
    """
    Summarize tide conditions for a given date.
    Returns: high/low times+heights, first morning tide direction.
    """
    events = tide_index.get(date_str, [])
    if not events:
        return {}

    highs = [e for e in events if e["type"] == "H"]
    lows  = [e for e in events if e["type"] == "L"]

    result = {
        "tide_events": events,
        "high_count": len(highs),
        "low_count": len(lows),
    }

    if highs:
        best_high = max(highs, key=lambda e: e["height_ft"])
        result["max_high_ft"] = best_high["height_ft"]
        result["max_high_time"] = best_high["time"]

    if lows:
        best_low = min(lows, key=lambda e: e["height_ft"])
        result["min_low_ft"] = best_low["height_ft"]
        result["min_low_time"] = best_low["time"]

    if highs and lows:
        result["tidal_range_ft"] = round(
            result.get("max_high_ft", 0) - result.get("min_low_ft", 0), 2
        )

    # Determine tide state at dawn (6 AM): is it rising or falling?
    dawn_hour = 6.0
    events_sorted = sorted(events, key=lambda e: e["time"])
    before_dawn = [e for e in events_sorted
                   if _time_to_float(e["time"]) <= dawn_hour]
    after_dawn  = [e for e in events_sorted
                   if _time_to_float(e["time"]) > dawn_hour]

    if before_dawn and after_dawn:
        last_before = before_dawn[-1]
        first_after = after_dawn[0]
        if last_before["type"] == "L" and first_after["type"] == "H":
            result["dawn_tide_direction"] = "rising"
        elif last_before["type"] == "H" and first_after["type"] == "L":
            result["dawn_tide_direction"] = "falling"
        result["dawn_last_event"] = last_before
        result["dawn_next_event"] = first_after
    elif after_dawn:
        result["dawn_tide_direction"] = "rising" if after_dawn[0]["type"] == "H" else "falling"

    # Is a major tide event (slack water) happening in prime AM window (5-9 AM)?
    prime_events = [e for e in events
                    if 5.0 <= _time_to_float(e["time"]) <= 9.0]
    result["prime_window_tide_event"] = prime_events[0] if prime_events else None

    return result


def _time_to_float(t: str) -> float:
    """'06:43' → 6.717"""
    try:
        h, m = t.split(":")
        return int(h) + int(m) / 60.0
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Determine which tide station to use for a report
# ---------------------------------------------------------------------------

def dominant_station(report: dict) -> str:
    """Pick the most-mentioned tide station for this report's zones."""
    counts = defaultdict(int)
    for zk in ["inshore_zones", "offshore_zones", "mexican_zones"]:
        for z in report.get(zk, []):
            station = ZONE_STATION.get(z.get("zone_id", ""), "south")
            counts[station] += 1
    if not counts:
        return "south"
    return max(counts, key=counts.get)


# ---------------------------------------------------------------------------
# Main enrichment
# ---------------------------------------------------------------------------

def enrich(fetch_tides: bool = True):
    master = Path(MASTER_DATASET)
    if not master.exists():
        print(f"Master dataset not found: {MASTER_DATASET}")
        print("Run: python pipeline.py --parse --export")
        return

    # Load all reports
    reports = []
    with open(master, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    reports.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    print(f"Loaded {len(reports):,} reports")

    # Get year range
    years = set()
    for r in reports:
        d = r.get("date", "")
        if len(d) >= 4 and d[:4].isdigit():
            years.add(int(d[:4]))

    # Pre-load tide data by station+year
    tide_indices = {}  # (station_key, year) → {date_str → events}
    if fetch_tides:
        print("Loading NOAA tide data...")
        station_years = set()
        for r in reports:
            d = r.get("date", "")
            if len(d) >= 4 and d[:4].isdigit():
                station = dominant_station(r)
                station_years.add((station, int(d[:4])))

        for station_key, year in sorted(station_years):
            preds = load_tide_cache(station_key, year, fetch_missing=True)
            tide_indices[(station_key, year)] = build_tide_index(preds)
        print(f"  Tide data loaded for {len(station_years)} station-year combos")

    # Enrich each report
    out_path = Path(ENRICHED_DATASET)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    enriched_count = 0

    with open(out_path, "w", encoding="utf-8") as f:
        for report in reports:
            date_str = report.get("date", "")

            # --- Moon phase ---
            moon = {}
            if date_str:
                try:
                    d = date.fromisoformat(date_str)
                    moon = moon_phase(d)
                except ValueError:
                    pass
            report["moon"] = moon

            # --- Marine forecast from zones ---
            all_forecasts = []
            for zk in ["inshore_zones", "offshore_zones", "mexican_zones"]:
                for z in report.get(zk, []):
                    fc = extract_marine_forecast(z.get("raw_text", ""))
                    if fc.get("wind_speed_kt_max") or fc.get("seas_ft_max"):
                        all_forecasts.append(fc)
                        # Attach forecast to zone too
                        z["marine_forecast"] = fc

            # Aggregate across zones for report-level summary
            if all_forecasts:
                report["marine_conditions"] = _aggregate_forecasts(all_forecasts)
            else:
                # Fallback: try report-level raw_text
                fc = extract_report_level_forecast(report.get("raw_text", ""))
                report["marine_conditions"] = fc if (fc.get("wind_speed_kt_max") or fc.get("seas_ft_max")) else {}

            # --- Tides ---
            if fetch_tides and date_str and len(date_str) >= 4 and date_str[:4].isdigit():
                year = int(date_str[:4])
                station = dominant_station(report)
                key = (station, year)
                if key in tide_indices:
                    report["tide"] = tide_state_for_date(date_str, tide_indices[key])
                    report["tide_station"] = station
                else:
                    report["tide"] = {}

            f.write(json.dumps(report) + "\n")
            enriched_count += 1

            if enriched_count % 500 == 0:
                print(f"  Enriched {enriched_count:,}/{len(reports):,}...")

    print(f"\nEnrichment complete: {enriched_count:,} reports → {out_path}")

    # Quick coverage stats
    _print_coverage_stats(out_path)


def _aggregate_forecasts(forecasts: list) -> dict:
    """Merge multiple zone forecasts into a single report-level summary."""
    if not forecasts:
        return {}
    # Use worst-case values (max seas, max wind) for overall conditions
    result = {}
    wind_dirs = [fc.get("wind_direction") for fc in forecasts if fc.get("wind_direction")]
    if wind_dirs:
        # Most common direction
        from collections import Counter
        result["wind_direction"] = Counter(wind_dirs).most_common(1)[0][0]

    for field in ["wind_speed_kt_max", "wind_gusts_kt", "seas_ft_max", "swell_period_sec"]:
        vals = [fc[field] for fc in forecasts if fc.get(field)]
        if vals:
            result[field] = max(vals)

    for field in ["wind_speed_kt_min", "seas_ft_min"]:
        vals = [fc[field] for fc in forecasts if fc.get(field)]
        if vals:
            result[field] = min(vals)

    result["has_sca"]           = any(fc.get("has_sca") for fc in forecasts)
    result["has_gale_warning"]  = any(fc.get("has_gale_warning") for fc in forecasts)
    result["has_storm_warning"] = any(fc.get("has_storm_warning") for fc in forecasts)
    result["sea_severity"]      = max((fc.get("sea_severity", 0) for fc in forecasts), default=0)
    result["zones_with_forecast"] = len(forecasts)

    return result


def _print_coverage_stats(path: Path):
    moon_count = tide_count = wind_count = seas_count = total = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line.strip())
            total += 1
            if r.get("moon", {}).get("phase_name"):
                moon_count += 1
            if r.get("tide", {}).get("dawn_tide_direction"):
                tide_count += 1
            mc = r.get("marine_conditions", {})
            if mc.get("wind_direction"):
                wind_count += 1
            if mc.get("seas_ft_max"):
                seas_count += 1

    print(f"\nCoverage ({total} reports):")
    print(f"  Moon phase:     {moon_count:,} ({100*moon_count//total}%)")
    print(f"  Tide direction: {tide_count:,} ({100*tide_count//total}%)")
    print(f"  Wind data:      {wind_count:,} ({100*wind_count//total}%)")
    print(f"  Seas data:      {seas_count:,} ({100*seas_count//total}%)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FishLark Enrichment Engine")
    parser.add_argument("--no-tides", action="store_true",
                        help="Skip NOAA tide fetching (offline mode)")
    args = parser.parse_args()
    enrich(fetch_tides=not args.no_tides)
