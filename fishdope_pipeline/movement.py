"""
FishLark Movement Analysis

Tracks species movement patterns through the season:
  - Daily GPS centroid per species
  - Hotspot clusters (residence zones)
  - Arrival and departure events
  - Residence duration (how long fish stayed)
  - Environmental conditions at time of movement events

Usage:
    python movement.py                       # all gamefish species
    python movement.py --species yellowtail
    python movement.py --min-gps 5           # require 5+ GPS points per day
"""
import argparse
import json
import math
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

ENRICHED_DATASET = "data/enriched_master.jsonl"
MOVEMENT_DIR = Path("data/movement")

GAMEFISH = [
    "yellowtail", "bluefin_tuna", "yellowfin_tuna", "dorado",
    "white_seabass", "halibut", "barracuda", "bonito",
    "calico_bass", "sand_bass", "lingcod", "sheephead",
    "rockfish", "sculpin", "whitefish",
]


# ---------------------------------------------------------------------------
# Haversine distance (miles)
# ---------------------------------------------------------------------------

def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.8  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def centroid(coords):
    """Compute mean lat/lon of a list of (lat, lon) tuples."""
    if not coords:
        return None, None
    return (
        sum(c[0] for c in coords) / len(coords),
        sum(c[1] for c in coords) / len(coords),
    )


# ---------------------------------------------------------------------------
# Load GPS observations per species
# ---------------------------------------------------------------------------

def load_species_gps(species_filter=None):
    """
    Returns dict: species → sorted list of {date, lat, lon, quality, zone, report_conditions}
    """
    path = Path(ENRICHED_DATASET)
    if not path.exists():
        path = Path("data/fishdope_master.jsonl")
        print(f"Note: using non-enriched master (run enrich.py for fuller analysis)")

    sp_obs = defaultdict(list)

    with open(path, encoding="utf-8") as f:
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

            moon     = report.get("moon", {})
            mc       = report.get("marine_conditions", {})
            tide     = report.get("tide", {})

            env = {
                "moon_phase": moon.get("phase_key", ""),
                "moon_illum": moon.get("illumination_pct"),
                "moon_days_to_full": moon.get("days_to_full"),
                "wind_dir": mc.get("wind_direction", ""),
                "wind_kt": mc.get("wind_speed_kt_avg"),
                "seas_ft": mc.get("seas_ft_avg"),
                "sea_severity": mc.get("sea_severity", 0),
                "has_gale": mc.get("has_gale_warning", False),
                "tide_dir": tide.get("dawn_tide_direction", ""),
                "tide_range_ft": tide.get("tidal_range_ft"),
                "max_high_ft": tide.get("max_high_ft"),
            }

            for zk in ["inshore_zones", "offshore_zones", "mexican_zones"]:
                for zone in report.get(zk, []):
                    zone_id = zone.get("zone_id", "")
                    for sp in zone.get("species_reports", []):
                        species = sp.get("species", "")
                        if species_filter and species != species_filter:
                            continue
                        if not species:
                            continue

                        coords = sp.get("gps_coords", [])
                        for c in coords:
                            lat = c.get("lat_decimal")
                            lon = c.get("lon_decimal")
                            if lat and lon and abs(lat) > 0.1:
                                sp_obs[species].append({
                                    "date": date_str,
                                    "lat": lat,
                                    "lon": lon,
                                    "quality": sp.get("catch_quality_score", 0),
                                    "zone": zone_id,
                                    "water_temp": sp.get("water_temp_f"),
                                    "env": env,
                                    "coord_name": c.get("name", ""),
                                })

    # Sort by date
    for sp in sp_obs:
        sp_obs[sp].sort(key=lambda x: x["date"])

    return sp_obs


# ---------------------------------------------------------------------------
# Daily centroid tracking
# ---------------------------------------------------------------------------

def daily_centroids(obs_list):
    """
    Group GPS observations by date, compute daily centroid + stats.
    Returns list of {date, lat, lon, n_points, avg_quality, zones}
    """
    by_date = defaultdict(list)
    for o in obs_list:
        by_date[o["date"]].append(o)

    days = []
    for d, pts in sorted(by_date.items()):
        lats = [p["lat"] for p in pts]
        lons = [p["lon"] for p in pts]
        c_lat, c_lon = centroid(list(zip(lats, lons)))
        days.append({
            "date": d,
            "lat": round(c_lat, 4),
            "lon": round(c_lon, 4),
            "n_points": len(pts),
            "avg_quality": round(sum(p["quality"] for p in pts) / len(pts), 2),
            "zones": list({p["zone"] for p in pts}),
            "env": pts[0]["env"],   # environment for this date
            "water_temps": [p["water_temp"] for p in pts if p.get("water_temp")],
        })
    return days


# ---------------------------------------------------------------------------
# Hotspot clustering (simple radius-based)
# ---------------------------------------------------------------------------

def cluster_hotspots(days, radius_miles=15.0):
    """
    Assign each daily centroid to a hotspot cluster.
    Returns days with added 'hotspot_id', plus hotspot summary dict.
    """
    hotspots = {}   # id → {lat, lon, first_seen, last_seen, days}
    next_id = 0

    for day in days:
        lat, lon = day["lat"], day["lon"]
        best_hs = None
        best_dist = radius_miles + 1

        for hs_id, hs in hotspots.items():
            dist = haversine_miles(lat, lon, hs["lat"], hs["lon"])
            if dist < best_dist:
                best_dist = dist
                best_hs = hs_id

        if best_hs is None:
            # New hotspot
            hs_id = f"hs_{next_id:03d}"
            hotspots[hs_id] = {
                "id": hs_id,
                "lat": lat,
                "lon": lon,
                "first_seen": day["date"],
                "last_seen": day["date"],
                "total_days": 1,
                "total_points": day["n_points"],
                "avg_quality": day["avg_quality"],
                "zones": set(day["zones"]),
            }
            next_id += 1
            day["hotspot_id"] = hs_id
            day["dist_to_hotspot"] = 0.0
        else:
            # Update centroid (running average)
            hs = hotspots[best_hs]
            n = hs["total_days"]
            hs["lat"] = round((hs["lat"] * n + lat) / (n + 1), 4)
            hs["lon"] = round((hs["lon"] * n + lon) / (n + 1), 4)
            hs["last_seen"] = day["date"]
            hs["total_days"] += 1
            hs["total_points"] += day["n_points"]
            hs["avg_quality"] = round(
                (hs["avg_quality"] * n + day["avg_quality"]) / (n + 1), 2
            )
            hs["zones"].update(day["zones"])
            day["hotspot_id"] = best_hs
            day["dist_to_hotspot"] = round(best_dist, 1)

    # Convert zone sets to sorted lists
    for hs in hotspots.values():
        hs["zones"] = sorted(hs["zones"])

    return days, hotspots


# ---------------------------------------------------------------------------
# Detect movement events (arrivals, departures, migrations)
# ---------------------------------------------------------------------------

def detect_movement_events(days, gap_days=14):
    """
    Detect:
      - ARRIVAL: first appearance in a hotspot after a gap
      - DEPARTURE: last day in a hotspot before switching or gap
      - MIGRATION: movement from one hotspot to another within gap_days
    Returns list of event dicts.
    """
    events = []

    for i, day in enumerate(days):
        prev = days[i - 1] if i > 0 else None
        nxt  = days[i + 1] if i < len(days) - 1 else None

        # Gap since last observation
        if prev:
            gap = _date_gap(prev["date"], day["date"])
            hs_changed = day.get("hotspot_id") != prev.get("hotspot_id")
        else:
            gap = 999
            hs_changed = True

        # ARRIVAL event: gap > threshold OR hotspot changed
        if gap > gap_days or (hs_changed and gap <= gap_days):
            event_type = "ARRIVAL" if gap > gap_days else "MIGRATION"
            events.append({
                "type": event_type,
                "date": day["date"],
                "to_hotspot": day.get("hotspot_id"),
                "to_lat": day["lat"],
                "to_lon": day["lon"],
                "from_hotspot": prev.get("hotspot_id") if prev else None,
                "from_lat": prev["lat"] if prev else None,
                "from_lon": prev["lon"] if prev else None,
                "gap_days": gap if gap < 999 else None,
                "env_at_event": day["env"],
                "quality_at_event": day["avg_quality"],
            })
            if event_type == "MIGRATION" and prev:
                dist = haversine_miles(prev["lat"], prev["lon"], day["lat"], day["lon"])
                events[-1]["migration_distance_miles"] = round(dist, 1)

        # DEPARTURE: this is the last day in current hotspot before a switch
        if nxt:
            nxt_gap = _date_gap(day["date"], nxt["date"])
            nxt_hs_changed = nxt.get("hotspot_id") != day.get("hotspot_id")
            if nxt_gap > gap_days or nxt_hs_changed:
                events.append({
                    "type": "DEPARTURE",
                    "date": day["date"],
                    "from_hotspot": day.get("hotspot_id"),
                    "from_lat": day["lat"],
                    "from_lon": day["lon"],
                    "days_until_next": nxt_gap if nxt_gap < 999 else None,
                    "env_at_event": day["env"],
                    "quality_at_event": day["avg_quality"],
                })

    return events


def _date_gap(d1_str, d2_str):
    try:
        d1 = date.fromisoformat(d1_str)
        d2 = date.fromisoformat(d2_str)
        return (d2 - d1).days
    except Exception:
        return 999


# ---------------------------------------------------------------------------
# Residence time analysis
# ---------------------------------------------------------------------------

def residence_analysis(days, hotspots):
    """
    For each hotspot, compute how long fish were continuously present.
    Returns list of residence periods.
    """
    periods = []
    current = None

    for day in days:
        hs_id = day.get("hotspot_id")
        if current is None:
            current = {"hotspot": hs_id, "start": day["date"], "end": day["date"],
                       "days": 1, "quality_scores": [day["avg_quality"]],
                       "envs": [day["env"]]}
        elif hs_id == current["hotspot"]:
            gap = _date_gap(current["end"], day["date"])
            if gap <= 21:  # allow up to 3-week gap (no report ≠ fish gone)
                current["end"] = day["date"]
                current["days"] = _date_gap(current["start"], current["end"]) + 1
                current["quality_scores"].append(day["avg_quality"])
                current["envs"].append(day["env"])
            else:
                periods.append(_finalize_period(current, hotspots))
                current = {"hotspot": hs_id, "start": day["date"], "end": day["date"],
                           "days": 1, "quality_scores": [day["avg_quality"]],
                           "envs": [day["env"]]}
        else:
            periods.append(_finalize_period(current, hotspots))
            current = {"hotspot": hs_id, "start": day["date"], "end": day["date"],
                       "days": 1, "quality_scores": [day["avg_quality"]],
                       "envs": [day["env"]]}

    if current:
        periods.append(_finalize_period(current, hotspots))

    return sorted(periods, key=lambda x: -x["calendar_days"])


def _finalize_period(p, hotspots):
    hs = hotspots.get(p["hotspot"], {})
    scores = p["quality_scores"]
    return {
        "hotspot": p["hotspot"],
        "hotspot_lat": hs.get("lat"),
        "hotspot_lon": hs.get("lon"),
        "zones": hs.get("zones", []),
        "start_date": p["start"],
        "end_date": p["end"],
        "calendar_days": p["days"],
        "report_days": len(scores),
        "avg_quality": round(sum(scores) / len(scores), 2),
        "peak_quality": max(scores),
        "dominant_moon": _mode([e.get("moon_phase", "") for e in p["envs"]]),
        "dominant_wind_dir": _mode([e.get("wind_dir", "") for e in p["envs"]]),
        "avg_seas_ft": _avg_nonempty([e.get("seas_ft") for e in p["envs"]]),
        "avg_tide_range": _avg_nonempty([e.get("tide_range_ft") for e in p["envs"]]),
    }


def _mode(lst):
    lst = [x for x in lst if x]
    if not lst:
        return ""
    from collections import Counter
    return Counter(lst).most_common(1)[0][0]


def _avg_nonempty(lst):
    vals = [v for v in lst if v is not None]
    return round(sum(vals) / len(vals), 2) if vals else None


# ---------------------------------------------------------------------------
# Environmental trigger analysis
# ---------------------------------------------------------------------------

def departure_triggers(events, days):
    """
    For DEPARTURE events, look at what changed in the 3 days before departure
    vs the 7 days before that (baseline).
    """
    by_date = {d["date"]: d for d in days}
    triggers = []

    for ev in events:
        if ev["type"] != "DEPARTURE":
            continue

        dep_date = ev["date"]
        try:
            dep_dt = date.fromisoformat(dep_date)
        except ValueError:
            continue

        # Collect 3-day window before departure
        pre3 = []
        for delta in range(1, 4):
            d_str = (dep_dt - timedelta(days=delta)).isoformat()
            if d_str in by_date:
                pre3.append(by_date[d_str]["env"])

        # Baseline: days 4-10 before departure
        baseline = []
        for delta in range(4, 11):
            d_str = (dep_dt - timedelta(days=delta)).isoformat()
            if d_str in by_date:
                baseline.append(by_date[d_str]["env"])

        if not pre3 or not baseline:
            continue

        # Compare key metrics
        changes = {}
        for field in ["wind_kt", "seas_ft", "sea_severity", "moon_illum"]:
            pre_avg  = _avg_nonempty([e.get(field) for e in pre3])
            base_avg = _avg_nonempty([e.get(field) for e in baseline])
            if pre_avg is not None and base_avg is not None and base_avg > 0:
                pct_change = (pre_avg - base_avg) / base_avg * 100
                changes[f"{field}_change_pct"] = round(pct_change, 1)

        # Wind direction change
        pre_wind  = _mode([e.get("wind_dir", "") for e in pre3])
        base_wind = _mode([e.get("wind_dir", "") for e in baseline])
        if pre_wind != base_wind:
            changes["wind_dir_changed"] = f"{base_wind} → {pre_wind}"

        # Moon transition
        pre_moon  = _mode([e.get("moon_phase", "") for e in pre3])
        base_moon = _mode([e.get("moon_phase", "") for e in baseline])
        if pre_moon != base_moon:
            changes["moon_transition"] = f"{base_moon} → {pre_moon}"

        triggers.append({
            "departure_date": dep_date,
            "from_hotspot": ev.get("from_hotspot"),
            "env_at_departure": ev["env_at_event"],
            "changes_pre_departure": changes,
        })

    return triggers


# ---------------------------------------------------------------------------
# Seasonal migration calendar
# ---------------------------------------------------------------------------

def migration_calendar(days):
    """
    Summarize monthly presence: where was the species each month?
    Returns month (1-12) → {zones, hotspots, avg_lat, avg_lon, avg_quality, n_days}
    """
    by_month = defaultdict(list)
    for day in days:
        try:
            m = int(day["date"][5:7])
        except (IndexError, ValueError):
            continue
        by_month[m].append(day)

    calendar = {}
    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    for m in range(1, 13):
        pts = by_month.get(m, [])
        if not pts:
            calendar[m] = None
            continue
        lats = [p["lat"] for p in pts]
        lons = [p["lon"] for p in pts]
        calendar[m] = {
            "month": month_names[m],
            "n_report_days": len(pts),
            "avg_lat": round(sum(lats) / len(lats), 3),
            "avg_lon": round(sum(lons) / len(lons), 3),
            "avg_quality": round(sum(p["avg_quality"] for p in pts) / len(pts), 2),
            "zones": sorted({z for p in pts for z in p["zones"]}),
            "hotspots": sorted({p.get("hotspot_id", "") for p in pts if p.get("hotspot_id")}),
        }

    return calendar


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def analyze_species(species, obs_list, min_gps_per_day=2):
    """Full movement analysis for one species. Returns structured results."""
    if not obs_list:
        return None

    days = daily_centroids(obs_list)
    days = [d for d in days if d["n_points"] >= min_gps_per_day]
    if not days:
        return None

    days, hotspots = cluster_hotspots(days)
    events = detect_movement_events(days)
    periods = residence_analysis(days, hotspots)
    triggers = departure_triggers(events, days)
    calendar = migration_calendar(days)

    return {
        "species": species,
        "total_report_days": len(days),
        "total_gps_points": sum(d["n_points"] for d in days),
        "date_range": f"{days[0]['date']} to {days[-1]['date']}",
        "hotspots": hotspots,
        "movement_events": events,
        "residence_periods": periods,
        "departure_triggers": triggers,
        "migration_calendar": calendar,
        "daily_track": days,
    }


def print_report(result, verbose=False):
    if not result:
        return
    sp = result["species"]

    print(f"\n{'='*70}")
    print(f"  {sp.upper().replace('_',' ')} — Movement Analysis")
    print(f"{'='*70}")
    print(f"  Data: {result['total_report_days']} report-days, "
          f"{result['total_gps_points']} GPS points, {result['date_range']}")

    # Hotspot summary
    hotspots = result["hotspots"]
    print(f"\n  TOP HOTSPOTS ({len(hotspots)} total, clustered at 15mi radius):")
    print(f"  {'ID':<10} {'Lat':>8} {'Lon':>10} {'Days':>6} {'Pts':>6} {'Avg Q':>7}  Zones")
    print(f"  {'-'*75}")
    top_hs = sorted(hotspots.values(), key=lambda h: -h["total_days"])[:10]
    for hs in top_hs:
        zones_str = ", ".join(hs["zones"][:2]) + ("..." if len(hs["zones"]) > 2 else "")
        print(f"  {hs['id']:<10} {hs['lat']:>8.4f} {hs['lon']:>10.4f} "
              f"{hs['total_days']:>6} {hs['total_points']:>6} {hs['avg_quality']:>7.2f}  {zones_str}")

    # Longest residence periods
    periods = result["residence_periods"]
    print(f"\n  LONGEST RESIDENCE PERIODS (top 10):")
    print(f"  {'Start':>12} {'End':>12} {'Cal Days':>10} {'Rpt Days':>10} {'Avg Q':>7}  Moon / Wind / Seas")
    print(f"  {'-'*80}")
    for p in periods[:10]:
        moon = p.get("dominant_moon", "-")
        wind = p.get("dominant_wind_dir", "-")
        seas = f"{p['avg_seas_ft']}ft" if p.get("avg_seas_ft") else "-"
        print(f"  {p['start_date']:>12} {p['end_date']:>12} {p['calendar_days']:>10} "
              f"{p['report_days']:>10} {p['avg_quality']:>7.2f}  {moon} / {wind} / {seas}")

    # Seasonal calendar
    cal = result["migration_calendar"]
    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    print(f"\n  SEASONAL PRESENCE CALENDAR:")
    print(f"  {'Month':<6} {'Days':>5} {'Avg Q':>6} {'Avg Lat':>9} {'Avg Lon':>10}  Primary Zones")
    print(f"  {'-'*75}")
    for m in range(1, 13):
        c = cal.get(m)
        if not c:
            print(f"  {month_names[m]:<6} {'--':>5}")
            continue
        zones_str = ", ".join(c["zones"][:2]) + ("..." if len(c["zones"]) > 2 else "")
        print(f"  {c['month']:<6} {c['n_report_days']:>5} {c['avg_quality']:>6.2f} "
              f"{c['avg_lat']:>9.4f} {c['avg_lon']:>10.4f}  {zones_str}")

    # Movement events summary
    events = result["movement_events"]
    arrivals   = [e for e in events if e["type"] == "ARRIVAL"]
    migrations = [e for e in events if e["type"] == "MIGRATION"]
    departures = [e for e in events if e["type"] == "DEPARTURE"]
    print(f"\n  MOVEMENT EVENTS: {len(arrivals)} arrivals, "
          f"{len(migrations)} migrations, {len(departures)} departures")

    if migrations:
        print(f"\n  NOTABLE MIGRATIONS (>30 miles):")
        big = sorted([e for e in migrations if e.get("migration_distance_miles", 0) > 30],
                     key=lambda e: -e.get("migration_distance_miles", 0))[:5]
        for e in big:
            print(f"    {e['date']}  {e.get('from_hotspot','?')} → {e['to_hotspot']}  "
                  f"{e.get('migration_distance_miles', 0):.0f} mi  "
                  f"moon:{e['env_at_event'].get('moon_phase','?')}  "
                  f"seas:{e['env_at_event'].get('seas_ft','?')}ft")

    # Departure triggers
    triggers = result["departure_triggers"]
    if triggers:
        print(f"\n  DEPARTURE TRIGGERS (what changed before fish left):")
        # Aggregate change patterns across all departures
        sea_increases = [t for t in triggers
                         if t["changes_pre_departure"].get("seas_ft_change_pct", 0) > 30]
        wind_increases = [t for t in triggers
                          if t["changes_pre_departure"].get("wind_kt_change_pct", 0) > 40]
        moon_transitions = [t for t in triggers
                            if t["changes_pre_departure"].get("moon_transition")]

        total = len(triggers)
        print(f"    Sea increase >30%:  {len(sea_increases)}/{total} departures ({100*len(sea_increases)//total if total else 0}%)")
        print(f"    Wind increase >40%: {len(wind_increases)}/{total} departures ({100*len(wind_increases)//total if total else 0}%)")
        print(f"    Moon phase change:  {len(moon_transitions)}/{total} departures ({100*len(moon_transitions)//total if total else 0}%)")

        # Most common moon phases at departure
        moon_at_dep = [t["env_at_departure"].get("moon_phase", "") for t in triggers]
        moon_at_dep = [m for m in moon_at_dep if m]
        if moon_at_dep:
            from collections import Counter
            print(f"    Moon phase AT departure: {dict(Counter(moon_at_dep).most_common(4))}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="FishLark Movement Analysis")
    parser.add_argument("--species", help="Single species (default: all gamefish)")
    parser.add_argument("--min-gps", type=int, default=2,
                        help="Min GPS points per day to include (default: 2)")
    parser.add_argument("--radius", type=float, default=15.0,
                        help="Hotspot cluster radius in miles (default: 15)")
    parser.add_argument("--save-json", action="store_true",
                        help="Save per-species JSON to data/movement/")
    args = parser.parse_args()

    target_species = [args.species] if args.species else GAMEFISH

    print(f"Loading GPS observations from {ENRICHED_DATASET}...")
    sp_obs = load_species_gps(args.species)
    print(f"Loaded GPS data for {len(sp_obs)} species")

    MOVEMENT_DIR.mkdir(parents=True, exist_ok=True)

    for species in target_species:
        obs_list = sp_obs.get(species, [])
        if len(obs_list) < 10:
            print(f"\n  {species}: insufficient data ({len(obs_list)} GPS points)")
            continue

        result = analyze_species(species, obs_list, min_gps_per_day=args.min_gps)
        if not result:
            print(f"\n  {species}: no results after filtering")
            continue

        print_report(result)

        if args.save_json:
            # Serialize (convert sets etc.)
            def _ser(obj):
                if isinstance(obj, set):
                    return list(obj)
                raise TypeError(f"Not serializable: {type(obj)}")
            out = MOVEMENT_DIR / f"{species}_movement.json"
            # Remove daily_track from JSON (too large) unless verbose
            result_json = {k: v for k, v in result.items() if k != "daily_track"}
            out.write_text(json.dumps(result_json, indent=2, default=_ser))
            print(f"  → saved {out}")

    print("\nDone.")


if __name__ == "__main__":
    main()
