"""
FishLark Analysis Engine

Four analysis types across the full Fishdope dataset:
  1. Seasonal  — species catch quality by month / season
  2. Spatial   — zone and GPS hotspot rankings by species
  3. Conditions — water temp, wind, sea height vs. catch quality
  4. Trending  — year-over-year shifts in species activity

Usage:
    python analyze.py                    # all analyses
    python analyze.py --type seasonal
    python analyze.py --type spatial
    python analyze.py --type conditions
    python analyze.py --type trending
    python analyze.py --species bluefin_tuna  # filter to one species
"""
import argparse
import json
import math
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median, stdev

from config import MASTER_DATASET, PARSED_JSON_DIR

ANALYSIS_DIR = "data/analysis"

SEASONS = {
    12: "Winter", 1: "Winter", 2: "Winter",
    3: "Spring", 4: "Spring", 5: "Spring",
    6: "Summer", 7: "Summer", 8: "Summer",
    9: "Fall", 10: "Fall", 11: "Fall",
}

MONTH_NAMES = [
    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_observations(species_filter=None):
    """
    Flatten the master JSONL into a list of per-species-per-zone observations.
    Each record has: date, year, month, season, zone_id, sector, species,
    catch_quality_score, water_temp_f, fish_size_lbs_min/max,
    depth_ft_min/max, best_bait, wind_direction, sea_height_ft_max,
    gps_coords, day_of_week.
    """
    master = Path(MASTER_DATASET)
    if not master.exists():
        print(f"Master dataset not found at {MASTER_DATASET}. Run: python pipeline.py --parse --export")
        return []

    obs = []
    with open(master, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                report = json.loads(line)
            except json.JSONDecodeError:
                continue

            date_str = report.get("date", "")
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                year, month = dt.year, dt.month
            except ValueError:
                year, month = None, None

            season = SEASONS.get(month, "") if month else ""
            day_of_week = report.get("day_of_week", "")
            weather = report.get("weather") or {}
            wind_dir = weather.get("wind_direction", "")
            sea_max = weather.get("sea_height_ft_max")

            for zone_key in ["inshore_zones", "mexican_zones", "offshore_zones"]:
                for zone in report.get(zone_key, []):
                    zone_id = zone.get("zone_id", "")
                    sector = zone.get("sector", "")
                    for sp in zone.get("species_reports", []):
                        species = sp.get("species", "")
                        if species_filter and species != species_filter:
                            continue
                        obs.append({
                            "date": date_str,
                            "year": year,
                            "month": month,
                            "season": season,
                            "day_of_week": day_of_week,
                            "zone_id": zone_id,
                            "sector": sector,
                            "species": species,
                            "catch_quality_score": sp.get("catch_quality_score", 0),
                            "catch_quality": sp.get("catch_quality", ""),
                            "water_temp_f": sp.get("water_temp_f"),
                            "fish_size_lbs_min": sp.get("fish_size_lbs_min"),
                            "fish_size_lbs_max": sp.get("fish_size_lbs_max"),
                            "depth_ft_min": sp.get("depth_ft_min"),
                            "depth_ft_max": sp.get("depth_ft_max"),
                            "best_bait": sp.get("best_bait", []),
                            "gps_coords": sp.get("gps_coords", []),
                            "wind_direction": wind_dir,
                            "sea_height_ft_max": sea_max,
                        })
    return obs


def _avg(values):
    vals = [v for v in values if v is not None]
    return round(mean(vals), 2) if vals else None


def _bar(score, max_score=5, width=20):
    filled = int(round(score / max_score * width))
    return "█" * filled + "░" * (width - filled)


def _print_header(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def _save(name, content):
    Path(ANALYSIS_DIR).mkdir(parents=True, exist_ok=True)
    path = Path(ANALYSIS_DIR) / f"{name}.txt"
    path.write_text(content, encoding="utf-8")
    print(f"  → saved to {path}")


# ---------------------------------------------------------------------------
# 1. Seasonal analysis
# ---------------------------------------------------------------------------

def analyze_seasonal(obs, species_filter=None):
    _print_header("SEASONAL ANALYSIS — Catch Quality by Species & Month")

    # Group: species → month → scores
    sp_month = defaultdict(lambda: defaultdict(list))
    sp_season = defaultdict(lambda: defaultdict(list))

    for o in obs:
        if not o["month"]:
            continue
        sp_month[o["species"]][o["month"]].append(o["catch_quality_score"])
        sp_season[o["species"]][o["season"]].append(o["catch_quality_score"])

    # Rank species by total mention count
    sp_counts = {sp: sum(len(v) for v in months.values()) for sp, months in sp_month.items()}
    top_species = sorted(sp_counts, key=lambda s: -sp_counts[s])
    if species_filter:
        top_species = [s for s in top_species if s == species_filter]
    else:
        top_species = top_species[:20]

    lines = []
    lines.append("SEASONAL ANALYSIS — Catch Quality by Species & Month\n")
    lines.append(f"{'Species':<25} {'Mentions':>8}  Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec  Best Month")
    lines.append("-" * 100)

    for sp in top_species:
        months = sp_month[sp]
        row_avgs = []
        for m in range(1, 13):
            scores = months.get(m, [])
            row_avgs.append(_avg(scores) if scores else None)

        best_month = None
        best_score = -1
        for m, avg in enumerate(row_avgs, 1):
            if avg is not None and avg > best_score:
                best_score = avg
                best_month = MONTH_NAMES[m]

        month_str = "  ".join(
            f"{v:.1f}" if v is not None else " -- " for v in row_avgs
        )
        line = f"{sp:<25} {sp_counts[sp]:>8}  {month_str}  {best_month or '-'}"
        lines.append(line)
        print(line)

    # Season summary per species
    lines.append("\n\nSEASON AVERAGES (0-5 quality scale)")
    lines.append(f"{'Species':<25} {'Winter':>8} {'Spring':>8} {'Summer':>8} {'Fall':>8}  Peak Season")
    lines.append("-" * 80)
    print()
    print("SEASON AVERAGES")
    print(f"{'Species':<25} {'Winter':>8} {'Spring':>8} {'Summer':>8} {'Fall':>8}  Peak Season")
    print("-" * 80)

    for sp in top_species:
        seasons = sp_season[sp]
        avgs = {s: _avg(seasons.get(s, [])) for s in ["Winter", "Spring", "Summer", "Fall"]}
        peak = max((s for s in avgs if avgs[s] is not None), key=lambda s: avgs[s] or 0, default="-")
        row = (f"{sp:<25} "
               f"{str(avgs.get('Winter') or '--'):>8} "
               f"{str(avgs.get('Spring') or '--'):>8} "
               f"{str(avgs.get('Summer') or '--'):>8} "
               f"{str(avgs.get('Fall') or '--'):>8}  {peak}")
        lines.append(row)
        print(row)

    _save("seasonal", "\n".join(lines))


# ---------------------------------------------------------------------------
# 2. Spatial analysis
# ---------------------------------------------------------------------------

def analyze_spatial(obs, species_filter=None):
    _print_header("SPATIAL ANALYSIS — Zone Rankings & GPS Hotspots")

    # Zone → species → scores
    zone_sp = defaultdict(lambda: defaultdict(list))
    # Zone → all scores (across species)
    zone_all = defaultdict(list)
    # Species → zone → scores
    sp_zone = defaultdict(lambda: defaultdict(list))
    # GPS coordinate clusters: (lat_rounded, lon_rounded) → list of observations
    gps_clusters = defaultdict(list)

    for o in obs:
        zid = o["zone_id"]
        sp = o["species"]
        score = o["catch_quality_score"]
        zone_sp[zid][sp].append(score)
        zone_all[zid].append(score)
        sp_zone[sp][zid].append(score)

        for coord in o.get("gps_coords", []):
            lat = round(coord.get("lat_decimal", 0), 2)
            lon = round(coord.get("lon_decimal", 0), 2)
            if lat and lon:
                gps_clusters[(lat, lon)].append({
                    "species": sp,
                    "score": score,
                    "zone": zid,
                    "date": o["date"],
                    "name": coord.get("name", ""),
                })

    lines = []
    lines.append("SPATIAL ANALYSIS — Zone Rankings & GPS Hotspots\n")

    # Overall zone ranking
    zone_scores = {z: _avg(scores) for z, scores in zone_all.items()}
    zone_mentions = {z: len(scores) for z, scores in zone_all.items()}
    top_zones = sorted(zone_scores, key=lambda z: -(zone_scores[z] or 0))

    header = f"\n{'Zone':<32} {'Avg Quality':>11} {'Mentions':>10}  Bar"
    lines.append("ZONE RANKINGS (overall avg catch quality)")
    lines.append(header)
    lines.append("-" * 75)
    print("ZONE RANKINGS (overall avg catch quality)")
    print(header)
    print("-" * 75)

    for z in top_zones:
        avg = zone_scores[z]
        if avg is None:
            continue
        bar = _bar(avg)
        row = f"{z:<32} {avg:>11.2f} {zone_mentions[z]:>10}  {bar}"
        lines.append(row)
        print(row)

    # Best zone per species
    sp_counts = {sp: sum(len(v) for v in zones.values()) for sp, zones in sp_zone.items()}
    top_species = sorted(sp_counts, key=lambda s: -sp_counts[s])
    if species_filter:
        top_species = [s for s in top_species if s == species_filter]
    else:
        top_species = top_species[:15]

    lines.append("\n\nBEST ZONES BY SPECIES")
    print("\nBEST ZONES BY SPECIES")

    for sp in top_species:
        zones = sp_zone[sp]
        zone_avgs = {z: _avg(scores) for z, scores in zones.items() if scores}
        best_zones = sorted(zone_avgs, key=lambda z: -(zone_avgs[z] or 0))[:3]
        best_str = " | ".join(f"{z}({zone_avgs[z]:.1f})" for z in best_zones)
        row = f"  {sp:<25} {best_str}"
        lines.append(row)
        print(row)

    # GPS hotspots
    hot = sorted(gps_clusters.items(), key=lambda x: -len(x[1]))[:30]
    lines.append("\n\nTOP 30 GPS HOTSPOTS (by number of reports)")
    lines.append(f"{'Lat':>8} {'Lon':>10}  {'Reports':>7}  {'Avg Quality':>11}  {'Name / Species'}")
    lines.append("-" * 75)
    print("\nTOP 30 GPS HOTSPOTS")
    print(f"{'Lat':>8} {'Lon':>10}  {'Reports':>7}  {'Avg Quality':>11}  {'Name / Species'}")
    print("-" * 75)

    for (lat, lon), records in hot:
        avg_score = _avg([r["score"] for r in records])
        name = next((r["name"] for r in records if r.get("name")), "")
        species_counts = defaultdict(int)
        for r in records:
            species_counts[r["species"]] += 1
        top_sp = sorted(species_counts, key=lambda s: -species_counts[s])[:3]
        sp_str = ", ".join(top_sp)
        row = f"{lat:>8.4f} {lon:>10.4f}  {len(records):>7}  {str(avg_score or '--'):>11}  {name or sp_str}"
        lines.append(row)
        print(row)

    _save("spatial", "\n".join(lines))


# ---------------------------------------------------------------------------
# 3. Conditions analysis
# ---------------------------------------------------------------------------

def analyze_conditions(obs, species_filter=None):
    _print_header("CONDITIONS ANALYSIS — Water Temp, Wind & Sea vs. Catch Quality")

    sp_counts = defaultdict(int)
    for o in obs:
        sp_counts[o["species"]] += 1
    top_species = sorted(sp_counts, key=lambda s: -sp_counts[s])
    if species_filter:
        top_species = [s for s in top_species if s == species_filter]
    else:
        top_species = top_species[:12]

    lines = []
    lines.append("CONDITIONS ANALYSIS — Water Temp, Wind & Sea vs. Catch Quality\n")

    # Water temperature buckets
    temp_buckets = [(50, 56), (56, 62), (62, 66), (66, 70), (70, 75), (75, 85)]
    temp_scores = defaultdict(list)
    for o in obs:
        t = o.get("water_temp_f")
        if t:
            for lo, hi in temp_buckets:
                if lo <= t < hi:
                    temp_scores[f"{lo}-{hi}°F"].append(o["catch_quality_score"])
                    break

    lines.append("CATCH QUALITY BY WATER TEMPERATURE (all species)")
    print("CATCH QUALITY BY WATER TEMPERATURE (all species)")
    print(f"{'Temp Range':<14} {'Avg Quality':>11} {'Observations':>14}  Bar")
    print("-" * 65)
    lines.append(f"{'Temp Range':<14} {'Avg Quality':>11} {'Observations':>14}  Bar")
    lines.append("-" * 65)

    for bucket_key in [f"{lo}-{hi}°F" for lo, hi in temp_buckets]:
        scores = temp_scores.get(bucket_key, [])
        if not scores:
            continue
        avg = _avg(scores)
        bar = _bar(avg)
        row = f"{bucket_key:<14} {avg:>11.2f} {len(scores):>14}  {bar}"
        lines.append(row)
        print(row)

    # Per-species optimal temperature
    lines.append("\n\nOPTIMAL WATER TEMP BY SPECIES")
    print("\nOPTIMAL WATER TEMP BY SPECIES")
    print(f"{'Species':<25} {'Obs w/ Temp':>11} {'Avg Temp':>10} {'Temp@Best Bite':>15} {'Quality@Temp':>13}")
    print("-" * 80)
    lines.append(f"{'Species':<25} {'Obs w/ Temp':>11} {'Avg Temp':>10} {'Temp@Best Bite':>15} {'Quality@Temp':>13}")
    lines.append("-" * 80)

    for sp in top_species:
        sp_obs = [o for o in obs if o["species"] == sp and o.get("water_temp_f")]
        if len(sp_obs) < 5:
            continue
        avg_temp = _avg([o["water_temp_f"] for o in sp_obs])
        # Find temp bucket with highest avg quality for this species
        sp_temp_buckets = defaultdict(list)
        for o in sp_obs:
            t = o["water_temp_f"]
            for lo, hi in temp_buckets:
                if lo <= t < hi:
                    sp_temp_buckets[f"{lo}-{hi}°F"].append(o["catch_quality_score"])
                    break
        best_bucket = max(sp_temp_buckets, key=lambda k: _avg(sp_temp_buckets[k]) or 0, default="-")
        best_quality = _avg(sp_temp_buckets.get(best_bucket, []))
        row = f"{sp:<25} {len(sp_obs):>11} {str(avg_temp or '--'):>10} {best_bucket:>15} {str(best_quality or '--'):>13}"
        lines.append(row)
        print(row)

    # Sea height vs catch quality
    sea_buckets = {"Calm (0-2ft)": (0, 2), "Moderate (2-4ft)": (2, 4),
                   "Rough (4-6ft)": (4, 6), "Very Rough (6+ft)": (6, 99)}
    sea_scores = defaultdict(list)
    for o in obs:
        s = o.get("sea_height_ft_max")
        if s is not None:
            for label, (lo, hi) in sea_buckets.items():
                if lo <= s < hi:
                    sea_scores[label].append(o["catch_quality_score"])
                    break

    lines.append("\n\nCATCH QUALITY BY SEA CONDITIONS (all species)")
    print("\nCATCH QUALITY BY SEA CONDITIONS (all species)")
    print(f"{'Sea Conditions':<22} {'Avg Quality':>11} {'Observations':>14}  Bar")
    print("-" * 65)
    lines.append(f"{'Sea Conditions':<22} {'Avg Quality':>11} {'Observations':>14}  Bar")
    lines.append("-" * 65)

    for label in sea_buckets:
        scores = sea_scores.get(label, [])
        if not scores:
            continue
        avg = _avg(scores)
        bar = _bar(avg)
        row = f"{label:<22} {avg:>11.2f} {len(scores):>14}  {bar}"
        lines.append(row)
        print(row)

    # Wind direction vs catch quality
    wind_scores = defaultdict(list)
    for o in obs:
        wd = o.get("wind_direction", "")
        if wd:
            wind_scores[wd].append(o["catch_quality_score"])

    lines.append("\n\nCATCH QUALITY BY WIND DIRECTION (all species)")
    print("\nCATCH QUALITY BY WIND DIRECTION (all species)")
    print(f"{'Wind Dir':<12} {'Avg Quality':>11} {'Observations':>14}  Bar")
    print("-" * 55)
    lines.append(f"{'Wind Dir':<12} {'Avg Quality':>11} {'Observations':>14}  Bar")
    lines.append("-" * 55)

    for wd in sorted(wind_scores, key=lambda w: -len(wind_scores[w])):
        scores = wind_scores[wd]
        avg = _avg(scores)
        bar = _bar(avg)
        row = f"{wd:<12} {avg:>11.2f} {len(scores):>14}  {bar}"
        lines.append(row)
        print(row)

    # Best bait by species
    lines.append("\n\nMOST EFFECTIVE BAITS BY SPECIES")
    print("\nMOST EFFECTIVE BAITS BY SPECIES")

    for sp in top_species:
        sp_obs = [o for o in obs if o["species"] == sp]
        bait_scores = defaultdict(list)
        for o in sp_obs:
            for bait in o.get("best_bait", []):
                bait_scores[bait].append(o["catch_quality_score"])
        if not bait_scores:
            continue
        best_baits = sorted(bait_scores, key=lambda b: -((_avg(bait_scores[b]) or 0) * len(bait_scores[b])))[:4]
        bait_str = " | ".join(f"{b}({_avg(bait_scores[b]):.1f}, n={len(bait_scores[b])})" for b in best_baits)
        row = f"  {sp:<25} {bait_str}"
        lines.append(row)
        print(row)

    _save("conditions", "\n".join(lines))


# ---------------------------------------------------------------------------
# 4. Trending analysis
# ---------------------------------------------------------------------------

def analyze_trending(obs, species_filter=None):
    _print_header("TRENDING ANALYSIS — Year-over-Year Species Activity")

    # Year → species → scores
    year_sp = defaultdict(lambda: defaultdict(list))
    # Species → year → scores
    sp_year = defaultdict(lambda: defaultdict(list))

    for o in obs:
        if not o["year"]:
            continue
        year_sp[o["year"]][o["species"]].append(o["catch_quality_score"])
        sp_year[o["species"]][o["year"]].append(o["catch_quality_score"])

    years = sorted(year_sp.keys())
    sp_counts = defaultdict(int)
    for o in obs:
        sp_counts[o["species"]] += 1

    top_species = sorted(sp_counts, key=lambda s: -sp_counts[s])
    if species_filter:
        top_species = [s for s in top_species if s == species_filter]
    else:
        top_species = top_species[:15]

    lines = []
    lines.append("TRENDING ANALYSIS — Year-over-Year Species Activity\n")

    # Year × species matrix (avg quality scores)
    year_cols = "  ".join(str(y) for y in years)
    lines.append(f"AVG CATCH QUALITY BY YEAR (0-5 scale)\n{'Species':<25}  {year_cols}")
    lines.append("-" * (27 + len(years) * 6))
    print(f"AVG CATCH QUALITY BY YEAR (0-5 scale)")
    print(f"{'Species':<25}  {year_cols}")
    print("-" * (27 + len(years) * 6))

    for sp in top_species:
        year_avgs = [_avg(sp_year[sp].get(y, [])) for y in years]
        row_vals = "  ".join(f"{v:.2f}" if v is not None else "  --" for v in year_avgs)
        row = f"{sp:<25}  {row_vals}"
        lines.append(row)
        print(row)

    # Trend direction: compare first 3 years vs last 3 years
    if len(years) >= 6:
        early_years = set(years[:3])
        recent_years = set(years[-3:])

        lines.append("\n\nTREND DIRECTION (early 3 years vs recent 3 years)")
        lines.append(f"  Early: {sorted(early_years)}  |  Recent: {sorted(recent_years)}")
        print(f"\nTREND DIRECTION")
        print(f"  Early: {sorted(early_years)}  |  Recent: {sorted(recent_years)}")
        print(f"{'Species':<25} {'Early Avg':>10} {'Recent Avg':>11} {'Change':>8}  Trend")
        print("-" * 70)
        lines.append(f"{'Species':<25} {'Early Avg':>10} {'Recent Avg':>11} {'Change':>8}  Trend")
        lines.append("-" * 70)

        for sp in top_species:
            early_scores = []
            recent_scores = []
            for y, scores in sp_year[sp].items():
                if y in early_years:
                    early_scores.extend(scores)
                elif y in recent_years:
                    recent_scores.extend(scores)

            early_avg = _avg(early_scores)
            recent_avg = _avg(recent_scores)
            if early_avg is None or recent_avg is None:
                continue
            change = recent_avg - early_avg
            trend = "▲ IMPROVING" if change > 0.2 else ("▼ DECLINING" if change < -0.2 else "→ STABLE")
            row = f"{sp:<25} {early_avg:>10.2f} {recent_avg:>11.2f} {change:>+8.2f}  {trend}"
            lines.append(row)
            print(row)

    # Annual report count (effort proxy)
    lines.append("\n\nANNUAL REPORT VOLUME (proxy for fleet effort)")
    print("\nANNUAL REPORT VOLUME")
    print(f"{'Year':>6}  {'Reports':>8}  Bar")
    print("-" * 45)
    lines.append(f"{'Year':>6}  {'Reports':>8}  Bar")
    lines.append("-" * 45)

    max_count = max(sum(len(v) for v in sp.values()) for sp in year_sp.values()) if year_sp else 1
    for y in years:
        count = sum(len(v) for v in year_sp[y].values())
        bar_width = int(count / max_count * 30)
        bar = "█" * bar_width
        row = f"{y:>6}  {count:>8}  {bar}"
        lines.append(row)
        print(row)

    # Species diversity per year
    lines.append("\n\nSPECIES DIVERSITY BY YEAR (# unique species reported)")
    print("\nSPECIES DIVERSITY BY YEAR")
    print(f"{'Year':>6}  {'Species':>8}  List")
    print("-" * 60)
    lines.append(f"{'Year':>6}  {'Species':>8}  List")
    lines.append("-" * 60)

    for y in years:
        sp_in_year = sorted(year_sp[y].keys())
        row = f"{y:>6}  {len(sp_in_year):>8}  {', '.join(sp_in_year[:8])}{'...' if len(sp_in_year) > 8 else ''}"
        lines.append(row)
        print(row)

    _save("trending", "\n".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="FishLark Analysis Engine")
    parser.add_argument("--type", choices=["seasonal", "spatial", "conditions", "trending"],
                        help="Analysis type (default: all)")
    parser.add_argument("--species", help="Filter to a specific species (e.g. bluefin_tuna)")
    args = parser.parse_args()

    print(f"Loading observations from {MASTER_DATASET}...")
    obs = load_observations(species_filter=args.species)

    if not obs:
        print("No data found. Make sure you've run: python pipeline.py --parse --export")
        return

    print(f"Loaded {len(obs):,} species observations")
    if args.species:
        print(f"Filtered to species: {args.species}")

    run_all = not args.type
    if run_all or args.type == "seasonal":
        analyze_seasonal(obs, args.species)
    if run_all or args.type == "spatial":
        analyze_spatial(obs, args.species)
    if run_all or args.type == "conditions":
        analyze_conditions(obs, args.species)
    if run_all or args.type == "trending":
        analyze_trending(obs, args.species)

    print(f"\nAll results saved to {ANALYSIS_DIR}/")


if __name__ == "__main__":
    main()
