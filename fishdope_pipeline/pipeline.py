"""
FishLark Pipeline Orchestrator

Coordinates scraping, parsing, and export of Fishdope fish reports
into structured JSON suitable for ML training and map overlay.

Usage:
    python pipeline.py --scrape --start-page 1 --end-page 10
    python pipeline.py --parse
    python pipeline.py --export
    python pipeline.py --scrape --parse --export  # Full pipeline
"""
import argparse
import json
import logging
from pathlib import Path
from datetime import datetime

from config import (
    RAW_TEXT_DIR, PARSED_JSON_DIR, MASTER_DATASET,
    GPS_DATABASE, TOTAL_PAGES,
)
from scraper import FishdopeScraper
from parser_rules import RuleBasedParser, parse_all_reports

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def run_scraper(start_page=1, end_page=TOTAL_PAGES):
    logger.info(f"Starting scraper: pages {start_page} to {end_page}")
    scraper = FishdopeScraper()
    reports = scraper.scrape_all(start_page, end_page)
    logger.info(f"Scraping complete: {len(reports)} reports")
    return reports


def run_parser():
    logger.info("Starting rule-based parser")
    results = parse_all_reports()
    logger.info(f"Parsing complete: {len(results)} reports parsed")
    return results


def run_export():
    logger.info("Exporting master dataset and GPS database")
    parsed_dir = Path(PARSED_JSON_DIR)
    if not parsed_dir.exists():
        logger.error(f"Parsed directory {PARSED_JSON_DIR} not found. Run --parse first.")
        return

    files = sorted(parsed_dir.glob("*.json"))
    files = [f for f in files if not f.name.startswith("_")]

    # Master JSONL dataset
    master_path = Path(MASTER_DATASET)
    master_path.parent.mkdir(parents=True, exist_ok=True)

    all_gps = []
    report_count = 0

    with open(master_path, "w", encoding="utf-8") as master_file:
        for f in files:
            try:
                report = json.loads(f.read_text(encoding="utf-8"))
                master_file.write(json.dumps(report) + "\n")
                report_count += 1

                # Collect GPS waypoints
                for zone_list in [
                    report.get("inshore_zones", []),
                    report.get("mexican_zones", []),
                    report.get("offshore_zones", []),
                ]:
                    for zone in zone_list:
                        for wp in zone.get("gps_waypoints", []):
                            wp["zone_id"] = zone.get("zone_id", "")
                            wp["zone_name"] = zone.get("zone_name", "")
                            wp["report_date"] = report.get("date", "")
                            all_gps.append(wp)

                        for sp in zone.get("species_reports", []):
                            for coord in sp.get("gps_coords", []):
                                coord["zone_id"] = zone.get("zone_id", "")
                                coord["species"] = sp.get("species", "")
                                coord["report_date"] = report.get("date", "")
                                all_gps.append(coord)

            except Exception as e:
                logger.error(f"Error processing {f.name}: {e}")

    logger.info(f"Master dataset: {report_count} reports -> {master_path}")

    # GPS waypoint database
    gps_path = Path(GPS_DATABASE)
    gps_path.parent.mkdir(parents=True, exist_ok=True)

    # Deduplicate GPS by rounding to 3 decimal places
    unique_gps = {}
    for wp in all_gps:
        lat = round(wp.get("lat_decimal", 0), 3)
        lon = round(wp.get("lon_decimal", 0), 3)
        key = f"{lat},{lon}"
        if key not in unique_gps:
            unique_gps[key] = wp
        else:
            # Merge: keep the one with a name
            if wp.get("name") and not unique_gps[key].get("name"):
                unique_gps[key]["name"] = wp["name"]

    gps_list = list(unique_gps.values())
    gps_path.write_text(json.dumps(gps_list, indent=2), encoding="utf-8")
    logger.info(f"GPS database: {len(gps_list)} unique waypoints -> {gps_path}")

    # Summary stats
    print("\n" + "=" * 60)
    print("FISHLARK PIPELINE EXPORT SUMMARY")
    print("=" * 60)
    print(f"Reports exported:     {report_count}")
    print(f"GPS waypoints found:  {len(all_gps)} total, {len(gps_list)} unique")
    print(f"Master dataset:       {master_path}")
    print(f"GPS database:         {gps_path}")
    print("=" * 60)


def run_stats():
    """Print stats about the current dataset."""
    parsed_dir = Path(PARSED_JSON_DIR)
    if not parsed_dir.exists():
        print("No parsed data found. Run --parse first.")
        return

    files = sorted(parsed_dir.glob("*.json"))
    files = [f for f in files if not f.name.startswith("_")]

    species_counts = {}
    zone_counts = {}
    total_gps = 0
    dates = []

    for f in files:
        report = json.loads(f.read_text(encoding="utf-8"))
        if report.get("date"):
            dates.append(report["date"])

        for zone_list_key in ["inshore_zones", "mexican_zones", "offshore_zones"]:
            for zone in report.get(zone_list_key, []):
                zid = zone.get("zone_id", "unknown")
                zone_counts[zid] = zone_counts.get(zid, 0) + 1
                total_gps += len(zone.get("gps_waypoints", []))

                for sp in zone.get("species_reports", []):
                    species = sp.get("species", "unknown")
                    species_counts[species] = species_counts.get(species, 0) + 1
                    total_gps += len(sp.get("gps_coords", []))

    print("\n" + "=" * 60)
    print("DATASET STATISTICS")
    print("=" * 60)
    print(f"Total reports: {len(files)}")
    if dates:
        print(f"Date range: {min(dates)} to {max(dates)}")
    print(f"Total GPS coordinates: {total_gps}")
    print(f"\nTop 15 species by mention count:")
    for sp, count in sorted(species_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {sp:25s} {count:6d}")
    print(f"\nZone coverage:")
    for z, count in sorted(zone_counts.items(), key=lambda x: -x[1]):
        print(f"  {z:30s} {count:6d}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FishLark Pipeline")
    parser.add_argument("--scrape", action="store_true", help="Run the scraper")
    parser.add_argument("--parse", action="store_true", help="Run the parser")
    parser.add_argument("--export", action="store_true", help="Export master dataset")
    parser.add_argument("--stats", action="store_true", help="Print dataset stats")
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--end-page", type=int, default=TOTAL_PAGES)
    args = parser.parse_args()

    if not any([args.scrape, args.parse, args.export, args.stats]):
        parser.print_help()
        exit(1)

    if args.scrape:
        run_scraper(args.start_page, args.end_page)
    if args.parse:
        run_parser()
    if args.export:
        run_export()
    if args.stats:
        run_stats()
