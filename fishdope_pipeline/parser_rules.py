"""
Phase 2a: Rule-based parser for extracting structured data from raw report text.

Handles GPS coordinates, weather, bait reports, zone splitting,
species identification, catch quality, depth, temperature, and technique extraction.
"""
import re
import json
import logging
from pathlib import Path
from typing import Optional

from config import ZONES, SPECIES_MAP, CATCH_QUALITY_MAP, RAW_TEXT_DIR, PARSED_JSON_DIR
from schema import (
    FishdopeReport, ZoneReport, SpeciesReport, WeatherData,
    BaitReport, GPSCoordinate, SquidReport,
)

logger = logging.getLogger(__name__)


class RuleBasedParser:

    # GPS: "32 51.490 x 117 16.910"
    GPS_PATTERN = re.compile(
        r"(\d{2})\s+(\d{1,2}(?:\.\d{1,3})?)\s*x\s*(\d{2,3})\s+(\d{1,2}(?:\.\d{1,3})?)"
    )

    # Section delimiters
    INSHORE_START = re.compile(r"\*+\s*INSHORE\s*[&]\s*ISLANDS\s*Section\s*\*+", re.I)
    MEXICAN_START = re.compile(r"[-\u2014]+\s*Mexican\s*Waters?\s*[-\u2014]+", re.I)
    OFFSHORE_START = re.compile(r"\*+\s*OFFSHORE\s*Section\s*\*+", re.I)

    # Zone headers (pattern, zone_id)
    ZONE_HEADERS = [
        (r"San Miguel and Santa Rosa", "san_miguel_santa_rosa"),
        (r"Santa Cruz Island.*?(?:Anacapa|Ventura|Oxnard)", "santa_cruz_anacapa"),
        (r"Channel Islands", "santa_cruz_anacapa"),
        (r"San Nicolas Island", "san_nicolas"),
        (r"Santa Barbara Island", "santa_barbara_island"),
        (r"Santa Monica Bay", "santa_monica_bay"),
        (r"Palos Verdes.*?(?:LA Harbor|Long Beach|Newport)", "palos_verdes_newport"),
        (r"Catalina Island", "catalina"),
        (r"San Clemente Island", "san_clemente_island"),
        (r"Dana Point.*?Oceanside", "dana_point_oceanside"),
        (r"(?:Del Mar|La Jolla|Point Loma|Imperial Beach)", "la_jolla_point_loma"),
        (r"Lower 9 Mile Bank", "lower_9_mile"),
        (r"Coronado Islands.*?Rockpile", "coronado_islands"),
        (r"Rockpile", "coronado_islands"),
        (r"Salsipuedes.*?(?:Ensenada|Santo Tomas|Isolete)", "ensenada_south"),
        (r"14 Mile Bank", "14_mile_bank"),
        (r"(?:Lower 500|West of Colonet)", "lower_500_colonet"),
        (r"(?:150-275|Ranger Bank|Mushroom|Hidden Bank)", "deep_offshore"),
    ]

    TEMP_PATTERN = re.compile(r"(\d{2,3})[\s-]*(?:degree|\u00b0)\s*(?:water|f)?", re.I)
    TEMP_F_PATTERN = re.compile(r"(\d{2})f\b", re.I)
    DEPTH_RANGE = re.compile(r"(\d{1,4})\s*(?:to|-)\s*(\d{1,4})\s*(?:feet|foot|ft)", re.I)
    DEPTH_SINGLE = re.compile(r"(\d{1,4})\s*(?:feet|foot|ft)", re.I)
    SIZE_RANGE = re.compile(r"(\d{1,3})[\s-]*(?:to|-)\s*(\d{1,3})\s*(?:lb|pound|#)", re.I)
    WIND_PATTERN = re.compile(r"(N|NE|E|SE|S|SW|W|NW)\s+wind\s+(\d{1,2})\s+to\s+(\d{1,2})\s+kt", re.I)
    SEA_PATTERN = re.compile(r"Seas?\s+(\d{1,2})\s+to\s+(\d{1,2})\s+ft", re.I)

    def parse_report(self, raw_data: dict) -> dict:
        text = raw_data.get("raw_text", "")
        report = FishdopeReport(
            report_id=raw_data.get("post_id", ""),
            url=raw_data.get("url", ""),
            date=raw_data.get("date", ""),
            title=raw_data.get("title", ""),
            timestamp=raw_data.get("timestamp", ""),
            raw_text=text,
        )

        dow_match = re.match(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)", report.title, re.I)
        report.day_of_week = dow_match.group(1) if dow_match else ""
        report.highlights = self._parse_highlights(text)
        report.weather = self._parse_weather(text)
        report.bait_reports = self._parse_bait_reports(text)
        report.closures = self._parse_closures(text)

        sections = self._split_sections(text)
        report.inshore_zones = self._parse_zones(sections.get("inshore", ""))
        report.mexican_zones = self._parse_zones(sections.get("mexican", ""))
        report.offshore_zones = self._parse_zones(sections.get("offshore", ""))

        return report.to_dict()

    def _parse_highlights(self, text):
        match = re.search(r"Today's Highlights:?\s*\n(.*?)(?:\n\n|\nAttention|\nDear)", text, re.DOTALL | re.I)
        if match:
            return [l.strip().rstrip(".").strip() for l in match.group(1).strip().split("\n") if l.strip()]
        return []

    def _parse_weather(self, text):
        weather = {}
        match = re.search(r"Weather\s*[-\u2014]+\s*\n(.*?)(?:\n\n[-\u2014]+|Weather Forecasts)", text, re.DOTALL | re.I)
        if match:
            section = match.group(1)
            weather["summary"] = section.strip()[:500]
            weather["small_craft_advisory"] = bool(re.search(r"SMALL CRAFT ADVISORY", section, re.I))

            wind = self.WIND_PATTERN.search(section)
            if wind:
                weather["wind_direction"] = wind.group(1)
                weather["wind_speed_kt_min"] = int(wind.group(2))
                weather["wind_speed_kt_max"] = int(wind.group(3))

            sea = self.SEA_PATTERN.search(section)
            if sea:
                weather["sea_height_ft_min"] = int(sea.group(1))
                weather["sea_height_ft_max"] = int(sea.group(2))
        return weather

    def _parse_bait_reports(self, text):
        reports = []
        match = re.search(r"Bait Report\s*[-\u2014]+.*?\n(.*?)(?:\*{5,}|INSHORE)", text, re.DOTALL | re.I)
        if not match:
            return reports
        section = match.group(1)

        locations = [
            "San Diego", "Mission Bay", "Oceanside", "Dana Point",
            "Newport", "San Pedro", "Redondo", "Marina Del Rey",
            "CISCOS", "Hooks Landing", "Ventura", "Santa Barbara",
        ]
        for loc in locations:
            pattern = re.compile(rf"{re.escape(loc)}.*?[-\u2013]\s*(.+?)(?:\n|$)", re.I)
            m = pattern.search(section)
            if m:
                line = m.group(1).strip()
                bait_type = ""
                bait_size = ""
                for bt in ["sardine", "anchovy", "mackerel", "squid"]:
                    if bt in line.lower():
                        bait_type = bt
                        break
                size_m = re.search(r"(\d+-\d+)[\u201d\"\u2033]", line)
                if size_m:
                    bait_size = size_m.group(1) + " inch"

                available = "out of bait" not in line.lower() and "closed" not in line.lower()

                reports.append({
                    "location": loc,
                    "bait_type": bait_type,
                    "bait_size": bait_size,
                    "available": available,
                    "notes": line[:200],
                })
        return reports

    def _parse_closures(self, text):
        closures = []
        if re.search(r"Rockfish CLOSED", text, re.I):
            m = re.search(r"Rockfish CLOSED.*?(?:until|through)\s+(.+?)(?:\*|\n)", text, re.I)
            closures.append(m.group(0).strip() if m else "Rockfish CLOSED")
        return closures

    def _split_sections(self, text):
        sections = {"inshore": "", "mexican": "", "offshore": ""}
        inshore_m = self.INSHORE_START.search(text)
        mexican_m = self.MEXICAN_START.search(text)
        offshore_m = self.OFFSHORE_START.search(text)

        if inshore_m and mexican_m:
            sections["inshore"] = text[inshore_m.end():mexican_m.start()]
        elif inshore_m:
            sections["inshore"] = text[inshore_m.end():]

        if mexican_m and offshore_m:
            sections["mexican"] = text[mexican_m.end():offshore_m.start()]
        elif mexican_m:
            sections["mexican"] = text[mexican_m.end():]

        if offshore_m:
            sections["offshore"] = text[offshore_m.end():]

        return sections

    def _parse_zones(self, section_text):
        if not section_text.strip():
            return []

        zones = []
        zone_splits = []

        for pattern, zone_id in self.ZONE_HEADERS:
            for m in re.finditer(pattern, section_text, re.I):
                zone_splits.append((m.start(), zone_id, m.group()))

        zone_splits.sort(key=lambda x: x[0])

        for i, (start, zone_id, header) in enumerate(zone_splits):
            end = zone_splits[i + 1][0] if i + 1 < len(zone_splits) else len(section_text)
            zone_text = section_text[start:end]

            zone_config = ZONES.get(zone_id, {})
            zone = {
                "zone_id": zone_id,
                "zone_name": zone_config.get("name", header),
                "sector": zone_config.get("sector", ""),
                "small_craft_advisory": bool(re.search(r"SMALL CRAFT ADVISORY", zone_text, re.I)),
                "squid_report": self._parse_squid(zone_text),
                "species_reports": self._parse_species(zone_text),
                "gps_waypoints": self._extract_gps(zone_text),
                "raw_text": zone_text.strip()[:2000],
            }
            zones.append(zone)
        return zones

    def _parse_squid(self, text):
        squid_m = re.search(r"Squid[;:]\s*(.*?)(?:\n\n|Fishing[;:])", text, re.DOTALL | re.I)
        if not squid_m:
            return None
        squid_text = squid_m.group(1).strip()
        present = not any(w in squid_text.lower() for w in ["no squid", "no update", "sketchy", "dry"])
        return {
            "present": present,
            "notes": squid_text[:300],
            "commercial_fleet_active": "commercial" in squid_text.lower() and "not fishing" not in squid_text.lower(),
        }

    def _parse_species(self, text):
        species_reports = []
        text_lower = text.lower()

        for name, normalized in SPECIES_MAP.items():
            if name in text_lower:
                if normalized in [s["species"] for s in species_reports]:
                    continue

                sr = {"species": normalized, "notes": ""}

                # Catch quality
                for quality_phrase, score in sorted(CATCH_QUALITY_MAP.items(), key=lambda x: -len(x[0])):
                    if quality_phrase in text_lower:
                        ctx_pattern = re.compile(
                            rf"(?:{re.escape(name)}).{{0,150}}(?:{re.escape(quality_phrase)})"
                            rf"|(?:{re.escape(quality_phrase)}).{{0,150}}(?:{re.escape(name)})",
                            re.I | re.DOTALL,
                        )
                        if ctx_pattern.search(text):
                            sr["catch_quality"] = quality_phrase
                            sr["catch_quality_score"] = score
                            break

                # Size range
                ctx = self._get_species_context(text, name, 300)
                size_m = self.SIZE_RANGE.search(ctx)
                if size_m:
                    sr["fish_size_lbs_min"] = float(size_m.group(1))
                    sr["fish_size_lbs_max"] = float(size_m.group(2))

                # Depth
                depth_m = self.DEPTH_RANGE.search(ctx)
                if depth_m:
                    sr["depth_ft_min"] = float(depth_m.group(1))
                    sr["depth_ft_max"] = float(depth_m.group(2))

                # Temperature
                temp_m = self.TEMP_PATTERN.search(ctx) or self.TEMP_F_PATTERN.search(ctx)
                if temp_m:
                    t = float(temp_m.group(1))
                    if 50 <= t <= 85:
                        sr["water_temp_f"] = t

                # Bait/technique
                baits = []
                for bait in ["sardine", "anchovy", "mackerel", "squid", "iron", "jig",
                             "plastics", "swimbaits", "live bait", "fly line", "dropper loop",
                             "yoyo", "colt sniper", "leadhead", "shrimp", "clams", "mussels"]:
                    if bait in ctx.lower():
                        baits.append(bait)
                sr["best_bait"] = baits

                # GPS coords in context
                sr["gps_coords"] = self._extract_gps(ctx)

                species_reports.append(sr)
        return species_reports

    def _get_species_context(self, text, species_name, window=300):
        idx = text.lower().find(species_name.lower())
        if idx == -1:
            return ""
        start = max(0, idx - window)
        end = min(len(text), idx + len(species_name) + window)
        return text[start:end]

    def _extract_gps(self, text):
        coords = []
        for m in self.GPS_PATTERN.finditer(text):
            lat_deg = float(m.group(1))
            lat_min = float(m.group(2))
            lon_deg = float(m.group(3))
            lon_min = float(m.group(4))

            if 30 <= lat_deg <= 35 and 116 <= lon_deg <= 121:
                coord = GPSCoordinate(lat_deg, lat_min, lon_deg, lon_min)

                # Try to find a name before the coordinate
                pre_text = text[max(0, m.start()-80):m.start()]
                name_m = re.search(r"([A-Z][a-zA-Z\s/']+?)\s*$", pre_text)
                if name_m:
                    coord.name = name_m.group(1).strip()

                coords.append({
                    "lat_decimal": coord.lat_decimal,
                    "lon_decimal": coord.lon_decimal,
                    "lat_deg": lat_deg, "lat_min": lat_min,
                    "lon_deg": lon_deg, "lon_min": lon_min,
                    "name": coord.name,
                })
        return coords


def parse_all_reports(input_dir=RAW_TEXT_DIR, output_dir=PARSED_JSON_DIR):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    parser = RuleBasedParser()
    input_path = Path(input_dir)

    files = sorted(input_path.glob("*.json"))
    files = [f for f in files if not f.name.startswith("_")]

    results = []
    for f in files:
        try:
            raw = json.loads(f.read_text(encoding="utf-8"))
            parsed = parser.parse_report(raw)
            out_file = Path(output_dir) / f.name
            out_file.write_text(json.dumps(parsed, indent=2), encoding="utf-8")
            results.append(parsed)
        except Exception as e:
            logger.error(f"Error parsing {f.name}: {e}")

    logger.info(f"Parsed {len(results)} reports")
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parse_all_reports()
