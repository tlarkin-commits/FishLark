"""
FishLark GeoJSON + KML Export

Exports all spatial data into standard formats compatible with:
  - Leaflet.js, MapLibre, OpenLayers (GeoJSON)
  - Google Earth, QGIS, ArcGIS (KML/KMZ)
  - Kepler.gl (GeoJSON drag-and-drop)

Outputs:
  data/geo/species_hotspots.geojson   — cluster circles per species
  data/geo/migration_tracks.geojson   — centroid movement lines per species
  data/geo/gps_waypoints.geojson      — all 8k named GPS points
  data/geo/daily_observations.geojson — raw GPS obs with quality + date
  data/geo/fishlark_map.kml           — combined KML for Google Earth

Usage:
    python export_geo.py
    python export_geo.py --species yellowtail bluefin_tuna
"""
import argparse
import json
import math
from collections import defaultdict
from pathlib import Path

GPS_DATABASE     = "data/gps_waypoints.json"
MOVEMENT_DIR     = Path("data/movement")
ENRICHED_DATASET = "data/enriched_master.jsonl"
GEO_DIR          = Path("data/geo")

# Species color palette
SPECIES_COLORS = {
    "yellowtail":      "#f59e0b",   # amber
    "bluefin_tuna":    "#3b82f6",   # blue
    "yellowfin_tuna":  "#f97316",   # orange
    "dorado":          "#10b981",   # emerald
    "white_seabass":   "#8b5cf6",   # violet
    "halibut":         "#64748b",   # slate
    "barracuda":       "#ef4444",   # red
    "bonito":          "#06b6d4",   # cyan
    "calico_bass":     "#84cc16",   # lime
    "sand_bass":       "#a3e635",   # light lime
    "rockfish":        "#dc2626",   # dark red
    "sculpin":         "#b45309",   # brown
    "whitefish":       "#e2e8f0",   # light gray
    "lingcod":         "#475569",   # dark slate
    "sheephead":       "#ec4899",   # pink
    "squid":           "#a78bfa",   # purple
    "bonito":          "#06b6d4",
}

QUALITY_COLORS = ["#374151", "#6b7280", "#f59e0b", "#f97316", "#ef4444", "#dc2626"]


def quality_color(score):
    idx = max(0, min(5, int(round(score))))
    return QUALITY_COLORS[idx]


def load_movement(species):
    p = MOVEMENT_DIR / f"{species}_movement.json"
    if not p.exists():
        return None
    return json.loads(p.read_text())


# ---------------------------------------------------------------------------
# GeoJSON builders
# ---------------------------------------------------------------------------

def build_hotspots_geojson(species_list):
    """One feature per hotspot cluster per species."""
    features = []
    for species in species_list:
        data = load_movement(species)
        if not data:
            continue
        color = SPECIES_COLORS.get(species, "#6b7280")
        for hs_id, hs in data.get("hotspots", {}).items():
            lat = hs.get("lat")
            lon = hs.get("lon")
            if not lat or not lon:
                continue
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "id": f"{species}_{hs_id}",
                    "species": species,
                    "species_display": species.replace("_", " ").title(),
                    "hotspot_id": hs_id,
                    "total_days": hs.get("total_days", 0),
                    "total_points": hs.get("total_points", 0),
                    "avg_quality": hs.get("avg_quality", 0),
                    "first_seen": hs.get("first_seen", ""),
                    "last_seen": hs.get("last_seen", ""),
                    "zones": ", ".join(hs.get("zones", [])),
                    "color": color,
                    "radius": max(4, min(30, hs.get("total_days", 1) * 0.4)),
                    "quality_color": quality_color(hs.get("avg_quality", 0)),
                },
            })
    return {"type": "FeatureCollection", "features": features}


def build_tracks_geojson(species_list):
    """
    One LineString per species showing centroid path over time.
    Also adds individual migration arc features.
    """
    features = []
    for species in species_list:
        data = load_movement(species)
        if not data:
            continue
        color = SPECIES_COLORS.get(species, "#6b7280")

        # Migration calendar centroids month-by-month
        cal = data.get("migration_calendar", {})
        monthly_coords = []
        monthly_props  = []
        month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        for m_str, c in sorted(cal.items(), key=lambda x: int(x[0])):
            if c is None:
                continue
            monthly_coords.append([c["avg_lon"], c["avg_lat"]])
            monthly_props.append({
                "month": c["month"],
                "n_days": c["n_report_days"],
                "avg_quality": c["avg_quality"],
                "zones": ", ".join(c.get("zones", [])),
            })

        if len(monthly_coords) >= 2:
            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": monthly_coords},
                "properties": {
                    "id": f"{species}_annual_track",
                    "type": "annual_migration",
                    "species": species,
                    "species_display": species.replace("_", " ").title(),
                    "color": color,
                    "monthly_data": monthly_props,
                },
            })

        # Individual monthly centroid markers
        for m_str, c in cal.items():
            if c is None:
                continue
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [c["avg_lon"], c["avg_lat"]]},
                "properties": {
                    "id": f"{species}_month_{m_str}",
                    "type": "monthly_centroid",
                    "species": species,
                    "species_display": species.replace("_", " ").title(),
                    "month": c["month"],
                    "n_report_days": c["n_report_days"],
                    "avg_quality": c["avg_quality"],
                    "zones": ", ".join(c.get("zones", [])),
                    "color": color,
                    "quality_color": quality_color(c["avg_quality"]),
                },
            })

        # Notable migration event arcs
        events = data.get("movement_events", [])
        for ev in events:
            if ev.get("type") != "MIGRATION":
                continue
            dist = ev.get("migration_distance_miles", 0)
            if dist < 30:
                continue
            from_lat = ev.get("from_lat")
            from_lon = ev.get("from_lon")
            to_lat   = ev.get("to_lat")
            to_lon   = ev.get("to_lon")
            if not all([from_lat, from_lon, to_lat, to_lon]):
                continue
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[from_lon, from_lat], [to_lon, to_lat]],
                },
                "properties": {
                    "type": "migration_event",
                    "species": species,
                    "species_display": species.replace("_", " ").title(),
                    "date": ev.get("date", ""),
                    "distance_miles": dist,
                    "moon_phase": ev.get("env_at_event", {}).get("moon_phase", ""),
                    "seas_ft": ev.get("env_at_event", {}).get("seas_ft", ""),
                    "color": color,
                },
            })

    return {"type": "FeatureCollection", "features": features}


def build_waypoints_geojson():
    """All named GPS waypoints from the GPS database."""
    gps_path = Path(GPS_DATABASE)
    if not gps_path.exists():
        print(f"  GPS database not found: {GPS_DATABASE}")
        return {"type": "FeatureCollection", "features": []}

    waypoints = json.loads(gps_path.read_text())
    features  = []
    for wp in waypoints:
        lat = wp.get("lat_decimal")
        lon = wp.get("lon_decimal")
        if not lat or not lon:
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "name": wp.get("name", ""),
                "zone": wp.get("zone_id", ""),
                "zone_name": wp.get("zone_name", ""),
                "species": wp.get("species", ""),
                "report_date": wp.get("report_date", ""),
            },
        })
    return {"type": "FeatureCollection", "features": features}


def build_observations_geojson(species_list, max_points=50000):
    """Raw GPS observations with date, quality, species, moon, tides."""
    features = []
    count = 0

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
            moon     = report.get("moon", {})
            mc       = report.get("marine_conditions", {})
            tide     = report.get("tide", {})

            for zk in ["inshore_zones", "offshore_zones", "mexican_zones"]:
                for zone in report.get(zk, []):
                    for sp in zone.get("species_reports", []):
                        sp_name = sp.get("species", "")
                        if species_list and sp_name not in species_list:
                            continue
                        quality = sp.get("catch_quality_score", 0)
                        color   = SPECIES_COLORS.get(sp_name, "#6b7280")

                        for coord in sp.get("gps_coords", []):
                            lat = coord.get("lat_decimal")
                            lon = coord.get("lon_decimal")
                            if not lat or not lon:
                                continue
                            features.append({
                                "type": "Feature",
                                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                                "properties": {
                                    "species": sp_name,
                                    "species_display": sp_name.replace("_", " ").title(),
                                    "date": date_str,
                                    "quality": quality,
                                    "quality_color": quality_color(quality),
                                    "color": color,
                                    "zone": zone.get("zone_id", ""),
                                    "water_temp": sp.get("water_temp_f"),
                                    "coord_name": coord.get("name", ""),
                                    "moon_phase": moon.get("phase_name", ""),
                                    "moon_illum": moon.get("illumination_pct"),
                                    "tide_dir": tide.get("dawn_tide_direction", ""),
                                    "seas_ft": mc.get("seas_ft_max"),
                                    "wind_dir": mc.get("wind_direction", ""),
                                },
                            })
                            count += 1
                            if count >= max_points:
                                break
                    if count >= max_points:
                        break
                if count >= max_points:
                    break
            if count >= max_points:
                break

    return {"type": "FeatureCollection", "features": features}


# ---------------------------------------------------------------------------
# KML export
# ---------------------------------------------------------------------------

def rgb_to_kml_color(hex_color, opacity=200):
    """Convert #rrggbb to KML aabbggrr format."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"{opacity:02x}{b:02x}{g:02x}{r:02x}"


def build_kml(species_list):
    """Build a KML document with placemarks and paths for Google Earth."""
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<kml xmlns="http://www.opengis.net/kml/2.2">',
             '<Document>',
             '<name>FishLark — SoCal Fishing Intelligence</name>']

    # Styles
    for species, color in SPECIES_COLORS.items():
        kml_color = rgb_to_kml_color(color)
        lines += [
            f'<Style id="{species}_hs">',
            f'  <IconStyle><color>{kml_color}</color><scale>1.2</scale>',
            '  <Icon><href>http://maps.google.com/mapfiles/kml/shapes/fishing.png</href></Icon>',
            '  </IconStyle>',
            '</Style>',
            f'<Style id="{species}_track">',
            f'  <LineStyle><color>{kml_color}</color><width>3</width></LineStyle>',
            '</Style>',
        ]

    for species in species_list:
        data = load_movement(species)
        if not data:
            continue

        color     = SPECIES_COLORS.get(species, "#6b7280")
        disp_name = species.replace("_", " ").title()

        lines.append(f'<Folder><name>{disp_name}</name>')

        # Hotspots folder
        lines.append('<Folder><name>Hotspots</name>')
        top_hs = sorted(
            data.get("hotspots", {}).values(),
            key=lambda h: -h.get("total_days", 0)
        )[:20]
        for hs in top_hs:
            lat, lon = hs.get("lat"), hs.get("lon")
            if not lat or not lon:
                continue
            desc = (f"Days: {hs.get('total_days')} | "
                    f"Quality: {hs.get('avg_quality')} | "
                    f"Zones: {', '.join(hs.get('zones', []))}")
            lines += [
                '<Placemark>',
                f'  <name>{disp_name} — {hs["id"]}</name>',
                f'  <description>{desc}</description>',
                f'  <styleUrl>#{species}_hs</styleUrl>',
                '  <Point>',
                f'    <coordinates>{lon},{lat},0</coordinates>',
                '  </Point>',
                '</Placemark>',
            ]
        lines.append('</Folder>')

        # Annual migration track
        cal = data.get("migration_calendar", {})
        coords = []
        for m_str, c in sorted(cal.items(), key=lambda x: int(x[0])):
            if c:
                coords.append(f"{c['avg_lon']},{c['avg_lat']},0")
        if len(coords) >= 2:
            lines += [
                '<Placemark>',
                f'  <name>{disp_name} Annual Migration Track</name>',
                f'  <styleUrl>#{species}_track</styleUrl>',
                '  <LineString>',
                f'    <coordinates>{" ".join(coords)}</coordinates>',
                '  </LineString>',
                '</Placemark>',
            ]

        lines.append('</Folder>')

    # Named GPS waypoints
    gps_path = Path(GPS_DATABASE)
    if gps_path.exists():
        lines.append('<Folder><name>GPS Waypoints</name>')
        wps = json.loads(gps_path.read_text())
        for wp in wps[:2000]:   # cap at 2000 for KML performance
            lat = wp.get("lat_decimal")
            lon = wp.get("lon_decimal")
            name = wp.get("name", "")
            if not lat or not lon or not name:
                continue
            lines += [
                '<Placemark>',
                f'  <name>{name}</name>',
                f'  <description>Zone: {wp.get("zone_id","")} | '
                f'Date: {wp.get("report_date","")}</description>',
                '  <Point>',
                f'    <coordinates>{lon},{lat},0</coordinates>',
                '  </Point>',
                '</Placemark>',
            ]
        lines.append('</Folder>')

    lines += ['</Document>', '</kml>']
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="FishLark GeoJSON + KML Export")
    parser.add_argument("--species", nargs="+",
                        help="Species to include (default: all with movement data)")
    args = parser.parse_args()

    GEO_DIR.mkdir(parents=True, exist_ok=True)

    # Auto-detect available species
    all_species = [f.stem.replace("_movement", "")
                   for f in MOVEMENT_DIR.glob("*_movement.json")]
    species_list = args.species or all_species

    if not species_list:
        print("No movement data found. Run: python movement.py --save-json")
        return

    print(f"Exporting {len(species_list)} species: {', '.join(species_list)}")

    # GeoJSON exports
    print("  Building hotspots GeoJSON...")
    hs_geo = build_hotspots_geojson(species_list)
    (GEO_DIR / "species_hotspots.geojson").write_text(json.dumps(hs_geo, indent=2))
    print(f"    {len(hs_geo['features'])} hotspot features")

    print("  Building migration tracks GeoJSON...")
    tr_geo = build_tracks_geojson(species_list)
    (GEO_DIR / "migration_tracks.geojson").write_text(json.dumps(tr_geo, indent=2))
    print(f"    {len(tr_geo['features'])} track features")

    print("  Building GPS waypoints GeoJSON...")
    wp_geo = build_waypoints_geojson()
    (GEO_DIR / "gps_waypoints.geojson").write_text(json.dumps(wp_geo, indent=2))
    print(f"    {len(wp_geo['features'])} waypoint features")

    print("  Building raw observations GeoJSON (capped at 50k points)...")
    obs_geo = build_observations_geojson(species_list)
    (GEO_DIR / "daily_observations.geojson").write_text(json.dumps(obs_geo, indent=2))
    print(f"    {len(obs_geo['features'])} observation features")

    # KML export
    print("  Building KML for Google Earth...")
    kml = build_kml(species_list)
    (GEO_DIR / "fishlark_map.kml").write_text(kml, encoding="utf-8")
    print(f"    KML written to data/geo/fishlark_map.kml")

    print(f"\nAll files in data/geo/")
    for f in sorted(GEO_DIR.iterdir()):
        size_kb = f.stat().st_size // 1024
        print(f"  {f.name:<40} {size_kb:>6} KB")


if __name__ == "__main__":
    main()
