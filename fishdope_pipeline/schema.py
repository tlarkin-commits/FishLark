"""
Normalized data schema for Fishdope reports.
Each daily report becomes one FishdopeReport with nested zone reports.
"""
from dataclasses import dataclass, field, asdict
from typing import Optional
import json


@dataclass
class GPSCoordinate:
    lat_deg: float
    lat_min: float
    lon_deg: float
    lon_min: float
    lat_decimal: float = 0.0
    lon_decimal: float = 0.0
    name: Optional[str] = None

    def __post_init__(self):
        self.lat_decimal = self.lat_deg + self.lat_min / 60.0
        self.lon_decimal = -(self.lon_deg + self.lon_min / 60.0)


@dataclass
class WeatherData:
    summary: str = ""
    wind_direction: Optional[str] = None
    wind_speed_kt: Optional[float] = None
    wind_gusts_kt: Optional[float] = None
    sea_height_ft: Optional[float] = None
    swell_direction: Optional[str] = None
    swell_height_ft: Optional[float] = None
    swell_period_sec: Optional[float] = None
    small_craft_advisory: bool = False
    advisory_zones: list = field(default_factory=list)


@dataclass
class BaitReport:
    location: str = ""
    bait_type: str = ""
    bait_size: str = ""
    last_updated: str = ""
    available: bool = True
    notes: str = ""


@dataclass
class SpeciesReport:
    species: str = ""
    catch_quality: str = ""
    catch_quality_score: int = 0
    fish_size_lbs_min: Optional[float] = None
    fish_size_lbs_max: Optional[float] = None
    depth_ft_min: Optional[float] = None
    depth_ft_max: Optional[float] = None
    depth_fathoms_min: Optional[float] = None
    depth_fathoms_max: Optional[float] = None
    bottom_type: Optional[str] = None
    water_temp_f: Optional[float] = None
    water_color: Optional[str] = None
    best_bait: list = field(default_factory=list)
    best_technique: list = field(default_factory=list)
    best_time_of_day: Optional[str] = None
    sportboat_vs_private: Optional[str] = None
    sonar_behavior: Optional[str] = None
    notes: str = ""
    gps_coords: list = field(default_factory=list)


@dataclass
class SquidReport:
    present: bool = False
    location: Optional[str] = None
    depth_ft: Optional[str] = None
    commercial_fleet_active: bool = False
    notes: str = ""


@dataclass
class ZoneReport:
    zone_id: str = ""
    zone_name: str = ""
    sector: str = ""
    marine_forecast: Optional[str] = None
    small_craft_advisory: bool = False
    squid_report: Optional[dict] = None
    species_reports: list = field(default_factory=list)
    gps_waypoints: list = field(default_factory=list)
    raw_text: str = ""
    user_reports: list = field(default_factory=list)


@dataclass
class UserReport:
    boat_name: Optional[str] = None
    reporter: Optional[str] = None
    weather_conditions: Optional[str] = None
    fishing_report: str = ""
    gps_coords: list = field(default_factory=list)
    species_caught: list = field(default_factory=list)
    bait_quality: Optional[str] = None


@dataclass
class FishdopeReport:
    report_id: str = ""
    url: str = ""
    date: str = ""
    day_of_week: str = ""
    timestamp: str = ""
    title: str = ""
    highlights: list = field(default_factory=list)
    weather: Optional[dict] = None
    bait_reports: list = field(default_factory=list)
    closures: list = field(default_factory=list)
    inshore_zones: list = field(default_factory=list)
    mexican_zones: list = field(default_factory=list)
    offshore_zones: list = field(default_factory=list)
    raw_text: str = ""

    def to_dict(self):
        return asdict(self)

    def to_json(self, indent=2):
        return json.dumps(self.to_dict(), indent=indent, default=str)
