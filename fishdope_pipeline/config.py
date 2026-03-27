"""
FishLark / Fishdope Pipeline Configuration
"""

# -- Authentication --
# All cookies required for aMember + WordPress auth on fishdope.com
# Refresh from Chrome DevTools > Application > Cookies > fishdope.com when expired
FISHDOPE_COOKIES = {
    "amember_nr": "0c94ecc5bd7ed46c38d1ac0a8d4e2fc9",
    "amember_rp": "39eff73c31eb24e849866eee041c8806638db653",
    "amember_ru": "tlarkin3",
    "PHPSESSID": "0aef0d36022f22717f34f3eb4e699e0e",
    "wordpress_logged_in_5e44dbdfb5841b744db4ddd7da2ac21c": "tlarkin3|1775241968|Cli572yxnO2XXLtHmqeIhJXIEpIpTY0murdqKRQE5sY|63be5da561111c879f6a4225279036f18188f6091780dbb47d5239cdd819c40d",
    "wordpress_sec_5e44dbdfb5841b744db4ddd7da2ac21c": "tlarkin3|1775241968|SLFf4DIQcrZPpjqOX3uKxLoHkHzFepAaiu87NMzZlel|71d367655536fd620eb33cc923b5ea09f8f017f0b077e88578b9d134b0b59118",
}

# Legacy single-cookie config (kept for reference)
FISHDOPE_SESSION_COOKIE_NAME = "wordpress_logged_in_5e44dbdfb5841b744db4ddd7da2ac21c"
FISHDOPE_SESSION_COOKIE_VALUE = FISHDOPE_COOKIES[FISHDOPE_SESSION_COOKIE_NAME]

# -- Scraper Settings --
BASE_URL = "https://www.fishdope.com/category/fish-reports/"
LISTING_PAGE_URL = "https://www.fishdope.com/category/fish-reports/page/{page}/"
TOTAL_PAGES = 2087
REPORTS_PER_PAGE = 3

SCRAPE_DELAY_SECONDS = 1.5
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
REQUEST_TIMEOUT = 30

# -- Output Paths --
RAW_HTML_DIR = "data/raw_html/"
RAW_TEXT_DIR = "data/raw_text/"
PARSED_JSON_DIR = "data/parsed_json/"
MASTER_DATASET = "data/fishdope_master.jsonl"
GPS_DATABASE = "data/gps_waypoints.json"

# -- LLM Parser Settings (optional) --
USE_LLM_PARSER = False
ANTHROPIC_API_KEY = ""
LLM_MODEL = "claude-sonnet-4-20250514"
LLM_BATCH_SIZE = 10

# -- Geographic Zones --
ZONES = {
    "san_miguel_santa_rosa": {
        "name": "San Miguel and Santa Rosa Islands",
        "aliases": ["san miguel", "santa rosa", "miguel and rosa"],
        "sector": "northern", "type": "island",
    },
    "santa_cruz_anacapa": {
        "name": "Santa Cruz / Anacapa / Ventura-Oxnard Coast",
        "aliases": ["santa cruz", "anacapa", "ventura", "oxnard", "channel islands"],
        "sector": "northern", "type": "island",
    },
    "san_nicolas": {
        "name": "San Nicolas Island",
        "aliases": ["san nicolas", "san nic"],
        "sector": "northern", "type": "island",
    },
    "santa_barbara_island": {
        "name": "Santa Barbara Island",
        "aliases": ["santa barbara island", "sbi"],
        "sector": "northern", "type": "island",
    },
    "santa_monica_bay": {
        "name": "Santa Monica Bay",
        "aliases": ["santa monica", "sm bay"],
        "sector": "inshore", "type": "coastal",
    },
    "palos_verdes_newport": {
        "name": "Palos Verdes / LA Harbor / Long Beach / Newport Beach",
        "aliases": ["palos verdes", "la harbor", "long beach", "newport", "pv"],
        "sector": "inshore", "type": "coastal",
    },
    "catalina": {
        "name": "Catalina Island",
        "aliases": ["catalina", "cat"],
        "sector": "island", "type": "island",
    },
    "san_clemente_island": {
        "name": "San Clemente Island",
        "aliases": ["san clemente", "sci", "clemente island"],
        "sector": "island", "type": "island",
    },
    "dana_point_oceanside": {
        "name": "Dana Point / Oceanside",
        "aliases": ["dana point", "oceanside", "dana", "oside"],
        "sector": "inshore", "type": "coastal",
    },
    "la_jolla_point_loma": {
        "name": "Del Mar / La Jolla / Point Loma / Imperial Beach",
        "aliases": ["la jolla", "point loma", "del mar", "imperial beach", "ib"],
        "sector": "inshore", "type": "coastal",
    },
    "lower_9_mile": {
        "name": "Lower 9 Mile Bank",
        "aliases": ["lower 9", "9 mile", "lower nine"],
        "sector": "mexico_inshore", "type": "bank",
    },
    "coronado_islands": {
        "name": "Coronado Islands / Rockpile",
        "aliases": ["coronado", "rockpile", "coronados", "the pile"],
        "sector": "mexico_inshore", "type": "island",
    },
    "ensenada_south": {
        "name": "Salsipuedes / Ensenada / Santo Tomas / Isolete",
        "aliases": ["salsipuedes", "ensenada", "santo tomas", "isolete", "punta banda"],
        "sector": "mexico_inshore", "type": "coastal",
    },
    "14_mile_bank": {
        "name": "14 Mile Bank",
        "aliases": ["14 mile", "fourteen mile"],
        "sector": "offshore_us", "type": "bank",
    },
    "lower_500_colonet": {
        "name": "Below the Lower 500 / West of Colonet",
        "aliases": ["lower 500", "colonet", "west of colonet"],
        "sector": "offshore_mexico", "type": "open_ocean",
    },
    "deep_offshore": {
        "name": "150-275 Miles / Ranger Bank / Mushroom Bank",
        "aliases": ["ranger bank", "mushroom", "60 mile", "hidden bank", "425", "390"],
        "sector": "offshore_mexico", "type": "open_ocean",
    },
}

# -- Species Normalization --
SPECIES_MAP = {
    "bluefin": "bluefin_tuna", "bluefin tuna": "bluefin_tuna", "bft": "bluefin_tuna",
    "yellowfin": "yellowfin_tuna", "yellowfin tuna": "yellowfin_tuna", "yft": "yellowfin_tuna",
    "albacore": "albacore_tuna", "albies": "albacore_tuna",
    "yellowtail": "yellowtail", "yellows": "yellowtail",
    "dorado": "dorado", "mahi": "dorado",
    "striped marlin": "striped_marlin", "marlin": "striped_marlin",
    "white seabass": "white_seabass", "seabass": "white_seabass", "wsb": "white_seabass",
    "barracuda": "barracuda",
    "bonito": "bonito", "bones": "bonito",
    "calico bass": "calico_bass", "calico": "calico_bass", "calicos": "calico_bass",
    "sand bass": "sand_bass", "sandies": "sand_bass",
    "halibut": "halibut", "flattys": "halibut",
    "sheephead": "sheephead", "goats": "sheephead",
    "sculpin": "sculpin", "whitefish": "whitefish", "sand dabs": "sand_dabs",
    "rockfish": "rockfish", "reds": "rockfish",
    "lingcod": "lingcod", "lings": "lingcod",
    "lobster": "lobster", "squid": "squid",
    "humboldt squid": "humboldt_squid",
    "thresher": "thresher_shark", "triggerfish": "triggerfish",
}

# -- Catch Quality Normalization --
CATCH_QUALITY_MAP = {
    "wide open": 5, "excellent": 5, "red hot": 5, "loaded": 5, "limits": 5,
    "very good": 4, "good": 4, "pretty good": 4,
    "steady": 3, "decent": 3, "fair": 3, "ok": 3, "improving": 3,
    "slow": 2, "slow picking": 2, "scratching": 2, "tough": 2,
    "very slow": 1, "dead": 1, "nothing": 1,
    "no report": 0, "no fish": 0,
}
