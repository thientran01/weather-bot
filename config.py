"""
config.py — Shared constants, credentials, city configuration, logging setup,
and the _et_now() time utility used across all modules.
"""

import os
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CREDENTIALS
# ============================================================

KALSHI_API_KEY   = os.getenv("KALSHI_API_KEY")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL")   # must be verified with SendGrid
ALERT_TO_EMAIL   = os.getenv("ALERT_TO_EMAIL")
# Set SEND_TEST_EMAIL=true in Railway (or .env) to fire one test email on startup.
SEND_TEST_EMAIL  = os.getenv("SEND_TEST_EMAIL", "").lower() == "true"
WEATHERAPI_KEY   = os.getenv("WEATHERAPI_KEY")     # weatherapi.com — free tier is sufficient

# ============================================================
# API URLS
# ============================================================

# The Kalshi REST API base URL (confirmed — all markets live here, no auth needed for reads)
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# ============================================================
# CITY CONFIGURATION
# ============================================================

# Cities that get full signal cards in email alerts (historically more reliable markets).
TIER1_CITIES = {"PHX", "MIA", "LAS", "HOU", "SAT", "DAL"}

# Each city entry contains:
#   name         — display name for alerts
#   nws_station  — ICAO station code Kalshi uses for official resolution
#   lat / lon    — exact coordinates of the NWS station
#   station_type — "hourly" or "5-minute" (ASOS airport)
#   lst_utc_offset — standard time offset (no DST adjustment)
#   high_series  — Kalshi series ticker for HIGH temp markets (None if unavailable)
#   low_series   — Kalshi series ticker for LOW temp markets (None if unavailable)
CITIES = {
    "NYC": {
        "name":           "New York City",
        "nws_station":    "KNYC",         # Central Park — official Kalshi resolution station
        "lat":            40.7790, "lon": -73.9692,
        "station_type":   "hourly",       # cooperative observer, NOT a standard ASOS airport
        "lst_utc_offset": -5,
        "high_series":    "KXHIGHNY",
        "low_series":     "KXLOWTNYC",
    },
    "CHI": {
        "name":           "Chicago",
        "nws_station":    "KMDW",         # Midway — Kalshi uses Midway, NOT O'Hare (KORD)
        "lat":            41.7841, "lon": -87.7551,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,
        "high_series":    "KXHIGHCHI",
        "low_series":     "KXLOWTCHI",
    },
    "LAX": {
        "name":           "Los Angeles",
        "nws_station":    "KLAX",
        "lat":            33.9382, "lon": -118.3870,
        "station_type":   "5-minute",
        "lst_utc_offset": -8,
        "high_series":    "KXHIGHLAX",
        "low_series":     "KXLOWTLAX",
    },
    "MIA": {
        "name":           "Miami",
        "nws_station":    "KMIA",
        "lat":            25.7881, "lon": -80.3169,
        "station_type":   "5-minute",
        "lst_utc_offset": -5,
        "high_series":    "KXHIGHMIA",
        "low_series":     "KXLOWTMIA",
    },
    "DEN": {
        "name":           "Denver",
        "nws_station":    "KDEN",
        "lat":            39.8466, "lon": -104.6560,
        "station_type":   "5-minute",
        "lst_utc_offset": -7,
        "high_series":    "KXHIGHDEN",
        "low_series":     "KXLOWTDEN",
    },
    "PHX": {
        "name":           "Phoenix",
        "nws_station":    "KPHX",
        "lat":            33.4373, "lon": -112.0078,
        "station_type":   "5-minute",
        "lst_utc_offset": -7,             # Arizona does not observe DST; UTC-7 year-round
        "high_series":    "KXHIGHTPHX",
        "low_series":     None,
    },
    "AUS": {
        "name":           "Austin",
        "nws_station":    "KAUS",
        "lat":            30.2099, "lon": -97.6806,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,
        "high_series":    "KXHIGHAUS",
        "low_series":     "KXLOWTAUS",
    },
    "PHL": {
        "name":           "Philadelphia",
        "nws_station":    "KPHL",
        "lat":            39.8721, "lon": -75.2407,
        "station_type":   "5-minute",
        "lst_utc_offset": -5,
        "high_series":    "KXHIGHPHIL",
        "low_series":     "KXLOWTPHIL",
    },
    "SFO": {
        "name":           "San Francisco",
        "nws_station":    "KSFO",
        "lat":            37.6213, "lon": -122.3790,
        "station_type":   "5-minute",
        "lst_utc_offset": -8,
        "high_series":    "KXHIGHTSFO",
        "low_series":     None,
    },
    "SEA": {
        "name":           "Seattle",
        "nws_station":    "KSEA",
        "lat":            47.4502, "lon": -122.3088,
        "station_type":   "5-minute",
        "lst_utc_offset": -8,
        "high_series":    "KXHIGHTSEA",
        "low_series":     None,
    },
    "DAL": {
        "name":           "Dallas",
        "nws_station":    "KDFW",
        "lat":            32.8998, "lon": -97.0403,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,
        "high_series":    "KXHIGHTDAL",
        "low_series":     None,
    },
    "ATL": {
        "name":           "Atlanta",
        "nws_station":    "KATL",
        "lat":            33.6304, "lon": -84.4221,
        "station_type":   "5-minute",
        "lst_utc_offset": -5,
        "high_series":    "KXHIGHTATL",
        "low_series":     None,
    },
    "LAS": {
        "name":           "Las Vegas",
        "nws_station":    "KLAS",
        "lat":            36.0840, "lon": -115.1537,
        "station_type":   "5-minute",
        "lst_utc_offset": -8,
        "high_series":    "KXHIGHTLV",
        "low_series":     None,
    },
    "HOU": {
        "name":           "Houston",
        "nws_station":    "KHOU",         # Houston Hobby — Kalshi uses Hobby, NOT Bush (KIAH)
        "lat":            29.6454, "lon": -95.2789,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,
        "high_series":    "KXHIGHTHOU",
        "low_series":     None,
    },
    "DCA": {
        "name":           "Washington DC",
        "nws_station":    "KDCA",
        "lat":            38.8512, "lon": -77.0402,
        "station_type":   "5-minute",
        "lst_utc_offset": -5,
        "high_series":    "KXHIGHTDC",
        "low_series":     None,
    },
    "BOS": {
        "name":           "Boston",
        "nws_station":    "KBOS",
        "lat":            42.3656, "lon": -71.0096,
        "station_type":   "5-minute",
        "lst_utc_offset": -5,
        "high_series":    "KXHIGHTBOS",
        "low_series":     None,
    },
    "MSY": {
        "name":           "New Orleans",
        "nws_station":    "KMSY",
        "lat":            29.9934, "lon": -90.2580,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,
        "high_series":    "KXHIGHTNOLA",
        "low_series":     None,
    },
    "MSP": {
        "name":           "Minneapolis",
        "nws_station":    "KMSP",
        "lat":            44.8848, "lon": -93.2223,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,
        "high_series":    "KXHIGHTMIN",
        "low_series":     None,
    },
    "SAT": {
        "name":           "San Antonio",
        "nws_station":    "KSAT",
        "lat":            29.5337, "lon": -98.4698,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,
        "high_series":    "KXHIGHTSATX",
        "low_series":     None,
    },
    "OKC": {
        "name":           "Oklahoma City",
        "nws_station":    "KOKC",
        "lat":            35.3931, "lon": -97.6007,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,
        "high_series":    "KXHIGHTOKC",
        "low_series":     None,
    },
}

# ============================================================
# BOT CONFIGURATION
# ============================================================

RUN_EVERY_MINUTES = 10
MIN_GAP_TO_SHOW   = 0

# ============================================================
# FILE PATHS
# ============================================================

LOG_FILE    = os.getenv("LOG_PATH",         "log.csv")
LOG_PATH    = LOG_FILE   # alias used by export_server
RESOLVE_LOG = os.getenv("RESOLVE_LOG_PATH", "resolve_log.csv")

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ============================================================
# TIME UTILITY
# ============================================================

def _et_now():
    """Returns the current datetime in America/New_York (Eastern Time)."""
    return datetime.now(tz=ZoneInfo("America/New_York"))
