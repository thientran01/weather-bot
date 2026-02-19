"""
Kalshi Climate Bot
------------------
Monitors Kalshi temperature markets for 20 cities and compares them
against NWS weather forecasts. Shows you where the odds look off so
you can decide whether to trade. Does NOT place trades automatically.

Runs every 10 minutes and sends a summary via text and/or email.
Logs every cycle to log.csv for historical tracking.
"""

import os
import csv
import math
import time
import logging
import schedule
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, HTTPServer
from dotenv import load_dotenv
from scipy.stats import norm
import requests

# ============================================================
# SECTION 1 — LOAD CREDENTIALS
# Reads all secrets from the .env file so nothing is hardcoded.
# ============================================================

load_dotenv()

KALSHI_API_KEY      = os.getenv("KALSHI_API_KEY")
SENDGRID_API_KEY    = os.getenv("SENDGRID_API_KEY")
ALERT_FROM_EMAIL    = os.getenv("ALERT_FROM_EMAIL")   # must be verified with SendGrid
ALERT_TO_EMAIL      = os.getenv("ALERT_TO_EMAIL")
# Set SEND_TEST_EMAIL=true in Railway (or .env) to fire one test email on startup.
# Remove or set to false after confirming SendGrid is working.
SEND_TEST_EMAIL     = os.getenv("SEND_TEST_EMAIL", "").lower() == "true"
WEATHERAPI_KEY      = os.getenv("WEATHERAPI_KEY")   # weatherapi.com — free tier is sufficient

# ============================================================
# SECTION 2 — CONFIGURATION
# All the cities and market settings in one easy-to-edit place.
# ============================================================

# The Kalshi REST API base URL (confirmed — all markets live here, no auth needed for reads)
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Cities that get full signal cards in email alerts (historically more reliable markets).
# All other cities still run, log to CSV, and appear in a collapsed "Other Cities" section.
TIER1_CITIES = {"PHX", "MIA", "LAS", "HOU", "SAT", "DAL"}

# Each city entry contains:
#   name         — display name for alerts
#   nws_station  — ICAO station code Kalshi uses for official resolution (CRITICAL: wrong
#                  station = wrong resolution price. E.g. Chicago is KMDW not O'Hare KORD)
#   lat / lon    — exact coordinates of the NWS station (used for /points grid lookup)
#   station_type — "hourly" (cooperative/manual observer) or "5-minute" (ASOS airport).
#                  Affects the probability model: hourly→80%, 5-min→65% when clearly inside.
#   high_series  — Kalshi series ticker for HIGH temp markets (None if unavailable)
#   low_series   — Kalshi series ticker for LOW temp markets (None if unavailable)
#
# Station coordinates and series tickers confirmed live on 2026-02-17.
CITIES = {
    "NYC": {
        "name":           "New York City",
        "nws_station":    "KNYC",         # Central Park — official Kalshi resolution station
        "lat":            40.7790, "lon": -73.9692,
        "station_type":   "hourly",       # cooperative observer, NOT a standard ASOS airport
        "lst_utc_offset": -5,             # Eastern Standard Time (UTC-5). No DST adjustment —
                                          # Kalshi resolution windows use LST, not civil time.
        "high_series":    "KXHIGHNY",     # confirmed: 12 open markets
        "low_series":     "KXLOWTNYC",    # confirmed: 12 open markets
    },
    "CHI": {
        "name":           "Chicago",
        "nws_station":    "KMDW",         # Midway — Kalshi uses Midway, NOT O'Hare (KORD)
        "lat":            41.7841, "lon": -87.7551,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,             # Central Standard Time (UTC-6)
        "high_series":    "KXHIGHCHI",    # confirmed: 12 open markets
        "low_series":     "KXLOWTCHI",    # confirmed: 12 open markets
    },
    "LAX": {
        "name":           "Los Angeles",
        "nws_station":    "KLAX",         # LAX Airport
        "lat":            33.9382, "lon": -118.3870,
        "station_type":   "5-minute",
        "lst_utc_offset": -8,             # Pacific Standard Time (UTC-8)
        "high_series":    "KXHIGHLAX",    # confirmed: 12 open markets
        "low_series":     "KXLOWTLAX",    # confirmed: 12 open markets
    },
    "MIA": {
        "name":           "Miami",
        "nws_station":    "KMIA",         # Miami International
        "lat":            25.7881, "lon": -80.3169,
        "station_type":   "5-minute",
        "lst_utc_offset": -5,             # Eastern Standard Time (UTC-5)
        "high_series":    "KXHIGHMIA",    # confirmed: 12 open markets
        "low_series":     "KXLOWTMIA",    # confirmed: 12 open markets
    },
    "DEN": {
        "name":           "Denver",
        "nws_station":    "KDEN",         # Denver International
        "lat":            39.8466, "lon": -104.6560,
        "station_type":   "5-minute",
        "lst_utc_offset": -7,             # Mountain Standard Time (UTC-7)
        "high_series":    "KXHIGHDEN",    # confirmed: 12 open markets
        "low_series":     "KXLOWTDEN",    # confirmed: 12 open markets
    },
    "PHX": {
        "name":           "Phoenix",
        "nws_station":    "KPHX",         # Phoenix Sky Harbor
        "lat":            33.4373, "lon": -112.0078,
        "station_type":   "5-minute",
        "lst_utc_offset": -7,             # Mountain Standard Time (UTC-7). Arizona does not
                                          # observe DST, so UTC-7 is correct year-round.
        "high_series":    "KXHIGHTPHX",   # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for Phoenix
    },
    "AUS": {
        "name":           "Austin",
        "nws_station":    "KAUS",         # Austin-Bergstrom
        "lat":            30.2099, "lon": -97.6806,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,             # Central Standard Time (UTC-6)
        "high_series":    "KXHIGHAUS",    # confirmed: 12 open markets
        "low_series":     "KXLOWTAUS",    # confirmed: 12 open markets
    },
    "PHL": {
        "name":           "Philadelphia",
        "nws_station":    "KPHL",         # Philadelphia International
        "lat":            39.8721, "lon": -75.2407,
        "station_type":   "5-minute",
        "lst_utc_offset": -5,             # Eastern Standard Time (UTC-5)
        "high_series":    "KXHIGHPHIL",   # confirmed: 12 open markets
        "low_series":     "KXLOWTPHIL",   # confirmed: 12 open markets
    },
    "SFO": {
        "name":           "San Francisco",
        "nws_station":    "KSFO",         # SFO Airport
        "lat":            37.6213, "lon": -122.3790,
        "station_type":   "5-minute",
        "lst_utc_offset": -8,             # Pacific Standard Time (UTC-8)
        "high_series":    "KXHIGHTSFO",   # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for SF
    },
    "SEA": {
        "name":           "Seattle",
        "nws_station":    "KSEA",         # Seattle-Tacoma
        "lat":            47.4502, "lon": -122.3088,
        "station_type":   "5-minute",
        "lst_utc_offset": -8,             # Pacific Standard Time (UTC-8)
        "high_series":    "KXHIGHTSEA",   # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for Seattle
    },
    "DAL": {
        "name":           "Dallas",
        "nws_station":    "KDFW",         # DFW Airport
        "lat":            32.8998, "lon": -97.0403,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,             # Central Standard Time (UTC-6)
        "high_series":    "KXHIGHTDAL",   # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for Dallas
    },
    "ATL": {
        "name":           "Atlanta",
        "nws_station":    "KATL",         # Hartsfield-Jackson
        "lat":            33.6304, "lon": -84.4221,
        "station_type":   "5-minute",
        "lst_utc_offset": -5,             # Eastern Standard Time (UTC-5)
        "high_series":    "KXHIGHTATL",   # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for Atlanta
    },
    "LAS": {
        "name":           "Las Vegas",
        "nws_station":    "KLAS",         # Harry Reid International
        "lat":            36.0840, "lon": -115.1537,
        "station_type":   "5-minute",
        "lst_utc_offset": -8,             # Pacific Standard Time (UTC-8)
        "high_series":    "KXHIGHTLV",    # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for Las Vegas
    },
    "HOU": {
        "name":           "Houston",
        "nws_station":    "KHOU",         # Houston Hobby — Kalshi uses Hobby, NOT Bush (KIAH)
        "lat":            29.6454, "lon": -95.2789,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,             # Central Standard Time (UTC-6)
        "high_series":    "KXHIGHTHOU",   # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for Houston
    },
    "DCA": {
        "name":           "Washington DC",
        "nws_station":    "KDCA",         # Reagan National
        "lat":            38.8512, "lon": -77.0402,
        "station_type":   "5-minute",
        "lst_utc_offset": -5,             # Eastern Standard Time (UTC-5)
        "high_series":    "KXHIGHTDC",    # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for DC
    },
    "BOS": {
        "name":           "Boston",
        "nws_station":    "KBOS",         # Logan International
        "lat":            42.3656, "lon": -71.0096,
        "station_type":   "5-minute",
        "lst_utc_offset": -5,             # Eastern Standard Time (UTC-5)
        "high_series":    "KXHIGHTBOS",   # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for Boston
    },
    "MSY": {
        "name":           "New Orleans",
        "nws_station":    "KMSY",         # Louis Armstrong Airport
        "lat":            29.9934, "lon": -90.2580,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,             # Central Standard Time (UTC-6)
        "high_series":    "KXHIGHTNOLA",  # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for New Orleans
    },
    "MSP": {
        "name":           "Minneapolis",
        "nws_station":    "KMSP",         # Minneapolis-Saint Paul
        "lat":            44.8848, "lon": -93.2223,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,             # Central Standard Time (UTC-6)
        "high_series":    "KXHIGHTMIN",   # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for Minneapolis
    },
    "SAT": {
        "name":           "San Antonio",
        "nws_station":    "KSAT",         # San Antonio International
        "lat":            29.5337, "lon": -98.4698,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,             # Central Standard Time (UTC-6)
        "high_series":    "KXHIGHTSATX",  # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for San Antonio
    },
    "OKC": {
        "name":           "Oklahoma City",
        "nws_station":    "KOKC",         # Will Rogers Airport
        "lat":            35.3931, "lon": -97.6007,
        "station_type":   "5-minute",
        "lst_utc_offset": -6,             # Central Standard Time (UTC-6)
        "high_series":    "KXHIGHTOKC",   # confirmed: 12 open markets
        "low_series":     None,           # no LOW market for OKC
    },
}

# How often the bot runs, in minutes
RUN_EVERY_MINUTES = 10

# Minimum gap (in probability %) between Kalshi odds and NWS forecast
# to bother including in the alert. Set to 0 to show everything.
MIN_GAP_TO_SHOW = 0

# CSV log file path
LOG_FILE = os.getenv("LOG_PATH", "log.csv")

# Cache for NWS grid info (office, gridX, gridY) so we only look it up once
# per city at startup rather than on every 10-minute cycle.
# Format: {"NYC": {"office": "OKX", "grid_x": 33, "grid_y": 37, "forecast_url": "..."}}
NWS_GRID_CACHE = {}

# Tracks markets whose model spread was ≥ 3°F in any cycle today.
# When a flagged market's spread later drops below 1°F, an intraday alert fires.
# Both sets reset at midnight UTC so each day starts fresh.
_HIGH_SPREAD_FLAGGED = set()   # tickers that have seen spread ≥ 3°F today
_SPREAD_ALERTED      = set()   # tickers that already got a convergence alert today

# ET date on which the morning briefing / evening summary was last sent.
# Prevents duplicate sends when the bot cycles through the 7:00–7:15 AM or
# 8:00–8:15 PM window more than once (it runs every 10 minutes, so at most
# one cycle falls in each 15-minute window under normal conditions, but this
# guard makes it robust to restarts or clock drift).
_MORNING_SENT_DATE = None
_EVENING_SENT_DATE = None

# ============================================================
# SECTION 3 — LOGGING SETUP
# Prints timestamped messages to the terminal so you can watch
# what the bot is doing while it runs.
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ============================================================
# SECTION 3b — MARKET DISCOVERY (TEMPORARY)
# Run this once to print the raw Kalshi API response so we can
# confirm the exact ticker format before building real logic.
# Once we know the format, this function won't be needed.
# ============================================================

def discover_markets():
    """
    Fetches all open markets for every city/series in CITIES and prints
    a clean summary: ticker, threshold direction, and current yes price.

    No API key needed — Kalshi market data is publicly readable.
    Run this to verify all series are live before enabling the main bot loop.
    """
    print(f"\n{'='*60}")
    print("KALSHI MARKET DISCOVERY")
    print(f"Checking {len(CITIES)} cities for open temperature markets...")
    print(f"{'='*60}\n")

    for city_key, city in CITIES.items():
        # Check both HIGH and LOW series for this city
        for market_type in ("HIGH", "LOW"):
            series = city["high_series"] if market_type == "HIGH" else city["low_series"]

            # Skip if this city has no series defined for this market type
            if series is None:
                continue

            try:
                response = requests.get(
                    f"{KALSHI_BASE_URL}/markets",
                    params={"series_ticker": series, "status": "open", "limit": 100},
                    timeout=10,
                )
                markets = response.json().get("markets", [])

                if not markets:
                    print(f"  [{city_key}] {market_type} ({series}): 0 open markets")
                    continue

                print(f"  [{city_key}] {city['name']} — {market_type} ({series}): {len(markets)} open markets")
                for m in markets:
                    # threshold is stored directly: floor_strike for ">" markets,
                    # cap_strike for "<" markets — no string parsing needed
                    threshold = m.get("floor_strike") or m.get("cap_strike", "?")
                    direction = ">" if m.get("strike_type") == "greater" else "<"
                    yes_price = m.get("last_price", "?")  # cents: 73 = 73% implied prob
                    ticker    = m.get("ticker", "?")
                    print(f"    {ticker}  |  {direction}{threshold}°F  |  YES: {yes_price}¢")

            except Exception as e:
                print(f"  [{city_key}] {market_type} ({series}): ERROR — {e}")

        print()  # blank line between cities


# ============================================================
# SECTION 4 — KALSHI API
# Fetches the current markets and their yes/no prices for each city.
# Returns a list of markets with their ticker, description, and prices.
# ============================================================

def fetch_kalshi_markets(city_key):
    """
    Fetches all open temperature markets for a city from the Kalshi API.
    Checks both HIGH and LOW series if the city has them.

    Each market is classified into one of three bucket types based on the
    strike_type field returned by the API:

      FLOOR  — strike_type "greater":  YES if actual temp > floor_strike
               Fields: floor_strike set, cap_strike absent
               Example: "Will high be >51°F?"

      CAP    — strike_type "less":     YES if actual temp < cap_strike
               Fields: cap_strike set, floor_strike absent
               Example: "Will high be <44°F?"

      RANGE  — strike_type "between":  YES if floor_strike <= actual temp <= cap_strike
               Fields: both floor_strike AND cap_strike set
               Example: "Will high be 48–49°F?"

    Returns a list of dicts, one per market:
    {
        "ticker":       "KXHIGHNY-26FEB18-B48.5",
        "event_ticker": "KXHIGHNY-26FEB18",
        "series_type":  "HIGH",
        "bucket_type":  "RANGE",          # "FLOOR", "CAP", or "RANGE"
        "floor":        48,               # lower bound (None for CAP markets)
        "cap":          49,               # upper bound (None for FLOOR markets)
        "subtitle":     "48° to 49°",     # human-readable label from Kalshi
        "kalshi_prob":  2,                # last_price = implied probability (0-100)
        "yes_ask":      3,
        "yes_bid":      2,
        "close_time":   "2026-02-19T04:59:00Z",
    }

    Returns an empty list if all API calls fail — never crashes.
    """
    city = CITIES[city_key]
    all_markets = []

    for series_type in ("HIGH", "LOW"):
        series = city["high_series"] if series_type == "HIGH" else city["low_series"]

        if series is None:
            continue

        try:
            response = requests.get(
                f"{KALSHI_BASE_URL}/markets",
                params={"series_ticker": series, "status": "open", "limit": 100},
                timeout=10,
            )
            response.raise_for_status()

            for m in response.json().get("markets", []):
                strike_type = m.get("strike_type", "")

                # Classify the market and read its boundary fields directly from the API.
                # We never assume bucket shape — we read whatever Kalshi actually sent.
                if strike_type == "greater":
                    bucket_type = "FLOOR"
                    floor       = m.get("floor_strike")
                    cap         = None
                    if floor is None:
                        log.warning(f"[{city_key}] FLOOR market missing floor_strike: {m['ticker']}")
                        continue

                elif strike_type == "less":
                    bucket_type = "CAP"
                    floor       = None
                    cap         = m.get("cap_strike")
                    if cap is None:
                        log.warning(f"[{city_key}] CAP market missing cap_strike: {m['ticker']}")
                        continue

                elif strike_type == "between":
                    bucket_type = "RANGE"
                    floor       = m.get("floor_strike")
                    cap         = m.get("cap_strike")
                    if floor is None or cap is None:
                        log.warning(f"[{city_key}] RANGE market missing bounds: {m['ticker']}")
                        continue

                else:
                    # Unknown strike type — Kalshi may add new types in the future
                    log.warning(f"[{city_key}] Unknown strike_type '{strike_type}' on {m['ticker']}")
                    continue

                all_markets.append({
                    "ticker":       m["ticker"],
                    "event_ticker": m["event_ticker"],
                    "series_type":  series_type,
                    "bucket_type":  bucket_type,
                    "floor":        floor,
                    "cap":          cap,
                    "subtitle":     m.get("subtitle", ""),  # e.g. "48° to 49°", "52° or above"
                    "kalshi_prob":  m.get("last_price", 0),
                    "yes_ask":      m.get("yes_ask", 0),
                    "yes_bid":      m.get("yes_bid", 0),
                    "close_time":   m.get("close_time", ""),
                })

        except requests.exceptions.RequestException as e:
            log.error(f"[{city_key}] Kalshi API error for series {series}: {e}")
        except Exception as e:
            log.error(f"[{city_key}] Unexpected error fetching Kalshi series {series}: {e}")

    return all_markets


# ============================================================
# SECTION 5 — NWS WEATHER API
# Fetches the NWS forecast for a city and figures out the
# probability that the temperature lands in each Kalshi range.
# ============================================================

def fetch_nws_forecast(city_key):
    """
    Fetches the NWS temperature forecast for a city using two API calls:
      1. /points/{lat},{lon}  — converts coordinates to the NWS grid office.
                                Cached after the first call so it's only done once.
      2. /gridpoints/.../forecast — returns the actual temperature periods.

    Returns a dict with today's and tomorrow's highs and lows:
    {
        "today_high":    47,
        "today_low":     32,
        "tomorrow_high": 55,
        "tomorrow_low":  38,
    }

    Any value can be None if NWS doesn't have a forecast for that period yet.
    Returns None entirely if the API call fails — never crashes.
    """
    city = CITIES[city_key]

    # NWS requires a User-Agent header identifying your app — requests without it may be rejected
    headers = {"User-Agent": "KalshiClimateBot/1.0"}

    # --- Step 1: Look up (or load from cache) the NWS grid info for this city ---
    if city_key not in NWS_GRID_CACHE:
        try:
            points_url = f"https://api.weather.gov/points/{city['lat']},{city['lon']}"
            response = requests.get(points_url, headers=headers, timeout=10)
            response.raise_for_status()

            props = response.json()["properties"]

            # Cache everything we need so we never have to call /points again this run
            NWS_GRID_CACHE[city_key] = {
                "office":       props["gridId"],       # e.g., "OKX"
                "grid_x":       props["gridX"],        # e.g., 33
                "grid_y":       props["gridY"],        # e.g., 37
                "forecast_url": props["forecast"],     # full URL for step 2
            }
            log.info(f"[{city_key}] NWS grid resolved: {props['gridId']} {props['gridX']},{props['gridY']}")

        except requests.exceptions.RequestException as e:
            log.error(f"[{city_key}] NWS /points lookup failed: {e}")
            return None
        except Exception as e:
            log.error(f"[{city_key}] Unexpected error in NWS /points lookup: {e}")
            return None

    grid = NWS_GRID_CACHE[city_key]

    # --- Step 2: Fetch the forecast using the URL we got from /points ---
    try:
        response = requests.get(grid["forecast_url"], headers=headers, timeout=10)
        response.raise_for_status()

        periods = response.json()["properties"]["periods"]

        # NWS returns a list of forecast periods. Each period looks like:
        # {"startTime": "2026-02-18T06:00:00-05:00", "temperature": 47,
        #  "temperatureUnit": "F", "isDaytime": true, "name": "Tuesday", ...}
        #
        # isDaytime=True  → this is a HIGH temp period (daytime high)
        # isDaytime=False → this is a LOW temp period (overnight low)
        #
        # We group by date (YYYY-MM-DD from startTime) to find each day's high and low.
        forecasts = {}  # {"2026-02-18": {"high": 47, "low": 32}}

        for period in periods:
            # startTime looks like "2026-02-18T06:00:00-05:00" — take just the date part
            date_str = period["startTime"][:10]
            temp     = period["temperature"]

            # NWS always uses Fahrenheit for US locations, but check just in case
            if period.get("temperatureUnit") == "C":
                temp = round(temp * 9 / 5 + 32)  # convert Celsius to Fahrenheit

            if date_str not in forecasts:
                forecasts[date_str] = {}

            if period["isDaytime"]:
                forecasts[date_str]["high"] = temp
            else:
                forecasts[date_str]["low"] = temp

        # Build today's and tomorrow's date strings to look up in our forecasts dict.
        # NWS startTime dates (e.g. "2026-02-18T06:00:00-05:00") use the station's
        # local time — effectively ET for all US cities in this bot. We must use
        # ET-aware dates here, not datetime.now(), because on Railway (UTC clock)
        # datetime.now() returns UTC time. After 7 PM ET, that would be tomorrow's
        # UTC date, causing today_high/today_low to be None and today's markets
        # to be silently skipped. Consistent with how analyze_gaps() does it.
        et_now   = datetime.now(tz=ZoneInfo("America/New_York"))
        today    = et_now.strftime("%Y-%m-%d")
        tomorrow = (et_now + timedelta(days=1)).strftime("%Y-%m-%d")

        result = {
            "today_high":         forecasts.get(today,    {}).get("high"),
            "today_low":          forecasts.get(today,    {}).get("low"),
            "tomorrow_high":      forecasts.get(tomorrow, {}).get("high"),
            "tomorrow_low":       forecasts.get(tomorrow, {}).get("low"),
            "today_running_high": None,   # filled below from live observations
            "today_running_low":  None,   # filled below from live observations
        }

        # Fetch actual observed running high and low for today from the station.
        # Once the day is in progress, real observations are more accurate than
        # a forecast issued hours earlier. Returns None if no observations exist
        # yet (e.g. very early morning) or if the API call fails — safe to ignore.
        result["today_running_high"] = get_current_running_high(city_key)
        result["today_running_low"]  = get_current_running_low(city_key)

        return result

    except requests.exceptions.RequestException as e:
        log.error(f"[{city_key}] NWS forecast fetch failed: {e}")
        return None
    except Exception as e:
        log.error(f"[{city_key}] Unexpected error parsing NWS forecast: {e}")
        return None


# ============================================================
# SECTION 5b — RUNNING HIGH FROM OBSERVATIONS
# Fetches actual observed temperatures from NWS since midnight
# LST and returns the highest value seen so far today.
# Used in place of the grid forecast for TODAY's HIGH markets.
# ============================================================

def get_current_running_high(city_key):
    """
    Fetches today's actual observed temperature readings from the NWS
    observations API and returns the highest value recorded since midnight LST.

    This replaces the NWS grid forecast for TODAY's HIGH markets because once
    the day is in progress, real observations are more accurate than a forecast
    that may have been issued many hours ago.

    --- WHY LST, NOT CIVIL TIME ---
    Kalshi resolves daily temperature markets using Local Standard Time (LST)
    midnight-to-midnight windows, NOT civil time. This means:

      - In summer, a city on EDT (UTC-4) still uses EST (UTC-5) for its
        resolution window. Midnight LST = 5:00 AM UTC, not 4:00 AM UTC.
      - Phoenix (KPHX) always uses MST = UTC-7 since Arizona has no DST.
        Its lst_utc_offset is always -7 regardless of calendar date.

    We compute "midnight LST today" by:
      1. Shifting now (UTC) by the station's standard offset → LST now
      2. Flooring to midnight in LST
      3. Converting that midnight back to UTC for the API start= parameter

    --- CELSIUS ROUNDING LOGIC (5-minute ASOS stations) ---
    ASOS stations measure temperature in Celsius to 0.1°C precision. The
    official °F value is defined as:

        official_F = floor(celsius × 9/5 + 32)

    NWS stores Celsius to one decimal place, so each reading carries up to
    ±0.05°C of precision uncertainty. This translates to up to ±0.09°F —
    usually zero, but occasionally 1°F at a floor() boundary. We track two
    values per reading:

        max_observed  = max of floor(C × 9/5 + 32)           — conservative
        probable_max  = max of floor((C + 0.05) × 9/5 + 32)  — upper bound

    For cooperative observer (hourly) stations like KNYC, temperatures are
    observed and recorded in whole Fahrenheit degrees. The NWS API still
    returns them as Celsius, so we convert back but use round() instead of
    floor() since the original value was already a whole °F.

    --- DSM HIGH ---
    ASOS stations broadcast a Daily Summary Message (DSM) roughly once per
    hour containing maxTemperatureLast24Hours. This is the official rolling
    maximum and is the most authoritative value available. If it exceeds what
    we compute from individual readings (e.g. due to gaps in the time series),
    we use the DSM value as the floor for our result.

    Returns:
      {"max_observed": int, "probable_max": int, "obs_count": int}
        on success (at least one valid reading found)
      None  if no readings exist yet or the API call fails.
    """
    city         = CITIES[city_key]
    station      = city["nws_station"]
    station_type = city["station_type"]
    lst_offset   = city["lst_utc_offset"]   # standard time offset, e.g. -5 for EST

    headers = {"User-Agent": "KalshiClimateBot/1.0"}

    # ── Step 1: Find midnight LST in UTC ────────────────────────────────────
    # We want all observations from 00:00 LST today onward.
    # "LST now" = UTC now shifted by the station's standard (non-DST) offset.
    # Example: it's 14:00 UTC on Feb 18. EST = UTC-5 → LST now = 09:00 Feb 18.
    # Midnight EST today = 00:00 Feb 18 EST = 05:00 UTC Feb 18.
    now_utc  = datetime.utcnow()
    now_lst  = now_utc + timedelta(hours=lst_offset)  # shift UTC → LST

    # Midnight in LST for the current LST date
    lst_midnight = now_lst.replace(hour=0, minute=0, second=0, microsecond=0)

    # Convert that LST midnight back to UTC for the API query
    # (subtract the offset, because LST = UTC + offset → UTC = LST - offset)
    utc_midnight = lst_midnight - timedelta(hours=lst_offset)

    # Format as ISO 8601 with Z suffix (NWS API requires this format)
    start_utc_str = utc_midnight.strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info(
        f"[{city_key}] Running high: querying {station} obs since "
        f"{start_utc_str} UTC (= midnight LST, offset {lst_offset:+d}h)"
    )

    try:
        response = requests.get(
            f"https://api.weather.gov/stations/{station}/observations",
            params={"start": start_utc_str, "limit": 500},
            headers=headers,
            timeout=15,
        )
        response.raise_for_status()
        features = response.json().get("features", [])

        if not features:
            log.info(f"[{city_key}] No observations found since midnight LST.")
            return None

        max_observed = None   # conservative max: floor(C × 9/5 + 32) for 5-min stations
        probable_max = None   # upper bound: floor((C + 0.05) × 9/5 + 32)
        dsm_high     = None   # Daily Summary Message high from maxTemperatureLast24Hours
        valid_count  = 0      # number of observations with a valid temperature reading

        for feature in features:
            props = feature.get("properties", {})

            # ── Check for DSM high ───────────────────────────────────────────
            # ASOS stations include maxTemperatureLast24Hours in their hourly
            # DSM broadcast. This is the most authoritative daily high value —
            # it's what Kalshi ultimately compares against for resolution.
            # Not every observation has it; we take the max across all that do.
            dsm_obj = props.get("maxTemperatureLast24Hours", {})
            if dsm_obj and dsm_obj.get("value") is not None:
                # NWS always returns this in Celsius
                dsm_c = dsm_obj["value"]
                dsm_f = math.floor(dsm_c * 9 / 5 + 32)
                if dsm_high is None or dsm_f > dsm_high:
                    dsm_high = dsm_f
                    log.debug(f"[{city_key}] DSM high reading: {dsm_c}°C → {dsm_f}°F")

            # ── Read the individual temperature observation ──────────────────
            temp_obj = props.get("temperature", {})
            if not temp_obj or temp_obj.get("value") is None:
                # Observation exists but has no temperature (e.g. a SPECI report
                # with only wind/pressure data). Skip it.
                continue

            raw_value = temp_obj["value"]
            unit_code = temp_obj.get("unitCode", "")

            # The NWS API always returns temperature in Celsius (wmoUnit:degC),
            # but we guard against surprises just in case.
            if "degF" in unit_code:
                # Unexpected Fahrenheit — convert to Celsius so the math below
                # is consistent for all branches.
                celsius = (raw_value - 32) * 5 / 9
            else:
                celsius = raw_value   # already °C

            if station_type == "5-minute":
                # ── ASOS 5-minute station ────────────────────────────────────
                # Official °F = floor(C × 9/5 + 32). The raw Celsius is stored
                # to 0.1°C precision, so the true value could be up to 0.05°C
                # higher than what's reported (rounding uncertainty). We track:
                #   conservative: floor(C × 9/5 + 32)
                #   upper_bound:  floor((C + 0.05) × 9/5 + 32)
                # The difference is 0 or 1°F depending on where the floor falls.
                conservative = math.floor(celsius * 9 / 5 + 32)
                upper_bound  = math.floor((celsius + 0.05) * 9 / 5 + 32)

            else:
                # ── Cooperative observer (hourly) station, e.g. KNYC ─────────
                # The observer records temperature in whole Fahrenheit degrees.
                # The NWS API converts that whole °F value to Celsius for
                # storage. Rounding to the nearest integer recovers the original
                # whole °F reading. No floor() rounding uncertainty applies.
                conservative = round(celsius * 9 / 5 + 32)
                upper_bound  = conservative   # no ambiguity for hourly stations

            # Update rolling maximums
            if max_observed is None or conservative > max_observed:
                max_observed = conservative
            if probable_max is None or upper_bound > probable_max:
                probable_max = upper_bound

            valid_count += 1

        # ── Incorporate the DSM high ─────────────────────────────────────────
        # If the DSM high exceeds our computed max (which can happen if the
        # observations time series has gaps), use the DSM as the authoritative
        # floor. We never let the DSM lower our computed max — only raise it.
        if dsm_high is not None:
            if max_observed is None or dsm_high > max_observed:
                log.info(f"[{city_key}] DSM high ({dsm_high}°F) > computed max — updating.")
                max_observed = dsm_high
            if probable_max is None or dsm_high > probable_max:
                probable_max = dsm_high

        if max_observed is None:
            log.info(f"[{city_key}] {len(features)} observations found but none had temperature data.")
            return None

        log.info(
            f"[{city_key}] Running high: {max_observed}°F "
            f"(probable_max: {probable_max}°F, DSM: {dsm_high}°F, "
            f"{valid_count} valid readings from {len(features)} obs)"
        )
        return {
            "max_observed": max_observed,   # conservative official high so far today
            "probable_max": probable_max,   # upper bound accounting for C→F rounding
            "obs_count":    valid_count,    # number of individual readings processed
        }

    except requests.exceptions.RequestException as e:
        log.error(f"[{city_key}] Running high fetch failed: {e}")
        return None
    except Exception as e:
        log.error(f"[{city_key}] Unexpected error in running high fetch: {e}")
        return None


# ============================================================
# SECTION 5b-LOW — RUNNING LOW FROM OBSERVATIONS
# Mirror image of get_current_running_high() for LOW markets.
# Fetches actual observed temperatures from NWS since midnight
# LST and returns the lowest value seen so far today.
# Used in place of the grid forecast for TODAY's LOW markets.
#
# Key differences from get_current_running_high():
#   - Tracks running minimum instead of maximum
#   - No DSM field: there is no minTemperatureLast24Hours in the
#     NWS observations API, so that section is omitted entirely
#   - probable_min uses floor((C - 0.05) × 9/5 + 32) as the lower
#     bound — the stored Celsius could be 0.05°C higher than true,
#     meaning the true converted minimum might be 1°F lower
# ============================================================

def get_current_running_low(city_key):
    """
    Fetches today's actual observed temperature readings from the NWS
    observations API and returns the lowest value recorded since midnight LST.

    This replaces the NWS grid forecast for TODAY's LOW markets because once
    the day is in progress, real observations are more accurate than a forecast
    that may have been issued many hours ago.

    Uses the same LST midnight calculation as get_current_running_high() —
    see that function's docstring for the full explanation of why we use LST
    rather than civil time (short version: Kalshi resolves on LST windows,
    not civil-time midnight, so a summer ET city still uses UTC-5, not UTC-4).

    --- CELSIUS ROUNDING LOGIC (5-minute ASOS stations) ---
    ASOS stations store temperature in Celsius to 0.1°C precision.
    Official °F = floor(C × 9/5 + 32). A stored value of, say, 10.0°C
    represents the range [9.95, 10.05)°C — the true temperature could be
    up to 0.05°C lower. We track two values:

        min_observed  = min of floor(C × 9/5 + 32)            — conservative
        probable_min  = min of floor((C - 0.05) × 9/5 + 32)  — lower bound

    For cooperative observer (hourly) stations the original reading was a
    whole °F, so round() recovers it exactly and there is no lower-bound
    ambiguity (probable_min == min_observed).

    --- NO DSM EQUIVALENT ---
    ASOS DSM broadcasts include maxTemperatureLast24Hours but NOT a
    minTemperatureLast24Hours field, so we cannot apply the same DSM
    override that get_current_running_high() uses. The observed readings
    from the time series are the only source for the running low.

    Returns:
      {"min_observed": int, "probable_min": int, "obs_count": int}
        on success (at least one valid reading found)
      None  if no readings exist yet or the API call fails.
    """
    city         = CITIES[city_key]
    station      = city["nws_station"]
    station_type = city["station_type"]
    lst_offset   = city["lst_utc_offset"]   # standard time offset, e.g. -5 for EST

    headers = {"User-Agent": "KalshiClimateBot/1.0"}

    # ── Step 1: Find midnight LST in UTC (identical to running high) ─────────
    now_utc      = datetime.utcnow()
    now_lst      = now_utc + timedelta(hours=lst_offset)
    lst_midnight = now_lst.replace(hour=0, minute=0, second=0, microsecond=0)
    utc_midnight = lst_midnight - timedelta(hours=lst_offset)
    start_utc_str = utc_midnight.strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info(
        f"[{city_key}] Running low: querying {station} obs since "
        f"{start_utc_str} UTC (= midnight LST, offset {lst_offset:+d}h)"
    )

    try:
        response = requests.get(
            f"https://api.weather.gov/stations/{station}/observations",
            params={"start": start_utc_str, "limit": 500},
            headers=headers,
            timeout=15,
        )
        response.raise_for_status()
        features = response.json().get("features", [])

        if not features:
            log.info(f"[{city_key}] No observations found since midnight LST.")
            return None

        min_observed = None   # conservative min: floor(C × 9/5 + 32) for 5-min stations
        probable_min = None   # lower bound: floor((C - 0.05) × 9/5 + 32)
        valid_count  = 0      # number of observations with a valid temperature reading

        for feature in features:
            props = feature.get("properties", {})

            # ── Read the individual temperature observation ──────────────────
            # (No DSM min field exists in the NWS observations API — skip.)
            temp_obj = props.get("temperature", {})
            if not temp_obj or temp_obj.get("value") is None:
                # Observation exists but has no temperature (e.g. a SPECI report
                # with only wind/pressure data). Skip it.
                continue

            raw_value = temp_obj["value"]
            unit_code = temp_obj.get("unitCode", "")

            # The NWS API always returns temperature in Celsius (wmoUnit:degC),
            # but we guard against surprises just in case.
            if "degF" in unit_code:
                celsius = (raw_value - 32) * 5 / 9
            else:
                celsius = raw_value   # already °C

            if station_type == "5-minute":
                # ── ASOS 5-minute station ────────────────────────────────────
                # Official °F = floor(C × 9/5 + 32). The stored Celsius could
                # be up to 0.05°C higher than the true value (precision limit),
                # meaning the true minimum might be 1°F lower. We track:
                #   conservative: floor(C × 9/5 + 32)
                #   lower_bound:  floor((C - 0.05) × 9/5 + 32)
                conservative = math.floor(celsius * 9 / 5 + 32)
                lower_bound  = math.floor((celsius - 0.05) * 9 / 5 + 32)

            else:
                # ── Cooperative observer (hourly) station, e.g. KNYC ─────────
                # Original whole °F reading; round() recovers it exactly.
                # No lower-bound ambiguity applies.
                conservative = round(celsius * 9 / 5 + 32)
                lower_bound  = conservative

            # Update rolling minimums
            if min_observed is None or conservative < min_observed:
                min_observed = conservative
            if probable_min is None or lower_bound < probable_min:
                probable_min = lower_bound

            valid_count += 1

        if min_observed is None:
            log.info(f"[{city_key}] {len(features)} observations found but none had temperature data.")
            return None

        log.info(
            f"[{city_key}] Running low: {min_observed}°F "
            f"(probable_min: {probable_min}°F, "
            f"{valid_count} valid readings from {len(features)} obs)"
        )
        return {
            "min_observed": min_observed,   # conservative official low so far today
            "probable_min": probable_min,   # lower bound accounting for C→F rounding
            "obs_count":    valid_count,    # number of individual readings processed
        }

    except requests.exceptions.RequestException as e:
        log.error(f"[{city_key}] Running low fetch failed: {e}")
        return None
    except Exception as e:
        log.error(f"[{city_key}] Unexpected error in running low fetch: {e}")
        return None


# ============================================================
# SECTION 5c — MULTI-MODEL FORECASTS
# Three additional forecast sources for cross-validation with NWS.
# All are non-fatal — failure here never skips a city or blocks
# the main cycle. NWS still drives all signal logic.
#
# Sources:
#   Open-Meteo ECMWF  (ecmwf_ifs04)  — free, no key
#   Open-Meteo GFS    (gfs_seamless)  — free, no key
#   WeatherAPI.com                    — requires WEATHERAPI_KEY
# ============================================================

def _fetch_openmeteo_model(city_key, model):
    """
    Shared helper for Open-Meteo model fetches.
    Calls the Open-Meteo daily forecast API with a specific model parameter.

    Returns {today_high, today_low, tomorrow_high, tomorrow_low} or None on failure.
    """
    city = CITIES[city_key]

    try:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":         city["lat"],
                "longitude":        city["lon"],
                "daily":            "temperature_2m_max,temperature_2m_min",
                "temperature_unit": "fahrenheit",
                "timezone":         "auto",
                "forecast_days":    2,
                "models":           model,
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        times = data["daily"]["time"]
        highs = data["daily"]["temperature_2m_max"]
        lows  = data["daily"]["temperature_2m_min"]

        # Use UTC date — Open-Meteo starts its daily forecast from the UTC current
        # date, so matching against local time would miss "today" after 7 PM ET
        # (when UTC has already rolled to the next day).
        today    = datetime.utcnow().strftime("%Y-%m-%d")
        tomorrow = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")

        result = {"today_high": None, "today_low": None, "tomorrow_high": None, "tomorrow_low": None}

        for i, date_str in enumerate(times):
            high = round(highs[i]) if highs[i] is not None else None
            low  = round(lows[i])  if lows[i]  is not None else None
            if date_str == today:
                result["today_high"] = high
                result["today_low"]  = low
            elif date_str == tomorrow:
                result["tomorrow_high"] = high
                result["tomorrow_low"]  = low

        log.info(
            f"[{city_key}] Open-Meteo ({model}): "
            f"today {result['today_high']}°F/{result['today_low']}°F, "
            f"tomorrow {result['tomorrow_high']}°F/{result['tomorrow_low']}°F"
        )
        return result

    except requests.exceptions.RequestException as e:
        log.error(f"[{city_key}] Open-Meteo ({model}) request failed: {e}")
        return None
    except Exception as e:
        log.error(f"[{city_key}] Unexpected error in Open-Meteo ({model}) fetch: {e}")
        return None


def fetch_ecmwf_forecast(city_key):
    """Fetches the ECMWF IFS 0.25° model forecast from Open-Meteo. Free, no key needed."""
    return _fetch_openmeteo_model(city_key, "ecmwf_ifs025")


def fetch_gfs_forecast(city_key):
    """Fetches the GFS Seamless model forecast from Open-Meteo. Free, no key needed."""
    return _fetch_openmeteo_model(city_key, "gfs_seamless")


def fetch_gem_forecast(city_key):
    """Fetches the Canadian GEM Seamless model forecast from Open-Meteo. Free, no key needed."""
    return _fetch_openmeteo_model(city_key, "gem_seamless")


def fetch_icon_forecast(city_key):
    """Fetches the DWD ICON Seamless model forecast from Open-Meteo. Free, no key needed."""
    return _fetch_openmeteo_model(city_key, "icon_seamless")


def fetch_hourly_forecast(city_key):
    """
    Fetches hourly temperature forecasts from Open-Meteo (GFS model) for the next 48 hours.
    Returns a dict of {iso_datetime_string: temp_f} for each hour, or None on failure.

    Used to estimate remaining high/low potential for today's markets:
    - For today's HIGH: what's the max forecasted temp for remaining hours today?
    - For today's LOW: what's the min forecasted temp for remaining hours today?
    """
    city = CITIES[city_key]

    try:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":         city["lat"],
                "longitude":        city["lon"],
                "hourly":           "temperature_2m",
                "temperature_unit": "fahrenheit",
                "timezone":         "auto",
                "forecast_days":    2,
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        times = data["hourly"]["time"]             # ["2026-02-19T00:00", "2026-02-19T01:00", ...]
        temps = data["hourly"]["temperature_2m"]   # [52.3, 51.1, ...] in °F

        result = {}
        for t, temp in zip(times, temps):
            if temp is not None:
                result[t] = round(temp)

        log.info(f"[{city_key}] Hourly forecast: {len(result)} hours fetched")
        return result

    except requests.exceptions.RequestException as e:
        log.error(f"[{city_key}] Hourly forecast request failed: {e}")
        return None
    except Exception as e:
        log.error(f"[{city_key}] Unexpected error in hourly forecast fetch: {e}")
        return None


def estimate_remaining_extreme(hourly_data, city_key, extreme_type):
    """
    Given hourly forecast data, estimates the max or min temperature
    for the remaining hours of today (in LST).

    extreme_type: "high" or "low"

    Returns the forecasted remaining extreme temp (int °F) or None.
    Uses the city's lst_utc_offset to determine which hours belong to "today".

    NOTE: Open-Meteo timezone="auto" returns times in the station's local civil
    time, which includes DST in summer. Our lst_utc_offset is fixed (no DST).
    In February this is fine (no DST active). Starting in March when clocks
    change, Open-Meteo times will run 1 hour ahead of LST — a future improvement
    would apply a DST correction for affected months.
    """
    lst_offset = CITIES[city_key]["lst_utc_offset"]
    now_lst    = datetime.utcnow() + timedelta(hours=lst_offset)
    today_lst  = now_lst.date()

    future_temps = []
    for time_str, temp_f in hourly_data.items():
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
        except ValueError:
            continue
        # Only include hours that are both on today's LST date AND in the future
        if dt.date() == today_lst and dt.hour > now_lst.hour:
            future_temps.append(temp_f)

    if not future_temps:
        return None

    return max(future_temps) if extreme_type == "high" else min(future_temps)


def fetch_weatherapi_forecast(city_key):
    """
    Fetches today's and tomorrow's high/low temperature forecast from WeatherAPI.com.
    Requires WEATHERAPI_KEY in env vars (free tier is sufficient — 1M calls/month).

    Returns {today_high, today_low, tomorrow_high, tomorrow_low} or None on failure.
    """
    if not WEATHERAPI_KEY:
        return None

    city = CITIES[city_key]

    try:
        response = requests.get(
            "https://api.weatherapi.com/v1/forecast.json",
            params={
                "key":    WEATHERAPI_KEY,
                "q":      f"{city['lat']},{city['lon']}",
                "days":   2,
                "aqi":    "no",
                "alerts": "no",
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        days   = data["forecast"]["forecastday"]   # list of up to 2 items: today + tomorrow
        result = {"today_high": None, "today_low": None, "tomorrow_high": None, "tomorrow_low": None}

        if len(days) >= 1:
            result["today_high"] = round(days[0]["day"]["maxtemp_f"])
            result["today_low"]  = round(days[0]["day"]["mintemp_f"])
        if len(days) >= 2:
            result["tomorrow_high"] = round(days[1]["day"]["maxtemp_f"])
            result["tomorrow_low"]  = round(days[1]["day"]["mintemp_f"])

        log.info(
            f"[{city_key}] WeatherAPI: "
            f"today {result['today_high']}°F/{result['today_low']}°F, "
            f"tomorrow {result['tomorrow_high']}°F/{result['tomorrow_low']}°F"
        )
        return result

    except requests.exceptions.RequestException as e:
        log.error(f"[{city_key}] WeatherAPI request failed: {e}")
        return None
    except Exception as e:
        log.error(f"[{city_key}] Unexpected error in WeatherAPI fetch: {e}")
        return None


def _get_model_data(city_key, g, all_forecasts):
    """
    Pulls forecast temperatures from all models for a single market gap result.

    Returns a dict with:
      nws_temp / ecmwf_temp / gfs_temp / gem_temp / icon_temp / wapi_temp — °F ints or None
      spread          — max−min across all available models (°F int or None)
      models_line     — pre-formatted "Models: NWS 79° | ECMWF 78° | ..." string
      has_enough_data — True only if NWS and at least one other model are present
    """
    fc_key = f"{g['market_date']}_{g['series_type'].lower()}"   # e.g. "today_high"

    forecasts  = all_forecasts.get(city_key, {})
    nws_temp   = (forecasts.get("nws")        or {}).get(fc_key)
    ecmwf_temp = (forecasts.get("ecmwf")      or {}).get(fc_key)
    gfs_temp   = (forecasts.get("gfs")        or {}).get(fc_key)
    gem_temp   = (forecasts.get("gem")        or {}).get(fc_key)
    icon_temp  = (forecasts.get("icon")       or {}).get(fc_key)
    wapi_temp  = (forecasts.get("weatherapi") or {}).get(fc_key)

    # Require NWS + at least one other model for a market to be shown
    other_temps     = [t for t in [ecmwf_temp, gfs_temp, gem_temp, icon_temp, wapi_temp] if t is not None]
    has_enough_data = nws_temp is not None and len(other_temps) >= 1

    # Build the models line parts
    parts = []
    if nws_temp   is not None: parts.append(f"NWS {nws_temp}°")
    if ecmwf_temp is not None: parts.append(f"ECMWF {ecmwf_temp}°")
    if gfs_temp   is not None: parts.append(f"GFS {gfs_temp}°")
    if gem_temp   is not None: parts.append(f"GEM {gem_temp}°")
    if icon_temp  is not None: parts.append(f"ICON {icon_temp}°")
    if wapi_temp  is not None: parts.append(f"WAPI {wapi_temp}°")

    all_temps = [t for t in [nws_temp, ecmwf_temp, gfs_temp, gem_temp, icon_temp, wapi_temp] if t is not None]
    spread    = (max(all_temps) - min(all_temps)) if len(all_temps) >= 2 else None
    consensus = round(sum(all_temps) / len(all_temps)) if all_temps else None

    if spread is not None:
        parts.append(f"Spread: {spread}°")

    models_line = "Models: " + " | ".join(parts) if parts else "Models: N/A"

    return {
        "nws_temp":        nws_temp,
        "ecmwf_temp":      ecmwf_temp,
        "gfs_temp":        gfs_temp,
        "gem_temp":        gem_temp,
        "icon_temp":       icon_temp,
        "wapi_temp":       wapi_temp,
        "spread":          spread,
        "consensus":       consensus,
        "models_line":     models_line,
        "has_enough_data": has_enough_data,
    }


def calculate_gaussian_probability(forecast_temp, bucket_type, floor, cap, std_dev=2.5):
    """
    Estimates the probability that a Kalshi market resolves YES using a Normal
    distribution CDF centered on the forecast temperature.

    Weather forecast error follows roughly a bell curve, not a step function.
    Using the CDF gives a smooth, continuous probability that naturally drops
    as you move away from a boundary — no arbitrary 55%/65%/35% cutoffs.

    std_dev controls the width of the uncertainty bell:
      2.0 — models agree tightly (spread < 1°F): narrow curve, high confidence
      2.5 — baseline for 1-day-out forecasts
      4.0 — models diverge (spread > 3°F): wide curve, lower confidence

    Works for all three bucket types:
      FLOOR  (">floor"):       P(actual > floor)
      CAP    ("<cap"):         P(actual < cap)
      RANGE  ("floor–cap"):   P(floor ≤ actual ≤ cap)

    Returns a float 0.0–100.0 (caller should round for display/logging).
    """
    dist = norm(loc=forecast_temp, scale=std_dev)

    if bucket_type == "FLOOR":
        return (1 - dist.cdf(floor)) * 100
    elif bucket_type == "CAP":
        return dist.cdf(cap) * 100
    elif bucket_type == "RANGE":
        return (dist.cdf(cap) - dist.cdf(floor)) * 100
    else:
        log.warning(f"calculate_gaussian_probability: unknown bucket_type '{bucket_type}'")
        return 50.0


def _bucket_label(market):
    """
    Returns a short human-readable label for a market's bucket, e.g.:
      FLOOR → ">51°F"
      CAP   → "<44°F"
      RANGE → "48–49°F"
    Uses the subtitle field from the API when available; falls back to building
    a label from floor/cap so it always works even if subtitle is missing.
    """
    if market.get("subtitle"):
        return market["subtitle"]
    bt = market["bucket_type"]
    if bt == "FLOOR":
        return f">{market['floor']}°F"
    if bt == "CAP":
        return f"<{market['cap']}°F"
    return f"{market['floor']}–{market['cap']}°F"


# ============================================================
# SECTION 6 — GAP ANALYSIS
# Compares Kalshi's market price against NWS's implied probability.
# The "gap" is the difference — a big gap = potentially interesting trade.
# ============================================================

def analyze_gaps(city_key, kalshi_markets, nws_forecast, city_forecasts=None):
    """
    For each Kalshi market, compares the Gaussian model-implied probability
    against the Kalshi market price and calculates the gap.

    Handles FLOOR, CAP, and RANGE bucket types dynamically — never assumes
    a fixed bucket shape. Works correctly even if Kalshi changes all buckets
    between runs.

    city_forecasts: optional dict {"nws": {...}, "ecmwf": {...}, "gfs": {...},
                    "weatherapi": {...}} — used to compute dynamic std_dev from
                    model spread. If omitted, std_dev=2.5 (baseline) is used.

    Dynamic std_dev logic per market period:
      spread < 1°F  → std_dev = 2.0  (models agree — higher confidence)
      spread > 3°F  → std_dev = 4.0  (models disagree — lower confidence)
      otherwise     → std_dev = 2.5  (baseline for 1-day-out forecasts)

    A positive gap means the model thinks YES is MORE likely than Kalshi does.
    A negative gap means the model thinks NO is more likely.

    Returns a list sorted by absolute gap size (largest gaps first).
    """
    # Use ET dates, not UTC. On Railway the system clock is UTC, so at 11 PM ET
    # datetime.now().date() returns tomorrow's UTC date — all "tomorrow" markets
    # would be mis-labelled as "unknown date" and silently dropped.
    et_now   = datetime.now(tz=ZoneInfo("America/New_York"))
    today    = et_now.date()
    tomorrow = today + timedelta(days=1)

    results = []

    # Current LST hour for this city — used in the time-decay logic below.
    # LST (Local Standard Time) never changes for DST because Kalshi's resolution
    # windows are defined in fixed LST, not civil time.
    now_utc  = datetime.utcnow()
    lst_hour = (now_utc + timedelta(hours=CITIES[city_key]["lst_utc_offset"])).hour

    for market in kalshi_markets:
        # --- Step 1: Determine which date this market resolves on ---
        # event_ticker is like "KXHIGHNY-26FEB18"; the date is the last "-" segment
        date_part = market["event_ticker"].split("-")[-1]
        try:
            market_date = datetime.strptime(date_part, "%y%b%d").date()
        except ValueError:
            log.warning(f"[{city_key}] Could not parse date from event_ticker: {market['event_ticker']}")
            continue

        if market_date == today:
            date_label = "today"
        elif market_date == tomorrow:
            date_label = "tomorrow"
        else:
            continue  # skip markets for other dates

        # --- Step 2: Pick the best available temperature for this date + series ---
        # For today's HIGH and LOW markets we prefer actual observed running
        # values (from Sections 5b/5b-LOW) over the grid forecast, because once
        # observations exist they are more accurate than a forecast issued hours
        # earlier. For tomorrow's markets we always use the NWS grid forecast.

        # probable_min_temp mirrors probable_max_temp for LOW markets:
        # set only when running low observations are available, None otherwise.
        probable_min_temp = None
        # observed_running: the actual running observed value when live observations
        # replace the NWS grid forecast. None for tomorrow markets or when no
        # observations exist yet. Stored so log_to_csv() can write it separately
        # from the grid forecast — keeping both for unambiguous historical analysis.
        observed_running  = None
        # running: the raw running obs dict (has max_observed/probable_max for HIGH,
        # min_observed/probable_min for LOW). Initialized to None so Step 3c can
        # always reference it safely even if we're in a tomorrow branch.
        running           = None

        if date_label == "today" and market["series_type"] == "HIGH":
            nws_grid_forecast = nws_forecast.get("today_high")   # raw grid, always
            running = nws_forecast.get("today_running_high")
            if running is not None:
                # Use the conservative observed max (floor(C×9/5+32)).
                # probable_max is stored in the result for display purposes.
                forecast_temp     = running["max_observed"]
                observed_running  = running["max_observed"]
                probable_max_temp = running["probable_max"]
            else:
                # Fall back to the grid forecast if observations aren't available
                # (e.g. very early morning before any readings today).
                forecast_temp     = nws_forecast["today_high"]
                probable_max_temp = None
        elif date_label == "today" and market["series_type"] == "LOW":
            nws_grid_forecast = nws_forecast.get("today_low")    # raw grid, always
            running = nws_forecast.get("today_running_low")
            if running is not None:
                # Use the conservative observed min (floor(C×9/5+32)).
                # probable_min is stored in the result for display purposes.
                forecast_temp     = running["min_observed"]
                observed_running  = running["min_observed"]
                probable_min_temp = running["probable_min"]
            else:
                # Fall back to the grid forecast if observations aren't available.
                forecast_temp     = nws_forecast["today_low"]
            probable_max_temp = None
        elif date_label == "tomorrow" and market["series_type"] == "HIGH":
            nws_grid_forecast = nws_forecast.get("tomorrow_high")
            forecast_temp     = nws_forecast["tomorrow_high"]
            probable_max_temp = None
        else:
            nws_grid_forecast = nws_forecast.get("tomorrow_low")
            forecast_temp     = nws_forecast["tomorrow_low"]
            probable_max_temp = None

        if forecast_temp is None:
            # No temperature available for this period yet — skip
            continue

        # --- Step 3: Determine dynamic std_dev from model spread ---
        # Pull all available model temps for this market's date+series period
        # and measure how much they disagree. High spread → wider uncertainty.
        std_dev = 2.5   # baseline for 1-day-out forecasts
        if city_forecasts:
            fc_key = f"{date_label}_{market['series_type'].lower()}"
            model_temps = [
                (city_forecasts.get("nws")        or {}).get(fc_key),
                (city_forecasts.get("ecmwf")      or {}).get(fc_key),
                (city_forecasts.get("gfs")        or {}).get(fc_key),
                (city_forecasts.get("gem")        or {}).get(fc_key),
                (city_forecasts.get("icon")       or {}).get(fc_key),
                (city_forecasts.get("weatherapi") or {}).get(fc_key),
            ]
            valid_temps = [t for t in model_temps if t is not None]
            if len(valid_temps) >= 2:
                spread = max(valid_temps) - min(valid_temps)
                if spread < 1:
                    std_dev = 2.0   # models tightly agree — narrow curve, high confidence
                elif spread > 3:
                    std_dev = 4.0   # models diverge — wide curve, low confidence

        # --- Step 3b: Time-decay adjustment for today's markets ---
        # As the day progresses, uncertainty about the final high or low shrinks
        # dramatically — the running observation is a far stronger signal than a
        # morning forecast. We tighten std_dev only when:
        #   (a) this is a today market, AND
        #   (b) we have actual observations (running high/low is not None).
        # Tomorrow's markets are never adjusted here.
        # Floor of 1.0 prevents std_dev from collapsing to near-zero.
        time_decay_multiplier = 1.0   # default: no decay applied

        if date_label == "today" and market["series_type"] == "HIGH" and observed_running is not None:
            # Daily highs typically occur between 10 AM and 5 PM LST.
            # Once past solar noon the high has very likely already occurred.
            if lst_hour >= 17:
                time_decay_multiplier = 0.3    # after 5 PM: high almost certainly done
            elif lst_hour >= 14:
                time_decay_multiplier = 0.5    # 2–5 PM: high very likely occurred
            elif lst_hour >= 10:
                time_decay_multiplier = 0.75   # 10 AM–2 PM: high is forming now
            # Before 10 AM: no adjustment — full uncertainty remains
            std_dev = max(std_dev * time_decay_multiplier, 1.0)

        elif date_label == "today" and market["series_type"] == "LOW" and observed_running is not None:
            # Overnight lows typically occur near sunrise (5–7 AM LST).
            # By mid-morning the low for the day has almost certainly passed.
            if lst_hour >= 12:
                time_decay_multiplier = 0.3    # after noon: definitely done, temp is rising
            elif lst_hour >= 8:
                time_decay_multiplier = 0.5    # 8 AM–noon: low very likely passed
            elif lst_hour >= 4:
                time_decay_multiplier = 0.75   # 4–8 AM: low is forming near sunrise
            # Before 4 AM: no adjustment — overnight low hasn't occurred yet
            std_dev = max(std_dev * time_decay_multiplier, 1.0)

        # --- Step 3c: Hourly forecast adjustment ---
        # If hourly model data is available, check whether the remaining hours
        # of today are forecast to exceed the current running extreme.
        # If the model says they won't, that's corroborating evidence the extreme
        # has already occurred — tighten std_dev one more notch (×0.7, floor 1.0).
        # Only applies to today's markets with live observations. Never tomorrow.
        # Does NOT change forecast_temp — observations are ground truth.
        hourly_remaining_extreme = None
        hourly_adjusted          = False

        if date_label == "today" and city_forecasts and city_forecasts.get("hourly"):
            if market["series_type"] == "HIGH" and observed_running is not None:
                remaining_high = estimate_remaining_extreme(
                    city_forecasts["hourly"], city_key, "high"
                )
                hourly_remaining_extreme = remaining_high
                if remaining_high is not None and remaining_high < running["max_observed"]:
                    # Hourly model agrees the high has already occurred
                    std_dev        = max(std_dev * 0.7, 1.0)
                    hourly_adjusted = True

            elif market["series_type"] == "LOW" and observed_running is not None:
                remaining_low = estimate_remaining_extreme(
                    city_forecasts["hourly"], city_key, "low"
                )
                hourly_remaining_extreme = remaining_low
                if remaining_low is not None and remaining_low > running["min_observed"]:
                    # Hourly model agrees the low has already occurred
                    std_dev        = max(std_dev * 0.7, 1.0)
                    hourly_adjusted = True

        # --- Step 4: Calculate Gaussian-implied probability for this bucket ---
        nws_prob_raw = calculate_gaussian_probability(
            forecast_temp,
            market["bucket_type"],
            market["floor"],
            market["cap"],
            std_dev,
        )
        nws_prob = round(nws_prob_raw)   # integer for gap math and display

        # --- Step 5: Compute the gap ---
        gap  = nws_prob - market["kalshi_prob"]
        edge = "BUY YES" if gap > 0 else "BUY NO"

        if abs(gap) < MIN_GAP_TO_SHOW:
            continue

        # Confidence: HIGH when the model is clearly on one side (≥65% YES or ≤35% YES).
        # LOW when it's in the 35–65% zone — too close to the boundary to trust.
        confidence = "HIGH" if nws_prob >= 65 or nws_prob <= 35 else "LOW"

        # was_settled: Kalshi has already priced this very strongly — market is nearly over.
        was_settled = market["kalshi_prob"] > 90 or market["kalshi_prob"] < 10

        results.append({
            "ticker":        market["ticker"],
            "series_type":   market["series_type"],
            "bucket_type":   market["bucket_type"],
            "bucket_label":  _bucket_label(market),   # human-readable, e.g. "48° to 49°"
            "floor":         market["floor"],
            "cap":           market["cap"],
            "market_date":   date_label,
            "forecast_temp": forecast_temp,
            "probable_max":  probable_max_temp,        # upper bound for today HIGH obs only
            "probable_min":  probable_min_temp,        # lower bound for today LOW obs only
            "kalshi_prob":   market["kalshi_prob"],
            "nws_prob":      nws_prob,
            "gap":           gap,
            "edge":          edge,
            "confidence":    confidence,
            "was_settled":      was_settled,
            "std_dev_used":            std_dev,
            "time_decay_multiplier":   time_decay_multiplier,
            "hourly_remaining_extreme": hourly_remaining_extreme,
            "hourly_adjusted":         hourly_adjusted,
            "nws_grid_forecast": nws_grid_forecast,  # raw NWS grid temp (never observed)
            "observed_running":  observed_running,    # live obs if used, else None
        })

    results.sort(key=lambda x: abs(x["gap"]), reverse=True)
    return results


# ============================================================
# SECTION 7 — ALERTS
# Formats the results into a clean, readable message and sends
# it via Twilio SMS and/or Gmail email.
# ============================================================

def _apply_email_filters(g, md):
    """
    Returns True if a market passes ALL four email quality gates.
    All conditions must be satisfied — any single failure excludes the market.

    Rule 1 — Exclude settled: Kalshi price must be 10–90 (inclusive).
              Prices outside this range mean the market has effectively
              resolved and there's no meaningful edge to evaluate.

    Rule 2 — Require an edge: |gap| ≥ 15%.
              A gap smaller than 15pp isn't worth acting on after fees.

    Rule 3 — Exclude high uncertainty: model spread < 4°F.
              When models disagree by ≥4° there's no reliable signal.

    Rule 4 — Exclude impossible outcomes: consensus within 5°F of bucket.
              If all models agree the temp is 10° below a FLOOR boundary,
              the YES outcome is essentially impossible and the market
              is already mispriced for structural reasons, not an edge.
    """
    # Rule 1: not settled
    if g["was_settled"]:
        return False

    # Rule 2: meaningful edge
    if abs(g["gap"]) < 15:
        return False

    # Rule 3: models must broadly agree
    if md["spread"] is not None and md["spread"] >= 4:
        return False

    # Rule 4: consensus within 5°F of bucket boundaries
    # (i.e., don't show markets where the outcome is obviously predetermined)
    if md["consensus"] is not None:
        c  = md["consensus"]
        bt = g["bucket_type"]
        fl = g["floor"]
        cp = g["cap"]

        if bt == "FLOOR" and fl is not None and c < fl - 5:
            return False   # consensus is far below floor — clearly NO
        if bt == "CAP"   and cp is not None and c > cp + 5:
            return False   # consensus is far above cap — clearly NO
        if bt == "RANGE":
            if fl is not None and c < fl - 5:
                return False   # consensus far below range
            if cp is not None and c > cp + 5:
                return False   # consensus far above range

    return True


def format_alert_message(all_results, all_forecasts):
    """
    Formats all markets into a plain-text email using model temperature cards.

    Card format per market:
      📍 CITY NAME — TYPE bucket_label
      Kalshi: X%
      Models: NWS Y° | ECMWF Z° | GFS W° | WAPI V° | Spread: N°
      ⚠️ HIGH SPREAD   (appended to models line only if spread ≥ 3°F)

    Filtering: only markets where NWS + at least one other model have data.
    Sort: Tier 1 cities first, then all others — each group sorted by Kalshi
    price ascending (lowest price = most potentially underpriced).
    """
    # Use ET time for date display — avoids UTC-vs-ET mismatch on Railway
    et_now        = _et_now()
    today_date    = et_now.strftime("%b %d")
    tomorrow_date = (et_now + timedelta(days=1)).strftime("%b %d")
    time_str      = et_now.strftime("%I:%M %p").lstrip("0")
    DIVIDER       = "————————————————"

    def build_market_list(date_filter):
        """Builds, filters, and sorts the market list for one date period."""
        markets = []
        for city_key, gaps in all_results.items():
            city_name = CITIES[city_key]["name"]
            for g in gaps:
                if g["market_date"] != date_filter:
                    continue
                md = _get_model_data(city_key, g, all_forecasts)
                if not md["has_enough_data"]:
                    continue
                if not _apply_email_filters(g, md):
                    continue
                markets.append({**g, "city_name": city_name, "city_key": city_key, **md})
        # Tier 1 first, then by gap size descending (highest-conviction first)
        markets.sort(key=lambda x: (0 if x["city_key"] in TIER1_CITIES else 1, -abs(x["gap"])))
        return markets

    def render_markets(market_list):
        """Renders a list of markets as card lines."""
        card_lines = []
        for g in market_list:
            spread_tag = "  ⚠️ HIGH SPREAD" if (g["spread"] is not None and g["spread"] >= 5) else ""
            card_lines.append(f"📍 {g['city_name'].upper()} — {g['series_type']} {g['bucket_label']}")
            card_lines.append(f"Kalshi: {g['kalshi_prob']}%")
            card_lines.append(f"Model: {g['nws_prob']}% → {g['edge']} (gap: {g['gap']:+d}%)")
            card_lines.append(g["models_line"] + spread_tag)
            card_lines.append(DIVIDER)
        return card_lines

    tomorrow_markets = build_market_list("tomorrow")
    today_markets    = build_market_list("today")
    total            = len(tomorrow_markets) + len(today_markets)

    lines = []
    lines.append(f"🤖 Kalshi Bot · {today_date} · {time_str}")
    lines.append(f"{total} markets shown")
    lines.append("")

    # ── TOMORROW ─────────────────────────────────────────
    lines.append(f"——— TOMORROW {tomorrow_date} ———")
    lines.append("")
    if tomorrow_markets:
        lines.extend(render_markets(tomorrow_markets))
    else:
        lines.append("No markets with sufficient model data for tomorrow.")
        lines.append("")

    # ── TODAY ────────────────────────────────────────────
    if today_markets:
        lines.append(f"——— TODAY {today_date} ———")
        lines.append("")
        lines.extend(render_markets(today_markets))

    lines.append("──────────────────────")
    lines.append("Not financial advice.")

    return "\n".join(lines)


def send_email(message, subject=None):
    """
    Sends the formatted alert as a plain-text email via SendGrid's HTTP API.
    Uses SENDGRID_API_KEY for authentication (set it in Railway env vars).
    Logs any error and continues — email failure never crashes the bot.

    subject — optional custom subject line. If omitted, falls back to the
              generic timestamped default (used by check_running_high_alerts).
    """
    if not all([SENDGRID_API_KEY, ALERT_FROM_EMAIL, ALERT_TO_EMAIL]):
        log.warning("SendGrid credentials missing in env — skipping email.")
        return

    if subject is None:
        now     = datetime.now()
        subject = f"Kalshi Climate Bot — {now.strftime('%b %d')} {now.strftime('%I:%M %p')}"

    try:
        response = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "personalizations": [{"to": [{"email": ALERT_TO_EMAIL}]}],
                "from":            {"email": ALERT_FROM_EMAIL},
                "subject":         subject,
                "content":         [{"type": "text/plain", "value": message}],
            },
            timeout=15,
        )
        # SendGrid returns 202 Accepted on success (no body)
        response.raise_for_status()
        log.info(f"Email sent → {ALERT_TO_EMAIL}  |  Subject: {subject}")

    except requests.exceptions.HTTPError as e:
        log.error(f"SendGrid HTTP error: {e.response.status_code} — {e.response.text}")
    except Exception as e:
        log.error(f"Email send failed: {e}")


def send_test_email(all_results, all_forecasts):
    """
    Sends a one-time test email on startup to verify SendGrid is configured
    correctly. Called when SEND_TEST_EMAIL=true is set in env vars.

    The body is identical to a normal full-cycle alert so you can confirm
    both the SendGrid connection AND the email formatting in one shot.
    If no city data was collected yet, sends a minimal "bot is alive" note.
    """
    log.info("SEND_TEST_EMAIL=true — sending test email now...")

    now = datetime.now()

    if all_results:
        # Reuse the standard formatter so the test email looks exactly like
        # a real one — no separate template to maintain.
        body = "🧪 TEST — This is a startup verification email.\n\n" + format_alert_message(all_results, all_forecasts)
    else:
        # No cycle data yet (all cities failed). Still useful to confirm delivery.
        body = (
            "🧪 TEST — Kalshi bot is alive but no market data was collected.\n"
            f"Timestamp: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "Check logs for API errors."
        )

    send_email(
        body,
        subject=f"🧪 Kalshi Bot TEST — {now.strftime('%b %d')} {now.strftime('%I:%M %p').lstrip('0')}",
    )


# ============================================================
# SECTION 8 — CSV LOGGING
# Appends every cycle's results to log.csv so you can track
# how markets and forecasts have changed over time.
# ============================================================

def log_to_csv(all_results, all_forecasts):
    """
    Appends one row per market per cycle to log.csv.
    Creates the file with a header row if it doesn't exist yet.
    Never overwrites — always appends (this is our ML training data).

    Columns (26 total):
      timestamp           — when this cycle ran (YYYY-MM-DD HH:MM:SS)
      city                — city display name
      market_type         — "HIGH" or "LOW"
      bucket_label        — human-readable bucket, e.g. "48° to 49°" or ">51°F"
      kalshi_price        — Kalshi market implied probability (0–100)
      nws_implied         — NWS-derived probability (0–100)
      gap                 — nws_implied - kalshi_price (positive = edge on YES)
      direction           — "BUY YES" or "BUY NO"
      confidence          — "HIGH" or "LOW" (based on distance to nearest boundary)
      was_settled         — True if Kalshi price was >90 or <10 (market nearly over)
      nws_grid_forecast   — raw NWS grid forecast temp for this period (°F).
                            NEVER the observed running value — always the grid number.
      observed_running    — live observed running high or low (°F) if observations
                            were available for a "today" market; empty for tomorrow
                            markets or when no obs exist yet (too early in the day).
      forecast_temp_used  — the actual temperature that drove the probability calc.
                            For today markets with observations this equals
                            observed_running; otherwise equals nws_grid_forecast.
      ecmwf_high          — Open-Meteo ECMWF IFS 0.25° model forecast temp (°F)
      gfs_high            — Open-Meteo GFS Seamless model forecast temp (°F)
      gem_high            — Open-Meteo Canadian GEM Seamless model forecast temp (°F)
      icon_high           — Open-Meteo DWD ICON Seamless model forecast temp (°F)
      weatherapi_high     — WeatherAPI.com forecast temp for this period (°F)
      consensus_high      — average of all available model temps (rounded, °F)
      model_spread        — max − min across all available models (°F); high = stay out
      std_dev_used        — Gaussian std_dev used for nws_implied (after all adjustments)
      time_decay_multiplier — multiplier applied to std_dev for today's markets as the day
                              progresses (1.0 = no decay; 0.75/0.5/0.3 = tightening).
                              Only set for today markets with live observations; always
                              1.0 for tomorrow markets and today markets without obs.
      hourly_remaining_extreme — Open-Meteo hourly model's forecasted max (HIGH) or min
                              (LOW) for the remaining hours of today in LST. Empty for
                              tomorrow markets and when hourly data is unavailable.
      hourly_adjusted     — True if the hourly model caused an additional ×0.7 std_dev
                              tightening (i.e. hourly agreed the extreme already occurred).
      ticker              — Kalshi market ticker (used by resolve.py for result lookup)
      market_date         — "today" or "tomorrow" from the signal's perspective

    SCHEMA CHANGE: added hourly_remaining_extreme and hourly_adjusted columns (now 26
    total, was 24). If log.csv exists with the old schema, delete it and restart —
    the bot will recreate it with the correct 26-column header.
    """
    FIELDNAMES = [
        "timestamp", "city", "market_type", "bucket_label",
        "kalshi_price", "nws_implied", "gap", "direction",
        "confidence", "was_settled",
        "nws_grid_forecast", "observed_running", "forecast_temp_used",
        "ecmwf_high", "gfs_high", "gem_high", "icon_high", "weatherapi_high",
        "consensus_high", "model_spread", "std_dev_used", "time_decay_multiplier",
        "hourly_remaining_extreme", "hourly_adjusted",
        "ticker", "market_date",
    ]

    now         = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_exists = os.path.isfile(LOG_FILE)
    total_rows  = 0

    # Detect stale schema: if the existing file still has the old "nws_forecast"
    # column (and not the new "nws_grid_forecast"), warn the user to delete it.
    if file_exists:
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as _check:
                first_line = _check.readline()
            if "nws_forecast" in first_line and "nws_grid_forecast" not in first_line:
                log.warning(
                    f"⚠️  SCHEMA CHANGE DETECTED in {LOG_FILE}: "
                    "the file uses the old 'nws_forecast' column. "
                    "Delete log.csv and restart the bot — it will recreate the file "
                    "with the new 21-column schema "
                    "(nws_grid_forecast / observed_running / forecast_temp_used)."
                )
        except Exception:
            pass  # non-fatal; proceed normally

    try:
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)

            # Write header only the first time the file is created
            if not file_exists:
                writer.writeheader()
                log.info(f"Created {LOG_FILE} with header row.")

            for city_key, gaps in all_results.items():
                city_name = CITIES[city_key]["name"]

                # Pull the raw forecast dicts for this city (safe if missing)
                forecasts   = all_forecasts.get(city_key, {})
                nws_fc      = forecasts.get("nws")        or {}
                ecmwf_fc    = forecasts.get("ecmwf")      or {}
                gfs_fc      = forecasts.get("gfs")        or {}
                gem_fc      = forecasts.get("gem")        or {}
                icon_fc     = forecasts.get("icon")       or {}
                wapi_fc     = forecasts.get("weatherapi") or {}

                for g in gaps:
                    # Build the lookup key that matches all forecast dicts:
                    # e.g. market_date="today", series_type="HIGH" → "today_high"
                    fc_key = f"{g['market_date']}_{g['series_type'].lower()}"

                    nws_temp    = nws_fc.get(fc_key)    # NWS grid °F
                    ecmwf_temp  = ecmwf_fc.get(fc_key)  # ECMWF IFS 0.25° °F
                    gfs_temp    = gfs_fc.get(fc_key)    # GFS Seamless °F
                    gem_temp    = gem_fc.get(fc_key)    # Canadian GEM Seamless °F
                    icon_temp   = icon_fc.get(fc_key)   # DWD ICON Seamless °F
                    wapi_temp   = wapi_fc.get(fc_key)   # WeatherAPI.com °F

                    # Consensus = average of all available model temps (including NWS)
                    available = [t for t in [nws_temp, ecmwf_temp, gfs_temp, gem_temp, icon_temp, wapi_temp] if t is not None]
                    consensus = round(sum(available) / len(available)) if available else None
                    # Spread = range across models; high spread = stay out (low confidence)
                    spread    = (max(available) - min(available)) if len(available) >= 2 else None

                    # nws_grid_forecast and observed_running come from analyze_gaps()
                    # result dict directly — they were captured there where the
                    # branching logic already knows exactly which value is which.
                    grid_fc   = g.get("nws_grid_forecast")   # raw NWS grid temp
                    obs_run   = g.get("observed_running")     # live obs (or None)
                    fc_used   = g.get("forecast_temp")        # what drove the signal

                    writer.writerow({
                        "timestamp":          now,
                        "city":               city_name,
                        "market_type":        g["series_type"],
                        "bucket_label":       g["bucket_label"],
                        "kalshi_price":       g["kalshi_prob"],
                        "nws_implied":        g["nws_prob"],
                        "gap":                g["gap"],
                        "direction":          g["edge"],
                        "confidence":         g["confidence"],
                        "was_settled":        g["was_settled"],
                        "nws_grid_forecast":  grid_fc   if grid_fc  is not None else "",
                        "observed_running":   obs_run   if obs_run  is not None else "",
                        "forecast_temp_used": fc_used   if fc_used  is not None else "",
                        "ecmwf_high":         ecmwf_temp if ecmwf_temp is not None else "",
                        "gfs_high":           gfs_temp   if gfs_temp   is not None else "",
                        "gem_high":           gem_temp   if gem_temp   is not None else "",
                        "icon_high":          icon_temp  if icon_temp  is not None else "",
                        "weatherapi_high":    wapi_temp  if wapi_temp  is not None else "",
                        "consensus_high":     consensus  if consensus  is not None else "",
                        "model_spread":       spread     if spread     is not None else "",
                        "std_dev_used":          g.get("std_dev_used", ""),
                        "time_decay_multiplier": g.get("time_decay_multiplier", 1.0),
                        "hourly_remaining_extreme": g.get("hourly_remaining_extreme", "") if g.get("hourly_remaining_extreme") is not None else "",
                        "hourly_adjusted":          g.get("hourly_adjusted", False),
                        "ticker":             g["ticker"],
                        "market_date":        g["market_date"],
                    })
                    total_rows += 1

        log.info(f"Logged {total_rows} rows to {LOG_FILE}.")

    except Exception as e:
        log.error(f"CSV logging failed: {e}")


# ============================================================
# SECTION 9a — EMAIL TIMING & CHANGE DETECTION
# Helpers that decide whether to send an email this cycle and
# which format to use (morning briefing / change alert / evening
# summary). All time comparisons use America/New_York so the
# scheduling stays correct regardless of where the bot is hosted.
# ============================================================

def _et_now():
    """Returns the current datetime in America/New_York (Eastern Time)."""
    return datetime.now(tz=ZoneInfo("America/New_York"))



def format_evening_summary(all_results, all_forecasts):
    """
    8 PM evening summary email body.
    Shows tomorrow's high-conviction markets — same filters as morning briefing.
    Observed highs section removed (noisy, not actionable at end of day).
    """
    et_now        = _et_now()
    today_date    = et_now.strftime("%b %d")
    tomorrow_date = (et_now + timedelta(days=1)).strftime("%b %d")
    time_str      = et_now.strftime("%I:%M %p").lstrip("0")
    DIVIDER       = "————————————————"

    # Tomorrow markets — same filter and sort as morning briefing
    tomorrow_markets = []
    for city_key, gaps in all_results.items():
        city_name = CITIES[city_key]["name"]
        for g in gaps:
            if g["market_date"] != "tomorrow":
                continue
            md = _get_model_data(city_key, g, all_forecasts)
            if not md["has_enough_data"]:
                continue
            if not _apply_email_filters(g, md):
                continue
            tomorrow_markets.append({**g, "city_name": city_name, "city_key": city_key, **md})

    # Tier 1 first, then by gap size descending (highest-conviction first)
    tomorrow_markets.sort(key=lambda x: (0 if x["city_key"] in TIER1_CITIES else 1, -abs(x["gap"])))

    lines = [
        f"🌙 Kalshi Evening Summary · {today_date} · {time_str}",
        "",
        f"——— TOP SIGNALS FOR TOMORROW {tomorrow_date} ———",
        "",
    ]

    if tomorrow_markets:
        for g in tomorrow_markets:
            spread_tag = "  ⚠️ HIGH SPREAD" if (g["spread"] is not None and g["spread"] >= 5) else ""
            lines.append(f"📍 {g['city_name'].upper()} — {g['series_type']} {g['bucket_label']}")
            lines.append(f"Kalshi: {g['kalshi_prob']}%")
            lines.append(f"Model: {g['nws_prob']}% → {g['edge']} (gap: {g['gap']:+d}%)")
            lines.append(g["models_line"] + spread_tag)
            lines.append(DIVIDER)
    else:
        lines.append("No high-conviction markets pass all filters for tomorrow.")
        lines.append("")

    lines.append("──────────────────────")
    lines.append("Not financial advice.")

    return "\n".join(lines)


# ============================================================
# SECTION 9 — MAIN CYCLE
# Runs every 10 minutes. Fetches all data, computes gaps, and
# decides whether to email (morning briefing, change alert, or
# evening summary). Always appends to log.csv.
# Each city is wrapped in its own try/except so one bad city
# cannot crash the whole cycle.
# ============================================================

def run_cycle():
    """
    One full cycle of the bot — called on startup and every 10 minutes.

    Order of operations per city:
      1. fetch_kalshi_markets()     — live market prices + bucket shapes
      2. fetch_nws_forecast()       — NWS grid forecast + running high
      3. analyze_gaps()             — compare NWS-implied prob vs Kalshi price

    After all cities, decides what (if anything) to email:
      • 7:00–7:14 AM ET → morning briefing (full signal list, once per day)
      • 8:00–8:14 PM ET → evening summary  (top signals + today's highs, once)
      • Otherwise       → silent (intraday alerts come from running-high check)
      • Always          → log_to_csv() and heartbeat print

    If a city fails at any step it is skipped; all other cities continue.
    """
    global _MORNING_SENT_DATE, _EVENING_SENT_DATE, _HIGH_SPREAD_FLAGGED, _SPREAD_ALERTED

    cycle_start   = time.time()
    all_results   = {}
    all_forecasts = {}   # {city_key: {"nws": {...}, "ecmwf": {...}, "gfs": {...}, "weatherapi": {...}}}
    cities_ok     = 0
    cities_failed = 0

    log.info(f"Starting full cycle — {len(CITIES)} cities to process.")

    for city_key, city_config in CITIES.items():
        try:
            # ── Step 1: Kalshi markets ───────────────────────────────────────
            kalshi_markets = fetch_kalshi_markets(city_key)
            if not kalshi_markets:
                log.warning(f"[{city_key}] 0 markets returned — skipping city.")
                cities_failed += 1
                continue

            # ── Step 2: NWS forecast + running high ─────────────────────────
            nws_forecast = fetch_nws_forecast(city_key)
            if nws_forecast is None:
                log.warning(f"[{city_key}] NWS forecast unavailable — skipping city.")
                cities_failed += 1
                continue

            # ── Step 3: Multi-model forecasts (non-blocking) ─────────────────
            # All six can fail without skipping the city — NWS drives signals.
            ecmwf_forecast      = fetch_ecmwf_forecast(city_key)
            gfs_forecast        = fetch_gfs_forecast(city_key)
            gem_forecast        = fetch_gem_forecast(city_key)
            icon_forecast       = fetch_icon_forecast(city_key)
            weatherapi_forecast = fetch_weatherapi_forecast(city_key)
            hourly_forecast     = fetch_hourly_forecast(city_key)

            # Log whether each model returned data or None
            def _fc_status(fc, label):
                if fc:
                    return f"{label}: ok (today_high={fc.get('today_high')})"
                return f"{label}: None"
            log.info(
                f"[{city_key}] Forecast status — NWS: ok, "
                + ", ".join([
                    _fc_status(ecmwf_forecast,      "ECMWF"),
                    _fc_status(gfs_forecast,         "GFS"),
                    _fc_status(gem_forecast,         "GEM"),
                    _fc_status(icon_forecast,        "ICON"),
                    "WAPI: ok" if weatherapi_forecast else "WAPI: None",
                    f"Hourly: ok ({len(hourly_forecast)} hrs)" if hourly_forecast else "Hourly: None",
                ])
            )

            # Bundle forecasts so analyze_gaps() can compute dynamic std_dev
            city_forecasts = {
                "nws":        nws_forecast,
                "ecmwf":      ecmwf_forecast,
                "gfs":        gfs_forecast,
                "gem":        gem_forecast,
                "icon":       icon_forecast,
                "weatherapi": weatherapi_forecast,
                "hourly":     hourly_forecast,
            }

            # ── Step 4: Gap analysis ─────────────────────────────────────────
            gaps = analyze_gaps(city_key, kalshi_markets, nws_forecast, city_forecasts)
            all_results[city_key]   = gaps
            all_forecasts[city_key] = city_forecasts   # reuse — no redundant copy
            cities_ok += 1

        except Exception as e:
            log.error(f"[{city_key}] Unexpected error — city skipped. ({e})")
            cities_failed += 1

    # ── Spread tracking + email decision ────────────────────────────────────
    if all_results:
        et      = _et_now()
        et_date = et.date()
        et_min  = et.hour * 60 + et.minute   # minutes since midnight ET

        # Update _HIGH_SPREAD_FLAGGED and find any markets whose spread just
        # converged below 1°F (previously had spread ≥ 3°F = "stay out" signal).
        converged_markets = []
        for city_key, gaps in all_results.items():
            for g in gaps:
                md = _get_model_data(city_key, g, all_forecasts)
                if not md["has_enough_data"] or md["spread"] is None:
                    continue
                ticker = g["ticker"]
                if md["spread"] >= 3:
                    _HIGH_SPREAD_FLAGGED.add(ticker)
                elif md["spread"] < 1 and ticker in _HIGH_SPREAD_FLAGGED and ticker not in _SPREAD_ALERTED:
                    converged_markets.append({
                        **g, "city_name": CITIES[city_key]["name"], "city_key": city_key, **md,
                    })
                    _SPREAD_ALERTED.add(ticker)

        if 420 <= et_min < 435 and _MORNING_SENT_DATE != et_date:
            # ── 7:00–7:14 AM ET: morning briefing ──────────────────────────
            message = format_alert_message(all_results, all_forecasts)
            send_email(
                message,
                subject=f"☀️ Kalshi Morning Briefing — {et.strftime('%b %d')}",
            )
            _MORNING_SENT_DATE = et_date
            log.info("Morning briefing sent.")

        elif 1200 <= et_min < 1215 and _EVENING_SENT_DATE != et_date:
            # ── 8:00–8:14 PM ET: evening summary ───────────────────────────
            message = format_evening_summary(all_results, all_forecasts)
            send_email(
                message,
                subject=f"🌙 Kalshi Evening Summary — {et.strftime('%b %d')}",
            )
            _EVENING_SENT_DATE = et_date
            log.info("Evening summary sent.")

        elif converged_markets:
            # ── Intraday: spread convergence alert ──────────────────────────
            # Fires when a previously high-spread (≥3°F) market's models
            # converge to <1°F spread — signalling a newly reliable setup.
            now     = datetime.now()
            DIVIDER = "————————————————"
            body_lines = [
                f"⚡ SPREAD ALERT · {now.strftime('%b %d')} {now.strftime('%I:%M %p').lstrip('0')}",
                "",
                "Model spread has converged on:",
                "",
            ]
            for g in converged_markets:
                body_lines.append(f"📍 {g['city_name'].upper()} — {g['series_type']} {g['bucket_label']}")
                body_lines.append(f"Kalshi: {g['kalshi_prob']}%")
                body_lines.append(g["models_line"])
                body_lines.append(DIVIDER)
            body_lines.extend(["", "Check Kalshi for these markets.", "Not financial advice."])
            send_email(
                "\n".join(body_lines),
                subject=f"⚡ Spread Convergence Alert — {now.strftime('%b %d')}",
            )
            log.info(f"Spread convergence alert sent ({len(converged_markets)} market(s)).")

        else:
            # ── Daytime: silent cycle ────────────────────────────────────────
            log.info("Daytime cycle complete — no email this cycle.")

        log_to_csv(all_results, all_forecasts)

    else:
        log.warning("No city data collected this cycle — email and CSV skipped.")

    # ── Heartbeat ────────────────────────────────────────────────────────────
    total_signals = sum(
        1 for gaps in all_results.values()
        for g in gaps
        if abs(g["gap"]) > 15 and not g["was_settled"] and g["market_date"] == "tomorrow"
    )

    elapsed = time.time() - cycle_start
    ts      = datetime.now().strftime("%H:%M:%S")
    print(
        f"[{ts}] Cycle complete — "
        f"{cities_ok}/{len(CITIES)} cities, "
        f"{total_signals} signals  "
        f"({elapsed:.1f}s)",
        flush=True,
    )
    if cities_failed:
        log.warning(f"{cities_failed} city/cities failed this cycle.")

    return all_results, all_forecasts


def reset_daily_state():
    """
    Clears spread-tracking state at midnight UTC so each day starts fresh.
    Called by the scheduler at 00:00 UTC.
    """
    _HIGH_SPREAD_FLAGGED.clear()
    _SPREAD_ALERTED.clear()
    log.info("Daily spread-tracking state reset at midnight UTC.")


# ============================================================
# SECTION 10 — HTTP EXPORT SERVER
#
# Serves log.csv as a file download at GET /  (and GET /export).
# Railway exposes the PORT env var; locally we default to 8080.
#
# Usage from browser or curl:
#   curl http://localhost:8080/export -o log.csv
# ============================================================

LOG_PATH = os.getenv("LOG_PATH", "log.csv")
RESOLVE_LOG = os.getenv("RESOLVE_LOG_PATH", "resolve_log.csv")


class ExportHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler — serves log.csv and resolve_log.csv as downloads."""

    def do_GET(self):
        # Route the request to the correct file based on the path
        if self.path in ("/", "/export"):
            file_path = LOG_PATH
            filename = "log.csv"
        elif self.path == "/resolve":
            file_path = RESOLVE_LOG
            filename = "resolve_log.csv"
        else:
            self.send_response(404)
            self.end_headers()
            return

        if not os.path.isfile(file_path):
            # No data yet — return an empty 200 so callers don't crash
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.end_headers()
            return

        try:
            with open(file_path, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as exc:
            log.error(f"ExportHandler error: {exc}")
            self.send_response(500)
            self.end_headers()

    def log_message(self, fmt, *args):
        # Silence the default per-request stdout noise; our logger handles it
        log.debug("HTTP %s", fmt % args)


def run_http_server():
    """Start the export HTTP server on PORT (default 8080). Blocks forever."""
    port = int(os.getenv("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), ExportHandler)
    log.info(f"Export server listening on port {port}  (GET / or /export → log.csv)")
    server.serve_forever()


# ============================================================
# SECTION 11 — SCHEDULER + ENTRY POINT
#
# Usage:
#   python bot.py          — production mode: runs forever
#   python bot.py --test   — test mode: runs one cycle then exits
#                            (useful for verifying the heartbeat without
#                            starting a long-running process)
#
# Threading model:
#   - Scheduler loop runs in a background daemon thread so it never
#     blocks the HTTP server.
#   - HTTP server runs on the main thread (required by Railway: the
#     process must bind the PORT it's given or it won't route traffic).
#   - Because the scheduler thread is a daemon, it exits automatically
#     when the main thread (HTTP server) stops.
# ============================================================

if __name__ == "__main__":
    import sys

    test_mode = "--test" in sys.argv

    # ── Startup banner ──────────────────────────────────────────────────────
    log.info("=" * 58)
    log.info("  Kalshi Climate Bot")
    log.info(f"  {len(CITIES)} cities monitored")
    log.info(f"  Market cycle every {RUN_EVERY_MINUTES} min")
    log.info( "  Email schedule (America/New_York):")
    log.info( "    ☀️  7:00 AM — morning briefing (all markets, model cards)")
    log.info( "    ⚡  Intraday — spread convergence alert (when ≥3° spread drops to <1°)")
    log.info( "    🌙  8:00 PM — evening summary (tomorrow markets + today's highs)")
    log.info( "  Daily state reset at midnight UTC")
    log.info( "  Export server: GET /export → log.csv")
    if test_mode:
        log.info("  Mode: --test (will exit after first cycle)")
    if SEND_TEST_EMAIL:
        log.info("  SEND_TEST_EMAIL=true — test email will fire after first cycle")
    log.info("=" * 58)

    # ── Run one full cycle immediately on startup ────────────────────────────
    log.info("Running initial cycle on startup...")
    startup_results, startup_forecasts = run_cycle()

    # ── Optional test email ──────────────────────────────────────────────────
    if SEND_TEST_EMAIL:
        send_test_email(startup_results, startup_forecasts)

    if test_mode:
        log.info("--test complete. Exiting.")
        sys.exit(0)

    # ── Schedule recurring jobs ──────────────────────────────────────────────
    schedule.every(RUN_EVERY_MINUTES).minutes.do(run_cycle)
    schedule.every().day.at("00:00").do(reset_daily_state)

    # ── Scheduler runs in a background daemon thread ─────────────────────────
    def _run_scheduler():
        log.info(
            f"Scheduler active. "
            f"Market cycle every {RUN_EVERY_MINUTES} min."
        )
        while True:
            schedule.run_pending()
            time.sleep(30)

    scheduler_thread = threading.Thread(target=_run_scheduler, daemon=True)
    scheduler_thread.start()

    # ── HTTP export server on main thread (blocks until Ctrl+C / SIGTERM) ───
    try:
        run_http_server()
    except KeyboardInterrupt:
        log.info("\nKalshi Climate Bot stopped by user. Goodbye.")
