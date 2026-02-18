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
import smtplib
import logging
import schedule
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import requests

# ============================================================
# SECTION 1 — LOAD CREDENTIALS
# Reads all secrets from the .env file so nothing is hardcoded.
# ============================================================

load_dotenv()

KALSHI_API_KEY      = os.getenv("KALSHI_API_KEY")
GMAIL_ADDRESS       = os.getenv("GMAIL_ADDRESS")
GMAIL_PASSWORD      = os.getenv("GMAIL_APP_PASSWORD")
ALERT_TO_EMAIL      = os.getenv("ALERT_TO_EMAIL")
# Minimum gap (%) required to fire a change-alert email mid-day.
# Override via WETHR_ALERT_THRESHOLD in .env without touching code.
ALERT_GAP_THRESHOLD = int(os.getenv("WETHR_ALERT_THRESHOLD", "25"))

# ============================================================
# SECTION 2 — CONFIGURATION
# All the cities and market settings in one easy-to-edit place.
# ============================================================

# The Kalshi REST API base URL (confirmed — all markets live here, no auth needed for reads)
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

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
LOG_FILE = "log.csv"

# Cache for NWS grid info (office, gridX, gridY) so we only look it up once
# per city at startup rather than on every 10-minute cycle.
# Format: {"NYC": {"office": "OKX", "grid_x": 33, "grid_y": 37, "forecast_url": "..."}}
NWS_GRID_CACHE = {}

# Tracks the highest running temperature seen for each city during the current day.
# Used by check_running_high_alerts() to detect when a new peak is set.
# Keyed by city_key; value is {"max_observed": int, "date_utc": "YYYY-MM-DD"}.
# Resets at midnight UTC via reset_running_high_cache().
RUNNING_HIGH_LAST = {}

# Previous cycle's gap results — used by should_send_change_alert() to detect
# meaningful shifts between cycles. Structure: {city_key: [gap_dict, ...]}
PREV_SIGNALS = {}

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

        # Build today's and tomorrow's date strings to look up in our forecasts dict
        today    = datetime.now().strftime("%Y-%m-%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        result = {
            "today_high":        forecasts.get(today,    {}).get("high"),
            "today_low":         forecasts.get(today,    {}).get("low"),
            "tomorrow_high":     forecasts.get(tomorrow, {}).get("high"),
            "tomorrow_low":      forecasts.get(tomorrow, {}).get("low"),
            "today_running_high": None,   # filled below from live observations
        }

        # Fetch actual observed running high for today from the station.
        # This is more reliable than the grid forecast once the day is in
        # progress. Returns None if no observations exist yet (e.g. very
        # early morning) or if the API call fails — safe to ignore.
        result["today_running_high"] = get_current_running_high(city_key)

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


def calculate_nws_probability(forecast_temp, bucket_type, floor, cap, station_type):
    """
    Estimates the probability that a Kalshi market resolves YES, given the
    NWS forecast temperature for that day.

    Works for all three bucket types:

      FLOOR  (">floor"): YES if actual > floor
      CAP    ("<cap"):   YES if actual < cap
      RANGE  ("floor–cap"): YES if floor <= actual <= cap

    Probability model (same for all types, based on distance to nearest boundary):

      dist to nearest boundary <= 1°    → 55%  (boundary zone, high uncertainty)
      dist > 1°, forecast on YES side   → 80% (hourly) or 65% (5-min)
      dist > 1°, forecast on NO side, dist <= 2° → 35%
      dist > 2°, forecast on NO side    → 10%

    For RANGE markets, "nearest boundary" is whichever edge of the range the
    forecast is closest to (whether inside or outside the range).

    Returns an integer 0–100.
    """
    if bucket_type == "FLOOR":
        # YES condition: actual temp > floor
        dist        = abs(forecast_temp - floor)
        on_yes_side = forecast_temp > floor

    elif bucket_type == "CAP":
        # YES condition: actual temp < cap
        dist        = abs(forecast_temp - cap)
        on_yes_side = forecast_temp < cap

    elif bucket_type == "RANGE":
        # YES condition: floor <= actual temp <= cap
        inside = (floor <= forecast_temp <= cap)

        if inside:
            # Distance to whichever edge of the range the forecast is nearest to
            dist        = min(forecast_temp - floor, cap - forecast_temp)
            on_yes_side = True
        else:
            # Forecast is outside the range — find distance to the nearer boundary
            if forecast_temp < floor:
                dist = floor - forecast_temp   # how far below the floor we are
            else:
                dist = forecast_temp - cap     # how far above the cap we are
            on_yes_side = False

    else:
        log.warning(f"calculate_nws_probability: unknown bucket_type '{bucket_type}'")
        return 50  # neutral fallback

    # Apply the step-function probability model.
    # Strict < 1 (not <= 1) so that a forecast exactly 1°F outside a boundary
    # correctly returns 35% (lean NO) rather than 55% (boundary zone).
    # dist = 0 still hits this branch and returns 55% (literally on the edge).
    if dist < 1:
        return 55   # right at a boundary — too close to call

    if on_yes_side:
        # Forecast is clearly on the YES side (>1° past threshold or inside range)
        return 80 if station_type == "hourly" else 65

    if dist <= 2:
        return 35   # forecast is on the NO side but only 1–2° away

    return 10       # forecast is clearly on the NO side


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

def analyze_gaps(city_key, kalshi_markets, nws_forecast):
    """
    For each Kalshi market, compares the NWS-implied probability against
    the Kalshi market price and calculates the gap.

    Handles FLOOR, CAP, and RANGE bucket types dynamically — never assumes
    a fixed bucket shape. Works correctly even if Kalshi changes all buckets
    between runs.

    A positive gap means NWS thinks YES is MORE likely than Kalshi does → edge on YES.
    A negative gap means NWS thinks NO is more likely → edge on NO.

    Returns a list sorted by absolute gap size (largest gaps first).
    """
    city         = CITIES[city_key]
    station_type = city["station_type"]

    today    = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    results = []

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
        # For today's HIGH markets we prefer the actual observed running high
        # (from Section 5b) over the grid forecast, because once observations
        # exist they are more accurate than a forecast issued hours earlier.
        # For all other combinations (today LOW, tomorrow HIGH/LOW) we use the
        # NWS grid forecast as usual.

        if date_label == "today" and market["series_type"] == "HIGH":
            running = nws_forecast.get("today_running_high")
            if running is not None:
                # Use the conservative observed max (floor(C×9/5+32)).
                # probable_max is stored in the result for display purposes.
                forecast_temp     = running["max_observed"]
                probable_max_temp = running["probable_max"]
            else:
                # Fall back to the grid forecast if observations aren't available
                # (e.g. very early morning before any readings today).
                forecast_temp     = nws_forecast["today_high"]
                probable_max_temp = None
        elif date_label == "today" and market["series_type"] == "LOW":
            forecast_temp     = nws_forecast["today_low"]
            probable_max_temp = None
        elif date_label == "tomorrow" and market["series_type"] == "HIGH":
            forecast_temp     = nws_forecast["tomorrow_high"]
            probable_max_temp = None
        else:
            forecast_temp     = nws_forecast["tomorrow_low"]
            probable_max_temp = None

        if forecast_temp is None:
            # No temperature available for this period yet — skip
            continue

        # --- Step 3: Calculate NWS-implied probability for this bucket ---
        nws_prob = calculate_nws_probability(
            forecast_temp,
            market["bucket_type"],
            market["floor"],
            market["cap"],
            station_type,
        )

        # --- Step 4: Compute the gap ---
        gap  = nws_prob - market["kalshi_prob"]
        edge = "BUY YES" if gap > 0 else "BUY NO"

        if abs(gap) < MIN_GAP_TO_SHOW:
            continue

        # Confidence: HIGH when forecast is clearly away from a boundary (nws_prob 65/80/10).
        # LOW when forecast is in the boundary zone (nws_prob 55) or only 1-2° past it (35).
        confidence = "LOW" if nws_prob in (55, 35) else "HIGH"

        # was_settled: Kalshi has already priced this very strongly — market is nearly over.
        # These go into the SETTLED section of the email rather than ACTIONABLE.
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
            "kalshi_prob":   market["kalshi_prob"],
            "nws_prob":      nws_prob,
            "gap":           gap,
            "edge":          edge,
            "confidence":    confidence,
            "was_settled":   was_settled,
        })

    results.sort(key=lambda x: abs(x["gap"]), reverse=True)
    return results


# ============================================================
# SECTION 7 — ALERTS
# Formats the results into a clean, readable message and sends
# it via Twilio SMS and/or Gmail email.
# ============================================================

def format_alert_message(all_results):
    """
    Formats all gap analysis results into a mobile-first plain-text email.

    all_results: dict  {city_key: [gap_result, ...]}  from run_cycle()

    Each market renders as a compact 3-line card:
      📍 CITY — TYPE bucket_label
      Kalshi X% → NWS Y% | Gap: +/-Z%
      ✅/🔴 BUY YES/NO | HIGH/LOW CONF  [⚠️ LOW LIQUIDITY]

    Filtering rules:
      - HIGH CONF markets always shown (regardless of gap size)
      - LOW CONF markets only shown if |gap| >= 10% (filter out noise)
      - SETTLED markets collapsed to one line each (no gap details)
    """
    now           = datetime.now()
    today_date    = now.strftime("%b %d")
    tomorrow_date = (now + timedelta(days=1)).strftime("%b %d")
    time_str      = now.strftime("%I:%M %p").lstrip("0")  # "9:00 PM" not "09:00 PM"

    # Flatten all results, attaching city key and display name for rendering
    all_gaps = []
    for city_key, gaps in all_results.items():
        city_name = CITIES[city_key]["name"]
        for g in gaps:
            all_gaps.append({**g, "city_name": city_name, "city_key": city_key})

    # Split into rendering buckets
    tomorrow_markets = [g for g in all_gaps if g["market_date"] == "tomorrow"]
    today_active     = [g for g in all_gaps if g["market_date"] == "today" and not g["was_settled"]]
    today_settled    = [g for g in all_gaps if g["market_date"] == "today" and     g["was_settled"]]

    # Apply filter: always show HIGH CONF, only show LOW CONF if |gap| >= 10
    def should_show(g):
        return g["confidence"] == "HIGH" or abs(g["gap"]) >= 10

    tomorrow_shown = sorted(
        [g for g in tomorrow_markets if should_show(g)],
        key=lambda x: abs(x["gap"]), reverse=True,
    )
    today_shown = sorted(
        [g for g in today_active if should_show(g)],
        key=lambda x: abs(x["gap"]), reverse=True,
    )

    # Summary stats
    actionable = [g for g in tomorrow_shown + today_shown if abs(g["gap"]) > 15]
    top        = tomorrow_shown[0] if tomorrow_shown else (today_shown[0] if today_shown else None)
    if top:
        gap_sign = "+" if top["gap"] > 0 else ""
        top_str  = f"{top['city_key']} {top['series_type']} {gap_sign}{top['gap']}%"
    else:
        top_str = "—"

    # Card separator — short enough to read on narrow screens
    DIVIDER = "————————————————"

    lines = []

    # ── HEADER (2 lines) ─────────────────────────────────
    lines.append(f"🤖 Kalshi Bot · {today_date} · {time_str}")
    lines.append(f"{len(all_gaps)} markets · {len(actionable)} signals · Top: {top_str}")
    lines.append("")

    # ── TOMORROW ─────────────────────────────────────────
    lines.append(f"——— TOMORROW {tomorrow_date} ———")
    lines.append("")

    if tomorrow_shown:
        for g in tomorrow_shown:
            gap_sign  = "+" if g["gap"] > 0 else ""
            edge_icon = "✅" if g["edge"] == "BUY YES" else "🔴"
            liq_tag   = " ⚠️ LOW LIQUIDITY" if g["kalshi_prob"] == 1 else ""
            lines.append(f"📍 {g['city_name'].upper()} — {g['series_type']} {g['bucket_label']}")
            lines.append(f"Kalshi {g['kalshi_prob']}% → NWS {g['nws_prob']}% | Gap: {gap_sign}{g['gap']}%")
            lines.append(f"{edge_icon} {g['edge']} | {g['confidence']} CONF{liq_tag}")
            lines.append(DIVIDER)
    else:
        lines.append("No signals for tomorrow.")
        lines.append("")

    # ── TODAY ACTIVE (if any passed the filter) ───────────
    if today_shown:
        lines.append(f"——— TODAY {today_date} ———")
        lines.append("")
        for g in today_shown:
            gap_sign  = "+" if g["gap"] > 0 else ""
            edge_icon = "✅" if g["edge"] == "BUY YES" else "🔴"
            liq_tag   = " ⚠️ LOW LIQUIDITY" if g["kalshi_prob"] == 1 else ""
            lines.append(f"📍 {g['city_name'].upper()} — {g['series_type']} {g['bucket_label']}")
            lines.append(f"Kalshi {g['kalshi_prob']}% → NWS {g['nws_prob']}% | Gap: {gap_sign}{g['gap']}%")
            lines.append(f"{edge_icon} {g['edge']} | {g['confidence']} CONF{liq_tag}")
            lines.append(DIVIDER)

    # ── TODAY SETTLED (collapsed — no details needed) ─────
    if today_settled:
        lines.append(f"——— TODAY (SETTLED) ———")
        lines.append("")
        for g in today_settled:
            lines.append(f"· {g['city_name']} {g['series_type']} {g['bucket_label']} — {g['kalshi_prob']}%")
        lines.append("")

    # ── FOOTER ───────────────────────────────────────────
    lines.append("──────────────────────")
    lines.append("Daily high may exceed highest hourly NWS reading.")
    lines.append("Near-boundary = extra uncertainty. Not financial advice.")

    return "\n".join(lines)


def send_email(message, subject=None):
    """
    Sends the formatted alert as a plain-text email via Gmail SMTP.
    Uses App Password auth (set GMAIL_APP_PASSWORD in .env).
    Logs any error and continues — email failure never crashes the bot.

    subject — optional custom subject line. If omitted, falls back to the
              generic timestamped default (used by check_running_high_alerts).
    """
    if not all([GMAIL_ADDRESS, GMAIL_PASSWORD, ALERT_TO_EMAIL]):
        log.warning("Gmail credentials missing in .env — skipping email.")
        return

    if subject is None:
        now     = datetime.now()
        subject = f"Kalshi Climate Bot — {now.strftime('%b %d')} {now.strftime('%I:%M %p')}"

    try:
        msg            = MIMEText(message, "plain")
        msg["Subject"] = subject
        msg["From"]    = GMAIL_ADDRESS
        msg["To"]      = ALERT_TO_EMAIL

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.ehlo()
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_PASSWORD)
            server.sendmail(GMAIL_ADDRESS, ALERT_TO_EMAIL, msg.as_string())

        log.info(f"Email sent → {ALERT_TO_EMAIL}  |  Subject: {subject}")

    except smtplib.SMTPAuthenticationError:
        log.error("Gmail auth failed — check GMAIL_APP_PASSWORD in .env.")
    except Exception as e:
        log.error(f"Email send failed: {e}")


# ============================================================
# SECTION 8 — CSV LOGGING
# Appends every cycle's results to log.csv so you can track
# how markets and forecasts have changed over time.
# ============================================================

def log_to_csv(all_results):
    """
    Appends one row per market per cycle to log.csv.
    Creates the file with a header row if it doesn't exist yet.
    Never overwrites — always appends (this is our ML training data).

    Columns:
      timestamp    — when this cycle ran (YYYY-MM-DD HH:MM:SS)
      city         — city display name
      market_type  — "HIGH" or "LOW"
      bucket_label — human-readable bucket, e.g. "48° to 49°" or ">51°F"
      kalshi_price — Kalshi market implied probability (0–100)
      nws_implied  — NWS-derived probability (0–100)
      gap          — nws_implied - kalshi_price (positive = edge on YES)
      direction    — "BUY YES" or "BUY NO"
      confidence   — "HIGH" or "LOW" (based on distance to nearest boundary)
      was_settled  — True if Kalshi price was >90 or <10 (market nearly over)
    """
    FIELDNAMES = [
        "timestamp", "city", "market_type", "bucket_label",
        "kalshi_price", "nws_implied", "gap", "direction",
        "confidence", "was_settled",
    ]

    now        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_exists = os.path.isfile(LOG_FILE)
    total_rows  = 0

    try:
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)

            # Write header only the first time the file is created
            if not file_exists:
                writer.writeheader()
                log.info(f"Created {LOG_FILE} with header row.")

            for city_key, gaps in all_results.items():
                city_name = CITIES[city_key]["name"]
                for g in gaps:
                    writer.writerow({
                        "timestamp":    now,
                        "city":         city_name,
                        "market_type":  g["series_type"],
                        "bucket_label": g["bucket_label"],
                        "kalshi_price": g["kalshi_prob"],
                        "nws_implied":  g["nws_prob"],
                        "gap":          g["gap"],
                        "direction":    g["edge"],
                        "confidence":   g["confidence"],
                        "was_settled":  g["was_settled"],
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


def should_send_change_alert(all_results):
    """
    Compares current cycle's signals against the previous cycle (PREV_SIGNALS).

    Returns (True, [reason_strings]) if something changed significantly enough
    to warrant an email, or (False, []) if the cycle should run silently.

    Triggers when, for any TOMORROW market that is NOT settled:
      - A new ticker appeared with |gap| >= ALERT_GAP_THRESHOLD
      - An existing ticker's gap shifted by >= 10 points AND the new |gap|
        still meets ALERT_GAP_THRESHOLD (filters out noise near zero)
    """
    reasons = []

    for city_key, gaps in all_results.items():
        prev_by_ticker = {g["ticker"]: g for g in PREV_SIGNALS.get(city_key, [])}

        for g in gaps:
            if g["was_settled"] or g["market_date"] != "tomorrow":
                continue

            abs_gap = abs(g["gap"])
            ticker  = g["ticker"]
            sign    = "+" if g["gap"] > 0 else ""

            if ticker not in prev_by_ticker:
                # Brand-new signal this cycle
                if abs_gap >= ALERT_GAP_THRESHOLD:
                    reasons.append(
                        f"NEW signal: {CITIES[city_key]['name']} {g['series_type']} "
                        f"{g['bucket_label']} — gap {sign}{g['gap']}% ({g['edge']})"
                    )
            else:
                # Existing signal — check for a large gap shift
                prev_gap = prev_by_ticker[ticker]["gap"]
                shift    = abs(g["gap"] - prev_gap)
                if shift >= 10 and abs_gap >= ALERT_GAP_THRESHOLD:
                    prev_sign = "+" if prev_gap > 0 else ""
                    reasons.append(
                        f"GAP SHIFT: {CITIES[city_key]['name']} {g['series_type']} "
                        f"{g['bucket_label']} — {prev_sign}{prev_gap}% → {sign}{g['gap']}%"
                    )

    return bool(reasons), reasons


def format_change_alert(reasons, all_results):
    """
    Compact change-alert email body.
    Lists what triggered the alert, then shows top tomorrow signals.
    """
    now        = _et_now()
    time_str   = now.strftime("%I:%M %p").lstrip("0")
    today_date = now.strftime("%b %d")
    DIVIDER    = "————————————————"

    lines = [
        f"⚡ Kalshi Alert · {today_date} · {time_str}",
        "",
    ]

    for r in reasons:
        lines.append(f"• {r}")

    lines.append("")
    lines.append("——— TOP TOMORROW SIGNALS ———")
    lines.append("")

    # Collect and sort all tomorrow non-settled signals
    all_tmrw = []
    for city_key, gaps in all_results.items():
        for g in gaps:
            if g["market_date"] == "tomorrow" and not g["was_settled"]:
                all_tmrw.append({**g, "city_name": CITIES[city_key]["name"], "city_key": city_key})

    all_tmrw.sort(key=lambda x: abs(x["gap"]), reverse=True)

    shown = 0
    for g in all_tmrw:
        if abs(g["gap"]) < 10 or shown >= 5:
            break
        gap_sign  = "+" if g["gap"] > 0 else ""
        edge_icon = "✅" if g["edge"] == "BUY YES" else "🔴"
        lines.append(f"📍 {g['city_name'].upper()} — {g['series_type']} {g['bucket_label']}")
        lines.append(f"Kalshi {g['kalshi_prob']}% → NWS {g['nws_prob']}% | Gap: {gap_sign}{g['gap']}%")
        lines.append(f"{edge_icon} {g['edge']} | {g['confidence']} CONF")
        lines.append(DIVIDER)
        shown += 1

    if shown == 0:
        lines.append("No signals above 10% gap currently.")
        lines.append("")

    lines.append("──────────────────────")
    lines.append("Not financial advice.")

    return "\n".join(lines)


def format_evening_summary(all_results):
    """
    8 PM evening summary email body.
    Shows top tomorrow signals (market preview) and today's observed highs.
    """
    now           = _et_now()
    today_date    = now.strftime("%b %d")
    tomorrow_date = (now + timedelta(days=1)).strftime("%b %d")
    time_str      = now.strftime("%I:%M %p").lstrip("0")
    DIVIDER       = "————————————————"

    lines = [
        f"🌙 Kalshi Evening Summary · {today_date} · {time_str}",
        "",
        f"——— TOP SIGNALS FOR TOMORROW {tomorrow_date} ———",
        "",
    ]

    # Tomorrow signals — same filter as morning briefing
    all_tmrw = []
    for city_key, gaps in all_results.items():
        for g in gaps:
            if g["market_date"] == "tomorrow" and not g["was_settled"]:
                all_tmrw.append({**g, "city_name": CITIES[city_key]["name"], "city_key": city_key})

    all_tmrw.sort(key=lambda x: abs(x["gap"]), reverse=True)
    shown_tmrw = [g for g in all_tmrw if g["confidence"] == "HIGH" or abs(g["gap"]) >= 10]

    if shown_tmrw:
        for g in shown_tmrw[:10]:
            gap_sign  = "+" if g["gap"] > 0 else ""
            edge_icon = "✅" if g["edge"] == "BUY YES" else "🔴"
            lines.append(f"📍 {g['city_name'].upper()} — {g['series_type']} {g['bucket_label']}")
            lines.append(f"Kalshi {g['kalshi_prob']}% → NWS {g['nws_prob']}% | Gap: {gap_sign}{g['gap']}%")
            lines.append(f"{edge_icon} {g['edge']} | {g['confidence']} CONF")
            lines.append(DIVIDER)
    else:
        lines.append("No significant signals for tomorrow.")
        lines.append("")

    # Today's observed running highs (one line per city that has data)
    lines.append(f"——— TODAY'S OBSERVED HIGHS ———")
    lines.append("")

    for city_key, gaps in all_results.items():
        city_name = CITIES[city_key]["name"]
        for g in gaps:
            if g["market_date"] != "today" or g["series_type"] != "HIGH":
                continue
            ft  = g.get("forecast_temp")
            pm  = g.get("probable_max")
            if ft is None:
                continue
            if pm and pm != ft:
                lines.append(f"· {city_name}: {ft}–{pm}°F")
            else:
                lines.append(f"· {city_name}: {ft}°F")
            break  # one HIGH entry per city is enough

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
      • Otherwise       → change alert only if a new/shifted signal appeared
      • Always          → log_to_csv() and heartbeat print

    If a city fails at any step it is skipped; all other cities continue.
    """
    global PREV_SIGNALS, _MORNING_SENT_DATE, _EVENING_SENT_DATE

    cycle_start   = time.time()
    all_results   = {}
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

            # ── Step 3: Gap analysis ─────────────────────────────────────────
            gaps = analyze_gaps(city_key, kalshi_markets, nws_forecast)
            all_results[city_key] = gaps
            cities_ok += 1

        except Exception as e:
            log.error(f"[{city_key}] Unexpected error — city skipped. ({e})")
            cities_failed += 1

    # ── Email decision ───────────────────────────────────────────────────────
    if all_results:
        et         = _et_now()
        et_date    = et.date()
        et_min     = et.hour * 60 + et.minute   # minutes since midnight ET

        if 420 <= et_min < 435 and _MORNING_SENT_DATE != et_date:
            # ── 7:00–7:14 AM ET: morning briefing ──────────────────────────
            message = format_alert_message(all_results)
            send_email(
                message,
                subject=f"☀️ Kalshi Morning Briefing — {et.strftime('%b %d')}",
            )
            _MORNING_SENT_DATE = et_date
            log.info("Morning briefing sent.")

        elif 1200 <= et_min < 1215 and _EVENING_SENT_DATE != et_date:
            # ── 8:00–8:14 PM ET: evening summary ───────────────────────────
            message = format_evening_summary(all_results)
            send_email(
                message,
                subject=f"🌙 Kalshi Evening Summary — {et.strftime('%b %d')}",
            )
            _EVENING_SENT_DATE = et_date
            log.info("Evening summary sent.")

        else:
            # ── Daytime: only email if something meaningful changed ──────────
            changed, reasons = should_send_change_alert(all_results)
            if changed:
                message = format_change_alert(reasons, all_results)
                send_email(
                    message,
                    subject=(
                        f"⚡ Kalshi Alert — "
                        f"{et.strftime('%b %d')} {et.strftime('%I:%M %p').lstrip('0')}"
                    ),
                )
                log.info(f"Change alert sent ({len(reasons)} trigger(s)).")
            else:
                log.info("No significant changes — cycle silent (no email).")

        # Always log to CSV and update PREV_SIGNALS for next cycle's comparison
        log_to_csv(all_results)
        PREV_SIGNALS = {k: list(v) for k, v in all_results.items()}

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


def check_running_high_alerts():
    """
    Lightweight check that runs every 2 minutes.

    For each city, fetches the current running high (observed since midnight
    LST) and compares it to the last recorded peak in RUNNING_HIGH_LAST.
    If a city sets a new peak, it's collected and a brief alert email is sent.

    First observation of the day (no prior entry) is recorded silently — we
    don't alert on first contact, only on subsequent new peaks.
    """
    new_peaks  = []   # list of (city_key, new_high, old_high)
    today_utc  = datetime.utcnow().strftime("%Y-%m-%d")

    for city_key in CITIES:
        try:
            result = get_current_running_high(city_key)
            if result is None:
                continue

            new_max = result["max_observed"]
            last    = RUNNING_HIGH_LAST.get(city_key, {})

            # If this is the first reading of a new UTC day, record it silently
            # (the prior day's peak is no longer meaningful)
            if last.get("date_utc") != today_utc:
                RUNNING_HIGH_LAST[city_key] = {
                    "max_observed": new_max,
                    "date_utc":     today_utc,
                }
                continue

            old_max = last["max_observed"]

            if new_max > old_max:
                # New peak! Record it and queue an alert
                new_peaks.append((city_key, new_max, old_max))
                RUNNING_HIGH_LAST[city_key]["max_observed"] = new_max
                log.info(f"[{city_key}] New running high: {old_max}°F → {new_max}°F")

        except Exception as e:
            log.error(f"[{city_key}] Running high alert check failed: {e}")

    if new_peaks:
        lines = ["🌡️ NEW HIGH ALERT\n"]
        for city_key, new_high, old_high in new_peaks:
            city_name = CITIES[city_key]["name"]
            lines.append(f"📍 {city_name}: {old_high}°F → {new_high}°F (new running high)")
        lines.append("\nCheck Kalshi for any boundary markets that may have shifted.")
        send_email("\n".join(lines))
        log.info(f"New high alert sent for: {[p[0] for p in new_peaks]}")
    else:
        log.debug("Running high check: no new peaks detected.")


def reset_running_high_cache():
    """
    Clears RUNNING_HIGH_LAST so the 2-minute check starts fresh each day.
    Called at midnight UTC by the scheduler.

    This is close enough to midnight LST for all US cities (within 8 hours
    at most) — the check naturally handles new-day detection per city using
    the date_utc field anyway, so this is just a memory cleanup.
    """
    global RUNNING_HIGH_LAST
    count = len(RUNNING_HIGH_LAST)
    RUNNING_HIGH_LAST = {}
    log.info(f"Running high cache reset at midnight UTC ({count} city entries cleared).")


# ============================================================
# SECTION 10 — SCHEDULER + ENTRY POINT
#
# Usage:
#   python bot.py          — production mode: runs forever
#   python bot.py --test   — test mode: runs one cycle then exits
#                            (useful for verifying the heartbeat without
#                            starting a long-running process)
# ============================================================

if __name__ == "__main__":
    import sys

    test_mode = "--test" in sys.argv

    # ── Startup banner ──────────────────────────────────────────────────────
    log.info("=" * 58)
    log.info("  Kalshi Climate Bot")
    log.info(f"  {len(CITIES)} cities monitored")
    log.info(f"  Gap-analysis cycle every {RUN_EVERY_MINUTES} min (silent unless triggered)")
    log.info( "  Email schedule (America/New_York):")
    log.info( "    ☀️  7:00 AM — morning briefing (full signal list)")
    log.info(f"    ⚡  Daytime — change alert if gap ≥ {ALERT_GAP_THRESHOLD}% or shift ≥ 10pt")
    log.info( "    🌙  8:00 PM — evening summary (top signals + today's highs)")
    log.info( "  Running-high alert check every 2 minutes")
    log.info( "  Running-high cache reset at midnight UTC")
    if test_mode:
        log.info("  Mode: --test (will exit after first cycle)")
    log.info("=" * 58)

    # ── Run one full cycle immediately on startup ────────────────────────────
    # This gives you output right away instead of waiting RUN_EVERY_MINUTES.
    log.info("Running initial cycle on startup...")
    run_cycle()

    if test_mode:
        # --test flag: verify the heartbeat printed, then stop cleanly.
        log.info("--test complete. Exiting.")
        sys.exit(0)

    # ── Schedule recurring jobs ──────────────────────────────────────────────

    # Main gap-analysis + email cycle every 10 minutes
    schedule.every(RUN_EVERY_MINUTES).minutes.do(run_cycle)

    # Lightweight running-high check every 2 minutes.
    # Only sends an email when a city sets a new daily peak.
    schedule.every(2).minutes.do(check_running_high_alerts)

    # Reset the running-high cache at midnight UTC each day.
    # (All US cities are within UTC-5 to UTC-8, so this falls 0–8 hours
    # after their local midnight — close enough for daily bookkeeping.)
    schedule.every().day.at("00:00").do(reset_running_high_cache)

    log.info(
        f"Scheduler active. "
        f"Full cycle every {RUN_EVERY_MINUTES} min, "
        f"high check every 2 min. "
        f"Press Ctrl+C to stop."
    )

    # ── Main loop ────────────────────────────────────────────────────────────
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("\nKalshi Climate Bot stopped by user. Goodbye.")
