"""
models.py — All data fetching: Kalshi API, NWS, Open-Meteo, WeatherAPI.

Includes market discovery, market price fetching, NWS grid forecasts,
and four Open-Meteo model fetchers for cross-validation.
"""

import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from config import CITIES, KALSHI_BASE_URL, WEATHERAPI_KEY, log
from observations import get_current_running_high, get_current_running_low


# Cache for NWS grid info (office, gridX, gridY) so we only look it up once
# per city at startup rather than on every 10-minute cycle.
# Format: {"NYC": {"office": "OKX", "grid_x": 33, "grid_y": 37, "forecast_url": "..."}}
NWS_GRID_CACHE = {}


# ============================================================
# SECTION 3b — MARKET DISCOVERY (TEMPORARY)
# Run this once to print the raw Kalshi API response so we can
# confirm the exact ticker format before building real logic.
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
# SECTION 5c — MULTI-MODEL FORECASTS
# Three additional forecast sources for cross-validation with NWS.
# All are non-fatal — failure here never skips a city or blocks
# the main cycle. NWS still drives all signal logic.
#
# Sources:
#   Open-Meteo ECMWF  (ecmwf_ifs025)  — free, no key
#   Open-Meteo GFS    (gfs_seamless)   — free, no key
#   Open-Meteo GEM    (gem_seamless)   — free, no key
#   Open-Meteo ICON   (icon_seamless)  — free, no key
#   WeatherAPI.com                     — requires WEATHERAPI_KEY
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

        # Take the first date as "today" and the second as "tomorrow".
        # Open-Meteo uses timezone=auto (station's local time), so its day
        # boundaries always match the station's local date — no UTC offset
        # arithmetic needed, and no risk of a None result after 7 PM ET.
        result = {"today_high": None, "today_low": None, "tomorrow_high": None, "tomorrow_low": None}

        if len(times) >= 1:
            result["today_high"] = round(highs[0]) if highs[0] is not None else None
            result["today_low"]  = round(lows[0])  if lows[0]  is not None else None

        if len(times) >= 2:
            result["tomorrow_high"] = round(highs[1]) if highs[1] is not None else None
            result["tomorrow_low"]  = round(lows[1])  if lows[1]  is not None else None

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
