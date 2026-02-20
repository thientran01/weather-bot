"""
observations.py — Running high/low trackers.

Fetches actual NWS station observations since midnight LST and returns
the running maximum (for HIGH markets) or minimum (for LOW markets).

These replace the NWS grid forecast for TODAY's markets once the day is
in progress, because live observations are more accurate than a forecast
issued hours earlier.
"""

import math
import requests
from datetime import datetime, timedelta
from config import CITIES, log


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
