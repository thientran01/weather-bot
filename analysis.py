"""
analysis.py — Probability calculations and gap analysis.

Converts NWS forecast temperatures into Gaussian-implied probabilities,
then compares them against Kalshi market prices to find gaps.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from scipy.stats import norm
from config import CITIES, MIN_GAP_TO_SHOW, log


# ============================================================
# SECTION 6 — PROBABILITY + GAP ANALYSIS
# ============================================================

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
            # BUT: after ~6 PM, temps are falling again toward tomorrow's overnight
            # low, so the "today" low reading is still in flux if the market
            # covers the full calendar day (midnight to midnight).
            if lst_hour >= 18:
                # Evening/night: temp is dropping, overnight low hasn't occurred yet.
                # Widen back to full uncertainty — the low could still change.
                time_decay_multiplier = 1.0
            elif lst_hour >= 12:
                time_decay_multiplier = 0.3    # after noon: morning low is done, temp is rising
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
