"""
logging_csv.py — CSV logging for every bot cycle.

Appends one row per market per cycle to log.csv so you can track
how markets and forecasts have changed over time.
"""

import os
import csv
from datetime import datetime
from config import CITIES, LOG_FILE, log


# ============================================================
# SECTION 8 — CSV LOGGING
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
