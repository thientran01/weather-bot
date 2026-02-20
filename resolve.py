"""
Kalshi Resolution Checker
-------------------------
Runs every morning at 9:30 AM ET. Reads yesterday's HIGH-confidence signals
from log.csv, checks the Kalshi API to see what each market actually
resolved to, and prints a daily PnL summary broken down by city and
by HIGH vs LOW market type.

9:30 AM ET (6:30 AM PT) gives West Coast markets (LAX, SFO, SEA) time to
settle before we check — their NWS offices may not have filed the official
daily report by 8 AM ET.

Results are also appended to resolve_log.csv for historical tracking.

Usage:
  python resolve.py          — schedule mode: checks at 9:30 AM ET daily
  python resolve.py --now    — run immediately (for testing)

Requirements: same .env as bot.py (no extra credentials needed — Kalshi
market data is publicly readable without an API key).
"""

import os
import csv
import math
import time
import logging
import schedule
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import requests
from config import CITIES

# ============================================================
# SECTION 1 — SETUP
# ============================================================

load_dotenv()

# Kalshi REST API base URL — same as bot.py
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Log files — respect same env vars as bot.py so Railway config stays simple
LOG_FILE    = os.getenv("LOG_PATH",         "log.csv")
RESOLVE_LOG = os.getenv("RESOLVE_LOG_PATH", "resolve_log.csv")

# Date guard — prevents the 9:30 AM check from firing twice if the process
# is still running when the scheduler ticks a minute later.
_RAN_FOR_DATE = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Columns written to resolve_log.csv
RESOLVE_FIELDNAMES = [
    "date", "city", "market_type", "bucket_label", "ticker",
    "direction", "kalshi_price", "nws_implied", "gap",
    "result", "resolved_correct", "pnl_cents",
    "actual_temp", "forecast_error",
]


# ============================================================
# SECTION 2 — FETCH RESOLUTION FROM KALSHI API
# ============================================================

def fetch_market_result(ticker):
    """
    Calls the Kalshi API for a single market by ticker and returns its
    resolution: "yes", "no", or None if the market hasn't settled yet.

    The API returns status="finalized" and result="yes"/"no" for settled
    markets. Unresolved markets have result="" (empty string).

    is_provisional=True means the settlement is tentative and may change —
    we still record it, but the caller can note this if needed.
    """
    try:
        response = requests.get(
            f"{KALSHI_BASE_URL}/markets/{ticker}",
            timeout=10,
        )

        if response.status_code == 404:
            log.warning(f"  {ticker}: market not found (404)")
            return None, False

        response.raise_for_status()

        # Single-market endpoint wraps the object in {"market": {...}}
        data        = response.json()
        market      = data.get("market") or data   # defensive: handle both formats
        result      = market.get("result", "")
        provisional = market.get("is_provisional", False)

        if result in ("yes", "no"):
            return result, provisional

        # Empty string means not yet settled
        return None, False

    except requests.exceptions.RequestException as e:
        log.error(f"  {ticker}: API request failed — {e}")
        return None, False
    except Exception as e:
        log.error(f"  {ticker}: unexpected error — {e}")
        return None, False


# ============================================================
# SECTION 2.5 — FETCH ACTUAL TEMPERATURE FROM NWS OBSERVATIONS
# ============================================================

def fetch_actual_temperature(station, date, station_type, lst_utc_offset):
    """
    Fetches the actual daily HIGH and LOW temperature from NWS observations
    for the given station and calendar date.

    The NWS observations endpoint is scoped to the station's midnight-to-midnight
    window in LST (Local Standard Time), converted to UTC via lst_utc_offset.

      station        — NWS ICAO station code (e.g. "KNYC")
      date           — datetime.date for the resolution day
      station_type   — "5-minute" (ASOS) or "hourly" (cooperative/manual observer)
      lst_utc_offset — int, e.g. -5 for EST. Negative because LST is behind UTC.

    Temperature conversion:
      ASOS (5-minute) : official_F = math.floor(celsius * 9/5 + 32)
      Hourly (manual) : official_F = round(celsius * 9/5 + 32)

    The maxTemperatureLast24Hours (DSM) field, if present on any observation,
    overrides the running computed high if it is higher.

    Returns {"actual_high": int, "actual_low": int} or None on failure.
    """
    try:
        # Convert the target date's midnight LST to UTC.
        # lst_utc_offset is negative (e.g. -5), so -lst_utc_offset gives the positive
        # hour offset: midnight LST + 5 h = 05:00 UTC for EST.
        utc_hour  = -lst_utc_offset
        start_utc = datetime(date.year, date.month, date.day, utc_hour, 0, 0)
        end_utc   = start_utc + timedelta(days=1)

        start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str   = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        response = requests.get(
            f"https://api.weather.gov/stations/{station}/observations",
            params={"start": start_str, "end": end_str, "limit": 500},
            headers={"User-Agent": "KalshiClimateBot/1.0"},
            timeout=15,
        )
        response.raise_for_status()

        features = response.json().get("features", [])

        if not features:
            log.warning(f"  {station}: no observations returned for {date}")
            return None

        actual_high = None
        actual_low  = None

        for obs in features:
            props  = obs.get("properties", {})

            # Current temperature reading from this observation
            temp_c = (props.get("temperature") or {}).get("value")
            if temp_c is not None:
                if station_type == "5-minute":
                    temp_f = math.floor(temp_c * 9 / 5 + 32)
                else:   # hourly / cooperative observer
                    temp_f = round(temp_c * 9 / 5 + 32)

                if actual_high is None or temp_f > actual_high:
                    actual_high = temp_f
                if actual_low is None or temp_f < actual_low:
                    actual_low = temp_f

            # DSM (Daily Summary Message) field — overrides computed high if higher.
            # Use the same rounding rule as the station type.
            max_24h_c = (props.get("maxTemperatureLast24Hours") or {}).get("value")
            if max_24h_c is not None:
                if station_type == "5-minute":
                    max_24h_f = math.floor(max_24h_c * 9 / 5 + 32)
                else:
                    max_24h_f = round(max_24h_c * 9 / 5 + 32)
                if actual_high is None or max_24h_f > actual_high:
                    actual_high = max_24h_f

        if actual_high is None and actual_low is None:
            log.warning(f"  {station}: no valid temperature readings for {date}")
            return None

        return {"actual_high": actual_high, "actual_low": actual_low}

    except requests.exceptions.RequestException as e:
        log.error(f"  {station}: NWS observations request failed — {e}")
        return None
    except Exception as e:
        log.error(f"  {station}: unexpected error in fetch_actual_temperature — {e}")
        return None


# ============================================================
# SECTION 2.8 — LOOK UP FORECAST TEMP FROM LOG.CSV
# ============================================================

def _get_forecast_for_ticker(ticker, log_path):
    """
    Scans log.csv for all rows matching this ticker and returns the
    forecast_temp_used value from the last (most recent) matching row.

    forecast_temp_used is the temperature that drove the Gaussian probability
    calculation — it's either the live observed running high/low (for today
    markets with observations) or the NWS grid forecast (for tomorrow markets
    or early-morning today markets with no observations yet).

    Returns an int (°F) if found, or None if the ticker has no logged rows
    with a non-empty forecast_temp_used.
    """
    if not os.path.isfile(log_path):
        return None

    forecast_temp = None
    try:
        with open(log_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("ticker") == ticker and row.get("forecast_temp_used", "").strip():
                    forecast_temp = row["forecast_temp_used"]
        # Returns the last match — most recent cycle logged for this ticker
        return int(float(forecast_temp)) if forecast_temp else None
    except Exception as e:
        log.warning(f"  _get_forecast_for_ticker({ticker}): {e}")
        return None


# ============================================================
# SECTION 3 — LOAD YESTERDAY'S SIGNALS FROM LOG.CSV
# ============================================================

def load_yesterday_signals():
    """
    Reads log.csv and returns all HIGH-confidence signals whose Kalshi
    market resolved yesterday.

    Filtering rules:
      - ticker column must be present (rows from old CSV schema are skipped)
      - Market resolution date (parsed from ticker) == yesterday in ET
      - was_settled == False  (markets already at >90% or <10% are excluded;
        they had no real signal, just confirming an obvious outcome)
      - confidence == HIGH    (only signals where NWS was clearly on one side)

    When the same ticker appears multiple times (the bot logs every 10 min),
    we keep the LAST row — the reading closest to market close, which is the
    most relevant snapshot for accuracy evaluation.

    Returns:
      by_ticker   — dict  {ticker: row_dict}
      yesterday   — "YYYY-MM-DD" string (the date being evaluated)
    """
    et            = datetime.now(tz=ZoneInfo("America/New_York"))
    yesterday_et  = (et - timedelta(days=1)).date()
    yesterday_str = yesterday_et.strftime("%Y-%m-%d")

    by_ticker = {}

    if not os.path.isfile(LOG_FILE):
        log.error(f"Log file not found: {LOG_FILE}")
        return by_ticker, yesterday_str

    try:
        with open(LOG_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:

                # Skip rows from old schema (no ticker column)
                if not row.get("ticker"):
                    continue

                # Parse the resolution date from the ticker.
                # Ticker format: "KXHIGHNY-26FEB18-B48.5"
                # The middle segment is the date in YYMONDD format.
                try:
                    date_part    = row["ticker"].split("-")[1]   # e.g. "26FEB18"
                    resolve_date = datetime.strptime(date_part, "%y%b%d").date()
                except (IndexError, ValueError):
                    continue

                # Only markets that resolved on yesterday's ET date
                if resolve_date != yesterday_et:
                    continue

                # Exclude already-settled markets (no edge, just noise)
                if row.get("was_settled", "").lower() == "true":
                    continue

                # Only high-confidence signals (NWS was clearly on one side)
                if row.get("confidence", "") != "HIGH":
                    continue

                # Last occurrence wins — most recent reading before close
                by_ticker[row["ticker"]] = row

    except Exception as e:
        log.error(f"Error reading {LOG_FILE}: {e}")

    return by_ticker, yesterday_str


# ============================================================
# SECTION 4 — WRITE RESOLUTION LOG
# ============================================================

def write_resolve_log(rows):
    """
    Appends resolution results to resolve_log.csv.
    Creates the file with a header on the first run.
    """
    if not rows:
        return

    file_exists = os.path.isfile(RESOLVE_LOG)
    try:
        with open(RESOLVE_LOG, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=RESOLVE_FIELDNAMES)
            if not file_exists:
                writer.writeheader()
                log.info(f"Created {RESOLVE_LOG} with header row.")
            writer.writerows(rows)
        log.info(f"Appended {len(rows)} rows to {RESOLVE_LOG}.")
    except Exception as e:
        log.error(f"Failed to write to {RESOLVE_LOG}: {e}")


# ============================================================
# SECTION 5 — PnL SUMMARY PRINTER
# ============================================================

def print_summary(date_str, rows):
    """
    Prints the daily PnL summary to stdout.

    pnl_cents is calculated per-trade assuming 1-unit position size:
      BUY YES at entry_price:  win = +(100 - entry), loss = -entry
      BUY NO  at entry_price:  win = +entry,          loss = -(100 - entry)

    Shows:
      - Net PnL and win rate for the day
      - Breakdown by market type (HIGH vs LOW)
      - Breakdown by city with net PnL per city
    """
    total   = len(rows)
    net_pnl = sum(r["pnl_cents"] for r in rows)
    correct = sum(1 for r in rows if r["resolved_correct"] is True)

    DIVIDER = "=" * 52

    print(f"\n{DIVIDER}")
    print(f"  PnL SUMMARY — {date_str}")
    print(f"{DIVIDER}")

    if total == 0:
        print("  No resolved markets found.")
        print("  (Results may not be posted yet — try again later.)")
        print(f"{DIVIDER}\n")
        return

    pnl_sign = "+" if net_pnl >= 0 else ""
    print(f"  Trades evaluated : {total}")
    print(f"  Net PnL          : {pnl_sign}{net_pnl:.0f}¢  (${net_pnl / 100:.2f} per unit)")
    print(f"  Win rate         : {correct}/{total} ({correct / total * 100:.1f}%)")
    print()

    # ── By market type ──────────────────────────────────────────────────
    print(f"  {'TYPE':<6}  {'TRADES':>6}  {'NET PnL':>9}  {'AVG PnL':>9}")
    print(f"  {'-' * 38}")
    for mtype in ("HIGH", "LOW"):
        subset = [r for r in rows if r["market_type"] == mtype]
        if not subset:
            continue
        sp   = sum(r["pnl_cents"] for r in subset)
        avg  = sp / len(subset)
        print(f"  {mtype:<6}  {len(subset):>6}  {'+' if sp >= 0 else ''}{sp:>7.0f}¢  {'+' if avg >= 0 else ''}{avg:>7.1f}¢")

    print()

    # ── By city ─────────────────────────────────────────────────────────
    def pnl_str(subset):
        """Returns 'PnL¢/N trades' or '—' if the subset is empty."""
        if not subset:
            return "—"
        sp = sum(r["pnl_cents"] for r in subset)
        return f"{'+' if sp >= 0 else ''}{sp:.0f}¢/{len(subset)}"

    print(f"  {'CITY':<22}  {'HIGH':>10}  {'LOW':>8}")
    print(f"  {'-' * 46}")
    for city in sorted({r["city"] for r in rows}):
        city_rows = [r for r in rows if r["city"] == city]
        high_str  = pnl_str([r for r in city_rows if r["market_type"] == "HIGH"])
        low_str   = pnl_str([r for r in city_rows if r["market_type"] == "LOW"])
        print(f"  {city:<22}  {high_str:>10}  {low_str:>8}")

    # ── NWS forecast accuracy (MAE by city) ─────────────────────────────────
    # Only rows where we successfully fetched the actual observed temperature
    # contribute. forecast_error = NWS forecast − actual (in °F); we report MAE.
    mae_rows = [r for r in rows if r.get("forecast_error", "") != ""]
    if mae_rows:
        print()
        print(f"  NWS FORECAST ERROR (mean absolute error vs observed)")
        print(f"  {'-' * 46}")
        for city in sorted({r["city"] for r in mae_rows}):
            city_subset = [r for r in mae_rows if r["city"] == city]
            errors = []
            for r in city_subset:
                try:
                    errors.append(abs(float(r["forecast_error"])))
                except (ValueError, TypeError):
                    pass
            if errors:
                mae = sum(errors) / len(errors)
                print(f"  {city:<22}  Avg Error: {mae:.1f}°F")

    print(f"{DIVIDER}\n")


# ============================================================
# SECTION 6 — MAIN RESOLUTION CHECK
# ============================================================

def run_resolution_check():
    """
    Full resolution check for yesterday:
      1. Load yesterday's HIGH-confidence signals from log.csv
      2. Fetch each market's result from the Kalshi API
      3. Determine correctness (our signal direction vs actual resolution)
      4. Fetch the actual observed temperature from NWS observations
      5. Append results to resolve_log.csv
      6. Print the PnL + forecast accuracy summary
    """
    global _RAN_FOR_DATE

    et            = datetime.now(tz=ZoneInfo("America/New_York"))
    _RAN_FOR_DATE = et.date()

    signals, yesterday_str = load_yesterday_signals()

    print(f"\n{'=' * 52}")
    print(f"  RESOLUTION CHECK — {yesterday_str}")
    print(f"{'=' * 52}")

    if not signals:
        log.info("No HIGH-confidence signals found for yesterday — nothing to resolve.")
        print("  No high-confidence signals in log.csv for yesterday.")
        print(f"  (Rows need the 'ticker' column — delete log.csv if it predates this)")
        print(f"{'=' * 52}\n")
        return 0

    log.info(f"Checking {len(signals)} unique markets for {yesterday_str}...")

    # Build a reverse lookup: display name → city_key (e.g. "New York City" → "NYC")
    name_to_key = {v["name"]: k for k, v in CITIES.items()}

    # Parse the resolution date once — used for NWS observation lookups below
    resolve_date_obj = datetime.strptime(yesterday_str, "%Y-%m-%d").date()

    # Cache NWS actual temps by city_key so we only hit the API once per city,
    # not once per ticker (a city can have many buckets in signals).
    # Value is {"actual_high": int, "actual_low": int} or None if fetch failed.
    actuals_cache = {}

    resolved_rows = []

    for ticker, row in signals.items():
        result, provisional = fetch_market_result(ticker)

        # Pause briefly between calls to be respectful of Kalshi's API
        time.sleep(0.15)

        if result is None:
            log.info(f"  {ticker}: not yet settled — skipping")
            continue

        direction   = row["direction"]   # "BUY YES" or "BUY NO"
        entry_price = float(row["kalshi_price"])
        correct     = (
            (direction == "BUY YES" and result == "yes") or
            (direction == "BUY NO"  and result == "no")
        )

        # PnL in cents, assuming 1-unit position at the logged Kalshi price.
        #
        # BUY YES at entry_price (e.g. 22¢):
        #   Win (YES settles 100): you paid 22¢, receive 100¢ → gain 78¢
        #   Loss (YES settles 0):  you paid 22¢, receive 0¢  → lose 22¢
        #
        # BUY NO at entry_price (e.g. 22¢ for YES = 78¢ for NO):
        #   "Buying NO" at a YES price of 22¢ means you pay (100 - 22) = 78¢.
        #   Win (NO wins, YES settles 0):  receive 100¢, paid 78¢ → gain 22¢ (= entry_price)
        #   Loss (NO loses, YES settles 100): receive 0¢, paid 78¢ → lose 78¢ (= 100 - entry_price)
        if result in ("void", "voided"):
            pnl = 0.0   # refunded — no gain or loss
        elif direction == "BUY YES":
            pnl = (100 - entry_price) if result == "yes" else -entry_price
        else:  # BUY NO
            pnl = entry_price if result == "no" else -(100 - entry_price)

        # Log each market result to console with a clear icon and PnL
        prov_tag = " (provisional)" if provisional else ""
        icon     = "✅" if correct else "❌"
        pnl_sign = "+" if pnl >= 0 else ""
        log.info(
            f"  {icon} {ticker}: signal={direction}, resolved={result}{prov_tag}, "
            f"pnl={pnl_sign}{pnl:.0f}¢"
        )

        # ── Fetch actual temperature from NWS ─────────────────────────────
        city_key  = name_to_key.get(row["city"])
        city_conf = CITIES.get(city_key) if city_key else None

        actual_temp    = ""
        forecast_error = ""

        if city_conf:
            # Fetch once per city; reuse the cached result for subsequent tickers
            if city_key not in actuals_cache:
                log.info(f"  Fetching NWS actuals for {row['city']} ({city_conf['nws_station']})...")
                actuals_cache[city_key] = fetch_actual_temperature(
                    city_conf["nws_station"],
                    resolve_date_obj,
                    city_conf["station_type"],
                    city_conf["lst_utc_offset"],
                )
                time.sleep(0.15)   # be polite to the NWS API

            actuals = actuals_cache[city_key]
            if actuals:
                actual_temp = (
                    actuals["actual_high"] if row["market_type"] == "HIGH"
                    else actuals["actual_low"]
                )

                # forecast_error = forecast_temp_used − actual observed temp (°F).
                # Positive = model predicted too high; negative = too low.
                # Look up from log.csv via the helper so we always get the correct
                # column name (forecast_temp_used) regardless of CSV schema history.
                forecast_temp = _get_forecast_for_ticker(ticker, LOG_FILE)
                if forecast_temp is not None and actual_temp != "":
                    forecast_error = round(forecast_temp - actual_temp, 1)
                else:
                    forecast_error = ""
        else:
            if row["city"]:
                log.warning(f"  {ticker}: city '{row['city']}' not found in CITIES config")

        resolved_rows.append({
            "date":             yesterday_str,
            "city":             row["city"],
            "market_type":      row["market_type"],
            "bucket_label":     row["bucket_label"],
            "ticker":           ticker,
            "direction":        direction,
            "kalshi_price":     row["kalshi_price"],
            "nws_implied":      row["nws_implied"],
            "gap":              row["gap"],
            "result":           result,
            "resolved_correct": correct,
            "pnl_cents":        pnl,
            "actual_temp":      actual_temp,
            "forecast_error":   forecast_error,
        })

    write_resolve_log(resolved_rows)
    print_summary(yesterday_str, resolved_rows)
    return len(resolved_rows)


def run_resolution():
    """
    Public entry point for running the daily resolution check.
    Called by bot.py's run_cycle() at 9:30 AM ET.
    Returns the number of markets resolved (0 if none found or already run today).
    """
    return run_resolution_check()


# ============================================================
# SECTION 7 — SCHEDULER + ENTRY POINT
#
# Usage:
#   python resolve.py          — schedule mode: fires at 9:30 AM ET daily
#   python resolve.py --now    — run one check immediately and exit
#
# Why not schedule.every().day.at("09:30")?
# The schedule library uses local system time. On Railway (UTC), "09:30"
# would mean 9:30 AM UTC, not 9:30 AM ET. Instead, we check every minute
# and fire when it's 9:30 AM in America/New_York — correct regardless of DST.
#
# Why 9:30 AM ET (not 8 AM)?
# West Coast stations (LAX, SFO, SEA) are UTC-8. Their Kalshi markets
# resolve at midnight PST = 8:00 AM UTC = 3:00 AM ET, but the NWS office
# may not publish the official daily summary until well after that. 9:30 AM
# ET = 6:30 AM PT gives them enough time to file before we check.
# ============================================================

def _maybe_run():
    """Called every minute — fires the check at 9:30 AM ET, once per day."""
    et_now = datetime.now(tz=ZoneInfo("America/New_York"))
    if et_now.hour == 9 and et_now.minute >= 30 and _RAN_FOR_DATE != et_now.date():
        run_resolution_check()


if __name__ == "__main__":
    import sys

    if "--now" in sys.argv:
        log.info("--now flag: running resolution check immediately.")
        run_resolution_check()
        sys.exit(0)

    log.info("=" * 52)
    log.info("  Kalshi Resolution Checker")
    log.info(f"  Reading signals from  : {LOG_FILE}")
    log.info(f"  Writing results to    : {RESOLVE_LOG}")
    log.info("  Schedule              : 9:30 AM ET daily")
    log.info("  Run with --now to test immediately")
    log.info("=" * 52)

    schedule.every(1).minutes.do(_maybe_run)

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        log.info("Resolution checker stopped.")
