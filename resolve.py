"""
Kalshi Resolution Checker
-------------------------
Runs every morning at 8 AM ET. Reads yesterday's HIGH-confidence signals
from log.csv, checks the Kalshi API to see what each market actually
resolved to, and prints a daily accuracy summary broken down by city and
by HIGH vs LOW market type.

Results are also appended to resolve_log.csv for historical tracking.

Usage:
  python resolve.py          — schedule mode: checks at 8 AM ET daily
  python resolve.py --now    — run immediately (for testing)

Requirements: same .env as bot.py (no extra credentials needed — Kalshi
market data is publicly readable without an API key).
"""

import os
import csv
import time
import logging
import schedule
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import requests

# ============================================================
# SECTION 1 — SETUP
# ============================================================

load_dotenv()

# Kalshi REST API base URL — same as bot.py
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Log files — respect same env vars as bot.py so Railway config stays simple
LOG_FILE    = os.getenv("LOG_PATH",         "log.csv")
RESOLVE_LOG = os.getenv("RESOLVE_LOG_PATH", "resolve_log.csv")

# Date guard — prevents the 8 AM check from firing twice if the process
# is still running when the scheduler ticks at 8:01 AM.
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
    "result", "resolved_correct",
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
# SECTION 5 — ACCURACY SUMMARY PRINTER
# ============================================================

def print_summary(date_str, rows):
    """
    Prints the daily accuracy summary to stdout.

    Shows:
      - Overall correct / incorrect / hit rate
      - Breakdown by market type (HIGH vs LOW)
      - Breakdown by city with HIGH and LOW columns side by side
    """
    total   = len(rows)
    correct = sum(1 for r in rows if r["resolved_correct"] is True)

    DIVIDER = "=" * 52

    print(f"\n{DIVIDER}")
    print(f"  ACCURACY SUMMARY — {date_str}")
    print(f"{DIVIDER}")

    if total == 0:
        print("  No resolved markets found.")
        print("  (Results may not be posted yet — try again later.)")
        print(f"{DIVIDER}\n")
        return

    print(f"  Signals checked : {total}")
    print(f"  Correct         : {correct}  ({correct / total * 100:.1f}%)")
    print(f"  Incorrect       : {total - correct}")
    print()

    # ── By market type ──────────────────────────────────────────────────
    print(f"  {'TYPE':<6}  {'CORRECT':>7}  {'TOTAL':>5}  {'HIT RATE':>9}")
    print(f"  {'-' * 34}")
    for mtype in ("HIGH", "LOW"):
        subset = [r for r in rows if r["market_type"] == mtype]
        if not subset:
            continue
        c = sum(1 for r in subset if r["resolved_correct"])
        print(f"  {mtype:<6}  {c:>7}  {len(subset):>5}  {c / len(subset) * 100:>8.1f}%")

    print()

    # ── By city ─────────────────────────────────────────────────────────
    def fraction(subset):
        """Returns 'C/T (R%)' or '—' if the subset is empty."""
        if not subset:
            return "—"
        c = sum(1 for r in subset if r["resolved_correct"])
        t = len(subset)
        return f"{c}/{t} ({c / t * 100:.0f}%)"

    print(f"  {'CITY':<22}  {'HIGH':>12}  {'LOW':>10}")
    print(f"  {'-' * 50}")
    for city in sorted({r["city"] for r in rows}):
        city_rows = [r for r in rows if r["city"] == city]
        high_str  = fraction([r for r in city_rows if r["market_type"] == "HIGH"])
        low_str   = fraction([r for r in city_rows if r["market_type"] == "LOW"])
        print(f"  {city:<22}  {high_str:>12}  {low_str:>10}")

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
      4. Append results to resolve_log.csv
      5. Print the accuracy summary
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
        return

    log.info(f"Checking {len(signals)} unique markets for {yesterday_str}...")

    resolved_rows = []

    for ticker, row in signals.items():
        result, provisional = fetch_market_result(ticker)

        # Pause briefly between calls to be respectful of Kalshi's API
        time.sleep(0.15)

        if result is None:
            log.info(f"  {ticker}: not yet settled — skipping")
            continue

        direction = row["direction"]   # "BUY YES" or "BUY NO"
        correct   = (
            (direction == "BUY YES" and result == "yes") or
            (direction == "BUY NO"  and result == "no")
        )

        # Log each market result to console with a clear icon
        prov_tag = " (provisional)" if provisional else ""
        icon     = "✅" if correct else "❌"
        log.info(
            f"  {icon} {ticker}: signal={direction}, resolved={result}{prov_tag}, correct={correct}"
        )

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
        })

    write_resolve_log(resolved_rows)
    print_summary(yesterday_str, resolved_rows)


# ============================================================
# SECTION 7 — SCHEDULER + ENTRY POINT
#
# Usage:
#   python resolve.py          — schedule mode: fires at 8:00 AM ET daily
#   python resolve.py --now    — run one check immediately and exit
#
# Why not schedule.every().day.at("08:00")?
# The schedule library uses local system time. On Railway (UTC), "08:00"
# would mean 8 AM UTC, not 8 AM ET. Instead, we check every minute and
# fire when it's 8 AM in America/New_York — correct regardless of DST.
# ============================================================

def _maybe_run():
    """Called every minute — fires the check at 8:00 AM ET, once per day."""
    et_now = datetime.now(tz=ZoneInfo("America/New_York"))
    if et_now.hour == 8 and _RAN_FOR_DATE != et_now.date():
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
    log.info("  Schedule              : 8:00 AM ET daily")
    log.info("  Run with --now to test immediately")
    log.info("=" * 52)

    schedule.every(1).minutes.do(_maybe_run)

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        log.info("Resolution checker stopped.")
