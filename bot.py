"""
Kalshi Climate Bot
------------------
Monitors Kalshi temperature markets for 20 cities and compares them
against NWS weather forecasts. Shows you where the odds look off so
you can decide whether to trade. Does NOT place trades automatically.

Runs every 10 minutes and sends a summary via text and/or email.
Logs every cycle to log.csv for historical tracking.

This file is the thin entry point — all logic lives in the modules below.
"""

import time
import threading
import schedule
from datetime import datetime

# ── Import all modules ────────────────────────────────────────────────────────
from config import (
    CITIES, RUN_EVERY_MINUTES, SEND_TEST_EMAIL, log, _et_now,
)
from models import (
    fetch_kalshi_markets, fetch_nws_forecast,
    fetch_ecmwf_forecast, fetch_gfs_forecast, fetch_gem_forecast,
    fetch_icon_forecast, fetch_hourly_forecast, fetch_weatherapi_forecast,
)
from analysis import analyze_gaps
from alerts import (
    format_alert_message, format_evening_summary,
    send_email, send_test_email,
)
import paper_trading as pt
from paper_trading import check_paper_entries, resolve_paper_trades, reset_paper_trading
from logging_csv import log_to_csv
from export_server import run_http_server
from resolve import run_resolution


# ============================================================
# MODULE-LEVEL STATE
# These sets and dates reset at midnight and track intraday events.
# ============================================================

# ET date on which the morning briefing / evening summary was last sent.
# Prevents duplicate sends when the bot cycles through the 7:00–7:15 AM or
# 8:00–8:15 PM window more than once (it runs every 10 minutes, so at most
# one cycle falls in each 15-minute window under normal conditions, but this
# guard makes it robust to restarts or clock drift).
_MORNING_SENT_DATE = None
_EVENING_SENT_DATE = None

# Guards for the 9:30 AM ET daily resolution check.
# _RESOLVE_DATE resets the flag each new ET day so it fires exactly once.
_RESOLVE_RAN_TODAY = False
_RESOLVE_DATE      = None


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
    global _MORNING_SENT_DATE, _EVENING_SENT_DATE, _RESOLVE_RAN_TODAY, _RESOLVE_DATE

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

    # ── Email decision ───────────────────────────────────────────────────────
    if all_results:
        et      = _et_now()
        et_date = et.date()
        et_min  = et.hour * 60 + et.minute   # minutes since midnight ET

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

        else:
            # ── Daytime: silent cycle ────────────────────────────────────────
            log.info("Daytime cycle complete — no email this cycle.")

        log_to_csv(all_results, all_forecasts)

        # ── Paper trading ────────────────────────────────────────────────────
        # Update the date tracker (doesn't clear positions — they resolve naturally)
        if pt._PAPER_TRADE_DATE != et.date():
            pt._PAPER_TRADE_DATE = et.date()

        check_paper_entries(all_results, all_forecasts)
        resolve_paper_trades()

        # ── Daily resolution check (9:30 AM ET) ─────────────────────────────
        # Reads yesterday's signals from log.csv, fetches actual Kalshi
        # results and NWS observed temps, then writes to resolve_log.csv.
        # Fires once per day at or after 9:30 AM ET — late enough for West
        # Coast markets (LAX, SFO, SEA) to have settled before we check.
        if _RESOLVE_DATE != et.date():
            _RESOLVE_DATE = et.date()
            _RESOLVE_RAN_TODAY = False

        if not _RESOLVE_RAN_TODAY and et.hour >= 9 and et.minute >= 30:
            try:
                log.info("🔍 Running daily resolution check...")
                resolved_count = run_resolution()
                log.info(f"🔍 Resolution complete: {resolved_count} markets resolved")
                _RESOLVE_RAN_TODAY = True
            except Exception as e:
                log.error(f"Resolution check failed: {e}")

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
    if pt._PAPER_POSITIONS:
        log.info(f"📝 Paper positions open: {len(pt._PAPER_POSITIONS)}")

    return all_results, all_forecasts


def reset_daily_state():
    """
    Resets daily state at midnight UTC so each day starts fresh.
    Called by the scheduler at 00:00 UTC.
    """
    log.info("Daily state reset at midnight UTC.")
    reset_paper_trading()


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
    log.info( "    🌙  8:00 PM — evening summary (tomorrow markets + today's highs)")
    log.info( "  Daily state reset at midnight UTC")
    log.info( "  Export server: GET /export → log.csv")
    log.info( "  📝 Paper trading: logging simulated trades to paper_trades.csv")
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
