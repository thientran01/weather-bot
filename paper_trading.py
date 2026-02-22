"""
paper_trading.py — Simulated paper trading.

Simulates trades the bot would have placed if it acted on signals the moment
they pass all quality filters. Completely passive — never touches the Kalshi
order API, only the read-only market data endpoint for resolution checks.

State is in-memory (_PAPER_POSITIONS dict). If the bot restarts mid-day,
open positions are lost. Resolved trades are written to paper_trades.csv.
"""

import os
import csv
import time
import requests
from datetime import datetime
from config import CITIES, KALSHI_BASE_URL, log
from analysis import _get_model_data
from alerts import _passes_paper_trade_filters


# ============================================================
# SECTION 8b — PAPER TRADING STATE
# ============================================================

# Path to paper trading CSV log — set via env var or default to local file
PAPER_TRADE_LOG = os.getenv("PAPER_TRADE_LOG_PATH", "paper_trades.csv")

# In-memory position tracker. Only ONE entry per ticker per day.
# Key: ticker string. Value: dict of entry metadata.
_PAPER_POSITIONS = {}
_PAPER_TRADE_DATE = None   # ET date; used to detect day rollover

PAPER_TRADE_FIELDNAMES = [
    "entry_time", "exit_time", "city", "market_type", "bucket_label", "ticker",
    "direction", "entry_price", "exit_result", "pnl_cents",
    "gap_at_entry", "spread_at_entry", "std_dev_at_entry", "time_decay_at_entry",
    "nws_prob_at_entry", "forecast_temp_at_entry", "consensus_at_entry",
    "hourly_adjusted_at_entry",
]


# ============================================================
# SECTION 8b — PAPER TRADING FUNCTIONS
# ============================================================

def check_paper_entries(all_results, all_forecasts):
    """
    Called at the end of every cycle. Scans all today's markets and
    records a paper trade entry for any that pass quality filters and
    haven't been entered yet today.

    Only today's markets are traded — tomorrow's markets are skipped
    because we need live observations to confirm the signal is real.
    """
    for city_key, gaps in all_results.items():
        for g in gaps:
            # Trade both today and tomorrow markets.
            # Tomorrow markets are entered at whatever price is current when the
            # signal first passes filters — this captures the actual entry timing
            # that a real trader would experience, unlike resolve.py which uses
            # end-of-day snapshots.

            ticker = g["ticker"]

            # Only enter each market once per day — don't re-enter on every cycle
            if ticker in _PAPER_POSITIONS:
                continue

            # Apply quality filters (same rules as email, separate function)
            md = _get_model_data(city_key, g, all_forecasts)
            if not md["has_enough_data"]:
                continue
            if not _passes_paper_trade_filters(g, md):
                continue

            # Record the entry in memory
            _PAPER_POSITIONS[ticker] = {
                "entry_time":               datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "city":                     CITIES[city_key]["name"],
                "market_type":              g["series_type"],
                "bucket_label":             g["bucket_label"],
                "ticker":                   ticker,
                "direction":                g["edge"],
                "entry_price":              g["kalshi_prob"],
                "gap_at_entry":             g["gap"],
                "spread_at_entry":          md["spread"],
                "std_dev_at_entry":         g["std_dev_used"],
                "time_decay_at_entry":      g.get("time_decay_multiplier", 1.0),
                "nws_prob_at_entry":        g["nws_prob"],
                "forecast_temp_at_entry":   g["forecast_temp"],
                "consensus_at_entry":       md["consensus"],
                "hourly_adjusted_at_entry": g.get("hourly_adjusted", False),
            }
            log.info(
                f"📝 PAPER TRADE: {g['edge']} {ticker} at {g['kalshi_prob']}¢"
                f" (gap: {g['gap']:+d}%)"
            )


def resolve_paper_trades():
    """
    Checks each open paper position against the Kalshi API to see if
    the market has resolved. If it has, calculates PnL and writes a row
    to PAPER_TRADE_LOG, then removes the position from _PAPER_POSITIONS.

    PnL logic (in cents, per 100-cent contract):
      BUY YES: win = +(100 - entry_price), loss = -entry_price
      BUY NO:  win = +entry_price,          loss = -(100 - entry_price)
    """
    if not _PAPER_POSITIONS:
        return

    resolved_tickers = []

    for ticker, pos in list(_PAPER_POSITIONS.items()):
        try:
            response = requests.get(
                f"{KALSHI_BASE_URL}/markets/{ticker}",
                timeout=10,
            )
            market = response.json().get("market", response.json())
            result = market.get("result", "")

            if result not in ("yes", "no"):
                # Not resolved yet — check again next cycle
                time.sleep(0.15)
                continue

            # Calculate PnL in cents
            entry_price = pos["entry_price"]
            direction   = pos["direction"]
            if direction == "BUY YES":
                pnl = (100 - entry_price) if result == "yes" else -entry_price
            else:  # BUY NO
                pnl = entry_price if result == "no" else -(100 - entry_price)

            exit_time  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file_exists = os.path.isfile(PAPER_TRADE_LOG)

            try:
                with open(PAPER_TRADE_LOG, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=PAPER_TRADE_FIELDNAMES)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow({
                        "entry_time":               pos["entry_time"],
                        "exit_time":                exit_time,
                        "city":                     pos["city"],
                        "market_type":              pos["market_type"],
                        "bucket_label":             pos["bucket_label"],
                        "ticker":                   ticker,
                        "direction":                direction,
                        "entry_price":              entry_price,
                        "exit_result":              result,
                        "pnl_cents":                pnl,
                        "gap_at_entry":             pos["gap_at_entry"],
                        "spread_at_entry":          pos["spread_at_entry"] if pos["spread_at_entry"] is not None else "",
                        "std_dev_at_entry":         pos["std_dev_at_entry"],
                        "time_decay_at_entry":      pos["time_decay_at_entry"],
                        "nws_prob_at_entry":        pos["nws_prob_at_entry"],
                        "forecast_temp_at_entry":   pos["forecast_temp_at_entry"] if pos["forecast_temp_at_entry"] is not None else "",
                        "consensus_at_entry":        pos["consensus_at_entry"] if pos["consensus_at_entry"] is not None else "",
                        "hourly_adjusted_at_entry": pos["hourly_adjusted_at_entry"],
                    })
            except Exception as csv_err:
                log.error(f"Paper trade CSV write failed for {ticker}: {csv_err}")

            resolved_tickers.append(ticker)
            log.info(f"📝 PAPER RESOLVED: {ticker} → {result}, PnL: {pnl:+.0f}¢")

        except Exception as exc:
            log.error(f"resolve_paper_trades: error checking {ticker}: {exc}")

        time.sleep(0.15)   # Be respectful to the Kalshi API

    for ticker in resolved_tickers:
        del _PAPER_POSITIONS[ticker]


def reset_paper_trading():
    """
    Called at midnight UTC. Logs how many positions are still open.
    Does NOT clear positions — tomorrow market entries need to survive
    overnight to resolve the next day.
    """
    if _PAPER_POSITIONS:
        log.info(
            f"reset_paper_trading: {len(_PAPER_POSITIONS)} position(s) still open "
            f"at midnight (will resolve naturally): {list(_PAPER_POSITIONS.keys())}"
        )
    else:
        log.info("Paper trading: no open positions at midnight.")
