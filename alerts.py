"""
alerts.py — Email formatting and sending via SendGrid.

Includes email quality filters, market card rendering, morning briefing,
evening summary, paper trade filters, and the SendGrid HTTP send call.
"""

import requests
from datetime import datetime, timedelta
from config import (
    CITIES, TIER1_CITIES,
    SENDGRID_API_KEY, ALERT_FROM_EMAIL, ALERT_TO_EMAIL, ALERT_TO_EMAIL_2,
    log, _et_now,
)
from analysis import _get_model_data


# ============================================================
# SECTION 7 — ALERTS
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

    Rule 3 — Exclude high uncertainty: model spread < 8°F.
              When models disagree by ≥8° there's no reliable signal.

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
    if md["spread"] is not None and md["spread"] >= 8:
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


def _passes_paper_trade_filters(g, md):
    """
    Returns True if a market passes all paper-trading quality gates.
    Mirrors _apply_email_filters() exactly, but kept as a separate function
    so the two systems can diverge independently in the future.

    Rule 1 — Exclude settled: Kalshi price must be 10–90 (inclusive).
    Rule 2 — Require an edge: |gap| >= 15%.
    Rule 3 — Exclude high uncertainty: model spread < 8°F.
    Rule 4 — Exclude impossible outcomes: consensus within 5°F of bucket.
    """
    # Rule 1: not settled
    if g["was_settled"]:
        return False

    # Rule 2: meaningful edge
    if abs(g["gap"]) < 15:
        return False

    # Rule 3: models must broadly agree (8°F threshold)
    if md["spread"] is not None and md["spread"] >= 8:
        return False

    # Rule 4: consensus within 5°F of bucket boundaries
    if md["consensus"] is not None:
        c  = md["consensus"]
        bt = g["bucket_type"]
        fl = g["floor"]
        cp = g["cap"]

        if bt == "FLOOR" and fl is not None and c < fl - 5:
            return False   # consensus far below floor — clearly NO
        if bt == "CAP"   and cp is not None and c > cp + 5:
            return False   # consensus far above cap — clearly NO
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
      ⚠️ HIGH SPREAD   (appended to models line only if spread ≥ 5°F)

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

    recipients = [{"email": ALERT_TO_EMAIL}]
    if ALERT_TO_EMAIL_2:
        recipients.append({"email": ALERT_TO_EMAIL_2})

    try:
        response = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "personalizations": [{"to": recipients}],
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
