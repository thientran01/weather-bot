# Kalshi Climate Bot

## Project Overview
A Python script that monitors Kalshi climate markets and compares them
against NWS weather forecasts to suggest potential trades. Does NOT
execute trades — suggestions only.

## My Skill Level
I am a beginner. Always:
- Write clear comments explaining what every section does
- Keep code in as few files as possible
- Avoid complex abstractions or clever patterns
- Explain your reasoning before writing code
- If something could break, add error handling

## Stack
- Python 3
- Kalshi REST API (read-only market data)
- api.weather.gov (NWS) — free, no API key needed
- Gmail SMTP for text/email alerts
- .env file for all credentials

## Cities
NYC, Chicago, Miami, Los Angeles

## Markets to Watch
- High temperature ranges
- Low temperature ranges

## Alert Logic
- Run every 10 minutes continuously
- Fetch Kalshi odds for each city's temp markets
- Fetch NWS forecast and calculate implied probability per range
- Compare and show ALL markets with gap size — I decide what to trade
- Send summary via text/email every refresh cycle
- Log everything to a CSV for historical tracking

## File Structure
- bot.py — main script
- .env — all credentials (never commit this)
- log.csv — historical data log
- README.md — setup instructions

## Commands to Run
- Start bot: python bot.py
- Install dependencies: pip install -r requirements.txt

## Hard Rules
- Never hardcode API keys or credentials
- Always use try/except around API calls so crashes don't stop the bot
- If an API is down, log the error and retry next cycle — don't crash
- Keep the text/email alert clean and scannable
