# screener-concall-bot
Upcoming Concalls 
# Screener Concall Bot v2

Runs at 6AM and 6PM IST via GitHub Actions.
Sends Telegram alerts for newly added upcoming concalls on Screener.in.

## Setup

1. Fork/clone this repo
2. Add `indianStocks.csv` to root folder
3. Add these GitHub Secrets:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - `WATCHLIST` (optional, e.g. `TCS,INFY,HDFCAMC`)
4. Enable GitHub Actions
5. Run manually once from Actions tab to initialize

## Watchlist examples
- `TCS` → matches by NSE symbol
- `532174` → matches by BSE code
- `Nazara` → matches by name keyword
- *(blank)* → sends alerts for ALL stocks
