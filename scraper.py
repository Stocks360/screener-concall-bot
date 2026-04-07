#!/usr/bin/env python3
"""
Screener Upcoming Concalls Bot - v2
- Scrapes screener.in/concalls/upcoming/
- Matches company names with indianStocks.csv
- Filters by WATCHLIST if provided
- Sends Telegram alerts for newly added concalls
"""

import os
import json
import csv
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
from difflib import SequenceMatcher

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_URL        = "https://www.screener.in/concalls/upcoming/"
SCREENER_BASE   = "https://www.screener.in"
DATA_FILE       = Path("data/known_concalls.json")
CSV_FILE        = Path("indianStocks.csv")

TELEGRAM_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT   = os.environ.get("TELEGRAM_CHAT_ID", "")
WATCHLIST_RAW   = os.environ.get("WATCHLIST", "")          # e.g. "TCS,INFY,HDFCAMC"

WATCHLIST = [w.strip().upper() for w in WATCHLIST_RAW.split(",") if w.strip()]

# ─── Load CSV ─────────────────────────────────────────────────────────────────
def load_stock_csv():
    """Load indianStocks.csv into a list of dicts."""
    stocks = []
    if not CSV_FILE.exists():
        print("⚠️  indianStocks.csv not found. Symbol lookup will be skipped.")
        return stocks
    with CSV_FILE.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stocks.append({k.strip(): v.strip() for k, v in row.items()})
    print(f"✅ Loaded {len(stocks)} stocks from CSV.")
    return stocks

# ─── Match company name to CSV ────────────────────────────────────────────────
def find_stock_info(company_name, stocks):
    """Try to match scraped company name to a CSV row."""
    name_clean = company_name.lower().replace(".", "").replace("ltd", "").strip()

    best_match = None
    best_score = 0.0

    for s in stocks:
        # Get name field — try common column names
        csv_name = (
            s.get("Company Name") or s.get("NAME") or
            s.get("name") or s.get("COMPANY") or ""
        ).lower().replace(".", "").replace("ltd", "").strip()

        if not csv_name:
            continue

        # Exact match
        if name_clean == csv_name:
            return s

        # Fuzzy match
        score = SequenceMatcher(None, name_clean, csv_name).ratio()
        if score > best_score:
            best_score = score
            best_match = s

        # Substring match
        if name_clean in csv_name or csv_name in name_clean:
            if score > 0.5:
                return s

    if best_score >= 0.75 and best_match:
        return best_match

    return None

# ─── Watchlist Filter ─────────────────────────────────────────────────────────
def passes_watchlist(company_name, stock_info):
    """Return True if no watchlist set, or stock matches watchlist."""
    if not WATCHLIST:
        return True  # no filter — send all

    name_upper = company_name.upper()

    for w in WATCHLIST:
        # Match against company name
        if w in name_upper:
            return True
        if stock_info:
            # Match against NSE symbol
            nse = (stock_info.get("NSE Symbol") or stock_info.get("SYMBOL") or
                   stock_info.get("symbol") or "").upper()
            # Match against BSE code
            bse = str(stock_info.get("BSE Code") or stock_info.get("BSE") or
                      stock_info.get("bse") or "").upper()
            if w == nse or w == bse:
                return True
    return False

# ─── Scrape Screener ──────────────────────────────────────────────────────────
def fetch_concalls():
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    r = requests.get(BASE_URL, headers=headers, timeout=15)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        print("❌ No table found on Screener page!")
        return []

    rows = table.find_all("tr")[1:]  # skip header
    concalls = []

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        link_tag = tds[0].find("a")
        if not link_tag:
            continue

        company  = link_tag.get_text(strip=True)
        href     = link_tag.get("href", "")

        # Build full URL
        if href.startswith("http"):
            doc_url = href
        else:
            doc_url = SCREENER_BASE + href

        date_str = tds[1].get_text(strip=True)
        time_str = tds[2].get_text(strip=True)

        # Try to get Screener company page link from second anchor if present
        all_links = tds[0].find_all("a")
        screener_link = ""
        for a in all_links:
            h = a.get("href", "")
            if "/company/" in h:
                screener_link = SCREENER_BASE + h
                break

        concalls.append({
            "company":       company,
            "doc_url":       doc_url,
            "screener_link": screener_link,
            "date":          date_str,
            "time":          time_str,
        })

    print(f"✅ Scraped {len(concalls)} concalls from Screener.")
    return concalls

# ─── Persistence ─────────────────────────────────────────────────────────────
def make_key(item):
    return f"{item['company']}|{item['date']}|{item['time']}"

def load_known():
    if not DATA_FILE.exists():
        return set()
    with DATA_FILE.open() as f:
        return set(json.load(f))

def save_known(keys):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w") as f:
        json.dump(sorted(list(keys)), f, indent=2)

# ─── Telegram ────────────────────────────────────────────────────────────────
def send_telegram(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        print("⚠️  Telegram credentials missing. Skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT,
        "text":       text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, data=payload, timeout=10)
    r.raise_for_status()
    print("✅ Telegram message sent.")

def build_message(new_items, stocks):
    now = datetime.now().strftime("%d %b %Y %I:%M %p")
    wl_label = ", ".join(WATCHLIST) if WATCHLIST else "All Stocks"

    lines = [
        f"🆕 <b>New Upcoming Concalls Added</b>",
        f"📊 Watchlist: {wl_label}",
        f"🕐 Checked: {now} IST",
        f"📌 {len(new_items)} new concall(s) found\n",
    ]

    for item in new_items:
        stock_info = find_stock_info(item["company"], stocks)

        nse = (stock_info.get("NSE Symbol") or stock_info.get("SYMBOL") or
               stock_info.get("symbol") or "N/A") if stock_info else "N/A"
        bse = str(stock_info.get("BSE Code") or stock_info.get("BSE") or
                  stock_info.get("bse") or "N/A") if stock_info else "N/A"
        industry = (stock_info.get("Industry") or stock_info.get("INDUSTRY") or
                    stock_info.get("industry") or "") if stock_info else ""

        block = [f"📞 <b>{item['company']}</b>"]
        if nse != "N/A" or bse != "N/A":
            block.append(f"NSE: de>{nse}</code>  |  BSE: de>{bse}</code>")
        if industry:
            block.append(f"🏭 {industry}")
        block.append(f"📅 {item['date']}  ⏰ {item['time']}")
        if item["doc_url"]:
            block.append(f'📄 <a href="{item["doc_url"]}">Concall Notice</a>')
        if item["screener_link"]:
            block.append(f'🔗 <a href="{item["screener_link"]}">Screener Page</a>')

        lines.append("\n".join(block))
        lines.append("─" * 20)

    return "\n".join(lines)

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*50}")
    print(f"Screener Concall Bot starting...")
    print(f"Watchlist: {WATCHLIST if WATCHLIST else 'All Stocks'}")
    print(f"{'='*50}\n")

    stocks   = load_stock_csv()
    current  = fetch_concalls()
    known    = load_known()

    new_items  = []
    all_keys   = set(known)

    for item in current:
        k = make_key(item)
        if k not in known:
            stock_info = find_stock_info(item["company"], stocks)
            if passes_watchlist(item["company"], stock_info):
                new_items.append(item)
            all_keys.add(k)

    save_known(all_keys)
    print(f"✅ {len(new_items)} new concall(s) matched filter.")

    if new_items:
        msg = build_message(new_items, stocks)
        send_telegram(msg)
    else:
        print("ℹ️  No new concalls to report.")

if __name__ == "__main__":
    main()
