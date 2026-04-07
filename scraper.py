import requests
import json
import os
import csv
import difflib
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

BASE_URL = "https://www.screener.in/concalls/upcoming/"
DATA_FILE = Path("data/known_concalls.json")
STOCKS_CSV = Path("indianStocks.csv")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
WATCHLIST_RAW = os.environ.get("WATCHLIST", "ALL").strip()

FUZZY_THRESHOLD = 0.75


def load_stock_master():
    master = {}

    if not STOCKS_CSV.exists():
        print("[WARN] indianStocks.csv not found. Continuing without symbol lookup.")
        return master

    with STOCKS_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue

            name = row[0].strip()
            bse = row[1].strip() if len(row) > 1 else ""
            nse = row[2].strip() if len(row) > 2 else ""
            industry = row[4].strip() if len(row) > 4 else ""

            if not name or name.lower() == "name":
                continue

            master[name.lower()] = {
                "name": name,
                "bse": bse,
                "nse": nse,
                "industry": industry,
            }

    print(f"[INFO] Loaded {len(master)} stocks from CSV")
    return master


def find_stock_info(company_name, master):
    query = company_name.lower().strip()

    if query in master:
        return master[query]

    keys = list(master.keys())
    matches = difflib.get_close_matches(query, keys, n=1, cutoff=FUZZY_THRESHOLD)
    if matches:
        return master[matches[0]]

    clean = query.rstrip(". ")
    for k, v in master.items():
        if clean in k or k in clean:
            return v
        if len(clean) >= 8 and k.startswith(clean[:8]):
            return v

    return {}


def build_watchlist():
    raw = WATCHLIST_RAW.upper()
    if not raw or raw == "ALL":
        return set()

    items = [x.strip().upper() for x in raw.split(",") if x.strip()]
    return set(items)


def is_in_watchlist(stock_info, company_name, watchlist):
    if not watchlist:
        return True

    nse = stock_info.get("nse", "").upper()
    bse = str(stock_info.get("bse", "")).upper()
    company_upper = company_name.upper()

    for item in watchlist:
        if item == nse or item == bse:
            return True
        if item in company_upper:
            return True

    return False


def fetch_concalls():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer": "https://www.screener.in/",
    }

    r = requests.get(BASE_URL, headers=headers, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")

    if not table:
        print("[ERROR] No table found on Screener page.")
        return []

    rows = table.find_all("tr")[1:]
    concalls = []

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        links = tds[0].find_all("a")
        if not links:
            continue

        company = ""
        pdf_url = ""

        for a in links:
            text = a.get_text(" ", strip=True).replace("", "").strip()
            href = a.get("href", "").strip()

            if text:
                company = text
                pdf_url = href
                break

        if not company:
            continue

        date_str = tds[1].get_text(" ", strip=True)
        time_str = tds[2].get_text(" ", strip=True)

        concalls.append({
            "company": company,
            "pdf": pdf_url,
            "date": date_str,
            "time": time_str,
        })

    print(f"[INFO] Fetched {len(concalls)} concalls from Screener")
    return concalls


def make_key(item):
    return f"{item['company']}|{item['date']}|{item['time']}"


def load_known():
    if not DATA_FILE.exists():
        return set()

    try:
        with DATA_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data)
    except Exception:
        return set()


def save_known(keys):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w", encoding="utf-8") as f:
        json.dump(sorted(list(keys)), f, indent=2)


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] Telegram token/chat id missing.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    r = requests.post(url, data=payload, timeout=20)

    if r.status_code != 200:
        print(f"[ERROR] Telegram failed: {r.status_code} | {r.text}")
        r.raise_for_status()

    print("[INFO] Telegram message sent.")


def send_in_batches(lines, header):
    separator = "\n\n--------------------\n\n"
    batch = header

    for line in lines:
        candidate = batch + separator + line if batch else line

        if len(candidate) > 3500:
            if batch:
                send_telegram(batch)
            batch = header + "\n\n" + line
        else:
            batch = candidate

    if batch:
        send_telegram(batch)


def notify():
    now = datetime.now().strftime("%d %b %Y %I:%M %p IST")

    stock_master = load_stock_master()
    watchlist = build_watchlist()
    current_concalls = fetch_concalls()
    known_keys = load_known()

    new_items_all = []
    new_items_filtered = []
    updated_keys = set(known_keys)

    for item in current_concalls:
        key = make_key(item)

        if key not in known_keys:
            new_items_all.append(item)

            stock_info = find_stock_info(item["company"], stock_master)
            item["nse"] = stock_info.get("nse", "")
            item["bse"] = stock_info.get("bse", "")
            item["industry"] = stock_info.get("industry", "")

            if is_in_watchlist(stock_info, item["company"], watchlist):
                new_items_filtered.append(item)

        updated_keys.add(key)

    save_known(updated_keys)

    watchlist_note = "(All Stocks)" if not watchlist else f"(Watchlist: {', '.join(sorted(watchlist))})"
    print(f"[{now}] New: {len(new_items_all)} | Notify: {len(new_items_filtered)} {watchlist_note}")

    if not new_items_filtered:
        print("[INFO] No matching new concalls to notify.")
        return

    header = (
        f"New Upcoming Concalls {watchlist_note}\n"
        f"Checked: {now}\n"
        f"Count: {len(new_items_filtered)}"
    )

    lines = []

    for item in new_items_filtered:
        parts = []
        parts.append(f"Company: {item['company']}")

        symbol_parts = []
        if item.get("nse"):
            symbol_parts.append(f"NSE: {item['nse']}")
        if item.get("bse"):
            symbol_parts.append(f"BSE: {item['bse']}")
        if symbol_parts:
            parts.append(" | ".join(symbol_parts))

        if item.get("industry"):
            parts.append(f"Industry: {item['industry']}")

        parts.append(f"Date: {item['date']}")
        parts.append(f"Time: {item['time']}")

        if item.get("pdf"):
            parts.append(f"Notice: {item['pdf']}")

        if item.get("nse"):
            parts.append(f"Screener: https://www.screener.in/company/{item['nse']}/")

        lines.append("\n".join(parts))

    send_in_batches(lines, header)


if __name__ == "__main__":
    notify()
