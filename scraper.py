import requests
import json
import os
import csv
import difflib
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL           = "https://www.screener.in/concalls/upcoming/"
DATA_FILE          = Path("data/known_concalls.json")
STOCKS_CSV         = Path("indianStocks.csv")
FUZZY_THRESHOLD    = 0.75

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
WATCHLIST_RAW      = os.environ.get("WATCHLIST", "ALL")


# ── Load CSV ──────────────────────────────────────────────────────────────────
def load_stock_master():
    master = {}
    if not STOCKS_CSV.exists():
        print("[WARN] indianStocks.csv not found.")
        return master

    with STOCKS_CSV.open(encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            name = row[0].strip()
            if not name or name.lower() == "name":
                continue
            master[name.lower()] = {
                "name": row[0].strip(),
                "bse": row[1].strip() if len(row) > 1 else "",
                "nse": row[2].strip() if len(row) > 2 else "",
                "industry": row[4].strip() if len(row) > 4 else "",
            }
    print(f"[INFO] Loaded {len(master)} stocks from CSV")
    return master


def find_stock_info(company_name, master):
    query = company_name.lower().strip()
    if query in master:
        return master[query]

    matches = difflib.get_close_matches(query, list(master.keys()), n=1, cutoff=FUZZY_THRESHOLD)
    if matches:
        return master[matches[0]]

    clean = query.rstrip(". ")
    for k, v in master.items():
        if clean in k or k in clean or (len(clean) >= 8 and k.startswith(clean[:8])):
            return v
    return {}


def build_watchlist():
    raw = WATCHLIST_RAW.strip().upper()
    if not raw or raw == "ALL":
        return set()
    return set(x.strip().upper() for x in WATCHLIST_RAW.split(",") if x.strip())


def is_in_watchlist(stock_info, company_name, watchlist):
    if not watchlist:
        return True
    nse = stock_info.get("nse", "").upper()
    bse = str(stock_info.get("bse", "")).upper()
    name_upper = company_name.upper()
    for item in watchlist:
        if item in (nse, bse) or item in name_upper:
            return True
    return False


# ── Scrape ────────────────────────────────────────────────────────────────────
def fetch_concalls():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.screener.in/",
    }

    r = requests.get(BASE_URL, headers=headers, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", {"id": "result_list"}) or soup.find("table")
    if not table:
        print("[ERROR] No table found.")
        return []

    concalls = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 3:
            continue

        link = cells[0].find("a")
        if not link:
            continue

        company = link.get_text(strip=True).strip()
        pdf_url = link.get("href", "").strip()
        if pdf_url and not pdf_url.startswith("http"):
            pdf_url = "https://www.screener.in" + pdf_url

        date_str = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        time_str = cells[2].get_text(strip=True) if len(cells) > 2 else ""

        if company and date_str:
            concalls.append({
                "company": company,
                "pdf": pdf_url,
                "date": date_str,
                "time": time_str,
            })

    print(f"[INFO] Fetched {len(concalls)} concalls from Screener")
    return concalls


def make_key(item):
    return f"{item['company']}|{item['date']}|{item['time']}|{item.get('pdf', '')}"


def load_known():
    if not DATA_FILE.exists():
        return set()
    with DATA_FILE.open(encoding="utf-8") as f:
        return set(json.load(f))


def save_known(keys):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w", encoding="utf-8") as f:
        json.dump(sorted(list(keys)), f, indent=2)


# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] Telegram credentials missing.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, data=payload, timeout=20)
        r.raise_for_status()
        print("[INFO] Beautiful message sent successfully.")
    except Exception as e:
        print(f"[ERROR] Telegram failed: {e}")


def send_in_batches(lines, header):
    sep = "\n\n─────────────────\n\n"
    batch = header
    for line in lines:
        candidate = batch + (sep if batch != header else "\n\n") + line
        if len(candidate) > 3900:
            send_telegram(batch)
            batch = header + "\n\n" + line
        else:
            batch = candidate
    if batch:
        send_telegram(batch)


# ── Main ──────────────────────────────────────────────────────────────────────
def notify():
    now = datetime.now().strftime("%d %b %Y %I:%M %p IST")
    master = load_stock_master()
    watchlist = build_watchlist()
    current = fetch_concalls()
    known = load_known()

    new_watch = []
    new_keys = set(known)
    skipped = 0

    for item in current:
        k = make_key(item)
        if k in known:
            skipped += 1
            continue

        info = find_stock_info(item["company"], master)
        item["nse"] = info.get("nse", "")
        item["bse"] = info.get("bse", "")
        item["industry"] = info.get("industry", "")

        if is_in_watchlist(info, item["company"], watchlist):
            new_watch.append(item)

        new_keys.add(k)

    save_known(new_keys)

    wl_note = " (All Stocks)" if not watchlist else f" (Watchlist: {', '.join(sorted(watchlist))})"
    print(f"[{now}] New: {len(new_watch)} | Skipped old: {skipped}{wl_note}")

    if not new_watch:
        print("[INFO] No new concalls today.")
        return

    header = f"🆕 <b>New Upcoming Concalls Added</b>{wl_note}\n🕐 {now}\n📊 <b>{len(new_watch)}</b> new concall(s) found"

    lines = []
    for item in new_watch:
        sym = []
        if item.get("nse"): sym.append(f'<code>NSE: {item["nse"]}</code>')
        if item.get("bse"): sym.append(f'<code>BSE: {item["bse"]}</code>')
        sym_line = "  |  ".join(sym) if sym else "Symbol: N/A"

        industry_line = f"🏭 {item.get('industry')}" if item.get("industry") else ""

        line = (
            f"📞 <b>{item['company']}</b>\n"
            f"{sym_line}\n"
            f"{industry_line}\n"
            f"📅 {item['date']}  ⏰ {item['time']}\n"
            f'📄 <a href="{item["pdf"]}">📥 Download Concall Notice (PDF)</a>'
        )
        if item.get("nse"):
            line += f'\n🔗 <a href="https://www.screener.in/company/{item["nse"]}/">🌐 View Company on Screener</a>'

        lines.append(line)

    send_in_batches(lines, header)


if __name__ == "__main__":
    notify()
