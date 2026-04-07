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

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
WATCHLIST_RAW = os.environ.get("WATCHLIST", "ALL")

FUZZY_THRESHOLD = 0.75

def load_stock_master():
    master = {}
    if not STOCKS_CSV.exists():
        print(f"[WARN] {STOCKS_CSV} not found. Symbol lookup disabled.")
        return master

    with STOCKS_CSV.open(encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue

            name = row[0].strip()
            bse = row[1].strip() if len(row) > 1 else ""
            nse = row[2].strip() if len(row) > 2 else ""
            industry = row[4].strip() if len(row) > 4 else ""

            if name and name.lower() != "name":
                master[name.lower()] = {
                    "name": name,
                    "bse": bse,
                    "nse": nse,
                    "industry": industry,
                }

    print(f"[INFO] Loaded {len(master)} stocks from {STOCKS_CSV}")
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
    raw = WATCHLIST_RAW.strip().upper()
    if not raw or raw == "ALL":
        return set()
    items = [x.strip().upper() for x in WATCHLIST_RAW.split(",") if x.strip()]
    return set(items)

def is_in_watchlist(stock_info, company_name, watchlist):
    if not watchlist:
        return True

    nse = stock_info.get("nse", "").upper()
    bse = str(stock_info.get("bse", "")).upper()
    name_upper = company_name.upper()

    for wl_item in watchlist:
        if wl_item == nse or wl_item == bse:
            return True
        if wl_item in name_upper:
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
        print("[ERROR] No table found on the page.")
        return []

    rows = table.find_all("tr")[1:]
    concalls = []

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        link_tag = tds[0].find("a")
        if not link_tag:
            continue

        company = link_tag.get_text(strip=True)
        href = link_tag.get("href", "")
        pdf_url = href

        date_str = tds[1].get_text(strip=True)
        time_str = tds[2].get_text(strip=True)

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
    with DATA_FILE.open() as f:
        return set(json.load(f))

def save_known(keys):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w") as f:
        json.dump(sorted(list(keys)), f, indent=2)

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
    r = requests.post(url, data=payload, timeout=20)
    r.raise_for_status()
    print("[INFO] Telegram message sent.")

def send_in_batches(lines, header):
    sep = "\n\n─────────────────\n\n"
    batch = header

    for line in lines:
        candidate = batch + (sep if batch != header else "\n\n") + line
        if len(candidate) > 4000:
            send_telegram(batch)
            batch = header + "\n\n" + line
        else:
            batch = candidate

    if batch:
        send_telegram(batch)

def notify():
    now = datetime.now().strftime("%d %b %Y %I:%M %p IST")
    master = load_stock_master()
    watchlist = build_watchlist()
    current = fetch_concalls()
    known = load_known()

    new_all = []
    new_watch = []
    new_keys = set(known)

    for item in current:
        k = make_key(item)
        if k not in known:
            new_all.append(item)

            info = find_stock_info(item["company"], master)
            item["nse"] = info.get("nse", "")
            item["bse"] = info.get("bse", "")
            item["industry"] = info.get("industry", "")

            if is_in_watchlist(info, item["company"], watchlist):
                new_watch.append(item)

        new_keys.add(k)

    save_known(new_keys)

    wl_note = " (All stocks)" if not watchlist else f" (Watchlist: {', '.join(sorted(watchlist))})"
    print(f"[{now}] New entries: {len(new_all)} | Notify: {len(new_watch)}{wl_note}")

    if not new_watch:
        print("[INFO] No matching new concalls to notify.")
        return

    header = (
        f"🆕 <b>New Upcoming Concalls Added</b>{wl_note}\n"
        f"🕐 {now}\n"
        f"📊 {len(new_watch)} new concall(s) found"
    )

    lines = []
    for item in new_watch:
        sym_parts = []
        if item["nse"]:
            sym_parts.append(f"NSE: <code>{item['nse']}</code>")
        if item["bse"]:
            sym_parts.append(f"BSE: <code>{item['bse']}</code>")
        sym_line = "  |  ".join(sym_parts) if sym_parts else "Symbol: N/A"

        industry_line = f"🏭 {item['industry']}" if item["industry"] else ""
        screener_link = f"https://www.screener.in/company/{item['nse']}/" if item["nse"] else ""

        line = (
            f"📞 <b>{item['company']}</b>\n"
            f"{sym_line}\n"
            + (f"{industry_line}\n" if industry_line else "")
            + f"📅 {item['date']}  ⏰ {item['time']}\n"
            + f"📄 <a href=\"{item['pdf']}\">Concall Notice</a>"
            + (f"\n🔗 <a href=\"{screener_link}\">Screener Page</a>" if screener_link else "")
        )
        lines.append(line)

    send_in_batches(lines, header)

if __name__ == "__main__":
    notify()
