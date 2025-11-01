# notifier_playwright.py  (zoneinfo version)
import os, json, time, math, re, traceback
import requests
from typing import Dict, List, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo
from playwright.sync_api import sync_playwright

# ---------- ENV ----------
LOGIN_USER = os.environ["LOGIN_USER"]
LOGIN_PASS = os.environ["LOGIN_PASS"]
PAGE_URL   = os.environ["PAGE_URL"]

TG_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

GIST_ID    = os.environ["GIST_ID"]
GIST_TOKEN = os.environ["GIST_TOKEN"]
GIST_FILENAME = os.getenv("GIST_FILENAME", "keitaro_spend_state.json")

# Пороги
SPEND_ABS  = float(os.getenv("SPEND_ABS_THRESHOLD", "1"))   # минимум $ для алерта
SPEND_PCT  = float(os.getenv("SPEND_PCT_THRESHOLD", "30"))  # опц. порог % (доп.)
SPEND_DIR  = os.getenv("SPEND_DIRECTION", "both").lower()   # up|down|both

# Київський час (вбудований zoneinfo)
KYIV_TZ = ZoneInfo(os.getenv("KYIV_TZ", "Europe/Kyiv"))
MIN_SPEND_DELTA = max(1.0, SPEND_ABS)  # як просив: не сповіщати < $1

# ---------- TIME ----------
def now_kyiv() -> datetime:
    return datetime.now(KYIV_TZ)

def kyiv_today_str() -> str:
    return now_kyiv().strftime("%Y-%m-%d")

# ---------- STATE ----------
def load_state() -> Dict:
    url = f"https://api.github.com/gists/{GIST_ID}"
    r = requests.get(url, headers={
        "Authorization": f"Bearer {GIST_TOKEN}",
        "Accept": "application/vnd.github+json"
    })
    if r.status_code == 200:
        files = r.json().get("files", {})
        if GIST_FILENAME in files and "content" in files[GIST_FILENAME]:
            try:
                return json.loads(files[GIST_FILENAME]["content"])
            except Exception:
                pass
    return {"date": kyiv_today_str(), "rows": {}}

def save_state(state: Dict):
    url = f"https://api.github.com/gists/{GIST_ID}"
    files = {GIST_FILENAME: {"content": json.dumps(state, ensure_ascii=False, indent=2)}}
    r = requests.patch(url, headers={
        "Authorization": f"Bearer {GIST_TOKEN}",
        "Accept": "application/vnd.github+json"
    }, json={"files": files})
    r.raise_for_status()

# ---------- TG ----------
def tg_send(text: str, parse_mode: str = "Markdown"):
    requests.post(
        f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
        json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True},
        timeout=20
    )

# ---------- UTILS ----------
def fmt_money(x: float) -> str:
    return f"${x:,.2f}"

def pct(delta: float, base: float) -> float:
    if base == 0:
        return 100.0 if delta != 0 else 0.0
    return abs(delta / base) * 100.0

def direction_ok(delta: float) -> bool:
    if SPEND_DIR == "up": return delta > 0
    if SPEND_DIR == "down": return delta < 0
    return True

# ---------- PARSE ----------
def parse_report_from_json(payload: dict) -> List[Dict]:
    rows = []
    for r in payload.get("rows", []):
        dims = r.get("dimensions", {}) if isinstance(r.get("dimensions"), dict) else {}
        def g(key):  # dimension value
            return r.get(key) or dims.get(key) or ""

        rows.append({
            "campaign": str(g("campaign")),
            "sub_id_6": str(g("sub_id_6")),
            "sub_id_5": str(g("sub_id_5")),
            "sub_id_4": str(g("sub_id_4")),
            "country_flag": str(g("country_flag")),
            "clicks": float(r.get("clicks", 0) or 0),
            "leads":  float(r.get("leads", 0) or 0),
            "sales":  float(r.get("sales", 0) or 0),
            "cpa":    float(r.get("cpa", 0) or 0),
            "cost":   float(r.get("cost", 0) or 0),
        })
    return rows

def parse_report_from_html(page) -> List[Dict]:
    rows = []
    page.wait_for_selector("table", timeout=15000)
    tables = page.query_selector_all("table")
    target = None
    for t in tables:
        head_text = (t.query_selector("thead") or t).inner_text().lower()
        if all(x in head_text for x in ["leads", "sales", "cpa", "cost"]):
            target = t
            break
    if not target:
        return rows

    headers = [ (th.inner_text() or "").strip().lower()
                for th in target.query_selector_all("thead tr th") ]

    def col_idx(names: List[str]) -> int:
        for i, h in enumerate(headers):
            for n in names:
                if n in h: return i
        return -1

    idx = {
        "campaign": col_idx(["campaign"]),
        "sid6":     col_idx(["sub id 6","sub_id_6","subid6","sub id6"]),
        "sid5":     col_idx(["sub id 5","sub_id_5","subid5","sub id5"]),
        "sid4":     col_idx(["sub id 4","sub_id_4","subid4","sub id4"]),
        "clicks":   col_idx(["clicks"]),
        "leads":    col_idx(["leads"]),
        "sales":    col_idx(["sales"]),
        "cpa":      col_idx(["cpa"]),
        "cost":     col_idx(["cost"]),
    }

    for tr in target.query_selector_all("tbody tr"):
        tds = tr.query_selector_all("td")
        def safe(i):
            try: return (tds[i].inner_text() or "").strip()
            except: return ""
        def to_f(s: str) -> float:
            s = s.replace("$","").replace(",","").strip()
            try: return float(s)
            except: return 0.0

        rows.append({
            "campaign": safe(idx["campaign"]),
            "sub_id_6": safe(idx["sid6"]),
            "sub_id_5": safe(idx["sid5"]),
            "sub_id_4": safe(idx["sid4"]),
            "country_flag": "",
            "clicks": to_f(safe(idx["clicks"])),
            "leads":  to_f(safe(idx["leads"])),
            "sales":  to_f(safe(idx["sales"])),
            "cpa":    to_f(safe(idx["cpa"])),
            "cost":   to_f(safe(idx["cost"])),
        })
    return rows

# ---------- SCRAPE ----------
def fetch_rows_via_playwright() -> List[Dict]:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # login
        page.goto("https://trident.partners/admin/")
        page.fill("input[type='text'], input[name='login']", LOGIN_USER)
        page.fill("input[type='password'], input[name='password']", LOGIN_PASS)
        page.get_by_role("button", name=re.compile("sign in", re.I)).click()

        captured: List[Dict] = []
        def on_response(resp):
            if "/report" in resp.url or "/reports" in resp.url:
                try:
                    data = resp.json()
                    rs = parse_report_from_json(data)
                    if rs: captured[:] = rs
                except: pass
        ctx.on("response", on_response)

        page.goto(PAGE_URL, wait_until="networkidle")
        for _ in range(12):
            if captured: break
            time.sleep(0.5)

        if not captured:
            try:
                captured = parse_report_from_html(page)
            except: pass

        browser.close()
        return captured

# ---------- DIFF ----------
def row_key(r: Dict) -> str:
    return f"{r.get('campaign','')}|{r.get('sub_id_6','')}|{r.get('sub_id_5','')}|{r.get('sub_id_4','')}"

def detect_changes(prev_map: Dict[str, Dict], rows: List[Dict]) -> Tuple[List[str], Dict]:
    msgs = []
    new_map = {}

    for r in rows:
        k = row_key(r)
        new_map[k] = {
            "campaign": r["campaign"], "sub_id_6": r["sub_id_6"],
            "sub_id_5": r["sub_id_5"], "sub_id_4": r["sub_id_4"],
            "clicks": r["clicks"], "leads": r["leads"], "sales": r["sales"],
            "cpa": r["cpa"], "cost": r["cost"]
        }

        prev = prev_map.get(k)
        if not prev:
            continue

        # spend
        delta_cost = r["cost"] - float(prev.get("cost", 0))
        if direction_ok(delta_cost) and abs(delta_cost) >= MIN_SPEND_DELTA:
            base = float(prev.get("cost", 0))
            pp = pct(delta_cost, base) if base else 100.0
            up = "🔺" if delta_cost > 0 else "🔻"
            msgs.append(
                "🧊 *SPEND ALERT*\n"
                f"*CAMPAIGN:* {r['campaign']}\n"
                f"*SubID5:* {r['sub_id_5']}  *SubID4:* {r['sub_id_4']}\n"
                f"*Cost:* {fmt_money(base)} → *{fmt_money(r['cost'])}*  (Δ {fmt_money(delta_cost)}, ~{pp:.0f}%) {up}"
            )

        # leads
        if r["leads"] > float(prev.get("leads", 0)):
            cpa_part = f"  • *CPA:* {fmt_money(r['cpa'])}" if r["cpa"] > 0 else ""
            msgs.append(
                "🟩 *LEAD ALERT*\n"
                f"*CAMPAIGN:* {r['campaign']}\n"
                f"*SubID5:* {r['sub_id_5']}  *SubID4:* {r['sub_id_4']}\n"
                f"*Leads:* {int(prev.get('leads', 0))} → *{int(r['leads'])}*{cpa_part}"
            )

        # sales
        if r["sales"] > float(prev.get("sales", 0)):
            msgs.append(
                "🟦 *SALE ALERT*\n"
                f"*CAMPAIGN:* {r['campaign']}\n"
                f"*SubID5:* {r['sub_id_5']}  *SubID4:* {r['sub_id_4']}\n"
                f"*Sales:* {int(prev.get('sales', 0))} → *{int(r['sales'])}*"
            )

    return msgs, new_map

# ---------- MAIN ----------
def main():
    state = load_state()
    prev_date = state.get("date")
    prev_rows = state.get("rows", {})

    try:
        rows = fetch_rows_via_playwright()
    except Exception as e:
        tg_send(f"⚠️ Не зміг отримати дані звіту: {e}\n```\n{traceback.format_exc()}\n```")
        return

    if not rows:
        tg_send("⚠️ Дані не отримані: жодного рядка. Перевір логін/URL/фільтри.")
        return

    today = kyiv_today_str()

    # М’який ресет у новий день (Europe/Kyiv) — без алертів
    if prev_date != today:
        new_state = {"date": today, "rows": {row_key(r): {
            "campaign": r["campaign"], "sub_id_6": r["sub_id_6"],
            "sub_id_5": r["sub_id_5"], "sub_id_4": r["sub_id_4"],
            "clicks": r["clicks"], "leads": r["leads"], "sales": r["sales"],
            "cpa": r["cpa"], "cost": r["cost"]
        } for r in rows}}
        save_state(new_state)
        tg_send("🕛 Новий день (Europe/Kyiv). Оновив базовий стан без алертів.")
        return

    msgs, new_map = detect_changes(prev_rows, rows)

    if not msgs:
        tg_send("accs on vacation...")
    else:
        for m in msgs:
            tg_send(m)

    save_state({"date": today, "rows": new_map})

if __name__ == "__main__":
    main()
