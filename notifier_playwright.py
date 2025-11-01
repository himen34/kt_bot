# notifier_playwright.py  — Keitaro → Telegram (Playwright, zoneinfo)
import os, json, time, re, traceback
from typing import Dict, List, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PWTimeout

# -------- ENV --------
LOGIN_USER = os.environ["LOGIN_USER"]
LOGIN_PASS = os.environ["LOGIN_PASS"]
PAGE_URL   = os.environ["PAGE_URL"]

TG_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

GIST_ID    = os.environ["GIST_ID"]
GIST_TOKEN = os.environ["GIST_TOKEN"]
GIST_FILENAME = os.getenv("GIST_FILENAME", "keitaro_spend_state.json")

# Пороги
SPEND_ABS  = float(os.getenv("SPEND_ABS_THRESHOLD", "1"))   # $-порог
SPEND_PCT  = float(os.getenv("SPEND_PCT_THRESHOLD", "30"))  # % (опционально)
SPEND_DIR  = os.getenv("SPEND_DIRECTION", "both").lower()   # up|down|both
MIN_SPEND_DELTA = max(1.0, SPEND_ABS)                       # не алертить < $1

KYIV_TZ = ZoneInfo(os.getenv("KYIV_TZ", "Europe/Kyiv"))

# -------- utils --------
def now_kyiv() -> datetime:
    return datetime.now(KYIV_TZ)

def kyiv_today_str() -> str:
    return now_kyiv().strftime("%Y-%m-%d")

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

# -------- state (Gist) --------
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

# -------- telegram --------
def tg_send(text: str, parse_mode: str = "Markdown"):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True},
            timeout=20
        )
    except Exception:
        pass

def diag(msg: str):
    tg_send(f"🛠️ Debug: {msg}")

# -------- parsers --------
def parse_report_from_json(payload: dict) -> List[Dict]:
    rows = []
    for r in payload.get("rows", []):
        dims = r.get("dimensions", {}) if isinstance(r.get("dimensions"), dict) else {}
        def g(key):  # dimension value
            return r.get(key) or dims.get(key) or ""

        def to_f(val):
            try: return float(val or 0)
            except: return 0.0

        rows.append({
            "campaign": str(g("campaign")),
            "sub_id_6": str(g("sub_id_6")),
            "sub_id_5": str(g("sub_id_5")),
            "sub_id_4": str(g("sub_id_4")),
            "country_flag": str(g("country_flag")),
            "clicks": to_f(r.get("clicks")),
            "leads":  to_f(r.get("leads")),
            "sales":  to_f(r.get("sales")),
            "cpa":    to_f(r.get("cpa")),
            "cost":   to_f(r.get("cost")),
        })
    return rows

def parse_report_from_html(page) -> List[Dict]:
    rows = []
    page.wait_for_selector("table", timeout=15000)
    tables = page.query_selector_all("table")
    target = None
    for t in tables:
        head = t.query_selector("thead")
        head_text = (head.inner_text() if head else t.inner_text() or "").lower()
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

# -------- scraping (login + XHR/HTML/ag-Grid) --------
def fetch_rows_via_playwright() -> List[Dict]:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
        )
        page = ctx.new_page()

        # --- Login ---
        page.goto("https://trident.partners/admin/", wait_until="domcontentloaded")
        try:
            page.wait_for_timeout(500)
            login_loc = page.locator("input[name='login'], input[type='text']")
            pass_loc  = page.locator("input[name='password'], input[type='password']")
            login_loc.first.fill(LOGIN_USER)
            pass_loc.first.fill(LOGIN_PASS)
            page.get_by_role("button", name=re.compile("sign in|увійти|войти", re.I)).click()
        except Exception as e:
            diag(f"login fill/click failed: {e}")

        try:
            page.wait_for_selector("app-login", state="detached", timeout=15000)
        except PWTimeout:
            pass  # возможно, уже авторизованы

        # --- Report page + XHR capture ---
        captured_rows: List[Dict] = []

        def on_response(resp):
            url = resp.url
            if "/report" in url or "/reports" in url:
                try:
                    data = resp.json()
                    rs = parse_report_from_json(data)
                    if rs:
                        captured_rows[:] = rs
                except Exception:
                    pass

        ctx.on("response", on_response)
        page.goto(PAGE_URL, wait_until="domcontentloaded")

        ok = False
        for _ in range(30):  # ~15s
            if captured_rows:
                ok = True
                break
            if page.locator("table tbody tr").count() > 0:
                ok = True
                break
            if page.locator(".ag-center-cols-container .ag-row").count() > 0:
                ok = True
                break
            page.wait_for_timeout(500)

        rows: List[Dict] = []
        if captured_rows:
            rows = captured_rows
        else:
            # HTML table fallback
            try:
                if page.locator("table tbody tr").count() > 0:
                    rows = parse_report_from_html(page)
            except Exception as e:
                diag(f"html parse failed: {e}")

            # ag-Grid fallback
            if not rows:
                try:
                    rws = page.locator(".ag-center-cols-container .ag-row")
                    if rws.count() > 0:
                        headers = [ (h.inner_text() or "").strip().lower()
                                    for h in page.locator(".ag-header-cell-text").all() ]

                        def idx(name_variants):
                            for i, h in enumerate(headers):
                                for v in name_variants:
                                    if v in h:
                                        return i
                            return -1

                        i_campaign = idx(["campaign"])
                        i_sid6 = idx(["sub id 6","sub_id_6"])
                        i_sid5 = idx(["sub id 5","sub_id_5"])
                        i_sid4 = idx(["sub id 4","sub_id_4"])
                        i_clicks = idx(["clicks"])
                        i_leads  = idx(["leads"])
                        i_sales  = idx(["sales"])
                        i_cpa    = idx(["cpa"])
                        i_cost   = idx(["cost"])

                        def to_f(s: str) -> float:
                            s = (s or "").replace("$","").replace(",","").strip()
                            try: return float(s)
                            except: return 0.0

                        for row in rws.all():
                            cells = [ (c.inner_text() or "").strip()
                                      for c in row.locator(".ag-cell-value").all() ]
                            def safe(i): 
                                try: return cells[i]
                                except: return ""
                            rows.append({
                                "campaign": safe(i_campaign),
                                "sub_id_6": safe(i_sid6),
                                "sub_id_5": safe(i_sid5),
                                "sub_id_4": safe(i_sid4),
                                "country_flag": "",
                                "clicks": to_f(safe(i_clicks)),
                                "leads":  to_f(safe(i_leads)),
                                "sales":  to_f(safe(i_sales)),
                                "cpa":    to_f(safe(i_cpa)),
                                "cost":   to_f(safe(i_cost)),
                            })
                except Exception as e:
                    diag(f"ag-grid parse failed: {e}")

        if not rows:
            try:
                diag(f"url={page.url}  title={page.title()}")
                diag(f"tableRows={page.locator('table tbody tr').count()}  agRows={page.locator('.ag-center-cols-container .ag-row').count()}")
            except Exception:
                pass

        browser.close()
        return rows

# -------- diff/alerts --------
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

        # --- Spend change ---
        delta_cost = r["cost"] - float(prev.get("cost", 0))
        base_cost  = float(prev.get("cost", 0))
        if direction_ok(delta_cost) and abs(delta_cost) >= MIN_SPEND_DELTA:
            p = pct(delta_cost, base_cost) if base_cost else 100.0
            up = "🔺" if delta_cost > 0 else "🔻"
            # %-порог опционален: если задан — тоже учитываем
            if SPEND_PCT <= 0 or p >= SPEND_PCT:
                msgs.append(
                    "🧊 *SPEND ALERT*\n"
                    f"*CAMPAIGN:* {r['campaign']}\n"
                    f"*SubID5:* {r['sub_id_5']}  *SubID4:* {r['sub_id_4']}\n"
                    f"*Cost:* {fmt_money(base_cost)} → *{fmt_money(r['cost'])}*  (Δ {fmt_money(delta_cost)}, ~{p:.0f}%) {up}"
                )

        # --- Leads ---
        if r["leads"] > float(prev.get("leads", 0)):
            cpa_part = f"  • *CPA:* {fmt_money(r['cpa'])}" if r["cpa"] > 0 else ""
            msgs.append(
                "🟩 *LEAD ALERT*\n"
                f"*CAMPAIGN:* {r['campaign']}\n"
                f"*SubID5:* {r['sub_id_5']}  *SubID4:* {r['sub_id_4']}\n"
                f"*Leads:* {int(prev.get('leads', 0))} → *{int(r['leads'])}*{cpa_part}"
            )

        # --- Sales ---
        if r["sales"] > float(prev.get("sales", 0)):
            msgs.append(
                "🟦 *SALE ALERT*\n"
                f"*CAMPAIGN:* {r['campaign']}\n"
                f"*SubID5:* {r['sub_id_5']}  *SubID4:* {r['sub_id_4']}\n"
                f"*Sales:* {int(prev.get('sales', 0))} → *{int(r['sales'])}*"
            )

    return msgs, new_map

# -------- main --------
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

    # Мягкий ресет в новый день (Europe/Kyiv): просто обновляем базу без алертов
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
