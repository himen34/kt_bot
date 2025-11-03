# notifier_playwright.py — per-change blocks, 2 TG chats, midnight reset (Europe/Kyiv), new-row as delta from 0, XHR+HTML+agGrid
import os, json, time, re
from typing import Dict, List
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PWTimeout

# ===== ENV =====
LOGIN_USER = os.environ["LOGIN_USER"]
LOGIN_PASS = os.environ["LOGIN_PASS"]
PAGE_URL   = os.environ["PAGE_URL"]

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT_ID_1 = os.getenv("TELEGRAM_CHAT_ID_1") or os.getenv("TELEGRAM_CHAT_ID")
TG_CHAT_ID_2 = os.getenv("TELEGRAM_CHAT_ID_2")
CHAT_IDS = [cid for cid in (TG_CHAT_ID_1, TG_CHAT_ID_2) if cid]

GIST_ID    = os.environ["GIST_ID"]
GIST_TOKEN = os.environ["GIST_TOKEN"]
GIST_FILENAME = os.getenv("GIST_FILENAME", "keitaro_spend_state.json")

SPEND_DIR = os.getenv("SPEND_DIRECTION", "both").lower()   # up|down|both
KYIV_TZ   = ZoneInfo(os.getenv("KYIV_TZ", "Europe/Kyiv"))
EPS = 0.009  # для float-порівнянь: все > 0.009 вважаємо зміною (0.01$ пройде)

# ===== small utils =====
def now_kyiv() -> datetime:
    return datetime.now(KYIV_TZ)

def kyiv_today_str() -> str:
    return now_kyiv().strftime("%Y-%m-%d")

def fmt_money(x: float) -> str:
    return f"${x:,.2f}"

def pct(delta: float, base: float) -> float:
    if abs(base) < EPS:
        return 100.0 if abs(delta) >= EPS else 0.0
    return abs(delta / base) * 100.0

def direction_ok(delta: float) -> bool:
    if SPEND_DIR == "up": return delta > EPS
    if SPEND_DIR == "down": return delta < -EPS
    return abs(delta) > EPS

# ===== state in Gist =====
def load_state() -> Dict:
    url = f"https://api.github.com/gists/{GIST_ID}"
    r = requests.get(url, headers={
        "Authorization": f"Bearer {GIST_TOKEN}",
        "Accept": "application/vnd.github+json"
    }, timeout=30)
    if r.status_code == 200:
        files = r.json().get("files", {})
        if GIST_FILENAME in files and "content" in files[GIST_FILENAME]:
            try:
                return json.loads(files[GIST_FILENAME]["content"])
            except:
                pass
    return {"date": kyiv_today_str(), "rows": {}}

def save_state(state: Dict):
    url = f"https://api.github.com/gists/{GIST_ID}"
    files = {GIST_FILENAME: {"content": json.dumps(state, ensure_ascii=False, indent=2)}}
    r = requests.patch(url, headers={
        "Authorization": f"Bearer {GIST_TOKEN}",
        "Accept": "application/vnd.github+json"
    }, json={"files": files}, timeout=30)
    r.raise_for_status()

# ===== Telegram =====
def tg_send(text: str):
    if not CHAT_IDS:
        return
    for cid in CHAT_IDS:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True},
                timeout=20
            )
        except Exception:
            pass

# ===== Parse helpers =====
def as_float(v):
    try: return float(v or 0)
    except: return 0.0

def parse_report_from_json(payload: dict) -> List[Dict]:
    rows = []
    for r in payload.get("rows", []):
        dims = r.get("dimensions", {}) if isinstance(r.get("dimensions"), dict) else {}
        def g(k): return r.get(k) or dims.get(k) or ""
        rows.append({
            "k": f"{g('campaign')}|{g('sub_id_6')}|{g('sub_id_5')}|{g('sub_id_4')}",
            "campaign": str(g("campaign")),
            "sub_id_6": str(g("sub_id_6")),
            "sub_id_5": str(g("sub_id_5")),
            "sub_id_4": str(g("sub_id_4")),
            "cost":  as_float(r.get("cost")),
            "leads": as_float(r.get("leads")),
            "sales": as_float(r.get("sales")),
            "cpa":   as_float(r.get("cpa")),
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
            "k": f"{safe(idx['campaign'])}|{safe(idx['sid6'])}|{safe(idx['sid5'])}|{safe(idx['sid4'])}",
            "campaign": safe(idx["campaign"]),
            "sub_id_6": safe(idx["sid6"]),
            "sub_id_5": safe(idx["sid5"]),
            "sub_id_4": safe(idx["sid4"]),
            "cost":  to_f(safe(idx["cost"])),
            "leads": to_f(safe(idx["leads"])),
            "sales": to_f(safe(idx["sales"])),
            "cpa":   to_f(safe(idx["cpa"])),
        })
    return rows

# ===== Playwright fetch: XHR -> HTML -> ag-Grid =====
def fetch_rows() -> List[Dict]:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
        )
        page = ctx.new_page()

        # login
        page.goto("https://trident.partners/admin/", wait_until="domcontentloaded")
        try:
            page.fill("input[name='login'], input[type='text']", LOGIN_USER)
            page.fill("input[name='password'], input[type='password']", LOGIN_PASS)
            page.get_by_role("button", name=re.compile("sign in|увійти|войти", re.I)).click()
        except Exception:
            pass
        try:
            page.wait_for_selector("app-login", state="detached", timeout=15000)
        except PWTimeout:
            pass

        captured: List[Dict] = []
        def on_response(resp):
            url = resp.url.lower()
            if "/report" in url or "/reports" in url:
                try:
                    data = resp.json()
                    rs = parse_report_from_json(data)
                    if rs:
                        captured[:] = rs
                except Exception:
                    pass
        ctx.on("response", on_response)

        page.goto(PAGE_URL, wait_until="domcontentloaded")
        # чек XHR до ~12 сек
        for _ in range(24):
            if captured: break
            time.sleep(0.5)

        rows = []
        if captured:
            rows = captured
        else:
            # HTML table fallback
            try:
                if page.locator("table tbody tr").count() > 0:
                    rows = parse_report_from_html(page)
            except Exception:
                pass

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
                                    if v in h: return i
                            return -1
                        i_campaign = idx(["campaign"])
                        i_sid6 = idx(["sub id 6","sub_id_6"])
                        i_sid5 = idx(["sub id 5","sub_id_5"])
                        i_sid4 = idx(["sub id 4","sub_id_4"])
                        i_leads  = idx(["leads"])
                        i_sales  = idx(["sales"])
                        i_cpa    = idx(["cpa"])
                        i_cost   = idx(["cost"])

                        def to_f(s: str) -> float:
                            s = (s or "").replace("$","").replace(",","").strip()
                            try: return float(s)
                            except: return 0.0

                        for row in rws.all():
                            cells = [ (c.inner_text() or "").strip() for c in row.locator(".ag-cell-value").all() ]
                            def safe(i): 
                                try: return cells[i]
                                except: return ""
                            rows.append({
                                "k": f"{safe(i_campaign)}|{safe(i_sid6)}|{safe(i_sid5)}|{safe(i_sid4)}",
                                "campaign": safe(i_campaign),
                                "sub_id_6": safe(i_sid6),
                                "sub_id_5": safe(i_sid5),
                                "sub_id_4": safe(i_sid4),
                                "cost":  to_f(safe(i_cost)),
                                "leads": to_f(safe(i_leads)),
                                "sales": to_f(safe(i_sales)),
                                "cpa":   to_f(safe(i_cpa)),
                            })
                except Exception:
                    pass

        browser.close()
        return rows

# ===== MAIN =====
def main():
    state = load_state()
    prev_date = state["date"]
    prev_rows = state["rows"]
    today = kyiv_today_str()

    rows = fetch_rows()
    if not rows:
        tg_send("🟥  NOTHING HAPPEND")
        return

    # midnight reset
    if prev_date != today:
        baseline = {r["k"]: r for r in rows}
        save_state({"date": today, "rows": baseline})
        tg_send("🟥  NOTHING HAPPEND")
        return

    # detect & format
    new_map: Dict[str, Dict] = {}
    blocks: List[str] = []

    for r in rows:
        k = r["k"]
        new_map[k] = r
        old = prev_rows.get(k)

        # Якщо рядок новий: виводимо зміни від 0 → поточне
        if not old:
            if r["cost"] > EPS:
                p = 100.0
                blocks.append(
                    "🧊 *SPEND ALERT*\n"
                    f"CAMPAIGN: {r['campaign']}\n"
                    f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                    f"Cost: {fmt_money(0)} → {fmt_money(r['cost'])}  (Δ {fmt_money(r['cost'])}, ~{p:.0f}%) 🔺"
                )
            if r["leads"] > EPS:
                cpa_part = f"  • CPA: {fmt_money(r['cpa'])}" if r['cpa'] > EPS else ""
                blocks.append(
                    "🟩 *LEAD ALERT*\n"
                    f"CAMPAIGN: {r['campaign']}\n"
                    f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                    f"Leads: 0 → {int(r['leads'])}{cpa_part}"
                )
            if r["sales"] > EPS:
                blocks.append(
                    "🟦 *SALE ALERT*\n"
                    f"CAMPAIGN: {r['campaign']}\n"
                    f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                    f"Sales: 0 → {int(r['sales'])}"
                )
            continue

        # існуючий рядок — класичні дельти
        # SPEND
        delta_cost = r["cost"] - old["cost"]
        if direction_ok(delta_cost):
            p = pct(delta_cost, old["cost"])
            arrow = "🔺" if delta_cost > 0 else "🔻"
            blocks.append(
                "🧊 *SPEND ALERT*\n"
                f"CAMPAIGN: {r['campaign']}\n"
                f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                f"Cost: {fmt_money(old['cost'])} → {fmt_money(r['cost'])}  (Δ {fmt_money(delta_cost)}, ~{p:.0f}%) {arrow}"
            )

        # LEAD
        if r["leads"] - old["leads"] > EPS:
            cpa_part = f"  • CPA: {fmt_money(r['cpa'])}" if r['cpa'] > EPS else ""
            blocks.append(
                "🟩 *LEAD ALERT*\n"
                f"CAMPAIGN: {r['campaign']}\n"
                f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                f"Leads: {int(old['leads'])} → {int(r['leads'])}{cpa_part}"
            )

        # SALE
        if r["sales"] - old["sales"] > EPS:
            blocks.append(
                "🟦 *SALE ALERT*\n"
                f"CAMPAIGN: {r['campaign']}\n"
                f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                f"Sales: {int(old['sales'])} → {int(r['sales'])}"
            )

    if blocks:
        tg_send("\n\n".join(blocks))
    else:
        tg_send("🟥  NOTHING HAPPEND")

    save_state({"date": today, "rows": new_map})

if __name__ == "__main__":
    main()
