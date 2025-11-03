# notifier_playwright.py — FINAL
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
TG_CHAT_ID_1 = os.environ["TELEGRAM_CHAT_ID_1"]
TG_CHAT_ID_2 = os.environ["TELEGRAM_CHAT_ID_2"]

GIST_ID    = os.environ["GIST_ID"]
GIST_TOKEN = os.environ["GIST_TOKEN"]
GIST_FILENAME = os.getenv("GIST_FILENAME", "keitaro_spend_state.json")

SPEND_DIR  = os.getenv("SPEND_DIRECTION", "both").lower()   # up|down|both
KYIV_TZ = ZoneInfo(os.getenv("KYIV_TZ", "Europe/Kyiv"))

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

# -------- state --------
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
            except:
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
def tg_send(text: str):
    for cid in [TG_CHAT_ID_1, TG_CHAT_ID_2]:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": cid, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True},
            timeout=20
        )

# -------- parsers --------
def parse_report_from_json(payload: dict) -> List[Dict]:
    rows=[]
    for r in payload.get("rows", []):
        dims = r.get("dimensions", {}) if isinstance(r.get("dimensions"), dict) else {}
        def g(k): return r.get(k) or dims.get(k) or ""
        def f(v): 
            try: return float(v or 0)
            except: return 0.0
        rows.append({
            "k": f"{g('campaign')}|{g('sub_id_6')}|{g('sub_id_5')}|{g('sub_id_4')}",
            "campaign": g("campaign"),
            "sub_id_6": g("sub_id_6"),
            "sub_id_5": g("sub_id_5"),
            "sub_id_4": g("sub_id_4"),
            "cost":   f(r.get("cost")),
            "leads":  f(r.get("leads")),
            "sales":  f(r.get("sales")),
            "cpa":    f(r.get("cpa")),
        })
    return rows

# -------- playwright fetch --------
def fetch_rows() -> List[Dict]:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        page.goto("https://trident.partners/admin/")
        page.fill("input[name='login']", LOGIN_USER)
        page.fill("input[name='password']", LOGIN_PASS)
        page.get_by_role("button", name=re.compile("sign in|увійти|войти", re.I)).click()
        try: page.wait_for_selector("app-login", state="detached", timeout=15000)
        except: pass

        captured=[]
        def on_response(resp):
            if "/report" in resp.url:
                try:
                    rs=parse_report_from_json(resp.json())
                    if rs: captured[:] = rs
                except: pass
        ctx.on("response", on_response)

        page.goto(PAGE_URL, wait_until="networkidle")

        for _ in range(20):
            if captured: break
            time.sleep(0.5)

        browser.close()
        return captured

# -------- diff --------
def main():
    state = load_state()
    prev_date = state["date"]
    prev = state["rows"]
    today = kyiv_today_str()

    rows = fetch_rows()
    if not rows:
        return

    # midnight reset
    if prev_date != today:
        new_map = {r["k"]: r for r in rows}
        save_state({"date": today, "rows": new_map})
        return

    # detect diffs
    spend=[]
    leads=[]
    sales=[]
    new_map={}

    for r in rows:
        k=r["k"]
        new_map[k]=r
        old=prev.get(k)
        if not old: continue

        # cost diff ANY > 0.00
        if direction_ok(r["cost"] - old["cost"]) and (r["cost"] != old["cost"]):
            delta = r["cost"] - old["cost"]
            p = pct(delta, old["cost"]) if old["cost"] else 100.0
            up = "🔺" if delta>0 else "🔻"
            spend.append(
                f"{r['campaign']}\nSubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\nCost: {fmt_money(old['cost'])} → {fmt_money(r['cost'])}  (Δ {fmt_money(delta)}, ~{p:.0f}%) {up}"
            )

        if r["leads"] > old["leads"]:
            cpa_part = f"  • CPA: {fmt_money(r['cpa'])}" if r['cpa']>0 else ""
            leads.append(
                f"{r['campaign']}\nSubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\nLeads: {int(old['leads'])} → {int(r['leads'])}{cpa_part}"
            )

        if r["sales"] > old["sales"]:
            sales.append(
                f"{r['campaign']}\nSubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\nSales: {int(old['sales'])} → {int(r['sales'])}"
            )

    if spend or leads or sales:
        msg=[]
        if spend:
            msg.append("🧊 *SPEND ALERTS*")
            for i,s in enumerate(spend,1):
                msg.append(f"{i}) {s}\n")

        if leads:
            msg.append("🟩 *LEAD ALERTS*")
            for i,s in enumerate(leads,1):
                msg.append(f"{i}) {s}\n")

        if sales:
            msg.append("🟦 *SALE ALERTS*")
            for i,s in enumerate(sales,1):
                msg.append(f"{i}) {s}\n")

        tg_send("\n".join(msg))

    save_state({"date": today, "rows": new_map})

if __name__=="__main__":
    main()
