# notifier_playwright.py — single message, per-change blocks, 2 TG chats, midnight reset (Europe/Kyiv)
import os, json, time, re
from typing import Dict, List
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from playwright.sync_api import sync_playwright

# ===== ENV =====
LOGIN_USER = os.environ["LOGIN_USER"]
LOGIN_PASS = os.environ["LOGIN_PASS"]
PAGE_URL   = os.environ["PAGE_URL"]

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
# безпечно читаємо 1 або 2 chat_id
TG_CHAT_ID_1 = os.getenv("TELEGRAM_CHAT_ID_1") or os.getenv("TELEGRAM_CHAT_ID")
TG_CHAT_ID_2 = os.getenv("TELEGRAM_CHAT_ID_2")
CHAT_IDS = [cid for cid in (TG_CHAT_ID_1, TG_CHAT_ID_2) if cid]

GIST_ID    = os.environ["GIST_ID"]
GIST_TOKEN = os.environ["GIST_TOKEN"]
GIST_FILENAME = os.getenv("GIST_FILENAME", "keitaro_spend_state.json")

SPEND_DIR = os.getenv("SPEND_DIRECTION", "both").lower()   # up|down|both
KYIV_TZ   = ZoneInfo(os.getenv("KYIV_TZ", "Europe/Kyiv"))

# ===== small utils =====
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

# ===== Keitaro JSON -> rows =====
def parse_report_from_json(payload: dict) -> List[Dict]:
    rows = []
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
            "cost":  f(r.get("cost")),
            "leads": f(r.get("leads")),
            "sales": f(r.get("sales")),
            "cpa":   f(r.get("cpa")),
        })
    return rows

# ===== Playwright: login + intercept report JSON =====
def fetch_rows() -> List[Dict]:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        page.goto("https://trident.partners/admin/", wait_until="domcontentloaded")
        page.fill("input[name='login']", LOGIN_USER)
        page.fill("input[name='password']", LOGIN_PASS)
        # кнопка "Sign in" / локалізовані варіанти
        page.get_by_role("button", name=re.compile("sign in|увійти|войти", re.I)).click()
        # чекаємо зникнення компонента login або завантаження дашборду
        try:
            page.wait_for_selector("app-login", state="detached", timeout=15000)
        except:
            pass

        captured: List[Dict] = []

        def on_response(resp):
            url = resp.url.lower()
            if "/report" in url:
                try:
                    data = resp.json()
                    rs = parse_report_from_json(data)
                    if rs:
                        captured[:] = rs
                except:
                    pass

        ctx.on("response", on_response)

        page.goto(PAGE_URL, wait_until="networkidle")

        # трошки почекаємо на перший report
        for _ in range(20):
            if captured: break
            time.sleep(0.5)

        browser.close()
        return captured

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

    # опівночі — скидаємо baseline (за твоїм запитом)
    if prev_date != today:
        baseline = {r["k"]: r for r in rows}
        save_state({"date": today, "rows": baseline})
        # сьогодні ще нема змін → нічого не шлемо
        tg_send("🟥  NOTHING HAPPEND")
        return

    # шукаємо зміни
    new_map: Dict[str, Dict] = {}
    blocks: List[str] = []  # кожна зміна — окремий блок

    for r in rows:
        k = r["k"]
        new_map[k] = r
        old = prev_rows.get(k)
        if not old:
            # новий ключ — в цей запуск не шлемо дельту, просто запам'ятаємо
            continue

        # SPEND — будь-яка дельта (навіть 0.01)
        delta_cost = r["cost"] - old["cost"]
        if (r["cost"] != old["cost"]) and direction_ok(delta_cost):
            p = pct(delta_cost, old["cost"]) if old["cost"] else 100.0
            arrow = "🔺" if delta_cost > 0 else "🔻"
            blocks.append(
                "🧊 *SPEND ALERT*\n"
                f"CAMPAIGN: {r['campaign']}\n"
                f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                f"Cost: {fmt_money(old['cost'])} → {fmt_money(r['cost'])}  (Δ {fmt_money(delta_cost)}, ~{p:.0f}%) {arrow}"
            )

        # LEAD
        if r["leads"] > old["leads"]:
            cpa_part = f"  • CPA: {fmt_money(r['cpa'])}" if r['cpa'] > 0 else ""
            blocks.append(
                "🟩 *LEAD ALERT*\n"
                f"CAMPAIGN: {r['campaign']}\n"
                f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                f"Leads: {int(old['leads'])} → {int(r['leads'])}{cpa_part}"
            )

        # SALE
        if r["sales"] > old["sales"]:
            blocks.append(
                "🟦 *SALE ALERT*\n"
                f"CAMPAIGN: {r['campaign']}\n"
                f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                f"Sales: {int(old['sales'])} → {int(r['sales'])}"
            )

    # відправляємо
    if blocks:
        tg_send("\n\n".join(blocks))
    else:
        tg_send("🟥  NOTHING HAPPEND")

    # зберігаємо новий стан
    save_state({"date": today, "rows": new_map})

if __name__ == "__main__":
    main()
