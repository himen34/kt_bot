import os, json, time, re
from typing import Dict, List, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ================= ENV =================
LOGIN_USER = os.environ["LOGIN_USER"]
LOGIN_PASS = os.environ["LOGIN_PASS"]
PAGE_URL   = os.environ["PAGE_URL"]

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT_ID_1 = os.getenv("TELEGRAM_CHAT_ID_1") or os.getenv("TELEGRAM_CHAT_ID")
TG_CHAT_ID_2 = os.getenv("TELEGRAM_CHAT_ID_2")
CHAT_IDS = [cid for cid in (TG_CHAT_ID_1, TG_CHAT_ID_2) if cid]

GIST_ID    = os.environ["GIST_ID"]
GIST_TOKEN = os.environ["GIST_TOKEN"]
GIST_FILENAME = os.getenv("GIST_FILENAME", "keitaro_state.json")

KYIV_TZ = ZoneInfo(os.getenv("KYIV_TZ", "Europe/Kyiv"))
EPS = 0.0001

# ================= utils =================
def now_kyiv() -> datetime:
    return datetime.now(KYIV_TZ)

def kyiv_today_str() -> str:
    return now_kyiv().strftime("%Y-%m-%d")

def as_float(v):
    try:
        return float(v or 0)
    except:
        return 0.0

# ================= Gist =================
def load_state() -> Dict:
    url = f"https://api.github.com/gists/{GIST_ID}"
    r = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {GIST_TOKEN}",
            "Accept": "application/vnd.github+json"
        },
        timeout=30
    )
    if r.status_code == 200:
        files = r.json().get("files", {})
        if GIST_FILENAME in files:
            try:
                return json.loads(files[GIST_FILENAME]["content"])
            except:
                pass
    return {"date": kyiv_today_str(), "rows": {}}

def save_state(state: Dict):
    url = f"https://api.github.com/gists/{GIST_ID}"
    r = requests.patch(
        url,
        headers={
            "Authorization": f"Bearer {GIST_TOKEN}",
            "Accept": "application/vnd.github+json"
        },
        json={
            "files": {
                GIST_FILENAME: {
                    "content": json.dumps(state, ensure_ascii=False, indent=2)
                }
            }
        },
        timeout=30
    )
    r.raise_for_status()

# ================= Telegram =================
def tg_send(text: str):
    for cid in CHAT_IDS:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={
                    "chat_id": cid,
                    "text": text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True
                },
                timeout=20
            )
        except:
            pass

# ================= Parsing =================
def parse_report_from_json(payload: dict) -> List[Dict]:
    rows = []
    for r in payload.get("rows", []):
        dims = r.get("dimensions", {}) or {}

        def g(k):
            return r.get(k) or dims.get(k) or ""

        rows.append({
            "k": f"{g('campaign')}|{g('sub_id_6')}|{g('sub_id_5')}|{g('sub_id_4')}",
            "campaign": str(g("campaign")),
            "sub6": str(g("sub_id_6")),
            "sub5": str(g("sub_id_5")),
            "sub4": str(g("sub_id_4")),
            "geo": str(g("country") or g("geo") or ""),
            "leads": as_float(r.get("conversions") or r.get("leads")),
            "sales": as_float(r.get("sales")),
            "cpa": as_float(r.get("cpa")),
        })
    return rows

def aggregate_rows_max(rows: List[Dict]) -> List[Dict]:
    acc: Dict[str, Dict] = {}
    for r in rows:
        k = r["k"]
        if k not in acc:
            acc[k] = dict(r)
        else:
            a = acc[k]
            a["leads"] = max(a["leads"], r["leads"])
            a["sales"] = max(a["sales"], r["sales"])
            a["cpa"]   = max(a["cpa"],   r["cpa"])
    return list(acc.values())

# ================= Fetch =================
def fetch_rows() -> List[Dict]:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        ctx = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124 Safari/537.36"
            )
        )
        page = ctx.new_page()

        # LOGIN
        page.goto("https://trident.partners/admin/", wait_until="domcontentloaded")
        try:
            page.fill("input[name='login'], input[type='text']", LOGIN_USER)
            page.fill("input[name='password'], input[type='password']", LOGIN_PASS)
            page.get_by_role(
                "button",
                name=re.compile("sign in|войти|увійти", re.I)
            ).click()
        except Exception:
            pass

        try:
            page.wait_for_selector("app-login", state="detached", timeout=15000)
        except PWTimeout:
            pass

        captured: List[Dict] = []
        best_len = 0

        def on_response(resp):
            nonlocal captured, best_len
            url = (resp.url or "").lower()
            if "/report" not in url:
                return
            try:
                data = resp.json()
            except:
                return
            rows = parse_report_from_json(data)
            if rows and len(rows) > best_len:
                captured = rows
                best_len = len(rows)

        ctx.on("response", on_response)

        page.goto(PAGE_URL, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeout:
            pass

        time.sleep(1.5)
        browser.close()

        return aggregate_rows_max(captured)

# ================= Main =================
def main():
    state = load_state()
    prev_date = state.get("date", kyiv_today_str())
    prev_rows = state.get("rows", {})
    today = kyiv_today_str()

    rows = fetch_rows()
    if not rows:
        tg_send("⚠️ Keitaro: no data")
        return

    if prev_date != today:
        save_state({"date": today, "rows": {r["k"]: r for r in rows}})
        return

    new_map: Dict[str, Dict] = {}
    lead_msgs: List[str] = []
    sale_msgs: List[str] = []

    for r in rows:
        k = r["k"]
        old = prev_rows.get(k)

        header = (
            f"CAMPAIGN: {r['campaign']}\n"
            f"Sub6: {r['sub6']}  Sub5: {r['sub5']}  Sub4: {r['sub4']}  Geo: {r['geo']}"
        )

        if old:
            if r["leads"] - old.get("leads", 0) > EPS:
                lead_msgs.append(
                    "🟩 *LEAD ALERT*\n"
                    f"{header}\n"
                    f"Leads: {int(old['leads'])} → {int(r['leads'])}  • CPA: {r['cpa']}"
                )
            if r["sales"] - old.get("sales", 0) > EPS:
                sale_msgs.append(
                    "🟦 *SALE ALERT*\n"
                    f"{header}\n"
                    f"Sales: {int(old['sales'])} → {int(r['sales'])}"
                )
        else:
            if r["leads"] > EPS:
                lead_msgs.append(
                    "🟩 *LEAD ALERT*\n"
                    f"{header}\n"
                    f"Leads: 0 → {int(r['leads'])}  • CPA: {r['cpa']}"
                )
            if r["sales"] > EPS:
                sale_msgs.append(
                    "🟦 *SALE ALERT*\n"
                    f"{header}\n"
                    f"Sales: 0 → {int(r['sales'])}"
                )

        new_map[k] = r

    blocks = lead_msgs + sale_msgs
    if blocks:
        tg_send("\n\n".join(blocks))

    save_state({"date": today, "rows": new_map})

if __name__ == "__main__":
    main()
