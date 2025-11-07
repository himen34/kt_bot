import os, json, time, re, sys
from typing import Dict, List, Tuple
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
EPS = 0.009  # ~1 cent

def now_kyiv() -> datetime: return datetime.now(KYIV_TZ)
def kyiv_today_str() -> str: return now_kyiv().strftime("%Y-%m-%d")
def fmt_money(x: float) -> str: return f"${x:,.2f}"

def pct(delta: float, base: float) -> float:
    if abs(base) < EPS:
        return 100.0 if abs(delta) >= EPS else 0.0
    return abs(delta / base) * 100.0

def direction_ok(delta: float) -> bool:
    if SPEND_DIR == "up": return delta > EPS
    if SPEND_DIR == "down": return delta < -EPS
    return abs(delta) > EPS

# ---------- Gist state ----------
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
            except Exception:
                pass
    return {"date": kyiv_today_str(), "rows": {}, "sent": {}}

def save_state(state: Dict) -> Tuple[int, str]:
    url = f"https://api.github.com/gists/{GIST_ID}"
    files = {GIST_FILENAME: {"content": json.dumps(state, ensure_ascii=False, indent=2)}}
    r = requests.patch(url, headers={
        "Authorization": f"Bearer {GIST_TOKEN}",
        "Accept": "application/vnd.github+json"
    }, json={"files": files}, timeout=30)
    # повернемо код/текст для явного логу
    return r.status_code, r.reason

# ---------- Telegram ----------
def tg_send(text: str):
    for cid in CHAT_IDS:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True},
                timeout=20
            )
        except Exception:
            pass

# ---------- Parsing ----------
def as_float(v):
    try: return float(v or 0)
    except: return 0.0

def norm(s: str) -> str:
    # обрізаємо пробіли/невидимі символи всередині/по краях
    if s is None: return ""
    s = re.sub(r"[\u200b-\u200d\uFEFF]", "", str(s))  # zero-width, etc.
    return re.sub(r"\s+", " ", s).strip()

def make_key(campaign, sid6, sid5, sid4) -> str:
    return "|".join([norm(campaign), norm(sid6), norm(sid5), norm(sid4)])

def parse_report_from_json(payload: dict) -> List[Dict]:
    rows = []
    for r in payload.get("rows", []):
        dims = r.get("dimensions", {}) if isinstance(r.get("dimensions"), dict) else {}
        def g(k): return r.get(k) or dims.get(k) or ""
        camp = norm(g("campaign"))
        sid6 = norm(g("sub_id_6"))
        sid5 = norm(g("sub_id_5"))
        sid4 = norm(g("sub_id_4"))
        rows.append({
            "k": make_key(camp, sid6, sid5, sid4),
            "campaign": camp,
            "sub_id_6": sid6,
            "sub_id_5": sid5,
            "sub_id_4": sid4,
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
            try: return norm(tds[i].inner_text())
            except: return ""
        def to_f(s: str) -> float:
            s = (s or "").replace("$","").replace(",","").strip()
            try: return float(s)
            except: return 0.0

        camp, sid6, sid5, sid4 = safe(idx['campaign']), safe(idx['sid6']), safe(idx['sid5']), safe(idx['sid4'])
        rows.append({
            "k": make_key(camp, sid6, sid5, sid4),
            "campaign": camp,
            "sub_id_6": sid6,
            "sub_id_5": sid5,
            "sub_id_4": sid4,
            "cost":  to_f(safe(idx["cost"])),
            "leads": to_f(safe(idx["leads"])),
            "sales": to_f(safe(idx["sales"])),
            "cpa":   to_f(safe(idx["cpa"])),
        })
    return rows

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
        for _ in range(24):
            if captured: break
            time.sleep(0.5)

        rows = captured
        if not rows:
            try:
                if page.locator("table tbody tr").count() > 0:
                    rows = parse_report_from_html(page)
            except Exception:
                pass

        browser.close()
        return rows or []

# ---------- MAIN ----------
def main():
    state = load_state()
    prev_date = state.get("date", kyiv_today_str())
    prev_rows = state.get("rows", {})
    sent      = state.get("sent", {})
    today = kyiv_today_str()

    rows = fetch_rows()
    print(f"[info] fetched rows: {len(rows)}", file=sys.stdout)

    if not rows:
        return

    # раз у ніч — baseline без алертів
    if prev_date != today and 0 <= now_kyiv().hour < 1:
        baseline = {r["k"]: r for r in rows}
        new_state = {"date": today, "rows": baseline, "sent": {}}
        code, reason = save_state(new_state)
        print(f"[save@midnight] status={code} reason={reason} keys={len(baseline)} sample_key={next(iter(baseline)) if baseline else '-'}")
        return

    merged_rows: Dict[str, Dict] = dict(prev_rows)
    new_sent: Dict[str, Dict] = dict(sent)

    blocks: List[str] = []

    for r in rows:
        k = r["k"]
        old = prev_rows.get(k)

        # --- compute deltas ---
        if not old:
            # новий рядок → але шлемо лише якщо ще НЕ відправляли ці значення
            last = sent.get(k, {})
            if r["cost"] > EPS and abs(r["cost"] - last.get("cost", 0.0)) > EPS:
                blocks.append(
                    "🧊 *SPEND ALERT*\n"
                    f"CAMPAIGN: {r['campaign']}\n"
                    f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                    f"Cost: {fmt_money(0)} → {fmt_money(r['cost'])}  (Δ {fmt_money(r['cost'])}, ~100%) 🔺"
                )
                new_sent.setdefault(k, {})["cost"] = r["cost"]

            if r["leads"] > EPS and int(r["leads"]) != int(last.get("leads", 0)):
                cpa_part = f"  • CPA: {fmt_money(r['cpa'])}" if r['cpa'] > EPS else ""
                blocks.append(
                    "🟩 *LEAD ALERT*\n"
                    f"CAMPAIGN: {r['campaign']}\n"
                    f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                    f"Leads: 0 → {int(r['leads'])}{cpa_part}"
                )
                new_sent.setdefault(k, {})["leads"] = int(r["leads"])

            if r["sales"] > EPS and int(r["sales"]) != int(last.get("sales", 0)):
                blocks.append(
                    "🟦 *SALE ALERT*\n"
                    f"CAMPAIGN: {r['campaign']}\n"
                    f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                    f"Sales: 0 → {int(r['sales'])}"
                )
                new_sent.setdefault(k, {})["sales"] = int(r["sales"])

            merged_rows[k] = r
            continue

        # існуючий рядок
        delta_cost = r["cost"] - old["cost"]
        last = sent.get(k, {})

        if direction_ok(delta_cost) and abs(r["cost"] - last.get("cost", -1e9)) > EPS:
            arrow = "🔺" if delta_cost > 0 else "🔻"
            blocks.append(
                "🧊 *SPEND ALERT*\n"
                f"CAMPAIGN: {r['campaign']}\n"
                f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                f"Cost: {fmt_money(old['cost'])} → {fmt_money(r['cost'])}  (Δ {fmt_money(delta_cost)}, ~{pct(delta_cost, old['cost']):.0f}%) {arrow}"
            )
            new_sent.setdefault(k, {})["cost"] = r["cost"]

        if r["leads"] - old["leads"] > EPS and int(r["leads"]) != int(last.get("leads", -10**9)):
            cpa_part = f"  • CPA: {fmt_money(r['cpa'])}" if r['cpa'] > EPS else ""
            blocks.append(
                "🟩 *LEAD ALERT*\n"
                f"CAMPAIGN: {r['campaign']}\n"
                f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                f"Leads: {int(old['leads'])} → {int(r['leads'])}{cpa_part}"
            )
            new_sent.setdefault(k, {})["leads"] = int(r["leads"])

        if r["sales"] - old["sales"] > EPS and int(r["sales"]) != int(last.get("sales", -10**9)):
            blocks.append(
                "🟦 *SALE ALERT*\n"
                f"CAMPAIGN: {r['campaign']}\n"
                f"SubID5: {r['sub_id_5']}  SubID4: {r['sub_id_4']}\n"
                f"Sales: {int(old['sales'])} → {int(r['sales'])}"
            )
            new_sent.setdefault(k, {})["sales"] = int(r["sales"])

        merged_rows[k] = r

    if blocks:
        tg_send("\n\n".join(blocks))

    new_state = {"date": today, "rows": merged_rows, "sent": new_sent}
    code, reason = save_state(new_state)
    sample_key = next(iter(merged_rows)) if merged_rows else "-"
    print(f"[save] status={code} reason={reason} keys={len(merged_rows)} sample_key={sample_key}", file=sys.stdout)

if __name__ == "__main__":
    main()
