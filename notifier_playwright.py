# notifier_playwright.py
import os, json, time, math, asyncio, re, traceback, base64, datetime as dt
from typing import Dict, List, Tuple
import pytz
import requests

from playwright.sync_api import sync_playwright, Route, Request

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
SPEND_ABS  = float(os.getenv("SPEND_ABS_THRESHOLD", "1"))  # мін. $ для алерта
SPEND_PCT  = float(os.getenv("SPEND_PCT_THRESHOLD", "30")) # відсоток (додатковий, можна залишити великим)
SPEND_DIR  = os.getenv("SPEND_DIRECTION", "both").lower()  # up|down|both

# Київський час
KYIV_TZ = pytz.timezone(os.getenv("KYIV_TZ", "Europe/Kyiv"))
MIN_SPEND_DELTA = max(1.0, SPEND_ABS)  # гарантія $1, як просив

# ---------- HELPERS ----------
def kyiv_today_str():
    return dt.datetime.now(KYIV_TZ).strftime("%Y-%m-%d")

def now_kyiv():
    return dt.datetime.now(KYIV_TZ)

def load_state() -> Dict:
    url = f"https://api.github.com/gists/{GIST_ID}"
    r = requests.get(url, headers={"Authorization": f"Bearer {GIST_TOKEN}",
                                   "Accept": "application/vnd.github+json"})
    if r.status_code == 200:
        files = r.json().get("files", {})
        if GIST_FILENAME in files and "content" in files[GIST_FILENAME]:
            try:
                return json.loads(files[GIST_FILENAME]["content"])
            except Exception:
                return {"date": kyiv_today_str(), "rows": {}}
    # if not found
    return {"date": kyiv_today_str(), "rows": {}}

def save_state(state: Dict):
    files = {GIST_FILENAME: {"content": json.dumps(state, ensure_ascii=False, indent=2)}}
    url = f"https://api.github.com/gists/{GIST_ID}"
    r = requests.patch(url, headers={"Authorization": f"Bearer {GIST_TOKEN}",
                                     "Accept": "application/vnd.github+json"},
                       json={"files": files})
    r.raise_for_status()

def tg_send(text: str, parse_mode: str = "Markdown"):
    requests.post(
        f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
        json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True},
        timeout=20
    )

def fmt_money(x: float) -> str:
    return f"${x:,.2f}"

def pct(a: float, b: float) -> float:
    if b == 0:
        return 100.0 if a != 0 else 0.0
    return abs(a / b) * 100.0

def direction_ok(delta: float) -> bool:
    if SPEND_DIR == "up": return delta > 0
    if SPEND_DIR == "down": return delta < 0
    return True

# ---------- PARSERS ----------
def parse_report_from_json(payload: dict) -> List[Dict]:
    """
    Очікуваний формат Keitaro: rows із полями cost, cpa, leads, sales та вимірами.
    В кожному рядку намагаємося витягти campaign, sub_id_5, sub_id_4, sub_id_6.
    """
    rows = []
    for r in payload.get("rows", []):
        dims = r.get("dimensions", {}) if isinstance(r.get("dimensions"), dict) else {}
        # На різних версіях Keitaro ключ може бути в  root
        dim = {}
        for k in ["campaign", "sub_id_6", "sub_id_5", "sub_id_4", "country_flag"]:
            dim[k] = r.get(k) or dims.get(k) or ""

        row = {
            "campaign":     str(dim.get("campaign", "")),
            "sub_id_6":     str(dim.get("sub_id_6", "")),
            "sub_id_5":     str(dim.get("sub_id_5", "")),
            "sub_id_4":     str(dim.get("sub_id_4", "")),
            "country_flag": str(dim.get("country_flag", "")),
            "clicks": float(r.get("clicks", 0) or 0),
            "leads":  float(r.get("leads", 0) or 0),
            "sales":  float(r.get("sales", 0) or 0),
            "cpa":    float(r.get("cpa", 0) or 0),
            "cost":   float(r.get("cost", 0) or 0),
        }
        rows.append(row)
    return rows

def parse_report_from_html(page) -> List[Dict]:
    """
    Фолбек: читаємо видиму таблицю. Працює з типовою розміткою Keitaro.
    Для надійності шукаємо заголовки і будуємо індекси колонок.
    """
    rows = []
    page.wait_for_selector("table", timeout=15000)
    # Знайдемо потрібну таблицю (де є заголовок Leads / Sales / CPA / Cost)
    tables = page.query_selector_all("table")
    target = None
    for t in tables:
        head_text = (t.query_selector("thead") or t).inner_text().lower()
        if all(x in head_text for x in ["leads", "sales", "cpa", "cost"]):
            target = t
            break
    if not target:
        return rows

    # map headers
    headers = []
    for th in target.query_selector_all("thead tr th"):
        headers.append((th.inner_text() or "").strip().lower())

    def col_idx(name_variants: List[str]) -> int:
        for i, h in enumerate(headers):
            for v in name_variants:
                if v in h:
                    return i
        return -1

    idx_campaign = col_idx(["campaign"])
    idx_sid6     = col_idx(["sub id 6", "sub_id_6", "subid6", "sub id6"])
    idx_sid5     = col_idx(["sub id 5", "sub_id_5", "subid5", "sub id5"])
    idx_sid4     = col_idx(["sub id 4", "sub_id_4", "subid4", "sub id4"])
    idx_clicks   = col_idx(["clicks"])
    idx_leads    = col_idx(["leads"])
    idx_sales    = col_idx(["sales"])
    idx_cpa      = col_idx(["cpa"])
    idx_cost     = col_idx(["cost"])

    for tr in target.query_selector_all("tbody tr"):
        tds = tr.query_selector_all("td")
        def safe(i):
            try:
                return (tds[i].inner_text() or "").strip()
            except:
                return ""

        def to_float(s: str) -> float:
            s = s.replace("$","").replace(",","").strip()
            try: return float(s)
            except: return 0.0

        rows.append({
            "campaign": safe(idx_campaign),
            "sub_id_6": safe(idx_sid6),
            "sub_id_5": safe(idx_sid5),
            "sub_id_4": safe(idx_sid4),
            "country_flag": "",
            "clicks": to_float(safe(idx_clicks)),
            "leads":  to_float(safe(idx_leads)),
            "sales":  to_float(safe(idx_sales)),
            "cpa":    to_float(safe(idx_cpa)),
            "cost":   to_float(safe(idx_cost)),
        })
    return rows

# ---------- SCRAPER ----------
def fetch_rows_via_playwright() -> List[Dict]:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # логін сторінка
        page.goto("https://trident.partners/admin/")
        # поля Angular login компоненту
        page.fill("input[type='text'], input[name='login']", LOGIN_USER)
        page.fill("input[type='password'], input[name='password']", LOGIN_PASS)
        # кнопка Sign in
        page.get_by_role("button", name=re.compile("sign in", re.I)).click()

        # перехоплення JSON звітів
        captured_rows: List[Dict] = []
        def on_response(resp):
            url = resp.url
            if "/reports" in url or "/report" in url:
                try:
                    data = resp.json()
                    rs = parse_report_from_json(data)
                    if rs:
                        captured_rows[:] = rs
                except:
                    pass
        ctx.on("response", on_response)

        page.goto(PAGE_URL, wait_until="networkidle")

        # даємо трохи часу XHR
        for _ in range(12):
            if captured_rows:
                break
            time.sleep(0.5)

        if not captured_rows:
            # fallback: HTML
            try:
                captured_rows = parse_report_from_html(page)
            except:
                pass

        browser.close()
        return captured_rows

# ---------- DIFF & MESSAGES ----------
def row_key(r: Dict) -> str:
    # ключ для зіставлення рядків між прогонами
    return f"{r.get('campaign','')}|{r.get('sub_id_6','')}|{r.get('sub_id_5','')}|{r.get('sub_id_4','')}"

def detect_changes(prev_map: Dict[str, Dict], rows: List[Dict]) -> Tuple[List[str], Dict]:
    """
    Формує список повідомлень і новий state.
    - SPEND ALERT: якщо |Δcost| ≥ $1 та напрямок ок
    - LEAD ALERT: якщо збільшились leads (CPA показуємо поряд)
    - SALE  ALERT: якщо збільшились sales
    """
    msgs = []
    new_state_rows = {}

    for r in rows:
        k = row_key(r)
        new_state_rows[k] = {
            "campaign": r["campaign"], "sub_id_6": r["sub_id_6"],
            "sub_id_5": r["sub_id_5"], "sub_id_4": r["sub_id_4"],
            "clicks": r["clicks"], "leads": r["leads"], "sales": r["sales"],
            "cpa": r["cpa"], "cost": r["cost"]
        }

        prev = prev_map.get(k)
        if not prev:
            # новий рядок — просто запам'ятовуємо
            continue

        # --- Spend ---
        delta_cost = r["cost"] - float(prev.get("cost", 0))
        if direction_ok(delta_cost) and abs(delta_cost) >= MIN_SPEND_DELTA:
            base = float(prev.get("cost", 0))
            pp = pct(delta_cost, base) if base else 100.0
            up = "🔺" if delta_cost > 0 else "🔻"
            msgs.append(
                "🧊 *SPEND ALERT*\n"
                f"*CAMPAIGN:* {r['campaign']}\n"
                f"*SubID5:* {r['sub_id_5']}  *SubID4:* {r['sub_id_4']}\n"
                f"*Cost:* {fmt_money(base)} → *{fmt_money(r['cost'])}*  "
                f"(Δ {fmt_money(delta_cost)}, ~{pp:.0f}%) {up}"
            )

        # --- Leads (рег) ---
        delta_leads = r["leads"] - float(prev.get("leads", 0))
        if delta_leads > 0:
            cpa_part = f"  • *CPA:* {fmt_money(r['cpa'])}" if r['cpa'] > 0 else ""
            msgs.append(
                "🟩 *LEAD ALERT*\n"
                f"*CAMPAIGN:* {r['campaign']}\n"
                f"*SubID5:* {r['sub_id_5']}  *SubID4:* {r['sub_id_4']}\n"
                f"*Leads:* {int(prev.get('leads', 0))} → *{int(r['leads'])}*{cpa_part}"
            )

        # --- Sales (деп) ---
        delta_sales = r["sales"] - float(prev.get("sales", 0))
        if delta_sales > 0:
            msgs.append(
                "🟦 *SALE ALERT*\n"
                f"*CAMPAIGN:* {r['campaign']}\n"
                f"*SubID5:* {r['sub_id_5']}  *SubID4:* {r['sub_id_4']}\n"
                f"*Sales:* {int(prev.get('sales', 0))} → *{int(r['sales'])}*"
            )

    return msgs, new_state_rows

# ---------- MAIN ----------
def main():
    state = load_state()
    prev_date = state.get("date")
    prev_rows = state.get("rows", {})

    # знімаємо поточні дані
    try:
        rows = fetch_rows_via_playwright()
    except Exception as e:
        tg_send(f"⚠️ Не зміг отримати дані звіту: {e}\n```\n{traceback.format_exc()}\n```", "Markdown")
        return

    # якщо порожньо — просто повідомляємо
    if not rows:
        tg_send("⚠️ Дані не отримані: жодного рядка. Перевір логін/URL/фільтри.", "Markdown")
        return

    today = kyiv_today_str()

    # Якщо день змінився (за Києвом) — робимо м'який ресет без алертів
    if prev_date != today:
        new_state = {"date": today, "rows": {row_key(r): {
            "campaign": r["campaign"], "sub_id_6": r["sub_id_6"],
            "sub_id_5": r["sub_id_5"], "sub_id_4": r["sub_id_4"],
            "clicks": r["clicks"], "leads": r["leads"], "sales": r["sales"],
            "cpa": r["cpa"], "cost": r["cost"]
        } for r in rows}}
        save_state(new_state)
        tg_send("🕛 Новий день (Europe/Kyiv). Оновив базовий стан без алертів.", "Markdown")
        return

    # Інакше — шукаємо зміни
    msgs, new_rows_map = detect_changes(prev_rows, rows)

    # Якщо нічого — напишемо відпустку :)
    if not msgs:
        tg_send("accs on vacation...", "Markdown")
    else:
        for m in msgs:
            tg_send(m, "Markdown")

    # зберігаємо стан
    save_state({"date": today, "rows": new_rows_map})


if __name__ == "__main__":
    main()
