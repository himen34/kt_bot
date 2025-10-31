import os, json, re, requests
from playwright.sync_api import sync_playwright

LOGIN_URL = "https://trident.partners/admin/"
PAGE_URL  = os.environ["PAGE_URL"]  # твой favourite/104/... полный URL
LOGIN_USER = os.environ["LOGIN_USER"]
LOGIN_PASS = os.environ["LOGIN_PASS"]

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT  = os.environ["TELEGRAM_CHAT_ID"]

ABS_THR = float(os.getenv("SPEND_ABS_THRESHOLD", "50"))
PCT_THR = float(os.getenv("SPEND_PCT_THRESHOLD", "50"))
DIRECTION = os.getenv("SPEND_DIRECTION", "up").lower()  # up|down|both

GIST_ID = os.environ["GIST_ID"]
GIST_TOKEN = os.environ["GIST_TOKEN"]
GIST_FILENAME = os.getenv("GIST_FILENAME", "keitaro_spend_state.json")

def get_gist_state():
    r = requests.get(f"https://api.github.com/gists/{GIST_ID}",
                     headers={"Authorization": f"token {GIST_TOKEN}"}, timeout=20)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    files = r.json().get("files", {})
    if GIST_FILENAME in files and files[GIST_FILENAME].get("content"):
        try:
            return json.loads(files[GIST_FILENAME]["content"])
        except:
            return {}
    return {}

def save_gist_state(state: dict):
    payload = {"files": {GIST_FILENAME: {"content": json.dumps(state, ensure_ascii=False, indent=2)}}}
    r = requests.patch(f"https://api.github.com/gists/{GIST_ID}",
                       headers={"Authorization": f"token {GIST_TOKEN}"},
                       json=payload, timeout=20)
    r.raise_for_status()

def send_tg(msg: str):
    requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                  json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML",
                        "disable_web_page_preview": True}, timeout=20).raise_for_status()

def is_spike(prev: float | None, curr: float):
    if prev is None: return (False, 0.0, 0.0)
    if prev == 0 and curr == 0: return (False, 0.0, 0.0)
    delta = curr - prev
    pct = (delta / prev * 100.0) if prev != 0 else (999999.0 if curr>0 else -999999.0)
    up = delta >= ABS_THR or pct >= PCT_THR
    down = (-delta) >= ABS_THR or (-pct) >= PCT_THR
    if DIRECTION == "up"   and delta > 0 and up:   return (True, delta, pct)
    if DIRECTION == "down" and delta < 0 and down: return (True, delta, pct)
    if DIRECTION == "both" and ((delta>0 and up) or (delta<0 and down)): return (True, delta, pct)
    return (False, delta, pct)

def format_alert(row, prev, delta, pct):
    arrow = "🔺" if delta > 0 else "🔻"
    return (
        f"<b>{arrow} Spend spike detected</b>\n"
        f"Campaign: <code>{row['campaign']}</code>\n"
        f"SubID6: <code>{row.get('sub_id_6','')}</code>\n"
        f"Cost: ${prev:.2f} → <b>${row['cost']:.2f}</b>  (Δ ${delta:.2f}, {pct:.1f}%)\n"
        f"Clicks: {row.get('clicks',0)} | Leads: {row.get('leads',0)} | Sales: {row.get('sales',0)} | ROI: {row.get('roi','')}"
    )

def fetch_rows_via_xhr() -> list[dict]:
    """
    Логинимся в SPA и перехватываем JSON-ответ отчёта.
    Возвращаем список строк: campaign, sub_id_6, cost, clicks, leads, sales, roi
    """
    rows_json = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # логин
        page.goto(LOGIN_URL, wait_until="domcontentloaded")

        # возможно, плейсхолдеры иконками — найдём по типам
        # пробуем по плейсхолдерам на англ. (судя по appTranslation)
        try:
            page.get_by_placeholder("Username").fill(LOGIN_USER)
        except:
            page.locator("input[type=text], input[name='login'], input[name='email']").first.fill(LOGIN_USER)
        try:
            page.get_by_placeholder("Password").fill(LOGIN_PASS)
        except:
            page.locator("input[type=password]").first.fill(LOGIN_PASS)

        # кнопка Sign in
        page.get_by_role("button", name=re.compile(r"Sign in", re.I)).click()

        # ждём авторизацию
        page.wait_for_load_state("networkidle")

        # перехватим XHR с данными отчёта
        def on_response(resp):
            nonlocal rows_json
            ct = resp.headers.get("content-type","")
            url = resp.url
            if "application/json" in ct and ("report" in url or "reports" in url or "stats" in url):
                try:
                    data = resp.json()
                    # эвристика: Keitaro-подобный ответ с полями rows/data
                    if isinstance(data, dict) and ("rows" in data or "data" in data):
                        rows_json = data.get("rows") or data.get("data")
                except Exception:
                    pass

        page.on("response", on_response)

        # открываем страницу отчёта (hash-route)
        page.goto(PAGE_URL, wait_until="networkidle")

        # если JSON не поймали — подождём немного
        page.wait_for_timeout(2000)

        # fallback: принудительно клик по "Обновить"/refresh если есть
        if rows_json is None:
            try:
                page.get_by_role("button", name=re.compile(r"(Refresh|Update|Apply|Применить|Обновить)", re.I)).click()
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(1500)
            except:
                pass

        html = page.content()
        browser.close()

    # если перехватили JSON — распарсим из него
    if isinstance(rows_json, list) and rows_json:
        rows = []
        for r in rows_json:
            # имена ключей предположительные — поправим после первого прогона
            campaign = r.get("campaign") or r.get("campaign_name") or ""
            sub6 = r.get("sub_id_6") or r.get("subid6") or ""
            cost = float(r.get("cost", 0))
            rows.append({
                "campaign": campaign,
                "sub_id_6": sub6,
                "cost": cost,
                "clicks": r.get("clicks", 0),
                "leads": r.get("leads", 0),
                "sales": r.get("sales", 0),
                "roi": r.get("roi_confirmed") or r.get("roi") or ""
            })
        return rows

    # fallback №2: если JSON не словили, можно дернуть DOM таблицы (если она реально рендерится)
    # но чаще в SPA таблица виртуальная — поэтому основной путь через XHR.
    return []

def main():
    state = get_gist_state()  # key -> last_cost (float)
    rows = fetch_rows_via_xhr()

    if not rows:
        # Сообщим об ошибке однажды
        try:
            send_tg("⚠️ Не удалось получить данные отчёта (ни один JSON не перехвачен). Проверьте селекторы/роль кнопки логина/URL.")
        except: pass
        return

    changed = False
    for row in rows:
        key = f"{row['campaign']}|{row.get('sub_id_6','')}"
        prev = state.get(key)
        spike, delta, pct = is_spike(prev, row["cost"])
        if spike:
            send_tg(format_alert(row, prev if prev is not None else 0.0, delta, pct))
        state[key] = row["cost"]
        changed = True

    if changed:
        save_gist_state(state)

if __name__ == "__main__":
    main()
