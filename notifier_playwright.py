import os, re, json, time, math
from typing import List, Dict, Any, Tuple
import requests
from playwright.sync_api import sync_playwright

LOGIN_URL = "https://trident.partners/admin/"
LOGIN_USER = os.environ["LOGIN_USER"]
LOGIN_PASS = os.environ["LOGIN_PASS"]
PAGE_URL  = os.environ["PAGE_URL"]

TG_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT   = os.environ["TELEGRAM_CHAT_ID"]

GIST_ID   = os.environ["GIST_ID"]
GIST_TOKEN= os.environ["GIST_TOKEN"]
GIST_FILENAME = os.getenv("GIST_FILENAME", "keitaro_spend_state.json")

SPEND_ABS = float(os.getenv("SPEND_ABS_THRESHOLD", "100"))
SPEND_PCT = float(os.getenv("SPEND_PCT_THRESHOLD", "40"))
SPEND_DIR = os.getenv("SPEND_DIRECTION", "up").lower()  # up|down|both

def tg_send(text: str):
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML"}, timeout=20)
    except Exception: pass

def load_state() -> Dict[str, Any]:
    try:
        r = requests.get(f"https://api.github.com/gists/{GIST_ID}", timeout=30)
        r.raise_for_status()
        files = r.json().get("files", {})
        if GIST_FILENAME in files:
            return json.loads(files[GIST_FILENAME]["content"] or "{}")
    except Exception:
        pass
    return {}

def save_state(state: Dict[str, Any]):
    headers={"Authorization": f"token {GIST_TOKEN}"}
    payload={"files":{GIST_FILENAME: {"content": json.dumps(state, ensure_ascii=False)}}}
    requests.patch(f"https://api.github.com/gists/{GIST_ID}", headers=headers, json=payload, timeout=30)

def _normalize_money(s: str) -> float:
    s = (s or "").strip()
    s = s.replace("$","").replace(",","").replace("\u00A0","")
    try: return float(s)
    except: return 0.0

def fetch_rows_via_dom() -> List[Dict[str, Any]]:
    """Логин + парс DOM-таблицы отчёта (без XHR). Возвращает список словарей по строкам."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # --- 1) Логин ---
        page.goto(LOGIN_URL, wait_until="domcontentloaded")
        # поля (Keitaro login.vue)
        filled = False
        try:
            page.get_by_placeholder(re.compile(r"Username|Login|Email", re.I)).fill(LOGIN_USER)
            page.get_by_placeholder(re.compile(r"Password", re.I)).fill(LOGIN_PASS)
            filled = True
        except Exception:
            # универсальные селекторы на всякий случай
            try:
                page.locator("input[type=text], input[name=login], input[name=email]").first.fill(LOGIN_USER)
                page.locator("input[type=password]").first.fill(LOGIN_PASS)
                filled = True
            except Exception:
                pass

        if filled:
            # кнопка Sign in
            try:
                page.get_by_role("button", name=re.compile(r"sign in|войти|login", re.I)).click()
            except Exception:
                page.locator("button").first.click()

        page.wait_for_load_state("networkidle")

        # --- 2) Страница отчёта ---
        page.goto(PAGE_URL, wait_until="domcontentloaded")

        # ждём таблицу
        page.wait_for_selector("table", timeout=15000)

        # --- 3) Читаем заголовки и строим индекс колонок ---
        headers = page.eval_on_selector_all("table thead th", "els => els.map(e => e.innerText.trim().toLowerCase())")
        idx = {h:i for i,h in enumerate(headers)}

        def gi(*keys, default=None):
            for k in idx:
                for key in keys:
                    if key in k:
                        return idx[k]
            return default

        i_campaign = gi("campaign")
        i_sub6    = gi("sub id 6","sub_id 6","sub_id_6")
        i_sub5    = gi("sub id 5","sub_id 5","sub_id_5")
        i_sub4    = gi("sub id 4","sub_id 4","sub_id_4")
        i_country = gi("country")
        i_clicks  = gi("clicks")
        i_leads   = gi("leads")
        i_sales   = gi("sales")
        i_cost    = gi("cost")
        i_cpa     = gi("cpa")
        i_roi     = gi("roi")

        trs = page.query_selector_all("table tbody tr")
        rows: List[Dict[str, Any]] = []
        for tr in trs:
            tds = tr.query_selector_all("td")
            if not tds or i_campaign is None:
                continue

            def val(i):
                try:
                    if i is None or i >= len(tds): return ""
                    return tds[i].inner_text().strip()
                except: return ""

            rows.append({
                "campaign": val(i_campaign),
                "sub_id_6": val(i_sub6),
                "sub_id_5": val(i_sub5),
                "sub_id_4": val(i_sub4),
                "country":  val(i_country),
                "clicks":   int(val(i_clicks) or 0),
                "leads":    int(val(i_leads)  or 0),
                "sales":    int(val(i_sales)  or 0),
                "cpa":      _normalize_money(val(i_cpa)),
                "roi":      val(i_roi),
                "cost":     _normalize_money(val(i_cost)),
            })

        browser.close()
        return rows

def key_of(row: Dict[str, Any]) -> str:
    # группируем по (campaign, sub_id_6) — при желании расширь
    return f"{row.get('campaign','')}|{row.get('sub_id_6','')}"

def detect_changes(prev: Dict[str, Any], curr_rows: List[Dict[str, Any]]) -> Tuple[List[str], Dict[str, Any]]:
    """Формируем сообщения: про spend, leads, sales. Возвращаем (messages, new_state)."""
    new_state = prev.copy()
    msgs: List[str] = []

    for r in curr_rows:
        k = key_of(r)
        now_cost  = float(r.get("cost", 0) or 0)
        now_leads = int(r.get("leads",0) or 0)
        now_sales = int(r.get("sales",0) or 0)

        old = prev.get(k, {"cost":0, "leads":0, "sales":0})
        old_cost, old_leads, old_sales = float(old.get("cost",0)), int(old.get("leads",0)), int(old.get("sales",0))

        # 1) Spend jump
        delta_cost = now_cost - old_cost
        pct = (abs(delta_cost) / old_cost * 100) if old_cost > 0 else (100 if now_cost>0 else 0)
        direction_ok = (SPEND_DIR=="both") or (SPEND_DIR=="up" and delta_cost>0) or (SPEND_DIR=="down" and delta_cost<0)
        if direction_ok and (abs(delta_cost) >= SPEND_AB or pct >= SPEND_PCT):
            arrow = "⬆️" if delta_cost>0 else "⬇️"
            msgs.append(
                f"<b>SPEND {arrow}</b>\n"
                f"<b>Campaign:</b> {r['campaign']}\n"
                f"<b>SubID6:</b> {r.get('sub_id_6','')}\n"
                f"<b>Cost:</b> ${old_cost:.2f} → ${now_cost:.2f} ({'+' if delta_cost>=0 else ''}{delta_cost:.2f}, ~{pct:.0f}%)"
            )

        # 2) Leads change
        if now_leads != old_leads:
            arrow = "🟢" if now_leads>old_leads else "🟠"
            msgs.append(
                f"<b>LEADS {arrow}</b>\n"
                f"<b>Campaign:</b> {r['campaign']} | <b>SubID6:</b> {r.get('sub_id_6','')}\n"
                f"{old_leads} → {now_leads}"
            )

        # 3) Sales change
        if now_sales != old_sales:
            arrow = "💰" if now_sales>old_sales else "🟨"
            msgs.append(
                f"<b>SALES {arrow}</b>\n"
                f"<b>Campaign:</b> {r['campaign']} | <b>SubID6:</b> {r.get('sub_id_6','')}\n"
                f"{old_sales} → {now_sales}"
            )

        new_state[k] = {"cost": now_cost, "leads": now_leads, "sales": now_sales}

    return msgs, new_state

def main():
    rows = fetch_rows_via_dom()
    if not rows:
        tg_send("⚠️ Не удалось прочитать таблицу (DOM пуст). Проверьте URL отчёта и доступы.")
        return

    prev = load_state()
    msgs, new_state = detect_changes(prev, rows)

    if msgs:
        for m in msgs:
            tg_send(m)
    else:
        # опционально — тихий проход без уведомлений
        pass

    save_state(new_state)

if __name__ == "__main__":
    main()
