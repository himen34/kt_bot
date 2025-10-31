# -*- coding: utf-8 -*-
"""
Логин в https://trident.partners/admin/, открытие favourite-репорта (PAGE_URL),
парсинг DOM-таблицы и сравнение с прошлым состоянием (Gist).
Шлёт отдельные уведомления в Telegram:
 - 🟦 резкий скачок spend (cost) по порогам ABS/PCT и направлению DIRECTION
 - 🟩 новая "регa" (рост leads)
 - 🟧 новый "деп" (рост sales)

ENV (GitHub Secrets):
  LOGIN_USER, LOGIN_PASS
  PAGE_URL
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  GIST_ID, GIST_TOKEN, [GIST_FILENAME=keitaro_spend_state.json]
  [SPEND_ABS_THRESHOLD=100], [SPEND_PCT_THRESHOLD=40], [SPEND_DIRECTION=up|down|both]
"""

import os
import re
import json
from typing import List, Dict, Any, Tuple
import requests
from playwright.sync_api import sync_playwright

# ----------- Конфиг из переменных окружения -----------
LOGIN_URL  = "https://trident.partners/admin/"
LOGIN_USER = os.environ["LOGIN_USER"]
LOGIN_PASS = os.environ["LOGIN_PASS"]
PAGE_URL   = os.environ["PAGE_URL"]

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT  = os.environ["TELEGRAM_CHAT_ID"]

GIST_ID       = os.environ["GIST_ID"]
GIST_TOKEN    = os.environ["GIST_TOKEN"]
GIST_FILENAME = os.getenv("GIST_FILENAME", "keitaro_spend_state.json")

SPEND_ABS = float(os.getenv("SPEND_ABS_THRESHOLD", "100"))   # $-порог
SPEND_PCT = float(os.getenv("SPEND_PCT_THRESHOLD", "40"))    # %-порог
SPEND_DIR = os.getenv("SPEND_DIRECTION", "up").lower()        # up|down|both


# ------------------- Утилиты -------------------
def tg_send(text: str) -> None:
    """Отправка сообщения в Telegram."""
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=25
        ).raise_for_status()
    except Exception:
        pass


def load_state() -> Dict[str, Any]:
    """Читаем прошлое состояние из Gist (key -> {cost, leads, sales})."""
    try:
        r = requests.get(f"https://api.github.com/gists/{GIST_ID}", timeout=30,
                         headers={"Authorization": f"token {GIST_TOKEN}"})
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        files = r.json().get("files", {})
        if GIST_FILENAME in files and files[GIST_FILENAME].get("content"):
            return json.loads(files[GIST_FILENAME]["content"])
    except Exception:
        pass
    return {}


def save_state(state: Dict[str, Any]) -> None:
    """Сохраняем состояние в Gist."""
    payload = {"files": {GIST_FILENAME: {"content": json.dumps(state, ensure_ascii=False, indent=2)}}}
    requests.patch(
        f"https://api.github.com/gists/{GIST_ID}",
        headers={"Authorization": f"token {GIST_TOKEN}"},
        json=payload,
        timeout=30
    ).raise_for_status()


def _to_int(s: Any) -> int:
    try:
        return int(str(s).strip())
    except Exception:
        return 0


def _to_money(s: str) -> float:
    try:
        return float(str(s).replace("$", "").replace(",", "").replace("\u00A0", "").strip() or 0)
    except Exception:
        return 0.0


# ------------------- Парсинг DOM-таблицы -------------------
def fetch_rows_via_dom() -> List[Dict[str, Any]]:
    """
    Логин + открытие страницы отчёта + парс таблицы (thead/tbody).
    Возвращает список словарей: campaign, sub_id_6, clicks, leads, sales, cost (+доп. поля).
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # --- 1) логин ---
        page.goto(LOGIN_URL, wait_until="domcontentloaded")

        # ввод логина/пароля (несколько стратегий селекторов)
        filled = False
        try:
            page.get_by_placeholder(re.compile(r"Username|Login|Email", re.I)).fill(LOGIN_USER)
            page.get_by_placeholder(re.compile(r"Password", re.I)).fill(LOGIN_PASS)
            filled = True
        except Exception:
            try:
                page.locator("input[type=text], input[name=login], input[name=email]").first.fill(LOGIN_USER)
                page.locator("input[type=password]").first.fill(LOGIN_PASS)
                filled = True
            except Exception:
                pass

        if filled:
            try:
                page.get_by_role("button", name=re.compile(r"(sign in|войти|login)", re.I)).click()
            except Exception:
                page.locator("button").first.click()

        page.wait_for_load_state("networkidle")

        # --- 2) страница отчёта ---
        page.goto(PAGE_URL, wait_until="domcontentloaded")
        page.wait_for_selector("table", timeout=15000)

        # --- 3) хэдеры и индексы колонок ---
        headers = page.eval_on_selector_all(
            "table thead th", "els => els.map(e => e.innerText.trim().toLowerCase())"
        )
        idx = {h: i for i, h in enumerate(headers)}

        def gi(*keys, default=None):
            for k in idx:
                for key in keys:
                    if key in k:
                        return idx[k]
            return default

        i_campaign = gi("campaign")
        i_sub6    = gi("sub id 6", "sub_id 6", "sub_id_6")
        i_sub5    = gi("sub id 5", "sub_id 5", "sub_id_5")
        i_sub4    = gi("sub id 4", "sub_id 4", "sub_id_4")
        i_country = gi("country")
        i_clicks  = gi("clicks")
        i_leads   = gi("leads")
        i_sales   = gi("sales")
        i_cost    = gi("cost")
        i_cpa     = gi("cpa")
        i_roi     = gi("roi")

        if i_campaign is None or i_cost is None:
            raise RuntimeError("Не найдены обязательные колонки (campaign/cost). Проверь заголовки таблицы.")

        # --- 4) строки ---
        trs = page.query_selector_all("table tbody tr")
        rows: List[Dict[str, Any]] = []

        for tr in trs:
            tds = tr.query_selector_all("td")
            if not tds:
                continue

            def val(i):
                if i is None or i >= len(tds):
                    return ""
                try:
                    return tds[i].inner_text().strip()
                except Exception:
                    return ""

            rows.append({
                "campaign": val(i_campaign),
                "sub_id_6": val(i_sub6),
                "sub_id_5": val(i_sub5),
                "sub_id_4": val(i_sub4),
                "country":  val(i_country),
                "clicks":   _to_int(val(i_clicks)),
                "leads":    _to_int(val(i_leads)),
                "sales":    _to_int(val(i_sales)),
                "cpa":      _to_money(val(i_cpa)),
                "roi":      val(i_roi),
                "cost":     _to_money(val(i_cost)),
            })

        browser.close()
        return rows


# ------------------- Сравнение и формирование алертов -------------------
def key_of(row: Dict[str, Any]) -> str:
    # Ключ агрегации: кампания + SubID6 (можно расширить)
    return f"{row.get('campaign','')}|{row.get('sub_id_6','')}"

def detect_changes(prev: Dict[str, Any], curr_rows: List[Dict[str, Any]]) -> Tuple[List[str], Dict[str, Any]]:
    """Возвращает (список сообщений, новое_состояние)."""
    new_state = prev.copy()
    messages: List[str] = []

    for r in curr_rows:
        k = key_of(r)
        now_cost  = float(r.get("cost") or 0.0)
        now_leads = int(r.get("leads") or 0)
        now_sales = int(r.get("sales") or 0)

        old = prev.get(k, {"cost": 0.0, "leads": 0, "sales": 0})
        old_cost  = float(old.get("cost", 0.0))
        old_leads = int(old.get("leads", 0))
        old_sales = int(old.get("sales", 0))

        # --- 1) Spend jump ---
        delta_cost = now_cost - old_cost
        pct = (abs(delta_cost) / old_cost * 100.0) if old_cost > 0 else (100.0 if now_cost > 0 else 0.0)

        direction_ok = (
            SPEND_DIR == "both" or
            (SPEND_DIR == "up" and delta_cost > 0) or
            (SPEND_DIR == "down" and delta_cost < 0)
        )
        if direction_ok and (abs(delta_cost) >= SPEND_ABS or pct >= SPEND_PCT):
            arrow = "🔺" if delta_cost > 0 else "🔻"
            messages.append(
                "🟦 <b>Spend change</b>\n"
                f"Campaign: <code>{r.get('campaign','')}</code>\n"
                f"SubID6: <code>{r.get('sub_id_6','')}</code>\n"
                f"Cost: ${old_cost:.2f} → <b>${now_cost:.2f}</b>  "
                f"(Δ {('+' if delta_cost>=0 else '')}{delta_cost:.2f}, ~{pct:.0f}%){' ' + arrow}"
            )

        # --- 2) Leads (регa) ---
        if now_leads != old_leads:
            if now_leads > old_leads:
                diff = now_leads - old_leads
                messages.append(
                    "🟩 <b>New reg</b> (leads)\n"
                    f"Campaign: <code>{r.get('campaign','')}</code>\n"
                    f"SubID6: <code>{r.get('sub_id_6','')}</code>\n"
                    f"{old_leads} → <b>{now_leads}</b>  (Δ +{diff})"
                )

        # --- 3) Sales (деп) ---
        if now_sales != old_sales:
            if now_sales > old_sales:
                diff = now_sales - old_sales
                messages.append(
                    "🟧 <b>New dep</b> (sales)\n"
                    f"Campaign: <code>{r.get('campaign','')}</code>\n"
                    f"SubID6: <code>{r.get('sub_id_6','')}</code>\n"
                    f"{old_sales} → <b>{now_sales}</b>  (Δ +{diff})"
                )

        # обновляем состояние по ключу
        new_state[k] = {"cost": now_cost, "leads": now_leads, "sales": now_sales}

    return messages, new_state


# ------------------- Точка входа -------------------
def main() -> None:
    try:
        rows = fetch_rows_via_dom()
    except Exception as e:
        tg_send(f"⚠️ Не удалось прочитать таблицу: <code>{e}</code>")
        return

    if not rows:
        tg_send("⚠️ Таблица пуста или не найдена. Проверьте URL отчёта/доступы.")
        return

    prev = load_state()
    msgs, new_state = detect_changes(prev, rows)

    # отправляем отдельными сообщениями
    for m in msgs:
        tg_send(m)

    # сохраняем состояние (даже если изменений не было)
    try:
        save_state(new_state)
    except Exception:
        pass


if __name__ == "__main__":
    main()
