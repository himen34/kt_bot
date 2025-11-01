# -*- coding: utf-8 -*-
import os, re, json
from collections import defaultdict
from typing import Dict, Any, List, Tuple
import requests
from playwright.sync_api import sync_playwright

# ---------- ENV ----------
LOGIN_URL  = "https://trident.partners/admin/"
LOGIN_USER = os.environ["LOGIN_USER"]
LOGIN_PASS = os.environ["LOGIN_PASS"]
PAGE_URL   = os.environ["PAGE_URL"]

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT  = os.environ["TELEGRAM_CHAT_ID"]

GIST_ID       = os.environ["GIST_ID"]
GIST_TOKEN    = os.environ["GIST_TOKEN"]
GIST_FILENAME = os.getenv("GIST_FILENAME", "keitaro_spend_state.json")

SPEND_ABS = float(os.getenv("SPEND_ABS_THRESHOLD", "20"))   # $ threshold
SPEND_PCT = float(os.getenv("SPEND_PCT_THRESHOLD", "20"))   # % threshold
SPEND_DIR = os.getenv("SPEND_DIRECTION", "up").lower()      # up|down|both

# ---------- helpers ----------
def tg_send(text: str) -> None:
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=25,
        ).raise_for_status()
    except Exception:
        pass

def load_state() -> Dict[str, Any]:
    try:
        r = requests.get(
            f"https://api.github.com/gists/{GIST_ID}",
            headers={"Authorization": f"token {GIST_TOKEN}"},
            timeout=30,
        )
        if r.status_code == 404: return {}
        r.raise_for_status()
        files = r.json().get("files", {})
        content = files.get(GIST_FILENAME, {}).get("content", "")
        return json.loads(content) if content else {}
    except Exception:
        return {}

def save_state(state: Dict[str, Any]) -> None:
    payload = {"files": {GIST_FILENAME: {"content": json.dumps(state, ensure_ascii=False, indent=2)}}}
    requests.patch(
        f"https://api.github.com/gists/{GIST_ID}",
        headers={"Authorization": f"token {GIST_TOKEN}"},
        json=payload,
        timeout=30,
    ).raise_for_status()

def _to_int(x: Any) -> int:
    try: return int(str(x).strip())
    except: return 0

def _to_money(s: Any) -> float:
    try:
        s = str(s).replace("$","").replace(",","").replace("\u00A0","").strip()
        return float(s or 0)
    except:
        return 0.0

# ---------- scraping ----------
def fetch_rows_via_dom() -> List[Dict[str, Any]]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # login
        page.goto(LOGIN_URL, wait_until="domcontentloaded")
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

        # report
        page.goto(PAGE_URL, wait_until="domcontentloaded")
        page.wait_for_selector("table", timeout=20000)

        headers = page.eval_on_selector_all("table thead th", "els => els.map(e => e.innerText.trim().toLowerCase())")
        idx = {h: i for i, h in enumerate(headers)}

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
        i_clicks  = gi("clicks")
        i_leads   = gi("leads")
        i_sales   = gi("sales")
        i_cost    = gi("cost")

        if i_campaign is None or i_cost is None:
            raise RuntimeError("Не найдены обязательные колонки (campaign/cost). Проверь заголовки таблицы.")

        rows: List[Dict[str, Any]] = []
        for tr in page.query_selector_all("table tbody tr"):
            tds = tr.query_selector_all("td")
            if not tds: continue

            def val(i):
                if i is None or i >= len(tds): return ""
                try: return tds[i].inner_text().strip()
                except: return ""

            rows.append({
                "campaign": val(i_campaign),
                "sub_id_6": val(i_sub6),
                "sub_id_5": val(i_sub5),
                "sub_id_4": val(i_sub4),
                "clicks":   _to_int(val(i_clicks)),
                "leads":    _to_int(val(i_leads)),
                "sales":    _to_int(val(i_sales)),
                "cost":     round(_to_money(val(i_cost)), 2),
            })

        browser.close()
        return rows

# ---------- diff & pretty messages ----------
def key_for_state(r: Dict[str, Any]) -> str:
    # стабильный ключ — по кампании + SubID5 + SubID4
    return f"{r.get('campaign','')}|{r.get('sub_id_5','')}|{r.get('sub_id_4','')}"

def detect_changes(prev: Dict[str, Any], curr: List[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, List[str]]], Dict[str, Any]]:
    new_state = prev.copy()
    out: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: {"spend": [], "regs": [], "deps": []})

    for r in curr:
        camp = (r.get("campaign") or "").strip()
        sub5 = (r.get("sub_id_5") or "").strip()
        sub4 = (r.get("sub_id_4") or "").strip()
        k = key_for_state(r)

        now_cost  = float(r.get("cost") or 0.0)
        now_leads = int(r.get("leads") or 0)
        now_sales = int(r.get("sales") or 0)

        old = prev.get(k)
        if old is None:
            new_state[k] = {"cost": now_cost, "leads": now_leads, "sales": now_sales}
            continue

        old_cost  = float(old.get("cost", 0.0))
        old_leads = int(old.get("leads", 0))
        old_sales = int(old.get("sales", 0))

        # spend
        delta = round(now_cost - old_cost, 2)
        pct = round((abs(delta) / old_cost * 100.0), 0) if old_cost > 0 else (100.0 if now_cost > 0 else 0.0)
        direction_ok = (SPEND_DIR == "both") or (SPEND_DIR == "up" and delta > 0) or (SPEND_DIR == "down" and delta < 0)

        if direction_ok and (abs(delta) >= SPEND_ABS or pct >= SPEND_PCT):
            arrow = "🔺" if delta > 0 else "🔻"
            out[camp]["spend"].append(
                f"SubID5: <code>{sub5}</code>  SubID4: <code>{sub4}</code>\n"
                f"Cost: ${old_cost:.2f} → <b>${now_cost:.2f}</b>  (Δ {('+' if delta>=0 else '')}{delta:.2f}, ~{int(pct)}%) {arrow}"
            )

        # regs
        if now_leads != old_leads:
            out[camp]["regs"].append(
                f"SubID5: <code>{sub5}</code>  SubID4: <code>{sub4}</code>  REG: {old_leads} → <b>{now_leads}</b>"
            )

        # deps
        if now_sales != old_sales:
            out[camp]["deps"].append(
                f"SubID5: <code>{sub5}</code>  SubID4: <code>{sub4}</code>  DEP: {old_sales} → <b>{now_sales}</b>"
            )

        new_state[k] = {"cost": now_cost, "leads": now_leads, "sales": now_sales}

    return out, new_state

def send_grouped_messages(changes: Dict[str, Dict[str, List[str]]]) -> int:
    sent = 0
    for camp, parts in changes.items():
        blocks: List[str] = []

        if parts["spend"]:
            blocks.append("🧊 <b>SPEND ALERT</b>")
            blocks.append(f"<b>CAMPAIGN:</b> {camp}")
            blocks.append("\n".join(parts["spend"]))

        if parts["regs"]:
            if blocks: blocks.append("")
            blocks.append("🟩 <b>REGS</b>")
            blocks.append("\n".join(parts["regs"]))

        if parts["deps"]:
            if blocks: blocks.append("")
            blocks.append("🟧 <b>DEPS</b>")
            blocks.append("\n".join(parts["deps"]))

        if not blocks:
            continue

        tg_send("\n".join(blocks))
        sent += 1

    return sent

# ---------- main ----------
def main() -> None:
    rows = fetch_rows_via_dom()
    prev = load_state()

    # тихая инициализация базы
    if not prev:
        base = {}
        for r in rows:
            base[key_for_state(r)] = {
                "cost": float(r.get("cost") or 0.0),
                "leads": int(r.get("leads") or 0),
                "sales": int(r.get("sales") or 0),
            }
        save_state(base)
        return

    changes, new_state = detect_changes(prev, rows)
    total = send_grouped_messages(changes)
    if total == 0:
        tg_send("🛌 <b>ACCS ON VACATION…</b>")

    save_state(new_state)

if __name__ == "__main__":
    main()
