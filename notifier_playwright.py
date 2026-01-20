# notifier_playwright.py — стабильные алерты без дублей
import os, json, time, re
from typing import Dict, List, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PWTimeout

# ========= ENV =========
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

SPEND_DIR = (os.getenv("SPEND_DIRECTION", "both") or "both").lower()  # up|down|both
KYIV_TZ   = ZoneInfo(os.getenv("KYIV_TZ", "Europe/Kyiv"))

EPS = 0.009  # всё что > ~1 цент — считаем изменением

# ========= utils =========
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
if SPEND_DIR == "up":   return delta >  EPS
if SPEND_DIR == "down": return delta < -EPS
return abs(delta) > EPS

# ========= state (Gist) =========
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
return {"date": kyiv_today_str(), "rows": {}}

def save_state(state: Dict):
url = f"https://api.github.com/gists/{GIST_ID}"
files = {GIST_FILENAME: {"content": json.dumps(state, ensure_ascii=False, indent=2)}}
r = requests.patch(url, headers={
"Authorization": f"Bearer {GIST_TOKEN}",
"Accept": "application/vnd.github+json"
}, json={"files": files}, timeout=30)
r.raise_for_status()

# ========= Telegram =========
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

# ========= parsing helpers =========
def as_float(v):
try:
return float(v or 0)
except:
return 0.0

def parse_report_from_json(payload: dict) -> List[Dict]:
rows = []
for r in payload.get("rows", []):
dims = r.get("dimensions", {}) if isinstance(r.get("dimensions"), dict) else {}
def g(k): 
return r.get(k) or dims.get(k) or ""
# пробуем разные ключи для geo
geo = g("country") or g("country_code") or g("country_iso2") or g("geo")
rows.append({
"k": f"{g('campaign')}|{g('sub_id_6')}|{g('sub_id_5')}|{g('sub_id_4')}",
"campaign": str(g("campaign")),
"sub_id_6": str(g("sub_id_6")),
"sub_id_5": str(g("sub_id_5")),
"sub_id_4": str(g("sub_id_4")),
"geo":      str(geo),
"cost":  as_float(r.get("cost")),
"leads": as_float(r.get("leads")),
"sales": as_float(r.get("sales")),
"cpa":   as_float(r.get("cpa")),
})
return rows

def parse_report_from_html(page) -> List[Dict]:
rows = []
try:
page.wait_for_selector("table", timeout=15000)
except PWTimeout:
return rows

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
if n in h:
return i
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
try:
return (tds[i].inner_text() or "").strip()
except:
return ""

def to_f(s: str) -> float:
s = s.replace("$", "").replace(",", "").strip()
try:
return float(s)
except:
return 0.0

# geo берём из td.grid-cell-country_flag[title]
geo = ""
try:
flag_td = tr.query_selector("td.grid-cell-country_flag")
if flag_td:
geo = (flag_td.get_attribute("title") or "").strip()
except Exception:
geo = ""

rows.append({
"k": f"{safe(idx['campaign'])}|{safe(idx['sid6'])}|{safe(idx['sid5'])}|{safe(idx['sid4'])}",
"campaign": safe(idx["campaign"]),
"sub_id_6": safe(idx["sid6"]),
"sub_id_5": safe(idx["sid5"]),
"sub_id_4": safe(idx["sid4"]),
"geo":      geo,
"cost":  to_f(safe(idx["cost"])),
"leads": to_f(safe(idx["leads"])),
"sales": to_f(safe(idx["sales"])),
"cpa":   to_f(safe(idx["cpa"])),
})
return rows

# ========= fetch with stabilisation =========
def aggregate_rows_max(rows: List[Dict]) -> List[Dict]:
"""Склейка дубликатов за запуск: берём максимум по cost/leads/sales на один ключ."""
acc: Dict[str, Dict] = {}
for r in rows:
k = r["k"]
if k not in acc:
acc[k] = dict(r)
else:
a = acc[k]
# максимум по основным метрикам
a["cost"]  = max(a["cost"],  r["cost"])
a["leads"] = max(a["leads"], r["leads"])
a["sales"] = max(a["sales"], r["sales"])
# cpa пусть будет последний/максимальный — не критично
a["cpa"]   = max(a.get("cpa", 0.0), r.get("cpa", 0.0))
return list(acc.values())

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

# Выбираем "самый полный" XHR-пакет (по сумме cost)
captured: List[Dict] = []
best_score = -1.0

def on_response(resp):
nonlocal captured, best_score
url = (resp.url or "").lower()
if "/report" in url or "/reports" in url:
try:
data = resp.json()
except Exception:
return
rows = parse_report_from_json(data)
if not rows:
return
score = sum((r.get("cost") or 0.0) for r in rows)
if score > best_score:
captured = rows
best_score = score

ctx.on("response", on_response)

page.goto(PAGE_URL, wait_until="domcontentloaded")
# дождаться затухания сети и дать SPA дорисоваться
try:
page.wait_for_load_state("networkidle", timeout=15000)
except PWTimeout:
pass
time.sleep(1.0)

# если XHR не словили — fallback
rows: List[Dict] = captured if captured else []
if not rows:
try:
# HTML-таблица
rows = parse_report_from_html(page)
except Exception:
rows = []

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
if v in h:
return i
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
try:
return float(s)
except:
return 0.0

tmp = []
for row in rws.all():
cells = [ (c.inner_text() or "").strip() 
for c in row.locator(".ag-cell-value").all() ]

def safe(i):
try:
return cells[i]
except:
return ""

tmp.append({
"k": f"{safe(i_campaign)}|{safe(i_sid6)}|{safe(i_sid5)}|{safe(i_sid4)}",
"campaign": safe(i_campaign),
"sub_id_6": safe(i_sid6),
"sub_id_5": safe(i_sid5),
"sub_id_4": safe(i_sid4),
"geo":      "",   # ag-grid fallback без geo
"cost":  to_f(safe(i_cost)),
"leads": to_f(safe(i_leads)),
"sales": to_f(safe(i_sales)),
"cpa":   to_f(safe(i_cpa)),
})
rows = tmp
except Exception:
rows = []

browser.close()
# анти-дубли за запуск
return aggregate_rows_max(rows)

# ========= formatting helpers for SubIDs & Geo =========
def format_subs_and_geo(r: Dict) -> str:
"""
   Собирает строку вида:
   'SubID6: xxx  SubID5: yyy  SubID4: zzz  Geo: DE'
   При этом:
     - если значение пустое -> не выводим;
     - если значение равно плейсхолдеру {sub6}/{sub5}/{sub4}/{geo} -> не выводим.
   """
parts: List[str] = []

def add(label: str, value: str, placeholder: str):
v = str(value or "").strip()
if not v:
return
if v.lower() == placeholder.lower():
return
parts.append(f"{label}: {v}")

add("SubID6", r.get("sub_id_6", ""), "{sub6}")
add("SubID5", r.get("sub_id_5", ""), "{sub5}")
add("SubID4", r.get("sub_id_4", ""), "{sub4}")
add("Geo",    r.get("geo", ""),       "{geo}")

return "  ".join(parts)

# ========= main logic =========
def clamp_monotonic(new_v: float, old_v: float) -> float:
"""Запрет «отката»: метрика не может стать меньше прошлой (иногда SPA даёт промежуточные значения)."""
if old_v is None:
return new_v
return new_v if new_v >= (old_v - 1e-6) else old_v

def main():
state = load_state()
prev_date: str = state.get("date", kyiv_today_str())
prev_rows: Dict[str, Dict] = state.get("rows", {})
today = kyiv_today_str()

rows = fetch_rows()
if not rows:
tg_send("accs on vacation...")
return

# Сброс у полуночи по Киеву
if prev_date != today:
baseline = {r["k"]: r for r in rows}
save_state({"date": today, "rows": baseline})
tg_send("accs on vacation...")
return

# Сформируем «новую карту» с монотоничными метриками
new_map: Dict[str, Dict] = {}
# Собираем сообщения; для SPEND — по ключу берём только самое большое |Δ|
best_spend_msg: Dict[str, Tuple[float, str]] = {}
lead_msgs: List[str] = []
sale_msgs: List[str] = []

for r in rows:
k = r["k"]
old = prev_rows.get(k)
if old:
# монотонность
r["cost"]  = clamp_monotonic(r["cost"],  old.get("cost", 0.0))
r["leads"] = clamp_monotonic(r["leads"], old.get("leads", 0.0))
r["sales"] = clamp_monotonic(r["sales"], old.get("sales", 0.0))

# строка с сабами и гео
subs_line = format_subs_and_geo(r)
subs_block = (subs_line + "\n") if subs_line else ""

# SPEND
delta_cost = r["cost"] - old.get("cost", 0.0)
if direction_ok(delta_cost):
p = pct(delta_cost, old.get("cost", 0.0))
arrow = "🔺" if delta_cost > 0 else "🔻"
msg = (
"🧊 *SPEND ALERT*\n"
f"CAMPAIGN: {r['campaign']}\n"
f"{subs_block}"
f"Cost: {fmt_money(old.get('cost', 0.0))} → {fmt_money(r['cost'])}  "
f"(Δ {fmt_money(delta_cost)}, ~{p:.0f}%) {arrow}"
)
# убираем лишнюю пустую строку, если subs_block пустой
msg = msg.replace("\n\nC", "\nC")

score = abs(delta_cost)
prev_best = best_spend_msg.get(k)
if (prev_best is None) or (score > prev_best[0] + 1e-9):
best_spend_msg[k] = (score, msg)

# LEADS
if r["leads"] - old.get("leads", 0.0) > EPS:
cpa_part = f"  • CPA: {fmt_money(r.get('cpa', 0.0))}" if r.get("cpa", 0.0) > EPS else ""
msg = (
"🟩 *LEAD ALERT*\n"
f"CAMPAIGN: {r['campaign']}\n"
f"{subs_block}"
f"Leads: {int(old.get('leads', 0))} → {int(r['leads'])}{cpa_part}"
)
msg = msg.replace("\n\nL", "\nL")
lead_msgs.append(msg)

# SALES
if r["sales"] - old.get("sales", 0.0) > EPS:
msg = (
"🟦 *SALE ALERT*\n"
f"CAMPAIGN: {r['campaign']}\n"
f"{subs_block}"
f"Sales: {int(old.get('sales', 0))} → {int(r['sales'])}"
)
msg = msg.replace("\n\nS", "\nS")
sale_msgs.append(msg)

else:
# Новый ключ: считаем дельту от 0
subs_line = format_subs_and_geo(r)
subs_block = (subs_line + "\n") if subs_line else ""

if r["cost"] > EPS:
p = 100.0
msg = (
"🧊 *SPEND ALERT*\n"
f"CAMPAIGN: {r['campaign']}\n"
f"{subs_block}"
f"Cost: {fmt_money(0)} → {fmt_money(r['cost'])}  (Δ {fmt_money(r['cost'])}, ~{p:.0f}%) 🔺"
)
msg = msg.replace("\n\nC", "\nC")
best_spend_msg[k] = (r["cost"], msg)

if r["leads"] > EPS:
cpa_part = f"  • CPA: {fmt_money(r.get('cpa', 0.0))}" if r.get("cpa", 0.0) > EPS else ""
msg = (
"🟩 *LEAD ALERT*\n"
f"CAMPAIGN: {r['campaign']}\n"
f"{subs_block}"
f"Leads: 0 → {int(r['leads'])}{cpa_part}"
)
msg = msg.replace("\n\nL", "\nL")
lead_msgs.append(msg)

if r["sales"] > EPS:
msg = (
"🟦 *SALE ALERT*\n"
f"CAMPAIGN: {r['campaign']}\n"
f"{subs_block}"
f"Sales: 0 → {int(r['sales'])}"
)
msg = msg.replace("\n\nS", "\nS")
sale_msgs.append(msg)

new_map[k] = r  # обновлённая (монотонная) запись

# Сборка сообщений: по SPEND оставляем только «лучший» на ключ
spend_msgs = [v[1] for v in best_spend_msg.values()]
blocks = spend_msgs + lead_msgs + sale_msgs

if blocks:
tg_send("\n\n".join(blocks))
    else:
        tg_send("accs on vacation...")

# Сохранить стейт
save_state({"date": today, "rows": new_map})

if __name__ == "__main__":
main()
