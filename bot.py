import os
import asyncio
import datetime
import json
import re
import logging
import time
import threading
from typing import List, Dict, Any, Optional, Set

import requests
from flask import Flask, request
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.constants import ChatType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Configuration from environment ----------
DATA_FILE = "income.json"
SYNC_STATE_FILE = "sync_state.json"

OWNER_ID = int(os.environ.get("OWNER_ID", 0))

# PayWay (ABA) API credentials – leave empty if not using API sync
PAYWAY_MERCHANT_ID = os.environ.get("PAYWAY_MERCHANT_ID", "")
PAYWAY_API_KEY = os.environ.get("PAYWAY_API_KEY", "")
PAYWAY_BASE_URL = os.environ.get("PAYWAY_BASE_URL", "https://www.payway.com.kh/api/v1")
PAYWAY_BUSINESS = os.environ.get("PAYWAY_BUSINESS", "birdnest")
SYNC_INTERVAL_MINUTES = int(os.environ.get("SYNC_INTERVAL_MINUTES", "5"))

# Group monitoring – list of group chat IDs to watch
MONITORED_GROUP_IDS = [
    int(gid.strip())
    for gid in os.environ.get("MONITORED_GROUP_IDS", "").split(",")
    if gid.strip()
]
GROUP_BUSINESS_TAG = os.environ.get("GROUP_BUSINESS_TAG", "group_payments")

# In‑memory set to avoid duplicate processing of the same group message
processed_group_messages: Set[int] = set()

# Categories
CATEGORIES = {
    "product": "🛒 ផលិតផល",
    "delivery": "🚚 ដឹកជញ្ជូន",
    "other": "💸 ផ្សេងៗ"
}

# ---------- Data helpers ----------
def load_data() -> List[Dict[str, Any]]:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return []

def save_data(data: List[Dict[str, Any]]) -> None:
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_sync_state() -> Dict[str, Any]:
    if os.path.exists(SYNC_STATE_FILE):
        with open(SYNC_STATE_FILE, "r") as f:
            return json.load(f)
    return {"last_sync": None, "imported_ids": []}

def save_sync_state(state: Dict[str, Any]) -> None:
    with open(SYNC_STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def extract_amounts(text: str):
    """Extract USD and KHR from a manually typed message."""
    khr_match = re.search(r"ចំនួន\s*([\d,]+)\s*រៀល", text)
    usd_match = re.search(r"\$([\d\.]+)", text)
    khr = int(khr_match.group(1).replace(",", "")) if khr_match else 0
    usd = float(usd_match.group(1)) if usd_match else 0.0
    note = text
    for pat in [r"\$\d+\.?\d*", r"ចំនួន\s*[\d,]+\s*រៀល"]:
        note = re.sub(pat, "", note)
    note = note.strip().strip(" -,/")
    return usd, khr, note

def summarise(entries: list, label: str) -> str:
    if not entries:
        return f"គ្មានប្រតិបត្តិការ {label}"
    total_usd = sum(e["usd"] for e in entries)
    total_khr = sum(e["khr"] for e in entries)
    count_usd = sum(1 for e in entries if e["usd"] > 0)
    count_khr = sum(1 for e in entries if e["khr"] > 0)
    cat_summary = {}
    for e in entries:
        cat = e.get("category", "other")
        cat_summary[cat] = cat_summary.get(cat, 0) + e.get("usd",0) + e.get("khr",0)/4000
    lines = [
        f"សរុបប្រតិបត្តិការ {label}",
        f"៛ (KHR): {total_khr:,}   ចំនួន: {count_khr}",
        f"$ (USD): {total_usd:.2f}   ចំនួន: {count_usd}",
    ]
    if cat_summary:
        lines.append("\nតាមប្រភេទ៖")
        for cat_key, cat_label in CATEGORIES.items():
            val = cat_summary.get(cat_key, 0)
            if val:
                lines.append(f"  {cat_label}: ${val:.2f}")
    return "\n".join(lines)

# ---------- Keyboard ----------
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("ប្រចាំថ្ងៃ"), KeyboardButton("ប្រចាំសប្ដាហ៍"), KeyboardButton("ប្រចាំខែ")],
        [KeyboardButton("📂 កំណត់ប្រភេទ")],
    ],
    resize_keyboard=True,
)

# ---------- Handlers (private chat) ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "👋 សូមស្វាគមន៍! ជ្រើសរើសរបាយការណ៍ ឬផ្ញើការទូទាត់។",
        reply_markup=MAIN_KEYBOARD,
    )

async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("📊 ម៉ឺនុយ", reply_markup=MAIN_KEYBOARD)

async def delete_last(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    data = load_data()
    if data:
        removed = data.pop()
        save_data(data)
        await update.message.reply_text(f"🗑 បានលុបធាតុចុងក្រោយ: {removed.get('note','')} | ${removed.get('usd',0):.2f} / ៛{removed.get('khr',0):,}")
    else:
        await update.message.reply_text("គ្មានទិន្នន័យសម្រាប់លុប។")

async def day_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        date_str = context.args[0] if context.args else datetime.date.today().strftime("%Y-%m-%d")
        data = load_data()
        entries = [e for e in data if e["date"] == date_str]
        await update.message.reply_text(summarise(entries, f"ថ្ងៃទី {date_str}"))
    except Exception:
        await update.message.reply_text("សូមប្រើទម្រង់ `/day 2026-05-20`")

async def category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = load_data()
    if not data:
        await query.edit_message_text("មិនមានធាតុថ្មីៗទេ។")
        return
    cat_key = query.data.split("_")[1]
    data[-1]["category"] = cat_key
    save_data(data)
    await query.edit_message_text(f"✅ បានកំណត់ប្រភេទ: {CATEGORIES[cat_key]}")

# ---------- PayWay Sync (only works if credentials are provided) ----------
def fetch_payway_transactions() -> List[Dict[str, Any]]:
    if not PAYWAY_MERCHANT_ID or not PAYWAY_API_KEY:
        return []
    today = datetime.date.today().strftime("%Y-%m-%d")
    url = f"{PAYWAY_BASE_URL}/transactions"
    headers = {
        "Authorization": f"Bearer {PAYWAY_API_KEY}",
        "Content-Type": "application/json"
    }
    params = {
        "merchant_id": PAYWAY_MERCHANT_ID,
        "start_date": today,
        "end_date": today,
        "status": "approved"
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", data.get("transactions", []))
    except Exception as e:
        logger.error(f"PayWay fetch error: {e}")
        return []

def import_payway_transaction(txn: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    state = load_sync_state()
    txn_id = str(txn.get("tran_id", ""))
    if not txn_id or txn_id in state["imported_ids"]:
        return None
    amount = float(txn.get("amount", 0))
    currency = txn.get("currency", "").upper()
    description = txn.get("description", "") or txn.get("note", "")
    entry = {
        "date": txn.get("payment_date", datetime.date.today().strftime("%Y-%m-%d")),
        "usd": amount if currency == "USD" else 0.0,
        "khr": int(amount) if currency == "KHR" else 0,
        "note": description,
        "category": "other",
        "business": PAYWAY_BUSINESS,
        "tran_id": txn_id
    }
    return entry

def sync_payway_transactions() -> int:
    data = load_data()
    state = load_sync_state()
    imported_ids = set(state.get("imported_ids", []))
    transactions = fetch_payway_transactions()
    added = 0
    for txn in transactions:
        entry = import_payway_transaction(txn)
        if entry is None:
            continue
        data.append(entry)
        imported_ids.add(entry["tran_id"])
        added += 1
    if added > 0:
        save_data(data)
        state["imported_ids"] = list(imported_ids)
        state["last_sync"] = datetime.datetime.now().isoformat()
        save_sync_state(state)
    return added

async def manual_sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    await update.message.reply_text("⏳ កំពុងទាញយកទិន្នន័យពី PayWay...")
    added = sync_payway_transactions()
    if added > 0:
        await update.message.reply_text(f"✅ បានបន្ថែម {added} ប្រតិបត្តិការថ្មី។")
    else:
        await update.message.reply_text("ℹ️ មិនមានប្រតិបត្តិការថ្មីទេ។")

async def sync_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    state = load_sync_state()
    last_sync = state.get("last_sync")
    if last_sync:
        msg = f"សមកាលកម្មចុងក្រោយ: {last_sync}"
    else:
        msg = "មិនទាន់បានសមកាលកម្មនៅឡើយទេ។"
    await update.message.reply_text(msg)

# ---------- Group Payment Monitoring ----------
def extract_group_payment(text: str) -> Optional[tuple]:
    """
    Try to extract (usd, khr, note) from bank/payway/ACLEDA notification messages.
    Returns None if it doesn't look like a payment notification.
    """
    usd = 0.0
    khr = 0.0
    note = ""

    # --- USD patterns ---
    m = re.search(r"\$(\d+\.?\d*)", text)
    if m:
        usd = float(m.group(1))

    m = re.search(r"SALE\s+(\d+\.?\d*)\s+USD", text, re.IGNORECASE)
    if m:
        usd = float(m.group(1))

    # --- KHR patterns ---
    m = re.search(r"៛\s*([\d,]+)", text)
    if m:
        khr = float(m.group(1).replace(",", ""))
    else:
        m = re.search(r"ចំនួន\s*([\d,]+)\s*រៀល", text)
        if m:
            khr = float(m.group(1).replace(",", ""))

    if usd == 0 and khr == 0:
        return None

    # --- Build a meaningful note ---
    payer_match = re.search(r"paid by\s+([A-Za-z\s]+?)(?:\s*\(\*?\d+\))?\s", text, re.IGNORECASE)
    if payer_match:
        note = payer_match.group(1).strip()

    if not note:
        khmer_payer = re.search(r"ពី\s+([\w\s\u1780-\u17FF]+)", text)
        if khmer_payer:
            note = khmer_payer.group(1).strip()

    if not note:
        card_match = re.search(r"card\s+(\d+\*+\d+)", text, re.IGNORECASE)
        if card_match:
            note = "Card: " + card_match.group(1)
        else:
            ref_match = re.search(r"Ref\.ID\s+(\d+)", text, re.IGNORECASE)
            if ref_match:
                note = "Ref: " + ref_match.group(1)
            else:
                pos_match = re.search(r"POS\s+ID:(\d+)", text, re.IGNORECASE)
                if pos_match:
                    note = "POS: " + pos_match.group(1)

    if not note:
        note = text.split("\n")[0].strip()[:80]

    trx_match = re.search(r"Trx\.\s*ID:\s*(\d+)", text, re.IGNORECASE)
    if trx_match:
        note += f" (Trx: {trx_match.group(1)})"

    return usd, khr, note

async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle messages from monitored groups – silently record payments."""
    if not update.message or not update.message.text:
        return
    msg = update.message
    if msg.chat.id not in MONITORED_GROUP_IDS:
        return

    if msg.message_id in processed_group_messages:
        return
    processed_group_messages.add(msg.message_id)

    result = extract_group_payment(msg.text)
    if not result:
        return

    usd, khr, note = result
    today = datetime.date.today()

    data = load_data()
    entry = {
        "date": today.strftime("%Y-%m-%d"),
        "usd": usd,
        "khr": khr,
        "note": note,
        "category": "other",
        "business": GROUP_BUSINESS_TAG,
        "source": f"group_{msg.chat.id}_msg{msg.message_id}"
    }
    data.append(entry)
    save_data(data)
    logger.info(f"Group payment recorded: ${usd:.2f} / {khr}៛ from group {msg.chat.id}")

# ---------- Group Command: /daily ----------
async def group_daily_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /daily command inside a monitored group – show today's totals for this group."""
    if not update.message or update.message.chat.id not in MONITORED_GROUP_IDS:
        return

    # Uncomment the next two lines to restrict to owner only
    # if update.effective_user.id != OWNER_ID:
    #     return

    today = datetime.date.today()
    date_str = today.strftime("%Y-%m-%d")
    data = load_data()

    group_id = update.message.chat.id
    entries = [
        e for e in data
        if e["date"] == date_str and e.get("source", "").startswith(f"group_{group_id}_")
    ]

    if not entries:
        await update.message.reply_text(f"គ្មានប្រតិបត្តិការថ្ងៃនេះសម្រាប់ក្រុមនេះទេ។")
        return

    total_usd = sum(e["usd"] for e in entries)
    total_khr = sum(e["khr"] for e in entries)
    count_usd = sum(1 for e in entries if e["usd"] > 0)
    count_khr = sum(1 for e in entries if e["khr"] > 0)

    text = (
        f"📊 សរុបថ្ងៃនេះ ({date_str}) សម្រាប់ក្រុមនេះ៖\n"
        f"៛ (KHR): {total_khr:,}   ចំនួន: {count_khr}\n"
        f"$ (USD): {total_usd:.2f}   ចំនួន: {count_usd}"
    )
    await update.message.reply_text(text)

# ---------- Main private message handler ----------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if OWNER_ID and update.effective_user.id != OWNER_ID:
        return
    text = update.message.text.strip()
    today = datetime.date.today()
    data = load_data()

    if text == "ប្រចាំថ្ងៃ":
        date_str = today.strftime("%Y-%m-%d")
        entries = [e for e in data if e["date"] == date_str]
        await update.message.reply_text(summarise(entries, f"ថ្ងៃទី {date_str}"), reply_markup=MAIN_KEYBOARD)
        return

    if text == "ប្រចាំសប្ដាហ៍":
        week_start = today - datetime.timedelta(days=today.weekday())
        week_dates = {(week_start + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)}
        entries = [e for e in data if e["date"] in week_dates]
        label = f"សប្ដាហ៍ ({week_start.strftime('%Y-%m-%d')} → {today.strftime('%Y-%m-%d')})"
        await update.message.reply_text(summarise(entries, label), reply_markup=MAIN_KEYBOARD)
        return

    if text == "ប្រចាំខែ":
        month_prefix = today.strftime("%Y-%m")
        entries = [e for e in data if e["date"].startswith(month_prefix)]
        await update.message.reply_text(summarise(entries, f"ខែ {today.strftime('%Y-%m')}"), reply_markup=MAIN_KEYBOARD)
        return

    if text == "📂 កំណត់ប្រភេទ":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(CATEGORIES["product"], callback_data="cat_product")],
            [InlineKeyboardButton(CATEGORIES["delivery"], callback_data="cat_delivery")],
            [InlineKeyboardButton(CATEGORIES["other"], callback_data="cat_other")],
        ])
        await update.message.reply_text("ជ្រើសរើសប្រភេទសម្រាប់ធាតុចុងក្រោយ៖", reply_markup=keyboard)
        return

    usd, khr, note = extract_amounts(text)
    if usd or khr:
        entry = {
            "date": today.strftime("%Y-%m-%d"),
            "usd": usd,
            "khr": khr,
            "note": note,
            "category": "other"
        }
        data.append(entry)
        save_data(data)
        parts = []
        if usd:
            parts.append(f"${usd:.2f}")
        if khr:
            parts.append(f"៛{khr:,}")
        msg = f"✅ បានកត់ត្រា: {' / '.join(parts)}"
        if note:
            msg += f"\n📝 {note}"
        await update.message.reply_text(msg, reply_markup=MAIN_KEYBOARD)
        return

    await update.message.reply_text(
        "❓ មិនយល់។ សូមប្រើប៊ូតុង ឬផ្ញើចំនួនទឹកប្រាក់។",
        reply_markup=MAIN_KEYBOARD,
    )

# ---------- Background PayWay sync thread ----------
def sync_worker(bot_token: str) -> None:
    while True:
        try:
            if PAYWAY_MERCHANT_ID and PAYWAY_API_KEY:
                count = sync_payway_transactions()
                if count > 0 and OWNER_ID:
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    text = f"🤖 បានទាញយក {count} ប្រតិបត្តិការថ្មីពី PayWay ដោយស្វ័យប្រវត្តិ។"
                    payload = {"chat_id": OWNER_ID, "text": text}
                    try:
                        requests.post(url, json=payload, timeout=10)
                    except:
                        pass
                logger.info(f"PayWay sync completed. {count} new transactions added.")
        except Exception as e:
            logger.error(f"Sync worker error: {e}")
        time.sleep(SYNC_INTERVAL_MINUTES * 60)

# ---------- Build Application ----------
BOT_TOKEN = os.environ["BOT_TOKEN"]

application = (
    Application.builder()
    .token(BOT_TOKEN)
    .updater(None)
    .build()
)
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("menu", show_menu))
application.add_handler(CommandHandler("delete", delete_last))
application.add_handler(CommandHandler("day", day_report))
application.add_handler(CommandHandler("sync", manual_sync))
application.add_handler(CommandHandler("sync_status", sync_status))
application.add_handler(CallbackQueryHandler(category_callback, pattern="^cat_"))
# Group monitoring handler (silent recording)
application.add_handler(MessageHandler(
    filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND,
    group_message_handler
))
# Group command: /daily (only in monitored groups)
application.add_handler(CommandHandler("daily", group_daily_command, filters=filters.ChatType.GROUPS))
# Private chat handler (fallback)
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

LOOP = asyncio.new_event_loop()
LOOP.run_until_complete(application.initialize())

# ---------- Flask ----------
flask_app = Flask(__name__)

@flask_app.post("/webhook")
def webhook():
    payload = request.get_json(force=True)
    update = Update.de_json(payload, application.bot)
    LOOP.run_until_complete(application.process_update(update))
    return "OK"

@flask_app.get("/")
def health():
    return "Bot is running ✅"

@flask_app.get("/set_webhook")
def set_webhook():
    base_url = os.environ.get("WEBHOOK_URL", request.host_url.rstrip("/"))
    url = f"{base_url}/webhook"
    result = LOOP.run_until_complete(application.bot.set_webhook(url))
    return f"Webhook set to {url} — Telegram replied: {result}"

# Start PayWay sync thread (does nothing if credentials empty)
sync_thread = threading.Thread(target=sync_worker, args=(BOT_TOKEN,), daemon=True)
sync_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port)