import os
import asyncio
import datetime
import json
import re
import logging
from typing import List, Dict, Any

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Config ----------
DATA_FILE = "income.json"
OWNER_ID = int(os.environ.get("OWNER_ID", 0))   # set your Telegram user ID

CATEGORIES = {
    "product": "🛒 ផលិតផល",
    "delivery": "🚚 ដឹកជញ្ជូន",
    "other": "💸 ផ្សេងៗ"
}
CATEGORY_EMOJI = {v: k for k, v in CATEGORIES.items()}   # reverse map

# ---------- Data helpers ----------
def load_data() -> List[Dict[str, Any]]:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return []

def save_data(data: List[Dict[str, Any]]) -> None:
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def extract_amounts(text: str):
    khr_match = re.search(r"ចំនួន\s*([\d,]+)\s*រៀល", text)
    usd_match = re.search(r"\$([\d\.]+)", text)
    khr = int(khr_match.group(1).replace(",", "")) if khr_match else 0
    usd = float(usd_match.group(1)) if usd_match else 0.0
    # extract note (everything except the amount parts)
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
    # category breakdown
    cat_summary = {}
    for e in entries:
        cat = e.get("category", "other")
        cat_summary[cat] = cat_summary.get(cat, 0) + e.get("usd",0) + e.get("khr",0)/4000  # approximate usd equivalent
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

# ---------- Handlers ----------
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
    """Inline button to set category for the last entry."""
    query = update.callback_query
    await query.answer()
    data = load_data()
    if not data:
        await query.edit_message_text("មិនមានធាតុថ្មីៗទេ។")
        return
    cat_key = query.data.split("_")[1]   # cat_product, cat_delivery, cat_other
    data[-1]["category"] = cat_key
    save_data(data)
    await query.edit_message_text(f"✅ បានកំណត់ប្រភេទ: {CATEGORIES[cat_key]}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if OWNER_ID and update.effective_user.id != OWNER_ID:
        return   # ignore non-owners if OWNER_ID is set
    text = update.message.text.strip()
    today = datetime.date.today()
    data = load_data()

    # ── Report buttons ───────────────────────────────────
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

    # ── Category button ──────────────────────────────────
    if text == "📂 កំណត់ប្រភេទ":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(CATEGORIES["product"], callback_data="cat_product")],
            [InlineKeyboardButton(CATEGORIES["delivery"], callback_data="cat_delivery")],
            [InlineKeyboardButton(CATEGORIES["other"], callback_data="cat_other")],
        ])
        await update.message.reply_text("ជ្រើសរើសប្រភេទសម្រាប់ធាតុចុងក្រោយ៖", reply_markup=keyboard)
        return

    # ── Recording amounts ────────────────────────────────
    usd, khr, note = extract_amounts(text)
    if usd or khr:
        entry = {
            "date": today.strftime("%Y-%m-%d"),
            "usd": usd,
            "khr": khr,
            "note": note,
            "category": "other"   # default category
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

    # ── Unknown input ────────────────────────────────────
    await update.message.reply_text(
        "❓ មិនយល់។ សូមប្រើប៊ូតុង ឬផ្ញើចំនួនទឹកប្រាក់។",
        reply_markup=MAIN_KEYBOARD,
    )

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
application.add_handler(CallbackQueryHandler(category_callback, pattern="^cat_"))
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port)