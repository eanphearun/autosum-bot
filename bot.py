import os
import asyncio
import datetime
import json
import re
import logging

from flask import Flask, request
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Data handling ----------
DATA_FILE = "income.json"

def load_data() -> list:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return []

def save_data(data: list) -> None:
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def extract_amounts(text: str) -> tuple[float, int]:
    """Return (usd, khr) parsed from a Telegram message."""
    khr_match = re.search(r"ចំនួន\s*([\d,]+)\s*រៀល", text)
    usd_match = re.search(r"\$([\d\.]+)", text)
    khr = int(khr_match.group(1).replace(",", "")) if khr_match else 0
    usd = float(usd_match.group(1)) if usd_match else 0.0
    return usd, khr

def summarise(entries: list, label: str) -> str:
    """Build a KHR/USD summary string from a list of entries."""
    total_usd = sum(e["usd"] for e in entries)
    total_khr = sum(e["khr"] for e in entries)
    count_usd = sum(1 for e in entries if e["usd"] > 0)
    count_khr = sum(1 for e in entries if e["khr"] > 0)
    return (
        f"សរុបប្រតិបត្តិការ {label}\n"
        f"៛ (KHR): {total_khr:,}   ចំនួន: {count_khr}\n"
        f"$ (USD): {total_usd:.2f}   ចំនួន: {count_usd}"
    )

# ---------- Keyboard ----------
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("ប្រចាំថ្ងៃ"), KeyboardButton("ប្រចាំសប្ដាហ៍")],
        [KeyboardButton("ប្រចាំខែ")],
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
    await update.message.reply_text(
        "📊 សូមជ្រើសរើសរបាយការណ៍៖",
        reply_markup=MAIN_KEYBOARD,
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.strip()
    today = datetime.date.today()
    data = load_data()

    if text == "ប្រចាំថ្ងៃ":
        date_str = today.strftime("%Y-%m-%d")
        entries = [e for e in data if e["date"] == date_str]
        await update.message.reply_text(
            summarise(entries, f"ថ្ងៃទី {date_str}"),
            reply_markup=MAIN_KEYBOARD,
        )
        return

    if text == "ប្រចាំសប្ដាហ៍":
        week_start = today - datetime.timedelta(days=today.weekday())
        week_dates = {
            (week_start + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(7)
        }
        entries = [e for e in data if e["date"] in week_dates]
        label = f"សប្ដាហ៍ ({week_start.strftime('%Y-%m-%d')} → {today.strftime('%Y-%m-%d')})"
        await update.message.reply_text(
            summarise(entries, label),
            reply_markup=MAIN_KEYBOARD,
        )
        return

    if text == "ប្រចាំខែ":
        month_prefix = today.strftime("%Y-%m")
        entries = [e for e in data if e["date"].startswith(month_prefix)]
        await update.message.reply_text(
            summarise(entries, f"ខែ {today.strftime('%Y-%m')}"),
            reply_markup=MAIN_KEYBOARD,
        )
        return

    usd, khr = extract_amounts(text)
    if usd or khr:
        data.append(
            {
                "date": today.strftime("%Y-%m-%d"),
                "usd": usd,
                "khr": khr,
            }
        )
        save_data(data)
        parts = []
        if usd:
            parts.append(f"${usd:.2f}")
        if khr:
            parts.append(f"៛{khr:,}")
        await update.message.reply_text(
            f"✅ បានកត់ត្រាការទូទាត់: {' / '.join(parts)}",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    await update.message.reply_text(
        "❓ មិនយល់ពាក្យបញ្ជា។ សូមប្រើប៊ូតុងខាងក្រោម ឬផ្ញើចំនួនទឹកប្រាក់។",
        reply_markup=MAIN_KEYBOARD,
    )

# ---------- Lazy Application builder ----------
_app = None

def get_app() -> Application:
    global _app
    if _app is None:
        _app = (
            Application.builder()
            .token(os.environ["BOT_TOKEN"])
            .updater(None)              # we use webhooks, no polling
            .build()
        )
        _app.add_handler(CommandHandler("start", start))
        _app.add_handler(CommandHandler("menu", show_menu))
        _app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        # Initialize the Application (fetches bot info, etc.)
        asyncio.run(_app.initialize())
    return _app

# ---------- Flask webhook ----------
flask_app = Flask(__name__)

@flask_app.post("/webhook")
def webhook():
    app = get_app()
    payload = request.get_json(force=True)
    update = Update.de_json(payload, app.bot)
    asyncio.run(app.process_update(update))
    return "OK"

@flask_app.get("/")
def health():
    return "Bot is running ✅"

@flask_app.get("/set_webhook")
def set_webhook():
    app = get_app()
    base_url = os.environ.get("WEBHOOK_URL", request.host_url.rstrip("/"))
    url = f"{base_url}/webhook"
    result = asyncio.run(app.bot.set_webhook(url))
    return f"Webhook set to {url} — Telegram replied: {result}"

# ---------- Main (for local dev / gunicorn entry) ----------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port)