import datetime
import json
import os
import re

from flask import Flask, request
from telegram import Update, Bot, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, filters, ContextTypes

DATA_FILE = 'income.json'

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            return json.load(f)
    return []

def save_data(data):
    with open(DATA_FILE, 'w') as f:
        json.dump(data, f)

def extract_amounts(text):
    """Extract USD and KHR amounts from payment notification text."""
    khr_match = re.search(r"ចំនួន\s*([\d,]+)\s*រៀល", text)
    usd_match = re.search(r"\$([\d\.]+)", text)

    khr = int(khr_match.group(1).replace(',', '')) if khr_match else 0
    usd = float(usd_match.group(1)) if usd_match else 0
    return usd, khr

async def send_summary_by_date(update: Update, target_date):
    data = load_data()

    total_usd = 0
    total_khr = 0
    count_usd = 0
    count_khr = 0
    for entry in data:
        if entry['date'] == target_date:
            if entry['usd'] > 0:
                total_usd += entry['usd']
                count_usd += 1
            if entry['khr'] > 0:
                total_khr += entry['khr']
                count_khr += 1

    reply = (
        f"សរុបប្រតិបត្តិការ ថ្ងៃទី {target_date}\n"
        f"៛ (KHR): {total_khr:,}   ចំនួនប្រតិបតិ្តការ​សរុប: {count_khr}\n"
        f"$ (USD): {total_usd:.2f}   ចំនួនប្រតិបតិ្តការ​សរុប: {count_usd}"
    )
    await update.message.reply_text(reply)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    # Handle back button
    if text == "⬅ ត្រឡប់ក្រោយ":
        await show_menu(update, context)
        return

    # Extract amounts from payment notification
    usd, khr = extract_amounts(text)
    if usd or khr:
        data = load_data()
        data.append({
            "date": datetime.datetime.now().strftime('%Y-%m-%d'),
            "usd": usd,
            "khr": khr
        })
        save_data(data)
        await update.message.reply_text("✅ Payment recorded!")
        return

    # If no amount found and not a known command, optionally ignore
    # (Do nothing – avoid spamming user)

async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [KeyboardButton("ប្រចាំថ្ងៃ")],
        [KeyboardButton("ប្រចាំសប្ដាហ៍")],
        [KeyboardButton("ប្រចាំខែ")]
    ]
    markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("📊 សូមជ្រើសរើសរបាយការណ៍៖", reply_markup=markup)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_menu(update, context)

# ---------- Flask Web App for Health Check & Webhook ----------
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is missing!")

app = Flask(__name__)
bot = Bot(token=BOT_TOKEN)
dispatcher = Dispatcher(bot, None, workers=0, use_context=True)

# Register handlers
dispatcher.add_handler(CommandHandler("menu", show_menu))
dispatcher.add_handler(CommandHandler("start", start))
dispatcher.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))

@app.route('/webhook', methods=['POST'])
def webhook():
    update = Update.de_json(request.get_json(force=True), bot)
    dispatcher.process_update(update)
    return 'OK'

@app.route('/')
def health_check():
    return "Bot is running"

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)