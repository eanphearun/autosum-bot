import datetime
import json
import os
import re
from flask import Flask, request
from telegram import Update, Bot, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# ---------- Data handling ----------
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
    khr_match = re.search(r"ចំនួន\s*([\d,]+)\s*រៀល", text)
    usd_match = re.search(r"\$([\d\.]+)", text)
    khr = int(khr_match.group(1).replace(',', '')) if khr_match else 0
    usd = float(usd_match.group(1)) if usd_match else 0
    return usd, khr

async def send_summary_by_date(update: Update, target_date):
    data = load_data()
    total_usd = sum(e['usd'] for e in data if e['date'] == target_date)
    total_khr = sum(e['khr'] for e in data if e['date'] == target_date)
    count_usd = sum(1 for e in data if e['date'] == target_date and e['usd'] > 0)
    count_khr = sum(1 for e in data if e['date'] == target_date and e['khr'] > 0)
    reply = (
        f"សរុបប្រតិបត្តិការ ថ្ងៃទី {target_date}\n"
        f"៛ (KHR): {total_khr:,}   ចំនួន: {count_khr}\n"
        f"$ (USD): {total_usd:.2f}   ចំនួន: {count_usd}"
    )
    await update.message.reply_text(reply)

# ---------- Handlers ----------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "⬅ ត្រឡប់ក្រោយ":
        await show_menu(update, context)
        return
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

async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[KeyboardButton("ប្រចាំថ្ងៃ")], [KeyboardButton("ប្រចាំសប្ដាហ៍")], [KeyboardButton("ប្រចាំខែ")]]
    markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("📊 សូមជ្រើសរើសរបាយការណ៍៖", reply_markup=markup)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_menu(update, context)

# ---------- Flask webhook ----------
app = Flask(__name__)

# Create the Application but without an internal webhook server (we'll use Flask)
application = Application.builder().token(os.getenv('BOT_TOKEN')).updater(None).build()
application.add_handler(CommandHandler('start', start))
application.add_handler(CommandHandler('menu', show_menu))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

# Initialize the application
import asyncio
asyncio.run(application.initialize())

@app.route('/webhook', methods=['POST'])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    asyncio.run(application.process_update(update))
    return 'OK'

@app.route('/')
def health():
    return 'Bot is running'

if __name__ == '__main__':
    # Run Flask (and thus the webhook) on Render's assigned port
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)