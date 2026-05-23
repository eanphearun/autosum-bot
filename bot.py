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
    """Extract USD and KHR amounts from ABA PayWay notification."""
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

# ---------- Handlers ----------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    # Back button
    if text == "⬅ ត្រឡប់ក្រោយ":
        await show_menu(update, context)
        return

    # Extract payment amounts
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
    # If no amount found, ignore (do nothing)

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

# ---------- Flask webhook integration ----------
app = Flask(__name__)

# We'll build the Application and store it in a global variable
bot_app = None

@app.route('/webhook', methods=['POST'])
def webhook():
    """Receive updates from Telegram via webhook."""
    if bot_app is None:
        return 'Bot not ready', 500
    update = Update.de_json(request.get_json(force=True), bot_app.bot)
    # Process the update asynchronously in a thread-safe way
    bot_app.update_queue.put_nowait(update)
    return 'OK'

@app.route('/')
def health_check():
    return 'Bot is running'

# ---------- Main entry point ----------
def setup_bot():
    """Build and configure the telegram Application."""
    token = os.getenv('BOT_TOKEN')
    if not token:
        raise ValueError('BOT_TOKEN environment variable is missing!')
    
    application = Application.builder().token(token).build()
    
    # Add handlers
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('menu', show_menu))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    return application

if __name__ == '__main__':
    # For local testing (not used on Render because Gunicorn runs this file as a module)
    bot_app = setup_bot()
    # Start the bot in polling mode (only for local)
    bot_app.run_polling()
else:
    # When imported by Gunicorn, set up the bot and start the Flask webhook server
    bot_app = setup_bot()
    # Initialize the Application (starts internal threads)
    bot_app.initialize()
    # Start the bot's update loop (this runs in background)
    bot_app.start()