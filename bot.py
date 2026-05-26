import os
import asyncio
import datetime
import json
import re
import logging
import time
import threading
import uuid
import csv
import io
import queue
import statistics
from collections import defaultdict
from typing import List, Dict, Any, Optional, Set, Tuple

import random
import pytz
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import SpreadsheetNotFound

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

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Global flags ----------
clear_confirmation_token = None
MANUAL_LOCKED = False
sheet_queue = queue.Queue()
SEEN_TRX_IDS: Set[str] = set() 

# ---------- Configuration ----------
DATA_FILE = "income.json"
SYNC_STATE_FILE = "sync_state.json"
MANAGERS_FILE = "managers.json"
DELETED_FILE = "deleted.json"
REMINDER_FILE = "reminder.txt"

OWNER_ID = int(os.environ.get("OWNER_ID", 0))

# Manager IDs from env (comma-separated) – initial load
MANAGER_IDS: Set[int] = set()
_manager_str = os.environ.get("MANAGER_IDS", "")
if _manager_str:
    for uid_str in _manager_str.split(","):
        try:
            MANAGER_IDS.add(int(uid_str.strip()))
        except ValueError:
            logger.warning(f"Invalid manager ID: {uid_str}")

# Manager group-specific assignments from env – initial load
manager_group_map: Dict[int, Set[int]] = {}
_mgr_group_str = os.environ.get("MANAGER_GROUP_MAP", "")
if _mgr_group_str:
    for block in _mgr_group_str.split(";"):
        parts = block.strip().split(":")
        if len(parts) == 2:
            try:
                mgr_id = int(parts[0].strip())
                group_ids = {int(gid.strip()) for gid in parts[1].split(",") if gid.strip()}
                manager_group_map[mgr_id] = group_ids
            except ValueError:
                logger.warning(f"Invalid entry in MANAGER_GROUP_MAP: {block}")

# PayWay (ABA) API – leave empty to disable
PAYWAY_MERCHANT_ID = os.environ.get("PAYWAY_MERCHANT_ID", "")
PAYWAY_API_KEY = os.environ.get("PAYWAY_API_KEY", "")
PAYWAY_BASE_URL = os.environ.get("PAYWAY_BASE_URL", "https://www.payway.com.kh/api/v1")
PAYWAY_BUSINESS = os.environ.get("PAYWAY_BUSINESS", "birdnest")
SYNC_INTERVAL_MINUTES = int(os.environ.get("SYNC_INTERVAL_MINUTES", "5"))

# Group monitoring
MONITORED_GROUP_IDS = [
    int(gid.strip())
    for gid in os.environ.get("MONITORED_GROUP_IDS", "").split(",")
    if gid.strip()
]
GROUP_BUSINESS_TAG = os.environ.get("GROUP_BUSINESS_TAG", "group_payments")

# Per‑group business tag mapping
group_business_map: Dict[int, str] = {}
_group_map_str = os.environ.get("GROUP_BUSINESS_MAP", "")
if _group_map_str:
    for pair in _group_map_str.split(","):
        parts = pair.strip().split(":")
        if len(parts) == 2:
            try:
                gid = int(parts[0].strip())
                tag = parts[1].strip()
                group_business_map[gid] = tag
            except ValueError:
                logger.warning(f"Invalid group ID in GROUP_BUSINESS_MAP: {parts[0]}")

# Security: allowed senders for group payment recording
ALLOWED_GROUP_SENDERS = os.environ.get("ALLOWED_GROUP_SENDERS", "")
allowed_sender_ids: Set[int] = set()
if ALLOWED_GROUP_SENDERS:
    for uid_str in ALLOWED_GROUP_SENDERS.split(","):
        try:
            allowed_sender_ids.add(int(uid_str.strip()))
        except ValueError:
            logger.warning(f"Invalid user ID in ALLOWED_GROUP_SENDERS: {uid_str}")

# Announcement settings – can now be a comma‑separated list of group IDs
_announce_ids_str = os.environ.get("ANNOUNCE_GROUP_ID", "") or ""
ANNOUNCE_GROUP_IDS: List[int] = []
if _announce_ids_str:
    for gid_str in _announce_ids_str.split(","):
        try:
            gid = int(gid_str.strip())
            if gid != 0:
                ANNOUNCE_GROUP_IDS.append(gid)
        except ValueError:
            logger.warning(f"Invalid ANNOUNCE_GROUP_ID entry: {gid_str}")

ANNOUNCE_TIME = os.environ.get("ANNOUNCE_TIME", "") or "21:00"
REMINDER_TIME = os.environ.get("REMINDER_TIME", "") or None
BUSINESS_SHEET_MAP = os.environ.get("BUSINESS_SHEET_MAP", "")

# Google Sheets
GSPREAD_CREDENTIALS_JSON = os.environ.get("GSPREAD_CREDENTIALS_JSON", "")
SHEET_NAME = os.environ.get("SHEET_NAME", "Bird Nest Income")

# Email webhook secret
EMAIL_WEBHOOK_SECRET = os.environ.get("EMAIL_WEBHOOK_SECRET", "")

# Categories
CATEGORIES = {
    "product": "🛒 ផលិតផល",
    "delivery": "🚚 ដឹកជញ្ជូន",
    "other": "💸 ផ្សេងៗ"
}

# ---------- Google Sheets Helpers ----------
def get_sheet():
    if not GSPREAD_CREDENTIALS_JSON:
        return None
    creds_dict = json.loads(GSPREAD_CREDENTIALS_JSON)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME).sheet1

def append_to_sheet(entry: dict) -> None:
    """Queue the row for background writing – fast and non‑blocking."""
    if not GSPREAD_CREDENTIALS_JSON:
        return
    try:
        now = datetime.datetime.now(pytz.timezone('Asia/Phnom_Penh')).strftime("%Y-%m-%d %H:%M:%S")
        if "tran_id" not in entry:
            entry["tran_id"] = str(uuid.uuid4())
        row = [
            entry.get("date", ""),
            entry.get("usd", 0),
            entry.get("khr", 0),
            CATEGORIES.get(entry.get("category", "other"), "💸 ផ្សេងៗ"),
            entry.get("note", ""),
            entry.get("business", ""),
            now,
            entry["tran_id"]
        ]
        sheet_queue.put((entry, row))
    except Exception as e:
        logger.error(f"Failed to queue sheet row: {e}")

def get_sheet_by_name(sheet_name: str):
    if not GSPREAD_CREDENTIALS_JSON:
        return None
    creds_dict = json.loads(GSPREAD_CREDENTIALS_JSON)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    try:
        return client.open(sheet_name).sheet1
    except SpreadsheetNotFound:
        logger.info(f"Creating new sheet: {sheet_name}")
        sheet = client.create(sheet_name)
        return sheet.sheet1

# Name of the single sheet for all business transactions
ALL_BUSINESSES_SHEET = f"{SHEET_NAME} - All Businesses"

def get_all_businesses_sheet():
    """Return the single sheet used for all business-tagged transactions.
    Does NOT create it automatically – must exist and be shared with the service account."""
    if not GSPREAD_CREDENTIALS_JSON:
        return None
    try:
        creds_dict = json.loads(GSPREAD_CREDENTIALS_JSON)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open(ALL_BUSINESSES_SHEET).sheet1
    except SpreadsheetNotFound:
        logger.error(f"'{ALL_BUSINESSES_SHEET}' not found. Please create it manually and share with the service account.")
        return None
    except Exception as e:
        logger.error(f"Error opening '{ALL_BUSINESSES_SHEET}': {e}")
        return None

def delete_sheet_row_by_tran_id(tran_id: str) -> bool:
    if not tran_id:
        return False
    sheet = get_sheet()
    if not sheet:
        return False
    try:
        rows = sheet.get_all_values()
        for i, row in enumerate(rows):
            if len(row) > 7 and row[7] == tran_id:
                sheet.delete_row(i + 1)
                logger.info(f"Deleted sheet row with tran_id {tran_id}")
                return True
        logger.warning(f"No sheet row found for tran_id {tran_id}")
        return False
    except Exception as e:
        logger.error(f"Error deleting sheet row: {e}")
        return False

def update_sheet_row_by_tran_id(tran_id: str, new_entry: dict) -> bool:
    if not tran_id:
        return False
    sheet = get_sheet()
    if not sheet:
        return False
    try:
        rows = sheet.get_all_values()
        for i, row in enumerate(rows):
            if len(row) > 7 and row[7] == tran_id:
                sheet.delete_row(i + 1)
                append_to_sheet(new_entry)
                return True
        return False
    except Exception as e:
        logger.error(f"Error updating sheet row: {e}")
        return False

def rebuild_from_sheet() -> None:
    sheet = get_sheet()
    if not sheet:
        logger.info("Sheet not configured, skipping rebuild.")
        return
    try:
        rows = sheet.get_all_values()
        if len(rows) < 2:
            logger.info("Sheet empty, nothing to rebuild.")
            return
        new_data = []
        imported_ids = set()
        for row in rows[1:]:
            if not any(row):
                continue
            date_val = row[0].strip()
            usd_str = row[1].replace("$", "").replace(",", "").strip() if row[1] else "0"
            khr_str = row[2].replace(",", "").strip() if row[2] else "0"
            usd_val = float(usd_str) if usd_str else 0.0
            khr_val = float(khr_str) if khr_str else 0.0
            category_val = row[3] if len(row) > 3 else "other"
            note_val = row[4] if len(row) > 4 else ""
            business_val = row[5] if len(row) > 5 else "manual"
            tran_id_val = row[7] if len(row) > 7 else ""
            entry = {
                "date": date_val,
                "usd": usd_val,
                "khr": khr_val,
                "category": category_val.lower(),
                "note": note_val,
                "business": business_val,
                "tran_id": tran_id_val
            }
            if tran_id_val:
                imported_ids.add(tran_id_val)
            new_data.append(entry)
        with open(DATA_FILE, "w") as f:
            json.dump(new_data, f, ensure_ascii=False, indent=2)
        state = load_sync_state()
        state["imported_ids"] = list(imported_ids)
        state["last_sync"] = datetime.datetime.now().isoformat() if imported_ids else None
        with open(SYNC_STATE_FILE, "w") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        logger.info(f"Rebuilt {len(new_data)} transactions from Google Sheets.")
    except Exception as e:
        logger.error(f"Rebuild failed: {e}")

def seed_seen_trx_ids() -> None:
    """Load all previously seen PayWay Trx. IDs from income.json."""
    data = load_data()
    for entry in data:
        note = entry.get("note", "")
        # Match both formats (the one appended by group capture, and the original PayWay format)
        m = re.search(r"Trx:\s*(\d+)", note) or re.search(r"Trx\.\s*ID:\s*(\d+)", note)
        if m:
            SEEN_TRX_IDS.add(m.group(1))

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

def load_managers() -> Dict[str, Any]:
    if os.path.exists(MANAGERS_FILE):
        with open(MANAGERS_FILE, "r") as f:
            return json.load(f)
    data = {"managers": {}, "group_map": {}}
    if MANAGER_IDS:
        for uid in MANAGER_IDS:
            data["managers"][str(uid)] = {"businesses": []}
    if manager_group_map:
        for uid, groups in manager_group_map.items():
            data["group_map"][str(uid)] = list(groups)
    return data

def save_managers(data: Dict[str, Any]) -> None:
    with open(MANAGERS_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_deleted() -> List[Dict[str, Any]]:
    if os.path.exists(DELETED_FILE):
        with open(DELETED_FILE, "r") as f:
            return json.load(f)
    return []

def save_deleted(deleted: List[Dict[str, Any]]) -> None:
    with open(DELETED_FILE, "w") as f:
        json.dump(deleted, f, ensure_ascii=False, indent=2)

def extract_amounts(text: str) -> Tuple[float, float, str]:
    khr_match = re.search(r"ចំនួន\s*([\d,]+)\s*រៀល", text)
    usd_match = re.search(r"\$([\d\.]+)", text)
    khr = int(khr_match.group(1).replace(",", "")) if khr_match else 0
    usd = float(usd_match.group(1)) if usd_match else 0.0
    m = re.search(r"៛\s*([\d,]+)", text)
    if m:
        khr = float(m.group(1).replace(",", ""))
    note = text
    for pat in [r"\$\d+\.?\d*", r"៛\s*[\d,]+", r"ចំនួន\s*[\d,]+\s*រៀល"]:
        note = re.sub(pat, "", note)
    payer = re.search(r"paid by\s+([A-Za-z\s]+?)(?:\s*\(\*?\d+\))?\s", text, re.IGNORECASE)
    if payer:
        note = payer.group(1).strip()
    else:
        note = note.strip().strip(" -,/")[:80]
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
        cat_summary[cat] = cat_summary.get(cat, 0) + e.get("usd", 0) + e.get("khr", 0) / 4000
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

def stats_data(entries: list) -> dict:
    if not entries:
        return {}
    usd_amounts = [e["usd"] for e in entries if e["usd"] > 0]
    khr_amounts = [e["khr"] for e in entries if e["khr"] > 0]
    all_combined = [e["usd"] + e["khr"]/4000 for e in entries]
    return {
        "count": len(entries),
        "total_usd": sum(usd_amounts),
        "total_khr": sum(khr_amounts),
        "avg_combined": statistics.mean(all_combined) if all_combined else 0,
        "max_usd": max(usd_amounts) if usd_amounts else 0,
        "min_usd": min(usd_amounts) if usd_amounts else 0,
        "max_khr": max(khr_amounts) if khr_amounts else 0,
        "min_khr": min(khr_amounts) if khr_amounts else 0,
    }

def business_breakdown(entries: list) -> str:
    """Return a string showing total USD & KHR per business tag."""
    if not entries:
        return "គ្មានទិន្នន័យ។"
    grouped = defaultdict(list)
    for e in entries:
        biz = e.get("business", "unknown")
        grouped[biz].append(e)

    lines = ["📊 សរុបតាមអាជីវកម្ម៖"]
    for biz in sorted(grouped.keys()):
        biz_entries = grouped[biz]
        usd = sum(e["usd"] for e in biz_entries)
        khr = sum(e["khr"] for e in biz_entries)
        count = len(biz_entries)
        lines.append(f"• {biz}: ${usd:.2f} / ៛{khr:,} ({count} ប្រតិបត្តិការ)")
    return "\n".join(lines)

def search_entries(data: list, keyword: str) -> list:
    kw = keyword.lower()
    return [e for e in data if kw in (e.get("note", "") or "").lower()]

# ---------- Keyboard ----------
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("ប្រចាំថ្ងៃ"), KeyboardButton("ប្រចាំសប្ដាហ៍"), KeyboardButton("ប្រចាំខែ")],
        [KeyboardButton("📂 កំណត់ប្រភេទ")],
    ],
    resize_keyboard=True,
)

# ---------- Handlers (private chat) – STRICTLY OWNER ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    await update.message.reply_text(
        "👋 សូមស្វាគមន៍! ជ្រើសរើសរបាយការណ៍ ឬផ្ញើការទូទាត់។\n"
        "សូមវាយ /help សម្រាប់បញ្ជីពាក្យបញ្ជា។",
        reply_markup=MAIN_KEYBOARD,
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    help_text = (
        "📋 *ពាក្យបញ្ជាមាន៖*\n\n"
        "/start – ចាប់ផ្តើម\n"
        "/help – បង្ហាញសារនេះ\n"
        "/recent [n] – បង្ហាញប្រតិបត្តិការចុងក្រោយ\n"
        "/delete_index <n> – លុបធាតុលេខ n\n"
        "/edit_index <n> <amount> [note] – កែប្រែធាតុ\n"
        "/undo_delete – ស្តារការលុបចុងក្រោយ\n"
        "/compare – ប្រៀបធៀបថ្ងៃនេះនឹងម្សិលមិញ\n"
        "/chart – គំនូសតាង 7ថ្ងៃ\n"
        "/summary – សង្ខេបខែនេះ\n"
        "/bysender [day|week|month] – សង្ខេបតាមអ្នកផ្ញើ\n"
        "/duplicates – រកធាតុស្ទួន\n"
        "/announce – ផ្ញើសេចក្តីសង្ខេបប្រចាំថ្ងៃទៅក្រុម\n"
        "/permissions – បញ្ជីអ្នកគ្រប់គ្រង\n"
        "/add_manager <user_id> – បន្ថែមអ្នកគ្រប់គ្រង\n"
        "/remove_manager <user_id> – ដកអ្នកគ្រប់គ្រង\n"
        "/settings – ការកំណត់បច្ចុប្បន្ន\n"
        "/ping – ពិនិត្យស្ថានភាព\n"
        "/sync – ធ្វើសមកាលកម្ម PayWay\n"
        "/sync_status – ស្ថានភាពសមកាលកម្ម\n"
        "/day YYYY-MM-DD – របាយការណ៍ថ្ងៃ\n"
        "/delete – លុបធាតុចុងក្រោយ\n"
        "/clear_all – លុបទិន្នន័យទាំងអស់ (ត្រូវបញ្ជាក់ជាមួយ /confirm_clear)\n"
        "/lock – ចាក់សោការបញ្ចូលដោយដៃ\n"
        "/unlock – ដោះសោ\n"
        "/remind HH:MM – កំណត់ពេលដាស់តឿនផ្ទាល់ខ្លួន\n"
        "/export [start] [end] – ទាញយកជា CSV\n"
        "/stats – ស្ថិតិថ្ងៃនេះ\n"
        "/search <keyword> – ស្វែងរកតាមចំណាំ\n"
        "/top [n] – ប្រតិបត្តិការធំជាងគេ\n"
        "/range YYYY-MM-DD YYYY-MM-DD – របាយការណ៍ជួរកាលបរិច្ឆេទ\n"
        "ប៊ូតុង៖ ប្រចាំថ្ងៃ, ប្រចាំសប្ដាហ៍, ប្រចាំខែ, កំណត់ប្រភេទ"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")

async def category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    data = load_data()
    if not data:
        await query.edit_message_text("មិនមានធាតុថ្មីៗទេ។")
        return
    cat_key = query.data.split("_")[1]
    data[-1]["category"] = cat_key
    save_data(data)
    await query.edit_message_text(f"✅ បានកំណត់ប្រភេទ: {CATEGORIES[cat_key]}")

async def clear_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    global clear_confirmation_token
    token = str(random.randint(100000, 999999))
    clear_confirmation_token = token
    await update.message.reply_text(
        f"⚠️ តើអ្នកពិតជាចង់លុបទិន្នន័យទាំងអស់មែនទេ?\n\n"
        f"សូមវាយ `/confirm_clear {token}` ដើម្បីបញ្ជាក់។\n"
        f"ប្រសិនបើអ្នកមិនចង់លុបទេ សូមមិនអើពើសារនេះ។"
    )

async def confirm_clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    global clear_confirmation_token
    try:
        token = context.args[0]
    except:
        await update.message.reply_text("សូមបញ្ចូលកូដបញ្ជាក់ពី `/confirm_clear <code>`")
        return
    if clear_confirmation_token is None or token != clear_confirmation_token:
        await update.message.reply_text("កូដមិនត្រឹមត្រូវ ឬផុតកំណត់។ សូមព្យាយាមម្តងទៀតដោយប្រើ `/clear_all`")
        return
    sheet = get_sheet()
    if sheet:
        try:
            rows = sheet.get_all_values()
            if len(rows) > 1:
                sheet.delete_rows(2, len(rows))
        except Exception as e:
            logger.error(f"Failed to clear sheet: {e}")
            await update.message.reply_text(f"❌ មានបញ្ហាក្នុងការលុប Sheet៖ {e}")
            return
    save_data([])
    save_sync_state({"last_sync": None, "imported_ids": []})
    save_deleted([])
    clear_confirmation_token = None
    await update.message.reply_text("✅ ទិន្នន័យទាំងអស់ត្រូវបានលុបចោលទាំងស្រុង (Google Sheets + local)។")

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

async def range_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        start_str = context.args[0]
        end_str = context.args[1]
        start_date = datetime.date.fromisoformat(start_str)
        end_date = datetime.date.fromisoformat(end_str)
    except (IndexError, ValueError):
        await update.message.reply_text("សូមប្រើទម្រង់ `/range 2026-05-01 2026-05-10`")
        return
    if start_date > end_date:
        await update.message.reply_text("ថ្ងៃចាប់ផ្តើមត្រូវមុនថ្ងៃបញ្ចប់។")
        return
    delta = end_date - start_date
    dates = [(start_date + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)]
    data = load_data()
    entries = [e for e in data if e["date"] in dates]
    label = f"ចាប់ពី {start_str} ដល់ {end_str}"
    await update.message.reply_text(summarise(entries, label))

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    await update.message.reply_text("🏓 Pong!")

async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    managers = load_managers()
    text = (
        f"OWNER_ID: {OWNER_ID}\n"
        f"MONITORED_GROUP_IDS: {MONITORED_GROUP_IDS}\n"
        f"GROUP_BUSINESS_TAG: {GROUP_BUSINESS_TAG}\n"
        f"GROUP_BUSINESS_MAP: {_group_map_str}\n"
        f"ALLOWED_GROUP_SENDERS: {ALLOWED_GROUP_SENDERS or 'owner + managers'}\n"
        f"MANAGER_IDS: {list(managers.get('managers', {}).keys())}\n"
        f"MANAGER_GROUP_MAP: {managers.get('group_map', {})}\n"
        f"ANNOUNCE_GROUP_IDS: {ANNOUNCE_GROUP_IDS}\n"
        f"ANNOUNCE_TIME: {ANNOUNCE_TIME}\n"
        f"PAYWAY_BUSINESS: {PAYWAY_BUSINESS}\n"
        f"SHEET_NAME: {SHEET_NAME}"
    )
    await update.message.reply_text(text)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    data = load_data()
    if not data:
        await update.message.reply_text("គ្មានទិន្នន័យ។")
        return
    today = datetime.date.today()
    entries_today = [e for e in data if e["date"] == today.strftime("%Y-%m-%d")]
    s = stats_data(entries_today)
    if not s:
        await update.message.reply_text("ថ្ងៃនេះមិនទាន់មានប្រតិបត្តិការទេ។")
        return
    msg = (
        f"📊 ស្ថិតិថ្ងៃនេះ៖\n"
        f"ចំនួនប្រតិបត្តិការ: {s['count']}\n"
        f"សរុប USD: ${s['total_usd']:.2f}\n"
        f"សរុប KHR: ៛{s['total_khr']:,}\n"
        f"មធ្យម (USD equiv): ${s['avg_combined']:.2f}\n"
        f"USD អតិបរមា: ${s['max_usd']:.2f}\n"
        f"USD អប្បបរមា: ${s['min_usd']:.2f}\n"
        f"KHR អតិបរមា: ៛{s['max_khr']:,}\n"
        f"KHR អប្បបរមា: ៛{s['min_khr']:,}"
    )
    await update.message.reply_text(msg)

async def search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    if not context.args:
        await update.message.reply_text("សូមប្រើ `/search <ពាក្យ>`")
        return
    keyword = " ".join(context.args)
    data = load_data()
    results = search_entries(data, keyword)
    if not results:
        await update.message.reply_text(f"រកមិនឃើញ '{keyword}' ទេ។")
        return
    lines = [f"🔍 លទ្ធផលសម្រាប់ '{keyword}'៖"]
    for e in results[-10:]:
        lines.append(f"{e['date']} | ${e['usd']:.2f} / ៛{e['khr']:,} | {e.get('note','')[:30]}")
    await update.message.reply_text("\n".join(lines))

async def top(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        n = int(context.args[0]) if context.args else 5
    except:
        n = 5
    data = load_data()
    if not data:
        await update.message.reply_text("គ្មានទិន្នន័យ។")
        return
    sorted_data = sorted(data, key=lambda e: e["usd"] + e["khr"]/4000, reverse=True)
    top_entries = sorted_data[:n]
    lines = [f"📈 ប្រតិបត្តិការធំជាងគេ {n}៖"]
    for i, e in enumerate(top_entries, 1):
        lines.append(f"{i}. {e['date']} | ${e['usd']:.2f} / ៛{e['khr']:,} | {e.get('note','')[:30]}")
    await update.message.reply_text("\n".join(lines))

async def bybusiness_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    period = "daily"
    if context.args:
        period = context.args[0].lower()
    data = load_data()
    entries = get_entries_for_period(data, period)
    if not entries:
        await update.message.reply_text(f"គ្មានប្រតិបត្តិការ {period}។")
        return
    await update.message.reply_text(business_breakdown(entries))

async def lock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    global MANUAL_LOCKED
    MANUAL_LOCKED = True
    await update.message.reply_text("🔒 ការកត់ត្រាដោយដៃត្រូវបានចាក់សោ។")

async def unlock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    global MANUAL_LOCKED
    MANUAL_LOCKED = False
    await update.message.reply_text("🔓 ការកត់ត្រាដោយដៃត្រូវបានដោះសោ។")

# ---------- Manager management commands ----------
async def add_manager(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        new_id = int(context.args[0])
    except:
        await update.message.reply_text("សូមប្រើ `/add_manager <user_id>`")
        return
    managers = load_managers()
    managers.setdefault("managers", {})
    managers["managers"][str(new_id)] = managers["managers"].get(str(new_id), {"businesses": []})
    save_managers(managers)
    await update.message.reply_text(f"✅ បានបន្ថែមអ្នកគ្រប់គ្រង {new_id}")

async def remove_manager(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        del_id = str(int(context.args[0]))
    except:
        await update.message.reply_text("សូមប្រើ `/remove_manager <user_id>`")
        return
    managers = load_managers()
    if del_id in managers.get("managers", {}):
        del managers["managers"][del_id]
        managers.get("group_map", {}).pop(del_id, None)
        save_managers(managers)
        await update.message.reply_text(f"✅ បានដកអ្នកគ្រប់គ្រង {del_id}")
    else:
        await update.message.reply_text("រកមិនឃើញអ្នកគ្រប់គ្រងនេះទេ។")

async def permissions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    managers = load_managers()
    data = managers.get("managers", {})
    group_map = managers.get("group_map", {})
    if not data:
        await update.message.reply_text("មិនមានអ្នកគ្រប់គ្រងទេ។")
        return
    lines = ["អ្នកគ្រប់គ្រង៖"]
    for uid, info in data.items():
        groups = group_map.get(uid, "ទាំងអស់")
        lines.append(f"  {uid}: ក្រុម {groups}")
    await update.message.reply_text("\n".join(lines))

# ---------- Enhanced private message handler ----------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    text = update.message.text.strip()
    today = datetime.date.today()
    data = load_data()

    # Report buttons (bypass lock)
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

    # Manual entry with lock check
    usd, khr, note = extract_amounts(text)
    if usd or khr:
                # Use original message text for Trx. ID, not the trimmed note
        trx_id_match = re.search(r"Trx\.\s*ID:\s*(\d+)", text)
        if trx_id_match:
            trx_id = trx_id_match.group(1)
            if trx_id in SEEN_TRX_IDS:
                await update.message.reply_text("⏭ ប្រតិបត្តិការនេះបានកត់ត្រារួចហើយ (Trx. ID ដូចគ្នា)។")
                return
            SEEN_TRX_IDS.add(trx_id)
        
        if MANUAL_LOCKED:
            await update.message.reply_text("⛔ ការកត់ត្រាដោយដៃត្រូវបានចាក់សោ។ សូមទាក់ទងម្ចាស់។")
            return
        entry = {
            "date": today.strftime("%Y-%m-%d"),
            "usd": usd,
            "khr": khr,
            "note": note,
            "category": "other",
            "business": "manual",
            "tran_id": str(uuid.uuid4())
        }
        data.append(entry)
        save_data(data)
        append_to_sheet(entry)
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
        "❓ មិនយល់។ សូមប្រើប៊ូតុង ឬផ្ញើចំនួនទឹកប្រាក់។\n"
        "វាយ /help សម្រាប់ជំនួយ។",
        reply_markup=MAIN_KEYBOARD,
    )

async def set_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        new_time = context.args[0]
        if not re.match(r"^\d{2}:\d{2}$", new_time):
            raise ValueError
    except:
        await update.message.reply_text("សូមប្រើ `/remind 21:00`")
        return
    with open(REMINDER_FILE, "w") as f:
        f.write(new_time)
    await update.message.reply_text(f"✅ ពេលដាស់តឿនត្រូវបានកំណត់ម៉ោង {new_time}")

async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    data = load_data()
    if not data:
        await update.message.reply_text("គ្មានទិន្នន័យ។")
        return
    if len(context.args) == 2:
        try:
            start = datetime.date.fromisoformat(context.args[0])
            end = datetime.date.fromisoformat(context.args[1])
            data = [e for e in data if start <= datetime.date.fromisoformat(e["date"]) <= end]
        except:
            pass
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "USD", "KHR", "Category", "Note", "Business", "Timestamp", "Tran ID"])
    for e in data:
        writer.writerow([
            e["date"], e["usd"], e["khr"], e.get("category","other"),
            e.get("note",""), e.get("business",""), "", e.get("tran_id","")
        ])
    output.seek(0)
    buf = io.BytesIO(output.getvalue().encode("utf-8"))
    buf.name = f"export_{datetime.date.today().isoformat()}.csv"
    await update.message.reply_document(document=buf)

# ---------- PayWay Sync (optional) ----------
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
        append_to_sheet(entry)
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
def extract_group_payment(text: str) -> Optional[Tuple[float, float, str]]:
    usd = 0.0
    khr = 0.0
    note = ""
    m = re.search(r"\$(\d+\.?\d*)", text)
    if m:
        usd = float(m.group(1))
    m = re.search(r"SALE\s+(\d+\.?\d*)\s+USD", text, re.IGNORECASE)
    if m:
        usd = float(m.group(1))
    m = re.search(r"៛\s*([\d,]+)", text)
    if m:
        khr = float(m.group(1).replace(",", ""))
    else:
        m = re.search(r"ចំនួន\s*([\d,]+)\s*រៀល", text)
        if m:
            khr = float(m.group(1).replace(",", ""))
    if usd == 0 and khr == 0:
        return None
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

def is_allowed_sender(user_id: int, chat_id: int) -> bool:
    if allowed_sender_ids:
        return user_id in allowed_sender_ids
    if user_id == OWNER_ID:
        return True
    managers = load_managers()
    mgrs = managers.get("managers", {})
    group_map = managers.get("group_map", {})
    if str(user_id) in mgrs:
        groups = group_map.get(str(user_id))
        if groups is None:
            return True
        return chat_id in set(groups)
    return False

async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    msg = update.message
    if msg.chat.id not in MONITORED_GROUP_IDS:
        return
    if not is_allowed_sender(msg.from_user.id, msg.chat.id):
        return
    result = extract_group_payment(msg.text)
    if not result:
        return
    usd, khr, note = result
    # Prevent duplicates based on PayWay transaction ID (if present)
    trx_id_match = re.search(r"Trx:\s*(\d+)", note)
    if trx_id_match:
        trx_id = trx_id_match.group(1)
        if trx_id in SEEN_TRX_IDS:
            logger.info(f"Duplicate Trx. ID {trx_id} ignored.")
            return
        SEEN_TRX_IDS.add(trx_id)
    today = datetime.date.today()
    business_tag = group_business_map.get(msg.chat.id, GROUP_BUSINESS_TAG)
    data = load_data()
    entry = {
        "date": today.strftime("%Y-%m-%d"),
        "usd": usd,
        "khr": khr,
        "note": note,
        "category": "other",
        "business": business_tag,
        "source": f"group_{msg.chat.id}_msg{msg.message_id}",
        "tran_id": str(uuid.uuid4())
    }
    data.append(entry)
    save_data(data)
    append_to_sheet(entry)
    logger.info(f"Group payment recorded: ${usd:.2f} / {khr}៛ from group {msg.chat.id} (business: {business_tag})")


# ---------- Group period reports ----------
def get_entries_for_period(data: List[Dict], period: str, today: Optional[datetime.date] = None) -> List[Dict]:
    if today is None:
        today = datetime.date.today()
    if period == "daily":
        return [e for e in data if e["date"] == today.strftime("%Y-%m-%d")]
    elif period == "weekly":
        week_start = today - datetime.timedelta(days=today.weekday())
        week_dates = {(week_start + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)}
        return [e for e in data if e["date"] in week_dates]
    elif period == "monthly":
        month_prefix = today.strftime("%Y-%m")
        return [e for e in data if e["date"].startswith(month_prefix)]
    elif period == "quarterly":
        quarter = (today.month - 1) // 3 + 1
        start_month = (quarter - 1) * 3 + 1
        start_date = datetime.date(today.year, start_month, 1)
        end_date = today
        dates = set()
        d = start_date
        while d <= end_date:
            dates.add(d.strftime("%Y-%m-%d"))
            d += datetime.timedelta(days=1)
        return [e for e in data if e["date"] in dates]
    elif period == "yearly":
        year_prefix = today.strftime("%Y")
        return [e for e in data if e["date"].startswith(year_prefix)]
    else:
        return []

def group_period_summary(chat_id: int, period: str, label: str, today: Optional[datetime.date] = None) -> str:
    data = load_data()
    all_entries = get_entries_for_period(data, period, today)
    group_tag = group_business_map.get(chat_id, GROUP_BUSINESS_TAG)
    group_entries = []
    for e in all_entries:
        src = e.get("source", "")
        if src.startswith(f"group_{chat_id}_"):
            group_entries.append(e)
        elif not src and e.get("business") == group_tag:
            group_entries.append(e)
    if not group_entries:
        return f"គ្មានប្រតិបត្តិការ {label} សម្រាប់ក្រុមនេះទេ។"
    total_usd = sum(e["usd"] for e in group_entries)
    total_khr = sum(e["khr"] for e in group_entries)
    count_usd = sum(1 for e in group_entries if e["usd"] > 0)
    count_khr = sum(1 for e in group_entries if e["khr"] > 0)
    return (
        f"📊 សរុប {label} សម្រាប់ក្រុមនេះ៖\n"
        f"៛ (KHR): {total_khr:,}   ចំនួន: {count_khr}\n"
        f"$ (USD): {total_usd:.2f}   ចំនួន: {count_usd}"
    )

def _can_use_group_commands(user_id: int, chat_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    managers = load_managers()
    if str(user_id) in managers.get("managers", {}):
        groups = managers.get("group_map", {}).get(str(user_id))
        if groups is None:
            return True
        return chat_id in set(groups)
    return False

# ---------- Existing group commands ----------
async def group_daily_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    today = datetime.date.today()
    label = f"ថ្ងៃទី {today.strftime('%Y-%m-%d')}"
    await update.message.reply_text(group_period_summary(update.message.chat.id, "daily", label, today))

async def group_weekly_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    today = datetime.date.today()
    week_start = today - datetime.timedelta(days=today.weekday())
    label = f"សប្ដាហ៍ ({week_start.strftime('%Y-%m-%d')} → {today.strftime('%Y-%m-%d')})"
    await update.message.reply_text(group_period_summary(update.message.chat.id, "weekly", label, today))

async def group_monthly_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    today = datetime.date.today()
    label = f"ខែ {today.strftime('%Y-%m')}"
    await update.message.reply_text(group_period_summary(update.message.chat.id, "monthly", label, today))

async def group_quarterly_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    today = datetime.date.today()
    quarter = (today.month - 1) // 3 + 1
    label = f"ត្រីមាសទី {quarter} ឆ្នាំ {today.year}"
    await update.message.reply_text(group_period_summary(update.message.chat.id, "quarterly", label, today))

async def group_yearly_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    today = datetime.date.today()
    label = f"ឆ្នាំ {today.year}"
    await update.message.reply_text(group_period_summary(update.message.chat.id, "yearly", label, today))

# ---------- New group commands (range, top, duplicates, edit_index, summary, bysender, compare) ----------
async def group_range_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    try:
        start_str = context.args[0]
        end_str = context.args[1]
        start_date = datetime.date.fromisoformat(start_str)
        end_date = datetime.date.fromisoformat(end_str)
    except (IndexError, ValueError):
        await update.message.reply_text("សូមប្រើទម្រង់ `/range 2026-05-01 2026-05-10`")
        return
    if start_date > end_date:
        await update.message.reply_text("ថ្ងៃចាប់ផ្តើមត្រូវមុនថ្ងៃបញ្ចប់។")
        return
    delta = end_date - start_date
    dates = [(start_date + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)]
    data = load_data()
    group_id = update.message.chat.id
    group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)
    entries = []
    for e in data:
        if e["date"] not in dates:
            continue
        src = e.get("source", "")
        if src.startswith(f"group_{group_id}_"):
            entries.append(e)
        elif not src and e.get("business") == group_tag:
            entries.append(e)
    label = f"ចាប់ពី {start_str} ដល់ {end_str}"
    if not entries:
        await update.message.reply_text(f"គ្មានប្រតិបត្តិការ {label} សម្រាប់ក្រុមនេះទេ។")
        return
    total_usd = sum(e["usd"] for e in entries)
    total_khr = sum(e["khr"] for e in entries)
    count_usd = sum(1 for e in entries if e["usd"] > 0)
    count_khr = sum(1 for e in entries if e["khr"] > 0)
    text = (
        f"📊 សរុប {label} សម្រាប់ក្រុមនេះ៖\n"
        f"៛ (KHR): {total_khr:,}   ចំនួន: {count_khr}\n"
        f"$ (USD): {total_usd:.2f}   ចំនួន: {count_usd}"
    )
    await update.message.reply_text(text)

async def group_top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    try:
        n = int(context.args[0]) if context.args else 5
    except:
        n = 5
    data = load_data()
    group_id = update.message.chat.id
    group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)
    entries = [e for e in data if e.get("source","").startswith(f"group_{group_id}_") or (not e.get("source") and e.get("business")==group_tag)]
    if not entries:
        await update.message.reply_text("គ្មានទិន្នន័យសម្រាប់ក្រុមនេះទេ។")
        return
    sorted_entries = sorted(entries, key=lambda e: e["usd"] + e["khr"]/4000, reverse=True)
    top_entries = sorted_entries[:n]
    lines = [f"📈 ប្រតិបត្តិការធំជាងគេ {n} សម្រាប់ក្រុមនេះ៖"]
    for i, e in enumerate(top_entries, 1):
        lines.append(f"{i}. {e['date']} | ${e['usd']:.2f} / ៛{e['khr']:,} | {e.get('note','')[:30]}")
    await update.message.reply_text("\n".join(lines))

async def group_duplicates_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return

    data = load_data()
    total = len(data)
    group_id = update.message.chat.id
    group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)

    # Keep only entries belonging to this group
    group_entries = [
        (i, e) for i, e in enumerate(data)
        if e.get("source", "").startswith(f"group_{group_id}_")
        or (not e.get("source") and e.get("business") == group_tag)
    ]

    if not group_entries:
        await update.message.reply_text("មិនមានទិន្នន័យសម្រាប់ក្រុមនេះទេ។")
        return

    dups = []
    # Compare all pairs within group entries
    for a in range(len(group_entries)):
        idx_a, e1 = group_entries[a]
        for b in range(a + 1, len(group_entries)):
            idx_b, e2 = group_entries[b]
            if e1["date"] == e2["date"] and e1["usd"] == e2["usd"] and e1["khr"] == e2["khr"]:
                n1 = (e1.get("note", "") or "").strip()
                n2 = (e2.get("note", "") or "").strip()
                if n1.split()[0].lower() == n2.split()[0].lower() if n1 and n2 else (not n1 and not n2):
                    # Convert absolute indices to /delete_index compatible (1 = most recent)
                    idx1 = total - idx_a
                    idx2 = total - idx_b
                    dups.append((idx1, idx2, e1, e2))

    if not dups:
        await update.message.reply_text("មិនមានធាតុស្ទួនក្នុងក្រុមនេះទេ។")
        return

    lines = ["🔍 ធាតុស្ទួនក្នុងក្រុមនេះ៖"]
    for idx1, idx2, e1, e2 in dups[:5]:
        lines.append(
            f"#{idx1} & #{idx2}: {e1['date']} ${e1['usd']} / {e1['khr']}៛ "
            f"({e1.get('note','')[:30]} / {e2.get('note','')[:30]})"
        )
    lines.append("សរុប /delete_index <លេខ> ដើម្បីលុប (យកលេខតូច)។")
    await update.message.reply_text("\n".join(lines))

async def group_edit_index_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    try:
        idx = int(context.args[0])
    except:
        await update.message.reply_text("សូមប្រើ `/edit_index <n> <amount> [note]`")
        return
    data = load_data()
    if idx < 1 or idx > len(data):
        await update.message.reply_text("លេខមិនត្រឹមត្រូវ។")
        return
    entry = data[len(data) - idx]
    group_id = update.message.chat.id
    group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)
    src = entry.get("source","")
    if not (src.startswith(f"group_{group_id}_") or (not src and entry.get("business")==group_tag)):
        await update.message.reply_text("ធាតុនេះមិនមែនជារបស់ក្រុមនេះទេ។")
        return
    if len(context.args) < 2:
        await update.message.reply_text("សូមបញ្ចូលចំនួនថ្មី។")
        return
    new_amount_str = context.args[1]
    new_usd, new_khr, _ = extract_amounts(new_amount_str)
    if new_usd == 0 and new_khr == 0:
        await update.message.reply_text("ចំនួនមិនត្រឹមត្រូវ។")
        return
    old_usd = entry["usd"]
    old_khr = entry["khr"]
    entry["usd"] = new_usd
    entry["khr"] = new_khr
    if len(context.args) >= 3:
        entry["note"] = " ".join(context.args[2:])
    save_data(data)
    update_sheet_row_by_tran_id(entry.get("tran_id"), entry)
    await update.message.reply_text(
        f"✅ បានកែប្រែធាតុលេខ {idx}: "
        f"ពី ${old_usd:.2f} / ៛{old_khr:,} → ${new_usd:.2f} / ៛{new_khr:,} | {entry['note']}"
    )

async def group_summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    today = datetime.date.today()
    month_prefix = today.strftime("%Y-%m")
    data = load_data()
    group_id = update.message.chat.id
    group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)
    entries = [e for e in data if e["date"].startswith(month_prefix) and (
        e.get("source","").startswith(f"group_{group_id}_") or (not e.get("source") and e.get("business")==group_tag)
    )]
    if not entries:
        await update.message.reply_text("គ្មានទិន្នន័យសម្រាប់ក្រុមនេះក្នុងខែនេះទេ។")
        return
    total_usd = sum(e["usd"] for e in entries)
    total_khr = sum(e["khr"] for e in entries)
    num = len(entries)
    days = today.day
    avg = total_usd / days if days else 0
    await update.message.reply_text(
        f"📊 សង្ខេបខែ {month_prefix} សម្រាប់ក្រុមនេះ៖\n"
        f"ប្រតិបត្តិការ: {num}\n"
        f"សរុប: ${total_usd:.2f} / ៛{total_khr:,}\n"
        f"មធ្យមក្នុងមួយថ្ងៃ: ${avg:.2f}"
    )

async def group_bysender_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    period = "daily"
    if context.args:
        period = context.args[0].lower()
    data = load_data()
    group_id = update.message.chat.id
    group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)
    entries = get_entries_for_period(data, period)
    group_entries = [e for e in entries if e.get("source","").startswith(f"group_{group_id}_") or (not e.get("source") and e.get("business")==group_tag)]
    sender_totals = defaultdict(float)
    for e in group_entries:
        payer = (e.get("note","") or "unknown").strip()
        if not payer:
            payer = "unknown"
        sender_totals[payer] += e["usd"] + e["khr"]/4000
    if not sender_totals:
        await update.message.reply_text("គ្មានទិន្នន័យ។")
        return
    sorted_senders = sorted(sender_totals.items(), key=lambda x: x[1], reverse=True)
    lines = [f"📊 អ្នកផ្ញើ ({period}) សម្រាប់ក្រុមនេះ៖"]
    for sender, amt in sorted_senders[:10]:
        lines.append(f"  {sender}: ${amt:.2f}")
    await update.message.reply_text("\n".join(lines))

async def group_compare_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    data = load_data()
    group_id = update.message.chat.id
    group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)

    def filter_group(entries):
        return [
            e for e in entries
            if e.get("source", "").startswith(f"group_{group_id}_")
            or (not e.get("source") and e.get("business") == group_tag)
        ]

    today_entries = filter_group([e for e in data if e["date"] == today.strftime("%Y-%m-%d")])
    yesterday_entries = filter_group([e for e in data if e["date"] == yesterday.strftime("%Y-%m-%d")])

    def combined_total(entries):
        return sum(e["usd"] + e["khr"] / 4000 for e in entries)

    total_today = combined_total(today_entries)
    total_yesterday = combined_total(yesterday_entries)

    if total_yesterday == 0:
        change = "N/A"
    else:
        change = f"{(total_today - total_yesterday) / total_yesterday * 100:+.1f}%"

    today_usd = sum(e["usd"] for e in today_entries)
    today_khr = sum(e["khr"] for e in today_entries)
    yesterday_usd = sum(e["usd"] for e in yesterday_entries)
    yesterday_khr = sum(e["khr"] for e in yesterday_entries)

    msg = (
        f"📊 ប្រៀបធៀបថ្ងៃនេះ vs ម្សិលមិញ សម្រាប់ក្រុមនេះ៖\n"
        f"ថ្ងៃនេះ: ${today_usd:.2f} / ៛{today_khr:,}  (≈ ${total_today:.2f})\n"
        f"ម្សិលមិញ: ${yesterday_usd:.2f} / ៛{yesterday_khr:,}  (≈ ${total_yesterday:.2f})\n"
        f"ផ្លាស់ប្តូរ: {change}"
    )
    await update.message.reply_text(msg)

async def group_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return

    await update.message.reply_text(
        "📊 ជ្រើសរើសរបាយការណ៍៖",
        reply_markup=group_menu_keyboard()
    )

async def group_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    chat_id = query.message.chat.id

    if not _can_use_group_commands(user_id, chat_id):
        await query.edit_message_text("អ្នកមិនមានសិទ្ធិប្រើម៉ឺនុយនេះទេ។")
        return

    data = query.data

    if data == "grpmenu_close":
        try:
            await query.message.delete()
        except:
            pass
        return

    today = datetime.date.today()
    label = ""
    if data == "grpmenu_daily":
        label = f"ថ្ងៃទី {today.strftime('%Y-%m-%d')}"
        summary_text = group_period_summary(chat_id, "daily", label, today)
    elif data == "grpmenu_weekly":
        week_start = today - datetime.timedelta(days=today.weekday())
        label = f"សប្ដាហ៍ ({week_start.strftime('%Y-%m-%d')} → {today.strftime('%Y-%m-%d')})"
        summary_text = group_period_summary(chat_id, "weekly", label, today)
    elif data == "grpmenu_monthly":
        label = f"ខែ {today.strftime('%Y-%m')}"
        summary_text = group_period_summary(chat_id, "monthly", label, today)
    elif data == "grpmenu_quarterly":
        quarter = (today.month - 1) // 3 + 1
        label = f"ត្រីមាសទី {quarter} ឆ្នាំ {today.year}"
        summary_text = group_period_summary(chat_id, "quarterly", label, today)
    elif data == "grpmenu_yearly":
        label = f"ឆ្នាំ {today.year}"
        summary_text = group_period_summary(chat_id, "yearly", label, today)
    elif data == "grpmenu_range":
        await query.edit_message_text(
            "📅 សូមប្រើ `/range YYYY-MM-DD YYYY-MM-DD`\n"
            "ឧទាហរណ៍: `/range 2026-05-01 2026-05-25`",
            reply_markup=group_menu_keyboard()
        )
        return

    elif data == "grpmenu_bybusiness":
        # Show business breakdown for today (group only)
        today = datetime.date.today()
        data = load_data()
        group_id = chat_id
        group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)
        # Filter entries belonging to this group
        entries = [
            e for e in data
            if e["date"] == today.strftime("%Y-%m-%d") and (
                e.get("source", "").startswith(f"group_{group_id}_") or
                (not e.get("source") and e.get("business") == group_tag)
            )
        ]
        text = business_breakdown(entries) if entries else "ថ្ងៃនេះមិនមានប្រតិបត្តិការសម្រាប់ក្រុមនេះទេ។"
        await context.bot.send_message(chat_id=chat_id, text=text)
        # Keep the menu visible
        await query.edit_message_text("📊 ជ្រើសរើសរបាយការណ៍៖", reply_markup=group_menu_keyboard())
        return

    else:
        return

    # Show the report as a new message
    await context.bot.send_message(chat_id=chat_id, text=summary_text)
    # Keep the menu visible
    if query.message.text != "📊 ជ្រើសរើសរបាយការណ៍៖" or query.message.reply_markup != group_menu_keyboard():
        await query.edit_message_text("📊 ជ្រើសរើសរបាយការណ៍៖", reply_markup=group_menu_keyboard())

def group_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("ថ្ងៃនេះ", callback_data="grpmenu_daily"),
            InlineKeyboardButton("សប្ដាហ៍នេះ", callback_data="grpmenu_weekly"),
            InlineKeyboardButton("ខែនេះ", callback_data="grpmenu_monthly")
        ],
        [
            InlineKeyboardButton("ត្រីមាសនេះ", callback_data="grpmenu_quarterly"),
            InlineKeyboardButton("ឆ្នាំនេះ", callback_data="grpmenu_yearly"),
            InlineKeyboardButton("ជួរកាលបរិច្ឆេទ", callback_data="grpmenu_range")
        ],
        [
            InlineKeyboardButton("អាជីវកម្ម", callback_data="grpmenu_bybusiness")
        ],
        [
            InlineKeyboardButton("❌ បិទ", callback_data="grpmenu_close")
        ]
    ])

# ---------- Previous owner-only commands (compare, chart, summary, bysender, undo_delete, duplicates, announce, edit_index, recent, delete_index) ----------
async def compare(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    data = load_data()
    today_entries = [e for e in data if e["date"] == today.strftime("%Y-%m-%d")]
    yesterday_entries = [e for e in data if e["date"] == yesterday.strftime("%Y-%m-%d")]

    def combined_total(entries):
        return sum(e["usd"] + e["khr"] / 4000 for e in entries)

    total_today = combined_total(today_entries)
    total_yesterday = combined_total(yesterday_entries)

    if total_yesterday == 0:
        change = "N/A"
    else:
        change = f"{(total_today - total_yesterday) / total_yesterday * 100:+.1f}%"

    today_usd = sum(e["usd"] for e in today_entries)
    today_khr = sum(e["khr"] for e in today_entries)
    yesterday_usd = sum(e["usd"] for e in yesterday_entries)
    yesterday_khr = sum(e["khr"] for e in yesterday_entries)

    msg = (
        f"📊 ប្រៀបធៀបថ្ងៃនេះ vs ម្សិលមិញ៖\n"
        f"ថ្ងៃនេះ: ${today_usd:.2f} / ៛{today_khr:,}  (≈ ${total_today:.2f})\n"
        f"ម្សិលមិញ: ${yesterday_usd:.2f} / ៛{yesterday_khr:,}  (≈ ${total_yesterday:.2f})\n"
        f"ផ្លាស់ប្តូរ: {change}"
    )
    await update.message.reply_text(msg)

async def chart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    data = load_data()
    today = datetime.date.today()
    days = [(today - datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6, -1, -1)]
    totals = []
    for d in days:
        entries = [e for e in data if e["date"] == d]
        totals.append(sum(e["usd"] for e in entries))
    plt.figure(figsize=(10, 5))
    plt.bar(days, totals, color='skyblue')
    plt.title("ប្រាក់ចំណូល ៧ថ្ងៃចុងក្រោយ (USD)")
    plt.xlabel("កាលបរិច្ឆេទ")
    plt.ylabel("USD")
    plt.xticks(rotation=45)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    await update.message.reply_photo(photo=buf)

async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    today = datetime.date.today()
    month_prefix = today.strftime("%Y-%m")
    data = load_data()
    entries = [e for e in data if e["date"].startswith(month_prefix)]
    total_usd = sum(e["usd"] for e in entries)
    total_khr = sum(e["khr"] for e in entries)
    num_txns = len(entries)
    days_passed = today.day
    avg_per_day = total_usd / days_passed if days_passed > 0 else 0
    msg = (
        f"📊 សង្ខេបខែ {month_prefix}\n"
        f"ប្រតិបត្តិការសរុប: {num_txns}\n"
        f"ចំណូលសរុប: ${total_usd:.2f} / ៛{total_khr:,}\n"
        f"មធ្យមក្នុងមួយថ្ងៃ: ${avg_per_day:.2f}"
    )
    await update.message.reply_text(msg)

async def bysender(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    period = "daily"
    if context.args:
        period = context.args[0].lower()
    data = load_data()
    entries = get_entries_for_period(data, period)
    sender_totals = {}
    for e in entries:
        payer = e.get("note", "unknown").strip()
        if not payer:
            payer = "unknown"
        sender_totals[payer] = sender_totals.get(payer, 0) + e["usd"] + e["khr"]/4000
    if not sender_totals:
        await update.message.reply_text("គ្មានទិន្នន័យ។")
        return
    sorted_senders = sorted(sender_totals.items(), key=lambda x: x[1], reverse=True)
    lines = [f"📊 អ្នកផ្ញើ ({period}):"]
    for sender, amt in sorted_senders[:10]:
        lines.append(f"  {sender}: ${amt:.2f}")
    await update.message.reply_text("\n".join(lines))

async def undo_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    deleted = load_deleted()
    if not deleted:
        await update.message.reply_text("មិនមានធាតុដែលត្រូវស្តារទេ។")
        return
    last_deleted = deleted.pop()
    save_deleted(deleted)
    data = load_data()
    data.append(last_deleted)
    save_data(data)
    append_to_sheet(last_deleted)
    await update.message.reply_text(
        f"✅ បានស្តារធាតុ: {last_deleted.get('note','')} | "
        f"${last_deleted.get('usd',0):.2f} / ៛{last_deleted.get('khr',0):,}"
    )

async def duplicates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    data = load_data()
    total = len(data)
    dups = []
    for i in range(total):
        for j in range(i+1, total):
            e1 = data[i]
            e2 = data[j]
            if e1["date"] == e2["date"] and e1["usd"] == e2["usd"] and e1["khr"] == e2["khr"]:
                n1 = (e1.get("note","") or "").strip()
                n2 = (e2.get("note","") or "").strip()
                if n1.split()[0].lower() == n2.split()[0].lower() if n1 and n2 else (not n1 and not n2):
                    # Convert to /delete_index‑compatible indices (1 = most recent)
                    idx1 = total - i
                    idx2 = total - j
                    dups.append((idx1, idx2, e1, e2))
    if not dups:
        await update.message.reply_text("មិនមានធាតុស្ទួនទេ។")
        return
    lines = ["🔍 ធាតុស្ទួនដែលអាចមាន៖"]
    for idx1, idx2, e1, e2 in dups[:10]:
        lines.append(
            f"#{idx1} & #{idx2}: {e1['date']} ${e1['usd']} / {e1['khr']}៛ ({e1.get('note','')[:30]} / {e2.get('note','')[:30]})"
        )
    lines.append("សរុប /delete_index <លេខ> ដើម្បីលុបធាតុដែលស្ទួន (យកលេខតូច)។")
    await update.message.reply_text("\n".join(lines))

async def announce(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    if not ANNOUNCE_GROUP_IDS:
        await update.message.reply_text("ANNOUNCE_GROUP_IDS មិនបានកំណត់ទេ។")
        return

    today = datetime.date.today()
    data = load_data()
    entries_today = [e for e in data if e["date"] == today.strftime("%Y-%m-%d")]

    if not entries_today:
        await update.message.reply_text("ថ្ងៃនេះមិនទាន់មានប្រតិបត្តិការទេ។")
        return

    # Optional: filter by a specific business tag if provided as argument
    if context.args:
        business_filter = context.args[0].lower()
        entries_today = [e for e in entries_today if e.get("business") == business_filter]
        if not entries_today:
            await update.message.reply_text(f"គ្មានប្រតិបត្តិការសម្រាប់ {business_filter} ថ្ងៃនេះទេ។")
            return

    # Group by business tag
    grouped = defaultdict(list)
    for e in entries_today:
        biz = e.get("business", "unknown")
        grouped[biz].append(e)

    # Send one message per business tag
    for biz in sorted(grouped.keys()):
        entries = grouped[biz]
        total_usd = sum(e["usd"] for e in entries)
        total_khr = sum(e["khr"] for e in entries)
        count = len(entries)
        msg = (
            f"📊 សេចក្តីសង្ខេបថ្ងៃនេះ ({today.strftime('%Y-%m-%d')})\n"
            f"អាជីវកម្ម: {biz}\n"
            f"ប្រតិបត្តិការ: {count}\n"
            f"សរុប: ${total_usd:.2f} / ៛{total_khr:,}"
        )
        for gid in ANNOUNCE_GROUP_IDS:
            try:
                await context.bot.send_message(chat_id=gid, text=msg)
            except Exception as e:
                await update.message.reply_text(f"បរាជ័យសម្រាប់ក្រុម {gid}៖ {e}")
                continue

    await update.message.reply_text("✅ បានផ្ញើសេចក្តីសង្ខេបតាមអាជីវកម្មទៅក្រុមទាំងអស់។")
async def edit_index(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        idx = int(context.args[0])
    except:
        await update.message.reply_text("សូមប្រើ `/edit_index <n> <amount> [note]`")
        return
    data = load_data()
    if idx < 1 or idx > len(data):
        await update.message.reply_text("លេខមិនត្រឹមត្រូវ។")
        return
    entry = data[len(data) - idx]
    if len(context.args) < 2:
        await update.message.reply_text("សូមបញ្ចូលចំនួនថ្មី។")
        return
    new_amount_str = context.args[1]
    new_usd, new_khr, _ = extract_amounts(new_amount_str)
    if new_usd == 0 and new_khr == 0:
        await update.message.reply_text("ចំនួនមិនត្រឹមត្រូវ។")
        return
    old_usd = entry["usd"]
    old_khr = entry["khr"]
    entry["usd"] = new_usd
    entry["khr"] = new_khr
    if len(context.args) >= 3:
        entry["note"] = " ".join(context.args[2:])
    save_data(data)
    update_sheet_row_by_tran_id(entry.get("tran_id"), entry)
    await update.message.reply_text(
        f"✅ បានកែប្រែធាតុលេខ {idx}: "
        f"ពី ${old_usd:.2f} / ៛{old_khr:,} → ${new_usd:.2f} / ៛{new_khr:,} | {entry['note']}"
    )

async def recent_entries(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        n = int(context.args[0]) if context.args else 5
    except:
        n = 5
    data = load_data()
    if not data:
        await update.message.reply_text("គ្មានទិន្នន័យ។")
        return
    recent = data[-n:][::-1]
    lines = ["📋 ប្រតិបត្តិការចុងក្រោយ៖"]
    for i, e in enumerate(recent, start=1):
        note = (e.get("note", "") or "")[:30]
        business = e.get("business", "?")
        lines.append(f"{i}. {e['date']} | ${e['usd']:.2f} / ៛{e['khr']:,} | {business} | {note}")
    lines.append("សរុប /delete_index <លេខ> ដើម្បីលុប។")
    await update.message.reply_text("\n".join(lines))

async def delete_index(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        idx = int(context.args[0]) if context.args else 1
    except:
        await update.message.reply_text("សូមប្រើ `/delete_index <លេខ>` (ឧទាហរណ៍ `/delete_index 2`)")
        return
    data = load_data()
    if not data or idx < 1 or idx > len(data):
        await update.message.reply_text("លេខមិនត្រឹមត្រូវ ឬគ្មានទិន្នន័យ។")
        return
    entry_index = len(data) - idx
    entry = data.pop(entry_index)
    save_data(data)
    deleted = load_deleted()
    deleted.append(entry)
    save_deleted(deleted)
    sheet_deleted = delete_sheet_row_by_tran_id(entry.get("tran_id"))
    msg = f"🗑 បានលុបធាតុលេខ {idx}: {entry.get('note','')} | ${entry.get('usd',0):.2f} / ៛{entry.get('khr',0):,}"
    if sheet_deleted:
        msg += "\n✅ ក៏បានលុបពី Google Sheets ផងដែរ។"
    else:
        msg += "\n⚠️ មិនអាចលុបពី Google Sheets ទេ។"
    await update.message.reply_text(msg)

# ---------- Background threads ----------
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
        except Exception as e:
            logger.error(f"Sync worker error: {e}")
        time.sleep(SYNC_INTERVAL_MINUTES * 60)

def announcement_worker(bot_token: str) -> None:
    """Check every minute and send daily summary to all announcement groups at the set time."""
    last_sent_date = None
    while True:
        try:
            if not ANNOUNCE_GROUP_IDS:
                time.sleep(60)
                continue
            tz = pytz.timezone("Asia/Phnom_Penh")
            now = datetime.datetime.now(tz)
            today_str = now.strftime("%Y-%m-%d")
            current_time = now.strftime("%H:%M")
            if current_time == ANNOUNCE_TIME and last_sent_date != today_str:
                data = load_data()
                entries = [e for e in data if e["date"] == today_str]
                if entries:
                    total_usd = sum(e["usd"] for e in entries)
                    total_khr = sum(e["khr"] for e in entries)
                    msg = (
                        f"📊 សេចក្តីសង្ខេបថ្ងៃនេះ ({today_str})\n"
                        f"ប្រតិបត្តិការ: {len(entries)}\n"
                        f"សរុប: ${total_usd:.2f} / ៛{total_khr:,}"
                    )
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    for gid in ANNOUNCE_GROUP_IDS:
                        payload = {"chat_id": gid, "text": msg}
                        try:
                            requests.post(url, json=payload, timeout=10)
                        except Exception as e:
                            logger.error(f"Announce send failed for group {gid}: {e}")
                last_sent_date = today_str
        except Exception as e:
            logger.error(f"Announcement worker error: {e}")
        time.sleep(60)

def reminder_worker(bot_token: str) -> None:
    last_sent_date = None
    while True:
        try:
            if not os.path.exists(REMINDER_FILE):
                time.sleep(60)
                continue
            with open(REMINDER_FILE, "r") as f:
                rem_time = f.read().strip()
            if not rem_time:
                time.sleep(60)
                continue
            tz = pytz.timezone("Asia/Phnom_Penh")
            now = datetime.datetime.now(tz)
            today_str = now.strftime("%Y-%m-%d")
            current_time = now.strftime("%H:%M")
            if current_time == rem_time and last_sent_date != today_str:
                data = load_data()
                entries = [e for e in data if e["date"] == today_str]
                if not entries:
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    text = f"⏰ ដាស់តឿន – ថ្ងៃនេះ {today_str} អ្នកមិនទាន់កត់ត្រាចំណូលទេ។ សូមបញ្ចូលចំនួនទឹកប្រាក់។"
                    payload = {"chat_id": OWNER_ID, "text": text}
                    requests.post(url, json=payload, timeout=10)
                    last_sent_date = today_str
        except Exception as e:
            logger.error(f"Reminder worker error: {e}")
        time.sleep(60)

def sheet_worker() -> None:
    """Continuously process queued sheet rows."""
    while True:
        try:
            entry, row = sheet_queue.get()
            if entry is None:   # sentinel to stop the thread
                break
            # Write to master sheet
            sheet = get_sheet()
            if sheet:
                try:
                    sheet.append_row(row, value_input_option="USER_ENTERED")
                    logger.info(f"Sheet export (background): {entry.get('usd',0)}$ / {entry.get('khr',0)}៛ (ID: {entry.get('tran_id','')})")
                except Exception as e:
                    logger.error(f"Master sheet write failed: {e}")
            # Write to the All Businesses sheet
            try:
                biz_sheet = get_all_businesses_sheet()
                if biz_sheet:
                    biz_sheet.append_row(row, value_input_option="USER_ENTERED")
            except Exception as e:
                logger.error(f"All Businesses sheet write failed: {e}")
        except Exception as e:
            logger.error(f"Sheet worker error: {e}")
        finally:
            sheet_queue.task_done()

# ---------- Build Application ----------
BOT_TOKEN = os.environ["BOT_TOKEN"]

application = (
    Application.builder()
    .token(BOT_TOKEN)
    .updater(None)
    .build()
)
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("help", help_command))
application.add_handler(CommandHandler("ping", ping))
application.add_handler(CommandHandler("settings", settings))
application.add_handler(CommandHandler("recent", recent_entries))
application.add_handler(CommandHandler("delete_index", delete_index))
application.add_handler(CommandHandler("delete", delete_index))
application.add_handler(CommandHandler("edit_index", edit_index))
application.add_handler(CommandHandler("undo_delete", undo_delete))
application.add_handler(CommandHandler("compare", compare))
application.add_handler(CommandHandler("chart", chart))
application.add_handler(CommandHandler("summary", summary))
application.add_handler(CommandHandler("bysender", bysender))
application.add_handler(CommandHandler("duplicates", duplicates))
application.add_handler(CommandHandler("announce", announce))
application.add_handler(CommandHandler("clear_all", clear_all_command))
application.add_handler(CommandHandler("confirm_clear", confirm_clear_command))
application.add_handler(CommandHandler("add_manager", add_manager))
application.add_handler(CommandHandler("remove_manager", remove_manager))
application.add_handler(CommandHandler("permissions", permissions))
application.add_handler(CommandHandler("sync", manual_sync))
application.add_handler(CommandHandler("sync_status", sync_status))
application.add_handler(CommandHandler("day", day_report))
application.add_handler(CommandHandler("range", range_report))
application.add_handler(CommandHandler("top", top))
application.add_handler(CommandHandler("stats", stats))
application.add_handler(CommandHandler("search", search))
application.add_handler(CommandHandler("lock", lock))
application.add_handler(CommandHandler("unlock", unlock))
application.add_handler(CommandHandler("remind", set_reminder))
application.add_handler(CommandHandler("export", export_csv))
application.add_handler(CommandHandler("bybusiness", bybusiness_command))
application.add_handler(CallbackQueryHandler(category_callback, pattern="^cat_"))

application.add_handler(MessageHandler(
    filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND,
    group_message_handler
))
application.add_handler(CommandHandler("daily", group_daily_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("weekly", group_weekly_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("monthly", group_monthly_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("quarterly", group_quarterly_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("yearly", group_yearly_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("range", group_range_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("top", group_top_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("duplicates", group_duplicates_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("edit_index", group_edit_index_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("summary", group_summary_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("bysender", group_bysender_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("compare", group_compare_command, filters=filters.ChatType.GROUPS))
application.add_handler(CommandHandler("menu", group_menu_command, filters=filters.ChatType.GROUPS))
application.add_handler(CallbackQueryHandler(group_menu_callback, pattern="^grpmenu_"))
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

@flask_app.route("/email_webhook", methods=["POST"])
def email_webhook():
    secret = request.headers.get("X-Webhook-Secret", "")
    if EMAIL_WEBHOOK_SECRET and secret != EMAIL_WEBHOOK_SECRET:
        return "Unauthorized", 403
    payload = request.get_json(force=True)
    usd = float(payload.get("usd", 0))
    khr = float(payload.get("khr", 0))
    note = payload.get("note", "")
    if usd == 0 and khr == 0:
        return "No amount", 400
    today = datetime.date.today()
    entry = {
        "date": today.strftime("%Y-%m-%d"),
        "usd": usd,
        "khr": khr,
        "note": note,
        "category": "other",
        "business": PAYWAY_BUSINESS,
        "tran_id": str(uuid.uuid4())
    }
    data = load_data()
    data.append(entry)
    save_data(data)
    append_to_sheet(entry)
    return "OK", 200

try:
    rebuild_from_sheet()
except Exception as e:
    logger.warning(f"Rebuild skipped due to error: {e}")


seed_seen_trx_ids()

sync_thread = threading.Thread(target=sync_worker, args=(BOT_TOKEN,), daemon=True)
sync_thread.start()
announce_thread = threading.Thread(target=announcement_worker, args=(BOT_TOKEN,), daemon=True)
announce_thread.start()
sheet_thread = threading.Thread(target=sheet_worker, daemon=True)
sheet_thread.start()
reminder_thread = threading.Thread(target=reminder_worker, args=(BOT_TOKEN,), daemon=True)
reminder_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port)