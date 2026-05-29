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
import hmac
import sqlite3
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

# -------------------------------------------------------------------
# Logging & configuration (shared)
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# AutoSum Bot Configuration
# -------------------------------------------------------------------
clear_confirmation_token = None
clear_confirmation_token_time: Optional[float] = None
CLEAR_TOKEN_TTL = 120
MANUAL_LOCKED = False
sheet_queue = queue.Queue()
SEEN_TRX_IDS: Set[str] = set()
_seen_trx_lock = threading.Lock()

_rate_limit_lock = threading.Lock()
_user_msg_times: Dict[int, List[float]] = defaultdict(list)
RATE_LIMIT_MAX = 20
RATE_LIMIT_WINDOW = 60

_group_semaphore = asyncio.Semaphore(5)

MAX_NOTE_LEN = 200
MAX_WEBHOOK_BODY = 64 * 1024

OWNER_ID = int(os.environ.get("OWNER_ID", 0))

MANAGER_IDS: Set[int] = set()
_manager_str = os.environ.get("MANAGER_IDS", "")
if _manager_str:
    for uid_str in _manager_str.split(","):
        try:
            MANAGER_IDS.add(int(uid_str.strip()))
        except ValueError:
            logger.warning("Invalid manager ID: %s", uid_str)

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
                logger.warning("Invalid entry in MANAGER_GROUP_MAP: %s", block)

PAYWAY_MERCHANT_ID = os.environ.get("PAYWAY_MERCHANT_ID", "")
PAYWAY_API_KEY = os.environ.get("PAYWAY_API_KEY", "")
PAYWAY_BASE_URL = os.environ.get("PAYWAY_BASE_URL", "https://www.payway.com.kh/api/v1")
PAYWAY_BUSINESS = os.environ.get("PAYWAY_BUSINESS", "birdnest")
SYNC_INTERVAL_MINUTES = int(os.environ.get("SYNC_INTERVAL_MINUTES", "5"))

MONITORED_GROUP_IDS = [int(gid.strip()) for gid in os.environ.get("MONITORED_GROUP_IDS", "").split(",") if gid.strip()]
GROUP_BUSINESS_TAG = os.environ.get("GROUP_BUSINESS_TAG", "group_payments")

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
                logger.warning("Invalid group ID in GROUP_BUSINESS_MAP: %s", parts[0])

ALLOWED_GROUP_SENDERS = os.environ.get("ALLOWED_GROUP_SENDERS", "")
allowed_sender_ids: Set[int] = set()
if ALLOWED_GROUP_SENDERS:
    for uid_str in ALLOWED_GROUP_SENDERS.split(","):
        try:
            allowed_sender_ids.add(int(uid_str.strip()))
        except ValueError:
            logger.warning("Invalid user ID in ALLOWED_GROUP_SENDERS: %s", uid_str)

ANNOUNCE_GROUP_IDS: List[int] = []
_announce_ids_str = os.environ.get("ANNOUNCE_GROUP_ID", "") or ""
if _announce_ids_str:
    for gid_str in _announce_ids_str.split(","):
        try:
            gid = int(gid_str.strip())
            if gid != 0:
                ANNOUNCE_GROUP_IDS.append(gid)
        except ValueError:
            logger.warning("Invalid ANNOUNCE_GROUP_ID entry: %s", gid_str)

ANNOUNCE_TIME = os.environ.get("ANNOUNCE_TIME", "") or "21:00"
GSPREAD_CREDENTIALS_JSON = os.environ.get("GSPREAD_CREDENTIALS_JSON", "")
SHEET_NAME = os.environ.get("SHEET_NAME", "Bird Nest Income")
EMAIL_WEBHOOK_SECRET = os.environ.get("EMAIL_WEBHOOK_SECRET", "")

CATEGORIES = {
    "product": "🛒 ផលិតផល",
    "delivery": "🚚 ដឹកជញ្ជូន",
    "other": "💸 ផ្សេងៗ"
}

DATA_FILE = "income.json"
SYNC_STATE_FILE = "sync_state.json"
MANAGERS_FILE = "managers.json"
DELETED_FILE = "deleted.json"
REMINDER_FILE = "reminder.txt"
DB_FILE = "income.db"
ALL_BUSINESSES_SHEET = f"{SHEET_NAME} - All Businesses"

# -------------------------------------------------------------------
# SQLite helpers (AutoSum)
# -------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        date TEXT NOT NULL,
        usd REAL NOT NULL,
        khr INTEGER NOT NULL,
        category TEXT,
        note TEXT,
        business TEXT,
        source TEXT,
        timestamp TEXT,
        payway_trx_id TEXT UNIQUE
    )
""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON transactions(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_business ON transactions(business)")
    conn.close()

def migrate_json_to_sqlite():
    if not os.path.exists(DB_FILE):
        init_db()
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM transactions")
    if cur.fetchone()[0] > 0:
        conn.close()
        return
    if not os.path.exists(DATA_FILE):
        conn.close()
        return
    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
        for entry in data:
            cur.execute(
                "INSERT OR IGNORE INTO transactions (id, date, usd, khr, category, note, business, source, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (entry.get("tran_id", str(uuid.uuid4())), entry["date"], entry["usd"], entry["khr"],
                 entry.get("category", "other"), entry.get("note", ""), entry.get("business", "manual"),
                 entry.get("source", ""), datetime.datetime.now().isoformat())
            )
        conn.commit()
        logger.info("Migrated %d entries from JSON to SQLite", len(data))
        os.rename(DATA_FILE, DATA_FILE + ".bak")
    except Exception as e:
        logger.error("Migration failed: %s", e)
    finally:
        conn.close()

def load_data() -> List[Dict[str, Any]]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT id as tran_id, date, usd, khr, category, note, business, source FROM transactions ORDER BY date, timestamp")
    rows = cur.fetchall()
    data = [dict(row) for row in rows]
    conn.close()
    return data

def delete_transaction(tran_id: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("DELETE FROM transactions WHERE id = ?", (tran_id,))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

def update_transaction(tran_id: str, new_entry: Dict[str, Any]) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "UPDATE transactions SET date=?, usd=?, khr=?, category=?, note=?, business=?, source=?, timestamp=? WHERE id=?",
        (new_entry["date"], new_entry["usd"], new_entry["khr"],
         new_entry.get("category", "other"), new_entry.get("note", ""),
         new_entry.get("business", "manual"), new_entry.get("source", ""),
         datetime.datetime.now().isoformat(), tran_id)
    )
    updated = cur.rowcount > 0
    conn.commit()
    conn.close()
    return updated

def load_sync_state() -> Dict[str, Any]:
    if os.path.exists(SYNC_STATE_FILE):
        with open(SYNC_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_sync": None, "imported_ids": []}

def save_sync_state(state: Dict[str, Any]) -> None:
    with open(SYNC_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def load_managers() -> Dict[str, Any]:
    if os.path.exists(MANAGERS_FILE):
        with open(MANAGERS_FILE, "r", encoding="utf-8") as f:
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
    with open(MANAGERS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_deleted() -> List[Dict[str, Any]]:
    if os.path.exists(DELETED_FILE):
        with open(DELETED_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_deleted(deleted: List[Dict[str, Any]]) -> None:
    with open(DELETED_FILE, "w", encoding="utf-8") as f:
        json.dump(deleted, f, ensure_ascii=False, indent=2)

# -------------------------------------------------------------------
# Google Sheets helpers (AutoSum)
# -------------------------------------------------------------------
_SHEET_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")
def _sanitise_sheet_value(value: str) -> str:
    value = str(value).strip()
    if value and value[0] in _SHEET_FORMULA_PREFIXES:
        value = "'" + value
    return value[:MAX_NOTE_LEN]

def get_sheet():
    if not GSPREAD_CREDENTIALS_JSON:
        return None
    creds_dict = json.loads(GSPREAD_CREDENTIALS_JSON)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME).sheet1

def write_master_sheet_row(entry: dict) -> bool:
    sheet = get_sheet()
    if not sheet:
        return False
    now = datetime.datetime.now(pytz.timezone('Asia/Phnom_Penh')).strftime("%Y-%m-%d %H:%M:%S")
    if "tran_id" not in entry:
        entry["tran_id"] = str(uuid.uuid4())
    row = [
        _sanitise_sheet_value(entry.get("date", "")),
        entry.get("usd", 0),
        entry.get("khr", 0),
        _sanitise_sheet_value(CATEGORIES.get(entry.get("category", "other"), "💸 ផ្សេងៗ")),
        _sanitise_sheet_value(entry.get("note", "")),
        _sanitise_sheet_value(entry.get("business", "")),
        now,
        entry["tran_id"],
        entry.get("payway_trx_id", "")
    ]
    for attempt in range(3):
        try:
            sheet.append_row(row, value_input_option="USER_ENTERED")
            logger.info("Sheet write (sync): %s", entry.get('note','')[:30])
            return True
        except Exception as e:
            logger.warning("Sheet write attempt %d failed: %s", attempt+1, e)
            time.sleep(2)
    return False

def append_to_sheet(entry: dict) -> None:
    if not GSPREAD_CREDENTIALS_JSON:
        return
    try:
        now = datetime.datetime.now(pytz.timezone('Asia/Phnom_Penh')).strftime("%Y-%m-%d %H:%M:%S")
        if "tran_id" not in entry:
            entry["tran_id"] = str(uuid.uuid4())
        row = [
            _sanitise_sheet_value(entry.get("date", "")),
            entry.get("usd", 0),
            entry.get("khr", 0),
            _sanitise_sheet_value(CATEGORIES.get(entry.get("category", "other"), "💸 ផ្សេងៗ")),
            _sanitise_sheet_value(entry.get("note", "")),
            _sanitise_sheet_value(entry.get("business", "")),
            now,
            entry["tran_id"],
            entry.get("payway_trx_id", "")
        ]
        sheet_queue.put((entry, row))
    except Exception as e:
        logger.error("Failed to queue sheet row: %s", e)

def get_all_businesses_sheet():
    if not GSPREAD_CREDENTIALS_JSON:
        return None
    try:
        creds_dict = json.loads(GSPREAD_CREDENTIALS_JSON)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open(ALL_BUSINESSES_SHEET).sheet1
    except SpreadsheetNotFound:
        logger.error("'%s' not found. Create it and share with the service account.", ALL_BUSINESSES_SHEET)
        return None
    except Exception as e:
        logger.error("Error opening '%s': %s", ALL_BUSINESSES_SHEET, e)
        return None

def delete_sheet_row_by_tran_id(tran_id: str) -> bool:
    sheet = get_sheet()
    if not sheet:
        return False
    try:
        rows = sheet.get_all_values()
        for i, row in enumerate(rows):
            if len(row) > 7 and row[7] == tran_id:
                sheet.delete_row(i + 1)
                logger.info("Deleted sheet row with tran_id %s", tran_id)
                return True
        return False
    except Exception as e:
        logger.error("Error deleting sheet row: %s", e)
        return False

def update_sheet_row_by_tran_id(tran_id: str, new_entry: dict) -> bool:
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
        logger.error("Error updating sheet row: %s", e)
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
        conn = sqlite3.connect(DB_FILE)
        conn.execute("PRAGMA journal_mode=WAL")
        cur = conn.cursor()
        cur.execute("DELETE FROM transactions")
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
            tran_id_val = row[7] if len(row) > 7 else str(uuid.uuid4())
            payway_trx_val = row[8] if len(row) > 8 else ""
            cur.execute(
                "INSERT OR IGNORE INTO transactions (id, date, usd, khr, category, note, business, timestamp, payway_trx_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (tran_id_val, date_val, usd_val, khr_val, category_val.lower(),
                 note_val[:MAX_NOTE_LEN], business_val,
                 datetime.datetime.now().isoformat(), payway_trx_val or None)
            )
            imported_ids.add(tran_id_val)
        conn.commit()
        conn.close()
        state = load_sync_state()
        state["imported_ids"] = list(imported_ids)
        state["last_sync"] = datetime.datetime.now().isoformat() if imported_ids else None
        save_sync_state(state)
        logger.info("Rebuilt %d transactions from Google Sheets into SQLite.", len(imported_ids))
    except Exception as e:
        logger.error("Rebuild failed: %s", e)

def seed_seen_trx_ids() -> None:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT payway_trx_id FROM transactions WHERE payway_trx_id IS NOT NULL")
    with _seen_trx_lock:
        for row in cur.fetchall():
            SEEN_TRX_IDS.add(row[0])
    conn.close()

# -------------------------------------------------------------------
# Core helper functions (AutoSum)
# -------------------------------------------------------------------
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
        note = note.strip().strip(" -,/")[:MAX_NOTE_LEN]
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
    return []

def _is_rate_limited(user_id: int) -> bool:
    now = time.monotonic()
    with _rate_limit_lock:
        times = _user_msg_times[user_id]
        cutoff = now - RATE_LIMIT_WINDOW
        _user_msg_times[user_id] = [t for t in times if t > cutoff]
        if len(_user_msg_times[user_id]) >= RATE_LIMIT_MAX:
            return True
        _user_msg_times[user_id].append(now)
        return False

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

def group_period_summary(chat_id: int, period: str, label: str, today: Optional[datetime.date] = None) -> str:
    data = load_data()
    all_entries = get_entries_for_period(data, period, today)
    group_tag = group_business_map.get(chat_id, GROUP_BUSINESS_TAG)
    group_entries = []
    for e in all_entries:
        src = e.get("source") or ""
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

# -------------------------------------------------------------------
# Group payment extraction (AutoSum)
# -------------------------------------------------------------------
def extract_group_payment(text: str) -> Optional[Tuple[float, float, str]]:
    usd = 0.0
    khr = 0
    note = ""
    usd_match = re.search(r'\$(\d+\.?\d*)', text)
    if not usd_match:
        usd_match = re.search(r'SALE\s+(\d+\.?\d*)\s+USD', text, re.IGNORECASE)
    if not usd_match:
        usd_match = re.search(r'Received\s+(\d+\.?\d*)\s+USD', text, re.IGNORECASE)
    if usd_match:
        usd = float(usd_match.group(1))
    khr_match = re.search(r'៛\s*([\d,]+)', text)
    if khr_match:
        khr = int(khr_match.group(1).replace(',', ''))
    if usd == 0 and khr == 0:
        return None
    payer_match = re.search(r'paid by\s+([^(*]+?)\s*\(\*', text, re.IGNORECASE)
    if payer_match:
        note = payer_match.group(1).strip()
    else:
        card_match = re.search(r'from card\s+([\d\*x]+)', text, re.IGNORECASE)
        if card_match:
            note = f"Card: {card_match.group(1)}"
        else:
            from_match = re.search(r'from\s+([\d\s\*]+)\s+([A-Za-z\s]+?)(?:,|$)', text, re.IGNORECASE)
            if from_match:
                number = from_match.group(1).strip()
                name = from_match.group(2).strip()
                note = f"{name} ({number})"
            else:
                note = text.split('.')[0].strip()[:MAX_NOTE_LEN]
    trx_match = re.search(r'Trx\.\s*ID:\s*(\d+)', text, re.IGNORECASE)
    if not trx_match:
        trx_match = re.search(r'Ref\.ID:?\s+(\d+)', text, re.IGNORECASE)
    if trx_match:
        trx_id = trx_match.group(1)
        note += f" (Trx: {trx_id})"
    return usd, khr, note[:MAX_NOTE_LEN]

# -------------------------------------------------------------------
# Group message handler (AutoSum)
# -------------------------------------------------------------------
async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and update.message.text:
        logger.info("📨 GROUP MSG: chat_id=%s, text=%s", update.message.chat.id, update.message.text[:200])
    else:
        logger.info("📨 GROUP MSG: no text")
    async with _group_semaphore:
        if not update.message or not update.message.text:
            return
        msg = update.message
        if msg.chat.id not in MONITORED_GROUP_IDS:
            return
        if not is_allowed_sender(msg.from_user.id, msg.chat.id):
            return
        if _is_rate_limited(msg.from_user.id):
            return
        result = extract_group_payment(msg.text)
        if not result:
            return
        usd, khr, note = result
        trx_id_match = re.search(r"Trx:\s*(\d+)", note)
        payway_id = trx_id_match.group(1) if trx_id_match else None
        if payway_id:
            with _seen_trx_lock:
                if payway_id in SEEN_TRX_IDS:
                    logger.info("Duplicate Trx. ID %s ignored (memory).", payway_id)
                    return
        today = datetime.date.today()
        business_tag = group_business_map.get(msg.chat.id, GROUP_BUSINESS_TAG)
        entry = {
            "date": today.strftime("%Y-%m-%d"),
            "usd": usd,
            "khr": khr,
            "note": note,
            "category": "other",
            "business": business_tag,
            "source": f"group_{msg.chat.id}_msg{msg.message_id}",
            "tran_id": str(uuid.uuid4()),
            "payway_trx_id": payway_id or ""
        }
                # Enqueue the insert (non‑blocking, duplicate already checked in memory)
        add_transaction(entry, payway_id)

        # Record the PayWay ID in memory so future duplicates are caught instantly
        if payway_id:
            with _seen_trx_lock:
                SEEN_TRX_IDS.add(payway_id)

        append_to_sheet(entry)
        logger.info("Group payment recorded: $%.2f / %d ៛ from group %d (business: %s)",
                    usd, khr, msg.chat.id, business_tag)

# -------------------------------------------------------------------
# AutoSum command handlers (the full list)
# -------------------------------------------------------------------
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("ប្រចាំថ្ងៃ"), KeyboardButton("ប្រចាំសប្ដាហ៍"), KeyboardButton("ប្រចាំខែ")],
        [KeyboardButton("📂 កំណត់ប្រភេទ")],
    ],
    resize_keyboard=True,
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    await update.message.reply_text(
        "👋 សូមស្វាគមន៍! ជ្រើសរើសរបាយការណ៍ ឬផ្ញើការទូទាត់។\nសូមវាយ /help សម្រាប់បញ្ជីពាក្យបញ្ជា។",
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
        "/force_sync – បង្ខំសមកាលកម្មទៅ Google Sheets\n"
        "/sheet_test – សាកល្បងការតភ្ជាប់ Sheet\n"
        "/check_rebuild – ពិនិត្យការស្ថាបនាឡើងវិញ\n"
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
    if cat_key not in CATEGORIES:
        return
    data[-1]["category"] = cat_key
    update_transaction(data[-1]["tran_id"], data[-1])
    await query.edit_message_text(f"✅ បានកំណត់ប្រភេទ: {CATEGORIES[cat_key]}")

async def clear_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    global clear_confirmation_token, clear_confirmation_token_time
    token = str(random.randint(100000, 999999))
    clear_confirmation_token = token
    clear_confirmation_token_time = time.monotonic()
    await update.message.reply_text(
        f"⚠️ តើអ្នកពិតជាចង់លុបទិន្នន័យទាំងអស់មែនទេ?\n\n"
        f"សូមវាយ `/confirm_clear {token}` ដើម្បីបញ្ជាក់។\n"
        f"ប្រសិនបើអ្នកមិនចង់លុបទេ សូមមិនអើពើសារនេះ (ផុតកំណត់ក្នុង {CLEAR_TOKEN_TTL}s)។"
    )

async def confirm_clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    global clear_confirmation_token, clear_confirmation_token_time
    try:
        token = context.args[0]
    except IndexError:
        await update.message.reply_text("សូមបញ្ចូលកូដបញ្ជាក់ពី `/confirm_clear <code>`")
        return
    if (clear_confirmation_token is None or clear_confirmation_token_time is None or
            time.monotonic() - clear_confirmation_token_time > CLEAR_TOKEN_TTL):
        clear_confirmation_token = None
        clear_confirmation_token_time = None
        await update.message.reply_text("កូដផុតកំណត់ ឬមិនមានទេ។ សូមប្រើ `/clear_all` ម្តងទៀត។")
        return
    if not hmac.compare_digest(token, clear_confirmation_token):
        await update.message.reply_text("កូដមិនត្រឹមត្រូវ។ សូមប្រើ `/clear_all` ម្តងទៀត។")
        return
    sheet = get_sheet()
    if sheet:
        try:
            rows = sheet.get_all_values()
            if len(rows) > 1:
                sheet.delete_rows(2, len(rows))
        except Exception as e:
            logger.error("Failed to clear sheet: %s", e)
            await update.message.reply_text(f"❌ មានបញ្ហាក្នុងការលុប Sheet៖ {e}")
            return
    conn = sqlite3.connect(DB_FILE)
    conn.execute("DELETE FROM transactions")
    conn.commit()
    conn.close()
    save_sync_state({"last_sync": None, "imported_ids": []})
    save_deleted([])
    with _seen_trx_lock:
        SEEN_TRX_IDS.clear()
    clear_confirmation_token = None
    clear_confirmation_token_time = None
    await update.message.reply_text("✅ ទិន្នន័យទាំងអស់ត្រូវបានលុបចោលទាំងស្រុង (Google Sheets + local)។")

async def day_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        date_str = context.args[0] if context.args else datetime.date.today().strftime("%Y-%m-%d")
        datetime.date.fromisoformat(date_str)
        data = load_data()
        entries = [e for e in data if e["date"] == date_str]
        await update.message.reply_text(summarise(entries, f"ថ្ងៃទី {date_str}"))
    except (ValueError, IndexError):
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
    if delta.days > 366:
        await update.message.reply_text("ជួរកាលបរិច្ឆេទធំពេក (អតិបរមា 366 ថ្ងៃ)។")
        return
    dates = {(start_date + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)}
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
    keyword = " ".join(context.args)[:100]
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
        n = min(int(context.args[0]), 50) if context.args else 5
    except (ValueError, IndexError):
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

async def add_manager(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        new_id = int(context.args[0])
    except (ValueError, IndexError):
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
    except (ValueError, IndexError):
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

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    if _is_rate_limited(OWNER_ID):
        await update.message.reply_text("⏳ សំណើរច្រើនពេក។ សូមរង់ចាំបន្តិច។")
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
               # Extract PayWay transaction ID from the original raw message
        trx_match = re.search(r"Trx\.\s*ID:\s*(\d+)", text)
        payway_id = trx_match.group(1) if trx_match else None

        # Quick in‑memory duplicate check (optional but fast)
        if payway_id:
            with _seen_trx_lock:
                if payway_id in SEEN_TRX_IDS:
                    await update.message.reply_text("⏭ ប្រតិបត្តិការនេះបានកត់ត្រារួចហើយ (Trx. ID ដូចគ្នា)។")
                    return

        if MANUAL_LOCKED:
            await update.message.reply_text("⛔ ការកត់ត្រាដោយដៃត្រូវបានចាក់សោ។ សូមទាក់ទងម្ចាស់។")
            return

        entry = {
            "date": today.strftime("%Y-%m-%d"),
            "usd": usd,
            "khr": khr,
            "note": note[:MAX_NOTE_LEN],
            "category": "other",
            "business": "manual",
            "tran_id": str(uuid.uuid4()),
            "payway_trx_id": payway_id or ""
        }

        # Fire‑and‑forget DB insert (duplicate already checked in memory)
        add_transaction(entry, payway_id)

        # Remember this PayWay ID so future duplicates are caught instantly
        if payway_id:
            with _seen_trx_lock:
                SEEN_TRX_IDS.add(payway_id)

        # Queue for background sheet writes (both master & business sheets)
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
        h, m = int(new_time[:2]), int(new_time[3:])
        if not (0 <= h < 24 and 0 <= m < 60):
            raise ValueError
    except (ValueError, IndexError):
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
        except ValueError:
            pass
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "USD", "KHR", "Category", "Note", "Business", "Timestamp", "Tran ID"])
    for e in data:
        writer.writerow([
            e["date"], e["usd"], e["khr"], e.get("category", "other"),
            e.get("note", ""), e.get("business", ""), "", e.get("tran_id", "")
        ])
    output.seek(0)
    buf = io.BytesIO(output.getvalue().encode("utf-8"))
    buf.name = f"export_{datetime.date.today().isoformat()}.csv"
    await update.message.reply_document(document=buf)

# PayWay sync functions (unchanged)
def fetch_payway_transactions() -> List[Dict[str, Any]]:
    if not PAYWAY_MERCHANT_ID or not PAYWAY_API_KEY:
        return []
    today = datetime.date.today().strftime("%Y-%m-%d")
    url = f"{PAYWAY_BASE_URL}/transactions"
    headers = {"Authorization": f"Bearer {PAYWAY_API_KEY}", "Content-Type": "application/json"}
    params = {"merchant_id": PAYWAY_MERCHANT_ID, "start_date": today, "end_date": today, "status": "approved"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", data.get("transactions", []))
    except Exception as e:
        logger.error("PayWay fetch error: %s", e)
        return []

def import_payway_transaction(txn: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    state = load_sync_state()
    txn_id = str(txn.get("tran_id", ""))
    if not txn_id or txn_id in state["imported_ids"]:
        return None
    amount = float(txn.get("amount", 0))
    currency = txn.get("currency", "").upper()
    description = str(txn.get("description", "") or txn.get("note", ""))[:MAX_NOTE_LEN]
    entry = {
        "date": txn.get("payment_date", datetime.date.today().strftime("%Y-%m-%d")),
        "usd": amount if currency == "USD" else 0.0,
        "khr": int(amount) if currency == "KHR" else 0,
        "note": description,
        "category": "other",
        "business": PAYWAY_BUSINESS,
        "tran_id": txn_id,
        "payway_trx_id": txn_id
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
        # Insert directly into SQLite
        add_transaction(entry, entry.get("payway_trx_id"))
        imported_ids.add(entry["tran_id"])
        added += 1
        append_to_sheet(entry)
    if added > 0:
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
    msg = f"សមកាលកម្មចុងក្រោយ: {last_sync}" if last_sync else "មិនទាន់បានសមកាលកម្មនៅឡើយទេ។"
    await update.message.reply_text(msg)

# Group commands
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
    if delta.days > 366:
        await update.message.reply_text("ជួរកាលបរិច្ឆេទធំពេក (អតិបរមា 366 ថ្ងៃ)។")
        return
    dates = {(start_date + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)}
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
        n = min(int(context.args[0]), 50) if context.args else 5
    except (ValueError, IndexError):
        n = 5
    data = load_data()
    group_id = update.message.chat.id
    group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)
    entries = [e for e in data if e.get("source", "").startswith(f"group_{group_id}_") or (not e.get("source") and e.get("business") == group_tag)]
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
    group_entries = [
        (i, e) for i, e in enumerate(data)
        if e.get("source", "").startswith(f"group_{group_id}_")
        or (not e.get("source") and e.get("business") == group_tag)
    ]
    if not group_entries:
        await update.message.reply_text("មិនមានទិន្នន័យសម្រាប់ក្រុមនេះទេ។")
        return
    dups = []
    for a in range(len(group_entries)):
        idx_a, e1 = group_entries[a]
        for b in range(a + 1, len(group_entries)):
            idx_b, e2 = group_entries[b]
            if e1["date"] == e2["date"] and e1["usd"] == e2["usd"] and e1["khr"] == e2["khr"]:
                n1 = (e1.get("note", "") or "").strip()
                n2 = (e2.get("note", "") or "").strip()
                if n1.split()[0].lower() == n2.split()[0].lower() if n1 and n2 else (not n1 and not n2):
                    idx1 = total - idx_a
                    idx2 = total - idx_b
                    dups.append((idx1, idx2, e1, e2))
    if not dups:
        await update.message.reply_text("មិនមានធាតុស្ទួនក្នុងក្រុមនេះទេ។")
        return
    lines = ["🔍 ធាតុស្ទួនក្នុងក្រុមនេះ៖"]
    for idx1, idx2, e1, e2 in dups[:5]:
        lines.append(f"#{idx1} & #{idx2}: {e1['date']} ${e1['usd']} / {e1['khr']}៛ ({e1.get('note','')[:30]} / {e2.get('note','')[:30]})")
    lines.append("សរុប /delete_index <លេខ> ដើម្បីលុប (យកលេខតូច)។")
    await update.message.reply_text("\n".join(lines))

async def group_edit_index_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.id not in MONITORED_GROUP_IDS:
        return
    if not _can_use_group_commands(update.effective_user.id, update.message.chat.id):
        return
    try:
        idx = int(context.args[0])
    except (ValueError, IndexError):
        await update.message.reply_text("សូមប្រើ `/edit_index <n> <amount> [note]`")
        return
    data = load_data()
    if idx < 1 or idx > len(data):
        await update.message.reply_text("លេខមិនត្រឹមត្រូវ។")
        return
    entry = data[len(data) - idx]
    group_id = update.message.chat.id
    group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)
    src = entry.get("source", "")
    if not (src.startswith(f"group_{group_id}_") or (not src and entry.get("business") == group_tag)):
        await update.message.reply_text("ធាតុនេះមិនមែនជារបស់ក្រុមនេះទេ។")
        return
    if len(context.args) < 2:
        await update.message.reply_text("សូមបញ្ចូលចំនួនថ្មី។")
        return
    new_usd, new_khr, _ = extract_amounts(context.args[1])
    if new_usd == 0 and new_khr == 0:
        await update.message.reply_text("ចំនួនមិនត្រឹមត្រូវ។")
        return
    old_usd = entry["usd"]
    old_khr = entry["khr"]
    entry["usd"] = new_usd
    entry["khr"] = new_khr
    if len(context.args) >= 3:
        entry["note"] = " ".join(context.args[2:])[:MAX_NOTE_LEN]
    update_transaction(entry["tran_id"], entry)
    update_sheet_row_by_tran_id(entry.get("tran_id"), entry)
    await update.message.reply_text(
        f"✅ បានកែប្រែធាតុលេខ {idx}: ពី ${old_usd:.2f} / ៛{old_khr:,} → ${new_usd:.2f} / ៛{new_khr:,} | {entry['note']}"
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
        e.get("source", "").startswith(f"group_{group_id}_") or (not e.get("source") and e.get("business") == group_tag)
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
    group_entries = [e for e in entries if e.get("source", "").startswith(f"group_{group_id}_") or (not e.get("source") and e.get("business") == group_tag)]
    sender_totals: Dict[str, float] = defaultdict(float)
    for e in group_entries:
        payer = (e.get("note", "") or "unknown").strip() or "unknown"
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
    change = "N/A" if total_yesterday == 0 else f"{(total_today - total_yesterday) / total_yesterday * 100:+.1f}%"
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
    await update.message.reply_text("📊 ជ្រើសរើសរបាយការណ៍៖", reply_markup=group_menu_keyboard())

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
        except Exception:
            pass
        return
    today = datetime.date.today()
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
            "📅 សូមប្រើ `/range YYYY-MM-DD YYYY-MM-DD`\nឧទាហរណ៍: `/range 2026-05-01 2026-05-25`",
            reply_markup=group_menu_keyboard()
        )
        return
    elif data == "grpmenu_bybusiness":
        loaded = load_data()
        group_id = chat_id
        group_tag = group_business_map.get(group_id, GROUP_BUSINESS_TAG)
        entries = [
            e for e in loaded
            if e["date"] == today.strftime("%Y-%m-%d") and (
                e.get("source", "").startswith(f"group_{group_id}_") or
                (not e.get("source") and e.get("business") == group_tag)
            )
        ]
        text = business_breakdown(entries) if entries else "ថ្ងៃនេះមិនមានប្រតិបត្តិការសម្រាប់ក្រុមនេះទេ។"
        await context.bot.send_message(chat_id=chat_id, text=text)
        await query.edit_message_text("📊 ជ្រើសរើសរបាយការណ៍៖", reply_markup=group_menu_keyboard())
        return
    else:
        return
    await context.bot.send_message(chat_id=chat_id, text=summary_text)
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
        [InlineKeyboardButton("អាជីវកម្ម", callback_data="grpmenu_bybusiness")],
        [InlineKeyboardButton("❌ បិទ", callback_data="grpmenu_close")]
    ])

# Owner commands (compare, chart, summary, bysender, undo_delete, duplicates, announce, edit_index, recent, delete_index, force_sync, sheet_test, check_rebuild)
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
    change = "N/A" if total_yesterday == 0 else f"{(total_today - total_yesterday) / total_yesterday * 100:+.1f}%"
    msg = (
        f"📊 ប្រៀបធៀបថ្ងៃនេះ vs ម្សិលមិញ៖\n"
        f"ថ្ងៃនេះ: ${sum(e['usd'] for e in today_entries):.2f} / ៛{sum(e['khr'] for e in today_entries):,}  (≈ ${total_today:.2f})\n"
        f"ម្សិលមិញ: ${sum(e['usd'] for e in yesterday_entries):.2f} / ៛{sum(e['khr'] for e in yesterday_entries):,}  (≈ ${total_yesterday:.2f})\n"
        f"ផ្លាស់ប្តូរ: {change}"
    )
    await update.message.reply_text(msg)

async def chart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    data = load_data()
    today = datetime.date.today()
    days = [(today - datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6, -1, -1)]
    totals = [sum(e["usd"] for e in data if e["date"] == d) for d in days]
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
    days_passed = today.day
    avg_per_day = total_usd / days_passed if days_passed > 0 else 0
    msg = (
        f"📊 សង្ខេបខែ {month_prefix}\n"
        f"ប្រតិបត្តិការសរុប: {len(entries)}\n"
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
    sender_totals: Dict[str, float] = {}
    for e in entries:
        payer = e.get("note", "unknown").strip() or "unknown"
        sender_totals[payer] = sender_totals.get(payer, 0) + e["usd"] + e["khr"]/4000
    if not sender_totals:
        await update.message.reply_text("គ្មានទិន្នន័យ។")
        return
    lines = [f"📊 អ្នកផ្ញើ ({period}):"]
    for sender, amt in sorted(sender_totals.items(), key=lambda x: x[1], reverse=True)[:10]:
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
    # Re-insert the entry into the database
    add_transaction(last_deleted)
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
            e1, e2 = data[i], data[j]
            if e1["date"] == e2["date"] and e1["usd"] == e2["usd"] and e1["khr"] == e2["khr"]:
                n1 = (e1.get("note", "") or "").strip()
                n2 = (e2.get("note", "") or "").strip()
                # Extract Trx. ID from each note (if present)
                trx1 = re.search(r"Trx:\s*(\d+)", n1)
                trx2 = re.search(r"Trx:\s*(\d+)", n2)
                # If both notes contain a Trx. ID, only flag as duplicate if they are the SAME ID
                if trx1 and trx2:
                    if trx1.group(1) != trx2.group(1):
                        continue   # different transactions, skip
                # Otherwise, use the old logic (first word match)
                if n1.split()[0].lower() == n2.split()[0].lower() if n1 and n2 else (not n1 and not n2):
                    dups.append((total - i, total - j, e1, e2))
    if not dups:
        await update.message.reply_text("មិនមានធាតុស្ទួនទេ។")
        return
    lines = ["🔍 ធាតុស្ទួនដែលអាចមាន៖"]
    for idx1, idx2, e1, e2 in dups[:10]:
        lines.append(f"#{idx1} & #{idx2}: {e1['date']} ${e1['usd']} / {e1['khr']}៛ ({e1.get('note','')[:30]} / {e2.get('note','')[:30]})")
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
    if context.args:
        business_filter = context.args[0].lower()
        entries_today = [e for e in entries_today if e.get("business") == business_filter]
        if not entries_today:
            await update.message.reply_text(f"គ្មានប្រតិបត្តិការសម្រាប់ {business_filter} ថ្ងៃនេះទេ។")
            return
    grouped = defaultdict(list)
    for e in entries_today:
        grouped[e.get("business", "unknown")].append(e)
    for biz in sorted(grouped.keys()):
        entries = grouped[biz]
        total_usd = sum(e["usd"] for e in entries)
        total_khr = sum(e["khr"] for e in entries)
        msg = (
            f"📊 សេចក្តីសង្ខេបថ្ងៃនេះ ({today.strftime('%Y-%m-%d')})\n"
            f"អាជីវកម្ម: {biz}\n"
            f"ប្រតិបត្តិការ: {len(entries)}\n"
            f"សរុប: ${total_usd:.2f} / ៛{total_khr:,}"
        )
        for gid in ANNOUNCE_GROUP_IDS:
            try:
                await context.bot.send_message(chat_id=gid, text=msg)
            except Exception as e:
                await update.message.reply_text(f"បរាជ័យសម្រាប់ក្រុម {gid}៖ {e}")
    await update.message.reply_text("✅ បានផ្ញើសេចក្តីសង្ខេបតាមអាជីវកម្មទៅក្រុមទាំងអស់។")

async def edit_index(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        idx = int(context.args[0])
    except (ValueError, IndexError):
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
    new_usd, new_khr, _ = extract_amounts(context.args[1])
    if new_usd == 0 and new_khr == 0:
        await update.message.reply_text("ចំនួនមិនត្រឹមត្រូវ។")
        return
    old_usd, old_khr = entry["usd"], entry["khr"]
    entry["usd"] = new_usd
    entry["khr"] = new_khr
    if len(context.args) >= 3:
        entry["note"] = " ".join(context.args[2:])[:MAX_NOTE_LEN]
    update_transaction(entry["tran_id"], entry)
    update_sheet_row_by_tran_id(entry.get("tran_id"), entry)
    await update.message.reply_text(
        f"✅ បានកែប្រែធាតុលេខ {idx}: ពី ${old_usd:.2f} / ៛{old_khr:,} → ${new_usd:.2f} / ៛{new_khr:,} | {entry['note']}"
    )

async def recent_entries(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        n = min(int(context.args[0]), 50) if context.args else 5
    except (ValueError, IndexError):
        n = 5
    data = load_data()
    if not data:
        await update.message.reply_text("គ្មានទិន្នន័យ។")
        return
    recent = data[-n:][::-1]
    lines = ["📋 ប្រតិបត្តិការចុងក្រោយ៖"]
    for i, e in enumerate(recent, start=1):
        lines.append(f"{i}. {e['date']} | ${e['usd']:.2f} / ៛{e['khr']:,} | {e.get('business','?')} | {e.get('note','')[:30]}")
    lines.append("សរុប /delete_index <លេខ> ដើម្បីលុប។")
    await update.message.reply_text("\n".join(lines))

async def delete_index(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        idx = int(context.args[0]) if context.args else 1
    except (ValueError, IndexError):
        await update.message.reply_text("សូមប្រើ `/delete_index <លេខ>` (ឧទាហរណ៍ `/delete_index 2`)")
        return
    data = load_data()
    if not data or idx < 1 or idx > len(data):
        await update.message.reply_text("លេខមិនត្រឹមត្រូវ ឬគ្មានទិន្នន័យ។")
        return
    entry = data[len(data) - idx]
    # Delete from SQLite
    if delete_transaction(entry["tran_id"]):
        # Save to deleted stack for undo
        deleted = load_deleted()
        deleted.append(entry)
        save_deleted(deleted)
        sheet_deleted = delete_sheet_row_by_tran_id(entry.get("tran_id"))
        msg = f"🗑 បានលុបធាតុលេខ {idx}: {entry.get('note','')} | ${entry.get('usd',0):.2f} / ៛{entry.get('khr',0):,}"
        msg += "\n✅ ក៏បានលុបពី Google Sheets ផងដែរ។" if sheet_deleted else "\n⚠️ មិនអាចលុបពី Google Sheets ទេ។"
        await update.message.reply_text(msg)
    else:
        await update.message.reply_text("លុបមិនបានសម្រេច។")

async def force_sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    data = load_data()
    if not data:
        await update.message.reply_text("គ្មានទិន្នន័យដើម្បីធ្វើសមកាលកម្មទេ។")
        return
    sheet = get_sheet()
    if not sheet:
        await update.message.reply_text("Sheet មិនអាចចូលប្រើបានទេ។")
        return
    count = 0
    for entry in data:
        now = datetime.datetime.now(pytz.timezone('Asia/Phnom_Penh')).strftime("%Y-%m-%d %H:%M:%S")
        row = [
            entry.get("date", ""),
            entry.get("usd", 0),
            entry.get("khr", 0),
            CATEGORIES.get(entry.get("category", "other"), "💸 ផ្សេងៗ"),
            entry.get("note", ""),
            entry.get("business", ""),
            now,
            entry.get("tran_id", ""),
            entry.get("payway_trx_id", "")
        ]
        try:
            sheet.append_row(row, value_input_option="USER_ENTERED")
            count += 1
        except Exception as e:
            logger.error("Force sync row error: %s", e)
    await update.message.reply_text(f"✅ បានធ្វើសមកាលកម្ម {count} ធាតុទៅ Google Sheets។")

async def sheet_test(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    try:
        sheet = get_sheet()
        if not sheet:
            await update.message.reply_text("❌ Master sheet not found. Check SHEET_NAME and sharing.")
            return
        now = datetime.datetime.now(pytz.timezone('Asia/Phnom_Penh')).strftime("%Y-%m-%d %H:%M:%S")
        row = ["TEST", 0, 0, "💸 ផ្សេងៗ", "Sheet health check", "manual", now, str(uuid.uuid4()), ""]
        sheet.append_row(row, value_input_option="USER_ENTERED")
        await update.message.reply_text("✅ Master sheet is accessible and test row written.")
    except Exception as e:
        await update.message.reply_text(f"❌ Sheet error: {e}")

async def check_rebuild(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != OWNER_ID:
        return
    sheet = get_sheet()
    if not sheet:
        await update.message.reply_text("❌ Master sheet not found.")
        return
    try:
        rows = sheet.get_all_values()
        await update.message.reply_text(f"📊 Sheet has {len(rows)} rows (including header).")
        if len(rows) > 1:
            first = rows[1] if len(rows) > 1 else ["-"]
            last = rows[-1] if len(rows) > 1 else ["-"]
            await update.message.reply_text(
                f"First data row (truncated): {first[0]}, {first[1]}, {first[2]}, {first[3]}, {first[4][:30]}, {first[5]}, {first[7]}, {first[8] if len(first)>8 else 'N/A'}\n"
                f"Last data row (truncated): {last[0]}, {last[1]}, {last[2]}, {last[3]}, {last[4][:30]}, {last[5]}, {last[7]}, {last[8] if len(last)>8 else 'N/A'}"
            )
        rebuild_from_sheet()
        data = load_data()
        await update.message.reply_text(f"🔄 Rebuild complete. Current database has {len(data)} entries.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

# -------------------------------------------------------------------
# Profile Bot (raw requests)
# -------------------------------------------------------------------
PROFILE_BOT_TOKEN = os.environ.get("PROFILE_BOT_TOKEN", "")
PROFILE_OWNER_ID = int(os.environ.get("PROFILE_OWNER_ID", 0))

if PROFILE_BOT_TOKEN:
    def profile_send_message(chat_id, text, reply_markup=None, parse_mode="Markdown"):
        url = f"https://api.telegram.org/bot{PROFILE_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if parse_mode:
            payload["parse_mode"] = parse_mode
        return requests.post(url, json=payload, timeout=10)

    def profile_answer_callback(callback_id, text=None):
        url = f"https://api.telegram.org/bot{PROFILE_BOT_TOKEN}/answerCallbackQuery"
        payload = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text
        requests.post(url, json=payload, timeout=10)

    PROFILE_BIRD_BOT_LINK = "https://t.me/bird_nest_house_bot"
    PROFILE_FUNNEL_BOT_LINK = "https://t.me/birdnest_funnel_bot"

    def profile_main_keyboard():
        return {
            "inline_keyboard": [
                [{"text": "🛒 បើក Mini App ទិញទំនិញ", "url": PROFILE_BIRD_BOT_LINK}],
                [{"text": "📖 របៀបប្រើ Mini App", "callback_data": "tutorial_start"}],
                [{"text": "📦 សាកសួរបោះដុំ", "callback_data": "wholesale"}],
                [{"text": "💬 និយាយជាមួយបុគ្គលិក", "callback_data": "human"}],
                [{"text": "🟢 ចាប់ផ្តើមបញ្ជាទិញរហ័ស", "url": PROFILE_FUNNEL_BOT_LINK}]
            ]
        }

    def tutorial_nav_keyboard(step, max_step):
        buttons = []
        if step > 1:
            buttons.append({"text": "⬅ ត្រឡប់ក្រោយ", "callback_data": f"tutorial_{step-1}"})
        if step < max_step:
            buttons.append({"text": "បន្ទាប់ ➡", "callback_data": f"tutorial_{step+1}"})
        return {"inline_keyboard": [buttons, [{"text": "❌ បិទមេរៀន", "callback_data": "tutorial_close"}]]}

    TUTORIAL_TEXTS = {
        1: (
            "📖 *មេរៀនទី១៖ ស្វាគមន៍!*\n\n"
            "🍃 *សំបុកត្រចៀកកាំ Bird Nest House* ជាហាងផលិតផលត្រចៀកកាំ!\n"
            "យើងផ្តល់ជូនការបញ្ជាទិញតាម Telegram Mini App ដែលងាយស្រួល និងមានប្រម៉ូសិនពិសេសៗជាច្រើន។\n\n"
            "📌 *ហេតុអ្វីត្រូវប្រើ Mini App?*\n"
            "• បញ្ជាទិញដោយមិនចាំបាច់ទាក់ទងផ្ទាល់\n"
            "• មានពិន្ទុសន្សំ បញ្ចុះតម្លៃ\n"
            "• ការដឹកជញ្ជូនឥតគិតថ្លៃលើសពី ៣០ដុល្លារក្នុងក្រុង\n"
            "• ការទូទាត់ងាយស្រួលតាម KHQR / សាច់ប្រាក់\n\n"
            "ចុច *បន្ទាប់ ➡* ដើម្បីមើលជំហានបន្ត។"
        ),
        2: (
            "📖 *មេរៀនទី២៖ ចាប់ផ្តើម*\n\n"
            "1. ចុចលើប៊ូតុង *ចាប់ផ្តើម* នៅក្នុង @bird_nest_house_bot\n"
            "2. រួចចុច *🍽️ Open Order Menu* ដើម្បីបើក Mini App\n\n"
            "👉 អ្នកក៏អាចចុច *🛒 បើក Mini App* ខាងក្រោមដោយផ្ទាល់។"
        ),
        3: (
            "📖 *មេរៀនទី៣៖ ជ្រើសរើសផលិតផល*\n\n"
            "នៅក្នុង Mini App អ្នកនឹងឃើញ៖\n"
            "• 🥤 ទឹកត្រចៀកកាំ (75ml, 100ml, 150ml...)\n"
            "• 🥚 សំបុកត្រចៀកកាំស្ងួត (ថ្នាក់ A, Konkat)\n"
            "• 🎁 ឈុតអំណោយ\n\n"
            "អ្នកអាចស្វែងរក ឬជ្រើសរើសតាមប្រភេទ។\n"
            "ផលិតផលដែលមានប្រម៉ូសិននឹងបង្ហាញផ្លាក *\"-10% OFF\"* ជាដើម។"
        ),
        4: (
            "📖 *មេរៀនទី៤៖ ដាក់ក្នុងកន្ត្រក និងប្រើពិន្ទុ*\n\n"
            "• ចុច '+' ដើម្បីបង្កើនចំនួន\n"
            "• ចុច 'Add to Cart' ដើម្បីបញ្ចូល\n"
            "• នៅផ្ទាំង 🛒 Cart អ្នកអាចឃើញសរុបថ្លៃ ការបញ្ចុះតម្លៃ និងថ្លៃដឹក។\n"
            "• បើមានពិន្ទុគ្រប់ អ្នកអាចធីកប្រើពិន្ទុដើម្បីបញ្ចុះតម្លៃ (100pts = $1)។"
        ),
        5: (
            "📖 *មេរៀនទី៥៖ បញ្ជាទិញ និងទូទាត់*\n\n"
            "• ជ្រើសរើសពេលវេលាដឹកជញ្ជូន\n"
            "• ចុច 'Proceed to Payment'\n"
            "• ជ្រើសរើសវិធីទូទាត់៖ KHQR / សាច់ប្រាក់ / កាត\n"
            "• បើជ្រើស KHQR សូមស្កេន QR និងផ្ទេរប្រាក់\n"
            "• ចុច 'I've Paid — Confirm Order'\n\n"
            "✅ ការបញ្ជាទិញរបស់អ្នកត្រូវបានបញ្ជូនទៅអ្នកលក់ហើយ!\n"
            "សូមចាំថា អ្នកអាចជជែកជាមួយអ្នកលក់បានគ្រប់ពេល។"
        )
    }
    MAX_TUTORIAL = 5

    profile_user_state = {}
    profile_tutorial_step = {}
    PROFILE_CURRENT_MODE = "business"

    def profile_process_message(chat_id, text, user_first_name, user_username):
        if chat_id == PROFILE_OWNER_ID:
            return
        if chat_id in profile_user_state:
            profile_handle_wholesale_step(chat_id, text, user_first_name, user_username)
            return
        if PROFILE_CURRENT_MODE == "business":
            profile_send_message(
                chat_id,
                f"សួស្ដី {user_first_name}! 👋\n\n"
                "ខ្ញុំជាជំនួយការរបស់លោកភារុន *ផ្ទះត្រចៀកកាំ Bird Nest House*។\n"
                "ខាងក្រោមនេះជាជម្រើសដែលអ្នកអាចជ្រើសរើស៖",
                reply_markup=profile_main_keyboard(),
                parse_mode="Markdown"
            )
        else:
            profile_send_message(
                chat_id,
                "👋 សួស្តី! ខ្ញុំឃើញសាររបស់អ្នក – ខ្ញុំនឹងឆ្លើយតបវិញភ្លាមៗពេលទំនេរ។\n"
                "បើបន្ទាន់ សូមផ្ញើ 🔥 មក។"
            )

    def profile_handle_wholesale_step(chat_id, text, first_name, username):
        state = profile_user_state[chat_id]
        step = state["step"]
        if step == "ask_name":
            state["name"] = text.strip()
            state["step"] = "ask_company"
            profile_send_message(chat_id, "អរគុណ! តើឈ្មោះក្រុមហ៊ុនរបស់អ្នកជាអ្វី?")
        elif step == "ask_company":
            state["company"] = text.strip()
            state["step"] = "ask_quantity"
            profile_send_message(chat_id, "តើអ្នកត្រូវការក្នុងបរិមាណប៉ុន្មានក្នុងមួយខែ?")
        elif step == "ask_quantity":
            state["quantity"] = text.strip()
            summary = (
                f"📦 *អតិថិជនបោះដុំថ្មី*\n"
                f"ឈ្មោះ: {state.get('name')}\n"
                f"ក្រុមហ៊ុន: {state.get('company')}\n"
                f"បរិមាណប៉ាន់ស្មាន: {state.get('quantity')}\n"
                f"លេខសម្គាល់: `{chat_id}`"
                f"{(' @' + username) if username else ''}"
            )
            profile_send_message(PROFILE_OWNER_ID, summary, parse_mode="Markdown")
            profile_send_message(chat_id, "✅ អរគុណ! សំណើរបស់អ្នកត្រូវបានបញ្ជូនទៅម្ចាស់ហាង។ ពួកគេនឹងទាក់ទងអ្នកឆាប់ៗនេះ។")
            del profile_user_state[chat_id]

    def profile_handle_callback(callback):
        data = callback["data"]
        cb_id = callback["id"]
        msg = callback.get("message", {})
        chat_id = msg.get("chat", {}).get("id")
        user_info = callback.get("from", {})
        first_name = user_info.get("first_name", "អ្នក")

        if data.startswith("tutorial_"):
            if data == "tutorial_start":
                profile_tutorial_step[chat_id] = 1
                step = 1
            elif data == "tutorial_close":
                profile_tutorial_step.pop(chat_id, None)
                profile_send_message(chat_id, "មេរៀនត្រូវបានបិទ។ អ្នកអាចចុចប៊ូតុងណាមួយខាងក្រោម។", reply_markup=profile_main_keyboard())
                profile_answer_callback(cb_id)
                return
            else:
                step = int(data.split("_")[1])
                profile_tutorial_step[chat_id] = step
            text = TUTORIAL_TEXTS.get(step, "មិនមានមេរៀននេះទេ")
            profile_send_message(chat_id, text, reply_markup=tutorial_nav_keyboard(step, MAX_TUTORIAL), parse_mode="Markdown")
            profile_answer_callback(cb_id)
            return

        if data == "wholesale":
            profile_user_state[chat_id] = {"step": "ask_name"}
            profile_send_message(chat_id, "📋 តោះបង្កើតគណនីបោះដុំ។ សូមប្រាប់ឈ្មោះពេញរបស់អ្នក។")
            profile_answer_callback(cb_id)
            return

        if data == "human":
            username = user_info.get("username", "")
            profile_send_message(
                PROFILE_OWNER_ID,
                f"📩 *សំណើទាក់ទងផ្ទាល់ពី* {first_name}"
                f"{(' @' + username) if username else ''}\n"
                f"លេខសម្គាល់: `{chat_id}`",
                parse_mode="Markdown"
            )
            profile_send_message(chat_id, "អរគុណ! ខ្ញុំបានជូនដំណឹងទៅម្ចាស់ហាង ហើយពួកគេនឹងទាក់ទងអ្នកផ្ទាល់ក្នុងពេលឆាប់ៗ។")
            profile_answer_callback(cb_id)
            return

        profile_answer_callback(cb_id)

    def profile_handle_command(chat_id, text):
        if chat_id != PROFILE_OWNER_ID:
            return
        parts = text.split()
        cmd = parts[0].lower()
        global PROFILE_CURRENT_MODE
        if cmd == "/mode" and len(parts) > 1:
            if parts[1] in ("business", "daily"):
                PROFILE_CURRENT_MODE = parts[1]
                profile_send_message(chat_id, f"✅ បានប្តូររបៀបទៅ *{PROFILE_CURRENT_MODE}*", parse_mode="Markdown")
            else:
                profile_send_message(chat_id, "សូមប្រើ /mode business ឬ /mode daily")
        elif cmd == "/myid":
            profile_send_message(chat_id, f"Your chat ID: `{chat_id}`", parse_mode="Markdown")

# -------------------------------------------------------------------
# Funnel Bot (raw requests)
# -------------------------------------------------------------------
FUNNEL_BOT_TOKEN = os.environ.get("FUNNEL_BOT_TOKEN", "")
FUNNEL_OWNER_ID = int(os.environ.get("FUNNEL_OWNER_ID", 0))
FUNNEL_GSPREAD_JSON = os.environ.get("FUNNEL_GSPREAD_CREDENTIALS_JSON", "")
FUNNEL_SHEET_NAME = os.environ.get("FUNNEL_SHEET_NAME", "Bird Nest Leads")

if FUNNEL_BOT_TOKEN:
    def funnel_send_message(chat_id, text, reply_markup=None, parse_mode="Markdown"):
        url = f"https://api.telegram.org/bot{FUNNEL_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if parse_mode:
            payload["parse_mode"] = parse_mode
        return requests.post(url, json=payload, timeout=10).json()

    def funnel_answer_callback(callback_id, text=None):
        url = f"https://api.telegram.org/bot{FUNNEL_BOT_TOKEN}/answerCallbackQuery"
        payload = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text
        requests.post(url, json=payload, timeout=10)

    def funnel_get_sheet():
        if not FUNNEL_GSPREAD_JSON:
            return None
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = json.loads(FUNNEL_GSPREAD_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open(FUNNEL_SHEET_NAME).sheet1

    def funnel_append_lead(first_name, product, purpose, budget, location, lead_score, hot, chat_id):
        try:
            sheet = funnel_get_sheet()
            if not sheet:
                return
            sheet.append_row([
                first_name, product, purpose, budget, location,
                lead_score, "HOT" if hot else "Cold", str(chat_id),
                datetime.datetime.now(pytz.timezone('Asia/Phnom_Penh')).strftime("%Y-%m-%d %H:%M:%S")
            ])
        except Exception as e:
            logger.error("Funnel sheet error: %s", e)

    def funnel_get_all_chat_ids():
        sheet = funnel_get_sheet()
        if not sheet:
            return []
        records = sheet.get_all_values()
        ids = set()
        for row in records[1:]:
            if len(row) > 7 and row[7].isdigit():
                ids.add(int(row[7]))
        return list(ids)

    def funnel_product_keyboard():
        return {"inline_keyboard": [
            [{"text": "🥤 ទឹកត្រចៀកកាំ (Drink)", "callback_data": "product_drink"}],
            [{"text": "🥚 ត្រចៀកកាំស្ងួត (Dry Nest)", "callback_data": "product_dry_nest"}],
            [{"text": "🎁 ឈុតអំណោយ (Gift Set)", "callback_data": "product_gift"}]
        ]}

    def funnel_purpose_keyboard():
        return {"inline_keyboard": [
            [{"text": "ផ្ទាល់ខ្លួន", "callback_data": "purpose_personal"}],
            [{"text": "សម្រាប់លក់បន្ត", "callback_data": "purpose_resale"}],
            [{"text": "ជាអំណោយ", "callback_data": "purpose_gift_purpose"}]
        ]}

    def funnel_budget_keyboard():
        return {"inline_keyboard": [
            [{"text": "ក្រោម $50", "callback_data": "budget_low"}],
            [{"text": "$50 - $200", "callback_data": "budget_medium"}],
            [{"text": "លើស $200", "callback_data": "budget_high"}]
        ]}

    def funnel_location_keyboard():
        return {"inline_keyboard": [
            [{"text": "ភ្នំពេញ", "callback_data": "location_pp"}],
            [{"text": "ខេត្ត", "callback_data": "location_provinces"}],
            [{"text": "ផ្សេងៗ", "callback_data": "location_other"}]
        ]}

    funnel_user_state = {}
    FUNNEL_SCORE_MAP = {
        "product": {"drink": 5, "dry_nest": 10, "gift": 8},
        "purpose": {"personal": 5, "resale": 20, "gift_purpose": 10},
        "budget": {"low": 5, "medium": 15, "high": 30},
        "location": {"pp": 10, "provinces": 5, "other": 5}
    }

    def funnel_start_funnel(chat_id, first_name):
        funnel_user_state[chat_id] = {"step": "ask_product", "score": 0, "answers": {}}
        funnel_send_message(
            chat_id,
            f"👋 សួស្ដី {first_name}! អរគុណដែលចាប់អារម្មណ៍ផលិតផលរបស់យើង។\n\n"
            "ដើម្បីជួយអ្នកបានល្អបំផុត សូមឆ្លើយសំណួរខ្លីៗមួយចំនួន។",
            reply_markup=funnel_product_keyboard()
        )

    def funnel_handle_callback(callback):
        data = callback["data"]
        cb_id = callback["id"]
        msg = callback.get("message", {})
        chat_id = msg.get("chat", {}).get("id")
        user_info = callback.get("from", {})
        first_name = user_info.get("first_name", "អ្នក")

        if chat_id not in funnel_user_state:
            funnel_start_funnel(chat_id, first_name)
            funnel_answer_callback(cb_id)
            return

        state = funnel_user_state[chat_id]
        step = state["step"]

        if step == "ask_product" and data.startswith("product_"):
            product = data.split("_")[1]
            state["answers"]["product"] = product
            state["score"] += FUNNEL_SCORE_MAP["product"].get(product, 0)
            state["step"] = "ask_purpose"
            funnel_send_message(chat_id, "តើអ្នកទិញសម្រាប់គោលបំណងអ្វី?", reply_markup=funnel_purpose_keyboard())
            funnel_answer_callback(cb_id)
            return

        if step == "ask_purpose" and data.startswith("purpose_"):
            purpose = data.split("_")[1] if data != "purpose_gift_purpose" else "gift_purpose"
            state["answers"]["purpose"] = purpose
            state["score"] += FUNNEL_SCORE_MAP["purpose"].get(purpose, 0)
            state["step"] = "ask_budget"
            funnel_send_message(chat_id, "តើថវិការបស់អ្នកប្រហែលប៉ុន្មាន?", reply_markup=funnel_budget_keyboard())
            funnel_answer_callback(cb_id)
            return

        if step == "ask_budget" and data.startswith("budget_"):
            budget = data.split("_")[1]
            state["answers"]["budget"] = budget
            state["score"] += FUNNEL_SCORE_MAP["budget"].get(budget, 0)
            state["step"] = "ask_location"
            funnel_send_message(chat_id, "តើអ្នកស្ថិតនៅទីតាំងណា?", reply_markup=funnel_location_keyboard())
            funnel_answer_callback(cb_id)
            return

        if step == "ask_location" and data.startswith("location_"):
            location = data.split("_")[1]
            state["answers"]["location"] = location
            state["score"] += FUNNEL_SCORE_MAP["location"].get(location, 0)

            lead_score = state["score"]
            hot = lead_score >= 40

            summary = (
                f"📊 *អតិថិជនថ្មី (Lead)*\n"
                f"ឈ្មោះ: {first_name}\n"
                f"ផលិតផល: {state['answers'].get('product')}\n"
                f"គោលបំណង: {state['answers'].get('purpose')}\n"
                f"ថវិកា: {state['answers'].get('budget')}\n"
                f"ទីតាំង: {state['answers'].get('location')}\n"
                f"ពិន្ទុ: {lead_score} {'🔥 HOT' if hot else '🧊 Cold'}\n"
                f"User ID: `{chat_id}`"
            )
            funnel_send_message(FUNNEL_OWNER_ID, summary, parse_mode="Markdown")

            funnel_append_lead(
                first_name,
                state['answers'].get('product'),
                state['answers'].get('purpose'),
                state['answers'].get('budget'),
                state['answers'].get('location'),
                lead_score,
                hot,
                chat_id
            )

            if hot:
                final_text = "🔥 អរគុណ! អ្នកជាអតិថិជនដែលមានសក្តានុពលខ្ពស់។\nម្ចាស់ហាងនឹងទាក់ទងអ្នកឆាប់ៗនេះ។"
            else:
                final_text = "អរគុណ! យើងបានកត់ត្រាចំណាប់អារម្មណ៍របស់អ្នក។\nសូមចូលមើលហាងយើងសម្រាប់ព័ត៌មានបន្ថែម។"
            funnel_send_message(
                chat_id,
                final_text,
                reply_markup={"inline_keyboard": [[{"text": "🛒 បើក Mini App", "url": PROFILE_BIRD_BOT_LINK}]]}
            )
            del funnel_user_state[chat_id]
            funnel_answer_callback(cb_id)
            return

        funnel_answer_callback(cb_id)

    def funnel_handle_owner_command(chat_id, text):
        if chat_id != FUNNEL_OWNER_ID:
            return False
        parts = text.split(" ", 1)
        cmd = parts[0].lower()
        if cmd == "/broadcast" and len(parts) > 1:
            message_text = parts[1]
            user_ids = funnel_get_all_chat_ids()
            if not user_ids:
                funnel_send_message(chat_id, "No leads found in the sheet.")
                return True
            funnel_send_message(chat_id, f"Starting broadcast to {len(user_ids)} users...")
            success, failed = 0, 0
            for uid in user_ids:
                try:
                    res = funnel_send_message(uid, message_text)
                    if res.get("ok"):
                        success += 1
                    else:
                        failed += 1
                except:
                    failed += 1
                time.sleep(0.05)
            funnel_send_message(chat_id, f"Broadcast finished: {success} sent, {failed} failed.")
            return True
        if cmd == "/list":
            user_ids = funnel_get_all_chat_ids()
            funnel_send_message(chat_id, f"Total leads: {len(user_ids)}")
            return True
        if cmd in ("/help", "/start"):
            funnel_send_message(chat_id,
                "Commands:\n"
                "/broadcast <message> – send promo to all leads\n"
                "/list – show lead count\n"
                "/help – this message"
            )
            return True
        return False

# -------------------------------------------------------------------
# Flask App (all routes)
# -------------------------------------------------------------------
flask_app = Flask(__name__)

@flask_app.post("/webhook/autosum")
def autosum_webhook():
    if request.content_length and request.content_length > MAX_WEBHOOK_BODY:
        return "Payload too large", 413
    payload = request.get_json(force=True, silent=True)
    if payload is None:
        return "Bad Request", 400
    update = Update.de_json(payload, application.bot)
    LOOP.run_until_complete(application.process_update(update))
    return "OK"

@flask_app.post("/webhook/profile")
def profile_webhook():
    if not PROFILE_BOT_TOKEN:
        return "Profile bot not configured", 503
    data = request.get_json()
    if "callback_query" in data:
        profile_handle_callback(data["callback_query"])
        return "ok", 200
    msg = data.get("message", {})
    if msg:
        chat_id = msg.get("chat", {}).get("id")
        text = msg.get("text", "").strip()
        if text.startswith("/"):
            profile_handle_command(chat_id, text)
        else:
            user = msg.get("from", {})
            profile_process_message(chat_id, text,
                                    user.get("first_name", "អ្នក"),
                                    user.get("username"))
        return "ok", 200
    bmsg = data.get("business_message", {})
    if bmsg:
        chat_id = bmsg.get("chat", {}).get("id")
        text = bmsg.get("text", "").strip()
        if not text.startswith("/"):
            user = bmsg.get("from", {})
            profile_process_message(chat_id, text,
                                    user.get("first_name", "អ្នក"),
                                    user.get("username"))
        return "ok", 200
    return "ok", 200

@flask_app.post("/webhook/funnel")
def funnel_webhook():
    if not FUNNEL_BOT_TOKEN:
        return "Funnel bot not configured", 503
    data = request.get_json()
    if "callback_query" in data:
        funnel_handle_callback(data["callback_query"])
        return "ok", 200
    msg = data.get("message", {})
    if msg:
        chat_id = msg.get("chat", {}).get("id")
        text = msg.get("text", "").strip()
        if funnel_handle_owner_command(chat_id, text):
            return "ok", 200
        user = msg.get("from", {})
        first_name = user.get("first_name", "អ្នក")
        funnel_start_funnel(chat_id, first_name)
        return "ok", 200
    return "ok", 200

@flask_app.get("/")
def health():
    return "Bot is running ✅"

@flask_app.get("/set_webhook")
def set_webhook():
    base_url = os.environ.get("WEBHOOK_URL", request.host_url.rstrip("/"))
    url = f"{base_url}/webhook/autosum"
    result = LOOP.run_until_complete(application.bot.set_webhook(url))
    return f"AutoSum webhook set to {url} — Telegram replied: {result}"

@flask_app.route("/email_webhook", methods=["POST"])
def email_webhook():
    secret = request.headers.get("X-Webhook-Secret", "")
    if EMAIL_WEBHOOK_SECRET and not hmac.compare_digest(secret, EMAIL_WEBHOOK_SECRET):
        return "Unauthorized", 403
    if request.content_length and request.content_length > MAX_WEBHOOK_BODY:
        return "Payload too large", 413
    payload = request.get_json(force=True, silent=True)
    if not payload:
        return "Bad Request", 400
    usd = float(payload.get("usd", 0))
    khr = float(payload.get("khr", 0))
    note = str(payload.get("note", ""))[:MAX_NOTE_LEN]
    if usd == 0 and khr == 0:
        return "No amount", 400
    entry = {
        "date": datetime.date.today().strftime("%Y-%m-%d"),
        "usd": usd,
        "khr": khr,
        "note": note,
        "category": "other",
        "business": PAYWAY_BUSINESS,
        "tran_id": str(uuid.uuid4()),
        "payway_trx_id": ""
    }
    add_transaction(entry)
    append_to_sheet(entry)
    return "OK", 200

# -------------------------------------------------------------------
# Build Application (AutoSum)
# -------------------------------------------------------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
application = (
    Application.builder()
    .token(BOT_TOKEN)
    .updater(None)
    .build()
)

# Register all AutoSum handlers
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
application.add_handler(CommandHandler("force_sync", force_sync))
application.add_handler(CommandHandler("sheet_test", sheet_test))
application.add_handler(CommandHandler("check_rebuild", check_rebuild))
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

# -------------------------------------------------------------------
# Background workers (AutoSum)
# -------------------------------------------------------------------
def sync_worker(bot_token: str) -> None:
    while True:
        try:
            if PAYWAY_MERCHANT_ID and PAYWAY_API_KEY:
                count = sync_payway_transactions()
                if count > 0 and OWNER_ID:
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    payload = {"chat_id": OWNER_ID, "text": f"🤖 បានទាញយក {count} ប្រតិបត្តិការថ្មីពី PayWay ដោយស្វ័យប្រវត្តិ។"}
                    try:
                        requests.post(url, json=payload, timeout=10)
                    except Exception:
                        pass
        except Exception as e:
            logger.error("Sync worker error: %s", e)
        time.sleep(SYNC_INTERVAL_MINUTES * 60)

def announcement_worker(bot_token: str) -> None:
    last_sent_date = None
    while True:
        try:
            if not ANNOUNCE_GROUP_IDS:
                time.sleep(60)
                continue
            tz = pytz.timezone("Asia/Phnom_Penh")
            now = datetime.datetime.now(tz)
            today_str = now.strftime("%Y-%m-%d")
            if now.strftime("%H:%M") == ANNOUNCE_TIME and last_sent_date != today_str:
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
                        try:
                            requests.post(url, json={"chat_id": gid, "text": msg}, timeout=10)
                        except Exception as e:
                            logger.error("Announce send failed for group %d: %s", gid, e)
                last_sent_date = today_str
        except Exception as e:
            logger.error("Announcement worker error: %s", e)
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
            if now.strftime("%H:%M") == rem_time and last_sent_date != today_str:
                data = load_data()
                if not [e for e in data if e["date"] == today_str]:
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    requests.post(url, json={
                        "chat_id": OWNER_ID,
                        "text": f"⏰ ដាស់តឿន – ថ្ងៃនេះ {today_str} អ្នកមិនទាន់កត់ត្រាចំណូលទេ។ សូមបញ្ចូលចំនួនទឹកប្រាក់។"
                    }, timeout=10)
                    last_sent_date = today_str
        except Exception as e:
            logger.error("Reminder worker error: %s", e)
        time.sleep(60)

def _write_row_with_retry(sheet, row: list, max_attempts: int = 4) -> bool:
    delay = 2.0
    for attempt in range(max_attempts):
        try:
            sheet.append_row(row, value_input_option="USER_ENTERED")
            return True
        except Exception as e:
            if attempt == max_attempts - 1:
                logger.error("Sheet write failed after %d attempts: %s", max_attempts, e)
                return False
            logger.warning("Sheet write attempt %d failed (%s), retrying in %.1fs", attempt + 1, e, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)
    return False

# ---------- Background DB writer (serialises SQLite inserts) ----------
def add_transaction(entry: Dict[str, Any], payway_id: Optional[str] = None) -> bool:
    """Insert a transaction immediately, using a busy timeout to wait for locks.
    Returns True on success, False if a duplicate payway_id already exists."""
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10.0)   # wait up to 10 s if DB is locked
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO transactions (id, date, usd, khr, category, note, business, source, timestamp, payway_trx_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (entry.get("tran_id", str(uuid.uuid4())), entry["date"], entry["usd"], entry["khr"],
                 entry.get("category", "other"), entry.get("note", ""), entry.get("business", "manual"),
                 entry.get("source", ""), datetime.datetime.now().isoformat(), payway_id)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Duplicate PayWay transaction ID – ignore
            return False
        finally:
            conn.close()
    except sqlite3.OperationalError as e:
        logger.error("DB insert error: %s", e)
        return False
    
def sheet_worker() -> None:
    while True:
        try:
            _process_sheet_queue()
        except Exception as e:
            logger.error("Sheet worker crashed, restarting in 5s: %s", e)
            time.sleep(5)

def _process_sheet_queue() -> None:
    while True:
        try:
            item = sheet_queue.get(timeout=5)
        except queue.Empty:
            continue
        if item is None:
            sheet_queue.task_done()
            break
        entry, row = item
        logger.info("📤 Sheet worker processing: %s", entry.get('note','')[:30])   # <-- NEW
        try:
            sheet = get_sheet()
            if sheet:
                _write_row_with_retry(sheet, row)
                logger.info("Sheet export (background): %s", entry.get('note','')[:30])
            biz_sheet = get_all_businesses_sheet()
            if biz_sheet:
                _write_row_with_retry(biz_sheet, row)
        except Exception as e:
            logger.error("Sheet worker item error: %s", e)
        finally:
            sheet_queue.task_done()

def _flush_sheet_queue() -> None:
    logger.info("Flushing remaining sheet queue items (%d)…", sheet_queue.qsize())
    sheet_queue.put(None)
    sheet_queue.join()

import atexit
atexit.register(_flush_sheet_queue)

# -------------------------------------------------------------------
# Startup
# -------------------------------------------------------------------
init_db()
migrate_json_to_sqlite()
rebuild_from_sheet()
seed_seen_trx_ids()

threading.Thread(target=sync_worker, args=(BOT_TOKEN,), daemon=True).start()
threading.Thread(target=announcement_worker, args=(BOT_TOKEN,), daemon=True).start()
threading.Thread(target=sheet_worker, daemon=True).start()
threading.Thread(target=reminder_worker, args=(BOT_TOKEN,), daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port)