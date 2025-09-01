# bot.py
import os
import asyncio
import json
import time
import logging
import sqlite3
import re
import ssl
import hashlib
from dataclasses import dataclass
from typing import Optional, List, Tuple
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.filters import Command, Text
from aiogram.fsm.storage.redis import RedisStorage, MemoryStorage
from aiogram.fsm.context import FSMContext
from fastapi import FastAPI, Request, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------- Конфиг ----------
class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    SHEET_ID = os.getenv("GOOGLE_SHEET_ID") or os.getenv("SHEET_ID")
    GOOGLE_SERVICE_ACCOUNT_KEY = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
    KOYEB_PUBLIC_DOMAIN = os.getenv("KOYEB_PUBLIC_DOMAIN")
    PUBLIC_URL = os.getenv("PUBLIC_URL")
    PORT = int(os.getenv("PORT", 8000))

    @property
    def WEBHOOK_URL(self):
        if self.PUBLIC_URL:
            return f"{self.PUBLIC_URL}/webhook"
        if self.KOYEB_PUBLIC_DOMAIN:
            return f"https://{self.KOYEB_PUBLIC_DOMAIN}/webhook"
        return None

config = Config()

# ---------- Логирование ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Глобальные объекты ---
bot: Optional[Bot] = None
dp = Dispatcher()
app = FastAPI()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]
CREDS = None
SHEETS_SERVICE = None
DRIVE_SERVICE = None

# --- Readiness / liveness flags ---
is_started = False
is_ready = False
first_ready_deadline = None

# --- Данные/кэш ---
main_buttons: List[str] = []
submenus = {}
texts = {}
main_menu = {}
last_modified_time = None

# ---------- Утилиты ----------
def _mask(s: Optional[str]) -> str:
    if not s:
        return "(empty)"
    return s[:3] + "*" * max(0, len(s) - 6) + s[-3:]

def init_db():
    # Создаёт набор sqlite баз/таблиц, используемых в боте
    conn = sqlite3.connect("bot.db")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            first_seen INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            user_id INTEGER PRIMARY KEY,
            expiry INTEGER
        )
    """)
    conn.commit()
    conn.close()
    logging.info("SQLite инициализирован")

def save_user(user_id: int, username: Optional[str]):
    conn = sqlite3.connect("bot.db")
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO users (id, username, first_seen) VALUES (?, ?, ?)", (user_id, username, int(time.time())))
    conn.commit()
    conn.close()

def has_active_session(user_id: int) -> bool:
    conn = sqlite3.connect("bot.db")
    cursor = conn.execute("SELECT expiry FROM sessions WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    conn.close()
    return bool(row and row[0] > time.time())

async def grant_access(user_id: int):
    expiry = time.time() + 1800  # 30 минут
    conn = sqlite3.connect("bot.db")
    conn.execute("INSERT OR REPLACE INTO sessions (user_id, expiry) VALUES (?, ?)", (user_id, expiry))
    conn.commit()
    conn.close()
    logging.info(f"Гостевой доступ {user_id} до {time.ctime(expiry)}")

def reset_all_sessions():
    conn = sqlite3.connect("bot.db")
    conn.execute("DELETE FROM sessions")
    conn.commit()
    conn.close()

# ---------- Проверка окружения ----------
def validate_env_vars():
    missing = []
    if not config.BOT_TOKEN:
        missing.append("BOT_TOKEN (или TELEGRAM_BOT_TOKEN) — строка вида 123456789:ABC...")
    if not config.GOOGLE_SERVICE_ACCOUNT_KEY:
        missing.append("GOOGLE_SERVICE_ACCOUNT_KEY (весь JSON сервис-аккаунта одной строкой)")
    if not config.SHEET_ID:
        missing.append("GOOGLE_SHEET_ID (или SHEET_ID)")
    if missing:
        raise EnvironmentError("Отсутствуют/некорректны переменные окружения: " + ", ".join(missing))

# ---------- Разбор ссылок / медиа ----------
URL_RE = re.compile(r'https?://(?:[a-zA-Z0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

# ---------- Загрузка гайдов из Google Sheets ----------
async def load_guides(force=False):
    """
    Подтягивает кнопки/тексты из Google Sheets.
    Транзиентные SSL-гличи ретраим.
    """
    global main_buttons, submenus, texts, main_menu, last_modified_time, SHEETS_SERVICE, DRIVE_SERVICE
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            if not SHEETS_SERVICE or not DRIVE_SERVICE or not config.SHEET_ID:
                logging.warning("Google services not available for load_guides")
                return
            file = DRIVE_SERVICE.files().get(fileId=config.SHEET_ID, fields="modifiedTime").execute()
            modified_time = file.get("modifiedTime")
            if not force and last_modified_time and modified_time == last_modified_time:
                logging.debug("No change in sheet modified time")
                return
            last_modified_time = modified_time
            sheet = SHEETS_SERVICE.spreadsheets()
            result = sheet.values().get(spreadsheetId=config.SHEET_ID, range="Main!A:D").execute()
            values = result.get("values", [])
            # Парсинг простой: первая строка — заголовки
            new_main = {}
            new_texts = {}
            new_buttons = []
            new_sub = {}
            for row in values[1:]:
                # ожидаем: main_button, submenu_key, sub_button, text
                if len(row) < 1:
                    continue
                main_btn = row[0].strip()
                submenu_key = row[1].strip() if len(row) > 1 else ""
                sub_btn = row[2].strip() if len(row) > 2 else ""
                text = row[3].strip() if len(row) > 3 else ""
                if main_btn and main_btn not in new_main:
                    new_main[main_btn] = []
                    new_buttons.append(main_btn)
                if submenu_key and sub_btn:
                    new_sub.setdefault(main_btn, []).append(sub_btn)
                    new_texts[sub_btn] = text
                elif text:
                    new_texts[main_btn] = text
            main_menu = new_main
            main_buttons = new_buttons
            submenus = new_sub
            texts = new_texts
            logging.info(f"Guides loaded: {len(main_buttons)} main buttons, {sum(len(v) for v in submenus.values()) if submenus else 0} sub buttons")
            return
        except Exception as e:
            msg = str(e)
            if isinstance(e, ssl.SSLError) or "EOF occurred" in msg or "SSLError" in type(e).__name__ or "Broken pipe" in msg:
                logging.warning(f"Transient network error on load_guides (attempt {attempt}/{max_retries}): {e}")
                await asyncio.sleep(1 + attempt)
                continue
            logging.error(f"Failed to load guides: {e}", exc_info=True)
            raise

# ---------- Хэндлеры ----------
async def cmd_start(message: types.Message):
    save_user(message.from_user.id, getattr(message.from_user, "username", None))
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    for b in main_buttons:
        kb.add(KeyboardButton(text=b))
    await message.answer("Привет! Выбери опцию:", reply_markup=kb)

async def text_handler(message: types.Message):
    txt = message.text.strip()
    if txt in main_buttons:
        # build submenu
        items = submenus.get(txt, [])
        if items:
            kb = InlineKeyboardMarkup(row_width=1)
            for it in items:
                kb.add(InlineKeyboardButton(text=it, callback_data=f"sub|{it}"))
            await message.answer("Выберите подраздел:", reply_markup=kb)
        else:
            guide_text = texts.get(txt, "Информация отсутствует")
            await message.answer(guide_text)
    else:
        await message.answer("Не понял команду. Используйте меню.")

async def callback_query_handler(callback: types.CallbackQuery):
    data = callback.data or ""
    if data.startswith("sub|"):
        key = data.split("|", 1)[1]
        guide_text = texts.get(key, "Информация отсутствует")
        await callback.message.answer(guide_text)
        await callback.answer()

# ---------- Утилиты вебхука ----------
async def ensure_webhook(bot: Bot, url: str, max_attempts: int = 5):
    if not url:
        logging.info("WEBHOOK_URL is empty; skipping set_webhook. You can set it manually to <domain>/webhook")
        return
    try:
        info = await bot.get_webhook_info()
        current = getattr(info, "url", "") or ""
    except Exception as e:
        logging.warning(f"get_webhook_info failed: {e}")
        current = ""
    if current == url:
        logging.info("Webhook already set; skipping set_webhook")
        return
    attempt = 0
    delay = 1.0
    while attempt < max_attempts:
        attempt += 1
        try:
            await bot.set_webhook(url)
            logging.info("Webhook set successfully")
            return
        except Exception as e:
            logging.warning(f"set_webhook attempt {attempt} failed: {e}")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)
    raise RuntimeError("Failed to set webhook after retries")

# ---------- Старт ----------
@app.on_event("startup")
async def on_startup():
    global bot, CREDS, SHEETS_SERVICE, DRIVE_SERVICE, last_modified_time, scheduler
    # readiness flags
    global is_started, is_ready, first_ready_deadline
    is_started = True
    first_ready_deadline = time.time() + 120  # grace period for external services (seconds)

    logging.info(f"BOT_TOKEN: {_mask(config.BOT_TOKEN)} (len={len(config.BOT_TOKEN) if config.BOT_TOKEN else 0})")
    logging.info(f"GOOGLE_SHEET_ID: {bool(config.SHEET_ID)}")
    logging.info(f"GOOGLE_SERVICE_ACCOUNT_KEY set: {bool(config.GOOGLE_SERVICE_ACCOUNT_KEY)}")
    logging.info(f"KOYEB_PUBLIC_DOMAIN: {os.getenv('KOYEB_PUBLIC_DOMAIN')}")
    logging.info(f"PUBLIC_URL: {os.getenv('PUBLIC_URL')}")
    logging.info(f"PORT: {config.PORT}")

    validate_env_vars()
    bot = Bot(token=config.BOT_TOKEN)

    try:
        creds_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_KEY)
    except json.JSONDecodeError as e:
        raise EnvironmentError(f"GOOGLE_SERVICE_ACCOUNT_KEY не JSON: {e}")
    CREDS = Credentials.from_service_account_info(creds_info, scopes=SCOPES)

    SHEETS_SERVICE = build("sheets", "v4", credentials=CREDS, cache_discovery=False)
    DRIVE_SERVICE  = build("drive",  "v3", credentials=CREDS, cache_discovery=False)

    init_db()
    last_modified_time = None

    await ensure_webhook(bot, config.WEBHOOK_URL)

    try:
        await load_guides(force=True)
    except Exception as e:
        logging.error(f"Failed to load guides on startup: {e}")

    if not main_menu:
        logging.warning("Guides not loaded on startup. Service may be limited.")

    # mark as ready if guides loaded
    if main_menu and not globals().get('is_ready'):
        is_ready = True

    # APScheduler for periodic tasks
    scheduler = AsyncIOScheduler()
    
    async def single_keep_alive():
        try:
            await bot.get_me()
        except Exception as e:
            logging.error(f"Keep-alive failed: {e}")

    async def single_periodic_reload():
        global last_modified_time, SHEETS_SERVICE, DRIVE_SERVICE
        try:
            if DRIVE_SERVICE and config.SHEET_ID:
                file_metadata = DRIVE_SERVICE.files().get(fileId=config.SHEET_ID, fields="modifiedTime").execute()
                current_modified_time = file_metadata.get("modifiedTime")
                if current_modified_time and (last_modified_time is None or last_modified_time != current_modified_time):
                    logging.info(f"Change detected at {current_modified_time}. Reloading guides...")
                    await load_guides(force=True)
                    # if guides loaded during periodic reload -> mark ready
                    try:
                        if main_menu and not globals().get('is_ready'):
                            is_ready = True
                    except Exception:
                        pass
                last_modified_time = current_modified_time
            else:
                logging.warning("DRIVE_SERVICE or SHEET_ID not available, skipping reload")
                try:
                    creds_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_KEY)
                    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
                    SHEETS_SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
                    DRIVE_SERVICE  = build("drive",  "v3", credentials=creds, cache_discovery=False)
                    logging.info("Reinitialized Google services")
                except Exception as e:
                    logging.error(f"Reinit failed: {e}")
        except Exception as e:
            logging.error(f"Error in single periodic reload: {e}", exc_info=True)
    
    scheduler.add_job(single_keep_alive, IntervalTrigger(minutes=3))
    scheduler.add_job(single_periodic_reload, IntervalTrigger(minutes=3))
    scheduler.start()

@app.on_event("shutdown")
async def on_shutdown():
    global scheduler
    if scheduler:
        await scheduler.shutdown()

# Health / liveness / readiness endpoints
@app.api_route("/live", methods=["GET", "HEAD"])
async def liveness():
    return {"status": "alive"}

@app.api_route("/ready", methods=["GET", "HEAD"])
async def readiness():
    # return 200 only when guides loaded (or after grace period)
    if globals().get("is_ready"):
        return {"status": "ready"}
    if globals().get("first_ready_deadline") and time.time() > globals().get("first_ready_deadline"):
        return {"status": "degraded_ready", "guides": bool(main_menu)}
    raise HTTPException(status_code=503, detail="Not ready")

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    # keep-alive endpoint for platform; always 200 to avoid restarts on slow dependency initialization
    return {"status": "alive"}

@app.get("/debug/env")
async def debug_env():
    return {
        "bot_token_present": bool(config.BOT_TOKEN),
        "bot_token_len": len(config.BOT_TOKEN) if config.BOT_TOKEN else 0,
        "sheet_id_present": bool(config.SHEET_ID),
        "gsak_present": bool(config.GOOGLE_SERVICE_ACCOUNT_KEY),
        "webhook_url": config.WEBHOOK_URL or "(empty)",
    }

# ---------- Обработка webhook ----------
@app.post("/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
        update = types.Update(**data)
        await dp.feed_update(bot, update)
        return {"ok": True}
    except Exception as e:
        logging.error(f"Webhook error: {e}", exc_info=True)
        return {"ok": False}

# ---------- Регистрация хэндлеров ----------
dp.message.register(cmd_start, Command(commands=["start"]))
dp.message.register(text_handler)
dp.callback_query.register(callback_query_handler)

# ---------- Прочие endpoint'ы / debug ----------
@app.get("/debug/guides")
async def debug_guides():
    return {"main_buttons": main_buttons, "submenus_len": len(submenus), "texts_len": len(texts)}

# ---------- Вспомогательные команды (пример) ----------
# Добавьте сюда ваши дополнительные handlers, кнопки, мультимедиа-обработчики и т.д.
# (В оригинальном файле может быть много кастомной логики — оставил основной каркас и встроил флаги readiness.)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("bot:app", host="0.0.0.0", port=config.PORT, workers=1)
