# bot.py
import os
import time
import json
import logging
import sqlite3
import asyncio
from random import uniform
from typing import Optional, Dict, List

from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------- Config ----------
class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    GOOGLE_SERVICE_ACCOUNT_KEY = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
    KOYEB_PUBLIC_DOMAIN = os.getenv("KOYEB_PUBLIC_DOMAIN")
    PUBLIC_URL = os.getenv("PUBLIC_URL")
    PORT = int(os.getenv("PORT", 8000))

    @property
    def WEBHOOK_URL(self) -> Optional[str]:
        if self.PUBLIC_URL:
            return f"{self.PUBLIC_URL}/webhook"
        if self.KOYEB_PUBLIC_DOMAIN:
            return f"https://{self.KOYEB_PUBLIC_DOMAIN}/webhook"
        return None

config = Config()

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("apscheduler.executors.default").setLevel(logging.ERROR)

# ---------- Globals ----------
app = FastAPI()

bot: Optional[Bot] = None
dp: Optional[Dispatcher] = None
scheduler: Optional[AsyncIOScheduler] = None

CREDS = None
SHEETS_SERVICE = None
DRIVE_SERVICE = None

main_buttons: List[str] = []
submenus: Dict[str, List[str]] = {}
texts: Dict[str, str] = {}
last_modified_time: Optional[str] = None

is_started = False
is_ready = False
first_ready_deadline: Optional[float] = None

# ---------- Utilities ----------
def _mask(s: Optional[str]) -> str:
    if not s:
        return "(empty)"
    if len(s) <= 6:
        return "*" * len(s)
    return s[:3] + "*" * (len(s) - 6) + s[-3:]

def get_sqlite_conn():
    conn = sqlite3.connect("bot.db", timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

# ---------- SQLite ----------
def init_sqlite():
    conn = get_sqlite_conn()
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS guides_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cached_at INTEGER,
            payload TEXT
        )
    """)
    conn.commit()
    conn.close()
    logging.info("SQLite инициализирован")

def save_guides_to_cache(payload: dict):
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO guides_cache (cached_at, payload) VALUES (?, ?)", (int(time.time()), json.dumps(payload, ensure_ascii=False)))
    conn.commit()
    conn.close()
    logging.info("Guides cached to SQLite")

def load_guides_from_cache() -> Optional[dict]:
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT payload, cached_at FROM guides_cache ORDER BY cached_at DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if row:
        try:
            payload = json.loads(row["payload"])
            logging.info(f"Loaded guides from cache (cached_at={row['cached_at']})")
            return payload
        except Exception as e:
            logging.error(f"Failed to decode guides cache: {e}")
    return None

# ---------- Validate env ----------
def validate_env_vars():
    if not config.BOT_TOKEN:
        raise EnvironmentError("BOT_TOKEN is required")
    if not config.GOOGLE_SERVICE_ACCOUNT_KEY:
        logging.warning("GOOGLE_SERVICE_ACCOUNT_KEY not set — guides unavailable")
    if not config.GOOGLE_SHEET_ID:
        logging.warning("GOOGLE_SHEET_ID not set — guides unavailable")

# ---------- Google ----------
def init_google_services():
    global CREDS, SHEETS_SERVICE, DRIVE_SERVICE
    if not config.GOOGLE_SERVICE_ACCOUNT_KEY:
        logging.warning("Skipping Google init: no key")
        return
    try:
        info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_KEY)
        CREDS = service_account.Credentials.from_service_account_info(
            info,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.metadata.readonly",
            ]
        )
        SHEETS_SERVICE = build("sheets", "v4", credentials=CREDS, cache_discovery=False)
        DRIVE_SERVICE = build("drive", "v3", credentials=CREDS, cache_discovery=False)
        logging.info("Google services initialized")
    except Exception as e:
        logging.error(f"Failed to init Google services: {e}")

# ---------- Load guides ----------
async def load_guides(force: bool = False, retries: int = 6, base_backoff: float = 1.5):
    global main_buttons, submenus, texts, last_modified_time

    if not SHEETS_SERVICE or not DRIVE_SERVICE or not config.GOOGLE_SHEET_ID:
        logging.warning("Google services or SHEET_ID missing; will try cache")
        cached = load_guides_from_cache()
        if cached:
            main_buttons = cached.get("main_buttons", [])
            submenus = cached.get("submenus", {})
            texts = cached.get("texts", {})
            last_modified_time = cached.get("last_modified_time")
            logging.info("Guides loaded from cache (no Google)")
            return
        logging.info("No cache found")
        return

    for attempt in range(1, retries + 1):
        try:
            file_meta = DRIVE_SERVICE.files().get(fileId=config.GOOGLE_SHEET_ID, fields="modifiedTime").execute()
            modified_time = file_meta.get("modifiedTime")
            if not force and last_modified_time and modified_time == last_modified_time:
                logging.debug("Sheet not modified, skipping load")
                return

            last_modified_time = modified_time
            result = SHEETS_SERVICE.spreadsheets().values().get(spreadsheetId=config.GOOGLE_SHEET_ID, range="Main!A:D").execute()
            values = result.get("values", [])
            nb: List[str] = []
            ns: Dict[str, List[str]] = {}
            nt: Dict[str, str] = {}
            for row in values[1:]:
                main = row[0].strip() if len(row) > 0 and row[0] else ""
                sub_btn = row[2].strip() if len(row) > 2 and row[2] else ""
                text = row[3].strip() if len(row) > 3 and row[3] else ""
                if main:
                    if main not in nb:
                        nb.append(main)
                    if sub_btn:
                        ns.setdefault(main, []).append(sub_btn)
                        if text:
                            nt[sub_btn] = text
                    else:
                        if text:
                            nt[main] = text
            main_buttons = nb
            submenus = ns
            texts = nt
            payload = {"main_buttons": main_buttons, "submenus": submenus, "texts": texts, "last_modified_time": last_modified_time}
            try:
                save_guides_to_cache(payload)
            except Exception as e:
                logging.warning(f"Failed to save guides cache: {e}")
            logging.info(f"Guides loaded: {len(main_buttons)} main, {sum(len(v) for v in submenus.values())} sub")
            return
        except HttpError as he:
            logging.error(f"HttpError load_guides {attempt}/{retries}: {he}")
        except Exception as e:
            logging.warning(f"Transient error load_guides {attempt}/{retries}: {e}")

        if attempt < retries:
            await asyncio.sleep(base_backoff * attempt + uniform(0, 1))

    cached = load_guides_from_cache()
    if cached:
        main_buttons = cached.get("main_buttons", [])
        submenus = cached.get("submenus", {})
        texts = cached.get("texts", {})
        last_modified_time = cached.get("last_modified_time")
        logging.warning("Loaded guides from cache after failures")
    else:
        logging.error("Failed to load guides from Google and no cache")

# ---------- Handlers ----------
async def cmd_start(message: types.Message):
    if not main_buttons:
        await message.answer("Меню пока пустое. Попробуйте позже.")
        return

    kb = types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text=b)] for b in main_buttons],
        resize_keyboard=True
    )
    await message.answer("Привет! Выберите опцию:", reply_markup=kb)

async def text_handler(message: types.Message):
    text = (message.text or "").strip()
    if not text:
        await message.answer("Пустое сообщение.")
        return
    if text in main_buttons:
        items = submenus.get(text, [])
        if items:
            kb = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text=it, callback_data=f"sub|{it}")] for it in items
            ])
            await message.answer("Выберите раздел:", reply_markup=kb)
        else:
            await message.answer(texts.get(text, "Информация отсутствует"))
    else:
        await message.answer("Не понял. Используйте меню.")

async def callback_handler(callback: types.CallbackQuery):
    data = (callback.data or "")
    if data.startswith("sub|"):
        key = data.split("|", 1)[1]
        await callback.message.answer(texts.get(key, "Информация отсутствует"))
        await callback.answer()

# ---------- Webhook ----------
async def ensure_webhook(bot_obj: Bot, url: Optional[str], retries: int = 4):
    if not url:
        logging.info("WEBHOOK_URL empty, skipping set_webhook")
        return
    for attempt in range(1, retries + 1):
        try:
            info = await bot_obj.get_webhook_info()
            current = getattr(info, "url", "") or ""
            if current == url:
                logging.info("Webhook already set; skipping")
                return
            await bot_obj.set_webhook(url)
            logging.info("Webhook set successfully")
            return
        except Exception as e:
            logging.warning(f"set_webhook attempt {attempt}/{retries} failed: {e}")
            await asyncio.sleep(1 + attempt)
    logging.error("Failed to set webhook after retries")

# ---------- Startup / Shutdown ----------
@app.on_event("startup")
async def on_startup():
    global bot, dp, scheduler, is_started, is_ready, first_ready_deadline

    is_started = True
    first_ready_deadline = time.time() + 120

    init_sqlite()
    validate_env_vars()
    init_google_services()

    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.register(cmd_start, Command(commands=["start"]))
    dp.message.register(text_handler)
    dp.callback_query.register(callback_handler)

    if config.WEBHOOK_URL:
        await ensure_webhook(bot, config.WEBHOOK_URL)
        logging.info("Running in WEBHOOK mode")

    try:
        await load_guides(force=True)
        if main_buttons:
            is_ready = True
    except Exception as e:
        logging.error(f"load_guides startup failed: {e}")
        cached = load_guides_from_cache()
        if cached:
            global main_buttons, submenus, texts, last_modified_time
            main_buttons = cached.get("main_buttons", [])
            submenus = cached.get("submenus", {})
            texts = cached.get("texts", {})
            last_modified_time = cached.get("last_modified_time")
            is_ready = True

    scheduler = AsyncIOScheduler()

    async def single_keep_alive():
        try:
            await bot.get_me()
            logging.debug("Keep-alive OK")
        except Exception as e:
            logging.error(f"Keep-alive failed: {e}")

    async def single_periodic_reload():
        global is_ready
        try:
            await load_guides()
            if main_buttons:
                is_ready = True
        except Exception as e:
            logging.error(f"Periodic reload error: {e}")

    scheduler.add_job(single_keep_alive, "interval", minutes=3)
    scheduler.add_job(single_periodic_reload, "interval", minutes=3)
    scheduler.start()
    logging.info("Scheduler started and app startup complete")

@app.on_event("shutdown")
async def on_shutdown():
    global scheduler, bot
    if scheduler:
        try:
            scheduler.shutdown(wait=False)
        except Exception as e:
            logging.warning(f"Scheduler shutdown failed: {e}")
    if bot:
        try:
            await bot.session.close()
        except Exception as e:
            logging.warning(f"Bot session close failed: {e}")

# ---------- Health ----------
@app.api_route("/live", methods=["GET", "HEAD"])
async def liveness():
    return {"status": "alive"}

@app.api_route("/ready", methods=["GET", "HEAD"])
async def readiness():
    if is_ready:
        return {"status": "ready"}
    if first_ready_deadline and time.time() > first_ready_deadline:
        return {"status": "degraded_ready", "guides": bool(main_buttons)}
    raise HTTPException(status_code=503, detail="Not ready")

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "alive"}

@app.get("/debug/env")
async def debug_env():
    return {
        "bot_token_present": bool(config.BOT_TOKEN),
        "bot_token_masked": _mask(config.BOT_TOKEN),
        "sheet_id_present": bool(config.GOOGLE_SHEET_ID),
        "gsak_present": bool(config.GOOGLE_SERVICE_ACCOUNT_KEY),
        "webhook_url": config.WEBHOOK_URL or "(empty)",
        "is_ready": is_ready,
    }

@app.get("/debug/guides")
async def debug_guides():
    return {
        "main_buttons_count": len(main_buttons),
        "main_buttons": main_buttons,
        "submenus_count": {k: len(v) for k, v in submenus.items()},
        "texts_count": len(texts),
        "last_modified_time": last_modified_time
    }

@app.get("/debug/cache")
async def debug_cache():
    cached = load_guides_from_cache()
    return {"cached": bool(cached), "cached_sample": (cached or {})}

# ---------- Webhook endpoint ----------
@app.post("/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
        update = types.Update(**data)
        await dp.feed_update(bot, update)
        return {"ok": True}
    except Exception as e:
        logging.error(f"Webhook handling error: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}

# ---------- Run ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("bot:app", host="0.0.0.0", port=config.PORT, workers=1)
