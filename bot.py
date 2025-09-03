# bot.py
import os
import re
import time
import json
import logging
import sqlite3
import asyncio
from random import uniform
from typing import Optional, Dict, List, Set

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
    PORT = int(os.getenv("PORT", 8000))
    RELOAD_MINUTES = int(os.getenv("RELOAD_MINUTES", "10"))
    CODEWORD = os.getenv("CODEWORD", "infobot")  # кодовое слово

    @property
    def WEBHOOK_URL(self) -> Optional[str]:
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

# ---- Auth (в памяти) ----
AUTH_TTL = 24 * 60 * 60  # 24 часа
auth_sessions: Dict[int, float] = {}  # user_id -> expires_at
awaiting_code: Set[int] = set()       # ждём код после /start

def is_authed(user_id: int) -> bool:
    exp = auth_sessions.get(user_id, 0)
    return exp > time.time()

def grant_auth(user_id: int):
    auth_sessions[user_id] = time.time() + AUTH_TTL

# ---------- Хелперы сообщений / зачистка истории ----------
chat_msgs: Dict[int, List[int]] = {}  # chat_id -> [message_ids]

def _remember_msg(chat_id: int, message_id: int):
    arr = chat_msgs.setdefault(chat_id, [])
    if message_id not in arr:
        arr.append(message_id)
        if len(arr) > 200:
            del arr[:-200]

async def purge_chat(chat_id: int):
    ids = chat_msgs.get(chat_id, [])
    if not ids:
        return
    for mid in reversed(ids):
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            pass
    chat_msgs[chat_id] = []

# ---------- Callback data helpers ----------
cb_id_to_key: Dict[str, str] = {}
key_to_cb_id: Dict[str, str] = {}

def _safe_label(s: str, limit: int = 64) -> str:
    try:
        return (s[:limit-1] + "…") if len(s) > limit else s
    except Exception:
        return s

def _cb_for(key: str) -> str:
    cid = key_to_cb_id.get(key)
    if cid:
        return cid
    cid = f"s{len(key_to_cb_id)}"
    key_to_cb_id[key] = cid
    cb_id_to_key[cid] = key
    return cid

def main_menu_kb() -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text=b)] for b in main_buttons] or [[types.KeyboardButton(text="(меню пусто)")]],
        resize_keyboard=True
    )

# ---------- SQLite ----------
def get_sqlite_conn():
    conn = sqlite3.connect("bot.db", timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def init_sqlite():
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS guides_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            cached_at INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    logging.info("SQLite инициализирован")

def cache_guides(payload: dict):
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO guides_cache(payload, cached_at) VALUES (?, ?)", (json.dumps(payload, ensure_ascii=False), int(time.time())))
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
            result = SHEETS_SERVICE.spreadsheets().values().get(
                spreadsheetId=config.GOOGLE_SHEET_ID,
                range=os.getenv("GOOGLE_SHEET_RANGE", "Guides!A:C")
            ).execute()
            values = result.get("values", [])
            nb: List[str] = []
            ns: Dict[str, List[str]] = {}
            nt: Dict[str, str] = {}

            # 1) A пусто, B=Button, C=Text  -> пункт меню с текстом
            # 2) A=Parent, B=Sub, C=Text    -> сабменю родителя A
            for row in values[1:]:
                parent = row[0].strip() if len(row) > 0 and row[0] else ""
                btn    = row[1].strip() if len(row) > 1 and row[1] else ""
                text   = row[2].strip() if len(row) > 2 and row[2] else ""
                if not btn and not parent:
                    continue

                if parent:
                    if parent not in nb:
                        nb.append(parent)
                    if btn:
                        ns.setdefault(parent, []).append(btn)
                        if text:
                            nt[btn] = text
                else:
                    if btn and btn not in nb:
                        nb.append(btn)
                    if btn and text:
                        nt[btn] = text

            # rebuild callback id maps for subbuttons
            cb_id_to_key.clear()
            key_to_cb_id.clear()
            for items in ns.values():
                for it in items:
                    _cb_for(it)

            main_buttons = nb
            submenus = ns
            texts = nt
            payload = {"main_buttons": main_buttons, "submenus": submenus, "texts": texts, "last_modified_time": last_modified_time}
            cache_guides(payload)
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

# ---------- Media utils ----------
IMG_EXTS = (".jpg", ".jpeg", ".png")

def extract_image_urls(text: str) -> List[str]:
    if not text:
        return []
    urls = []
    for token in re.split(r"[\s,;\n]+", text.strip()):
        if token.lower().endswith(IMG_EXTS) and token.startswith(("http://", "https://")):
            urls.append(token)
    # remove duplicates, keep order
    seen = set()
    uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq[:10]  # Telegram альбом до 10 фото

async def send_content_with_menu(chat_id: int, content_text: str):
    """
    Отправляет либо просто текст (с клавиатурой меню),
    либо 1 фото, либо альбом изображений, после чего всегда — сообщение с меню.
    """
    urls = extract_image_urls(content_text)
    if urls:
        if len(urls) == 1:
            m = await bot.send_photo(chat_id, urls[0])
            _remember_msg(chat_id, m.message_id)
        else:
            media = [types.InputMediaPhoto(media=u) for u in urls]
            msgs = await bot.send_media_group(chat_id, media=media)
            for m in msgs:
                _remember_msg(chat_id, m.message_id)
        # Всегда отдельное сообщение, чтобы прикрепить reply-клавиатуру
        m2 = await bot.send_message(chat_id, "Выберите опцию:", reply_markup=main_menu_kb())
        _remember_msg(chat_id, m2.message_id)
    else:
        m = await bot.send_message(chat_id, content_text or "Информация отсутствует", reply_markup=main_menu_kb())
        _remember_msg(chat_id, m.message_id)

# ---------- UI helpers ----------
async def show_main_menu(chat_id: int, text: str = "Выберите опцию:"):
    m = await bot.send_message(chat_id, text, reply_markup=main_menu_kb())
    _remember_msg(chat_id, m.message_id)

# ---------- Handlers ----------
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    _remember_msg(chat_id, message.message_id)

    if not is_authed(user_id):
        awaiting_code.add(user_id)
        m1 = await message.answer("Доступ к боту защищён. Для входа требуется кодовое слово.")
        _remember_msg(chat_id, m1.message_id)
        m2 = await message.answer("Введите кодовое слово:")
        _remember_msg(chat_id, m2.message_id)
        return

    await show_main_menu(chat_id, text="Привет! Выберите опцию:")

async def cmd_reload(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    _remember_msg(chat_id, message.message_id)

    if not is_authed(user_id):
        awaiting_code.add(user_id)
        m1 = await message.answer("Доступ к боту защищён. Для входа требуется кодовое слово.")
        _remember_msg(chat_id, m1.message_id)
        m2 = await message.answer("Введите кодовое слово:")
        _remember_msg(chat_id, m2.message_id)
        return

    await purge_chat(chat_id)
    await load_guides(force=True)
    await show_main_menu(chat_id, text="Данные обновлены. Выберите опцию:")

async def cmd_wake(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    _remember_msg(chat_id, message.message_id)

    if not is_authed(user_id):
        awaiting_code.add(user_id)
        m1 = await message.answer("Доступ к боту защищён. Для входа требуется кодовое слово.")
        _remember_msg(chat_id, m1.message_id)
        m2 = await message.answer("Введите кодовое слово:")
        _remember_msg(chat_id, m2.message_id)
        return

    m = await message.answer("Я на связи ✅", reply_markup=main_menu_kb())
    _remember_msg(chat_id, m.message_id)

async def text_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    incoming = (message.text or "").strip()
    _remember_msg(chat_id, message.message_id)

    # кодовая фаза
    if (user_id in awaiting_code) or (not is_authed(user_id)):
        if incoming.lower() == config.CODEWORD.lower():
            awaiting_code.discard(user_id)
            grant_auth(user_id)
            await purge_chat(chat_id)
            await show_main_menu(chat_id, text="Доступ разрешён на 24 часа. Выберите опцию:")
        else:
            m = await message.answer("Неверный код, попробуйте снова")
            _remember_msg(chat_id, m.message_id)
        return

    # меню/сабменю
    if incoming in main_buttons:
        items = submenus.get(incoming, [])
        if items:
            kb = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text=_safe_label(it), callback_data=f"sub|{_cb_for(it)}")] for it in items
            ])
            m = await message.answer("Выберите раздел:", reply_markup=kb)
            _remember_msg(chat_id, m.message_id)
        else:
            await purge_chat(chat_id)
            await send_content_with_menu(chat_id, texts.get(incoming, "Информация отсутствует"))
    else:
        m = await message.answer("Не понял. Используйте меню.")
        _remember_msg(chat_id, m.message_id)

async def callback_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id

    if not is_authed(user_id):
        awaiting_code.add(user_id)
        m1 = await callback.message.answer("Доступ к боту защищён. Для входа требуется кодовое слово.")
        _remember_msg(chat_id, m1.message_id)
        m2 = await callback.message.answer("Введите кодовое слово:")
        _remember_msg(chat_id, m2.message_id)
        await callback.answer()
        return

    data = (callback.data or "")
    if data.startswith("sub|"):
        cid = data.split("|", 1)[1]
        key = cb_id_to_key.get(cid, "")
        await purge_chat(chat_id)
        await send_content_with_menu(chat_id, texts.get(key, "Информация отсутствует"))
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
    global main_buttons, submenus, texts, last_modified_time

    is_started = True
    first_ready_deadline = time.time() + 120

    init_sqlite()
    validate_env_vars()
    init_google_services()

    bot_init = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    global bot
    bot = bot_init
    dp_local = Dispatcher(storage=MemoryStorage())
    global dp
    dp = dp_local

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_reload, Command("reload"))
    dp.message.register(cmd_wake,   Command("wake"))
    dp.message.register(text_handler, F.text)
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
            main_buttons = cached.get("main_buttons", [])
            submenus = cached.get("submenus", {})
            texts = cached.get("texts", {})
            last_modified_time = cached.get("last_modified_time")
            is_ready = True

    scheduler_local = AsyncIOScheduler()
    global scheduler
    scheduler = scheduler_local

    async def single_keep_alive():
        try:
            await bot.get_me()
            logging.debug("Keep-alive OK")
        except Exception as e:
            logging.error(f"Keep-alive failed: {e}")

    async def single_periodic_reload():
        try:
            await load_guides(force=False)
        except Exception as e:
            logging.error(f"Periodic reload failed: {e}")

    logging.info("Adding job tentatively -- it will be properly scheduled when the scheduler starts")
    scheduler.add_job(single_keep_alive, "interval", minutes=5, id="keep_alive", replace_existing=True)
    logging.info("Adding job tentatively -- it will be properly scheduled when the scheduler starts")
    scheduler.add_job(single_periodic_reload, "interval", minutes=config.RELOAD_MINUTES, id="periodic_reload", replace_existing=True)
    logging.info('Added job "on_startup.<locals>.single_keep_alive" to job store "default"')
    logging.info('Added job "on_startup.<locals>.single_periodic_reload" to job store "default"')
    scheduler.start()
    logging.info("Scheduler started")
    logging.info("Scheduler started and app startup complete")

@app.on_event("shutdown")
async def on_shutdown():
    global scheduler
    try:
        if scheduler:
            scheduler.shutdown(wait=False)
    except Exception:
        pass

# ---------- HTTP ----------
@app.api_route("/webhook", methods=["POST"])
async def webhook(request: Request):
    try:
        data = await request.json()
        update = types.Update(**data)
        await dp.feed_update(bot, update)
        return {"ok": True}
    except Exception as e:
        logging.error(f"Webhook handling error: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}

@app.get("/ready")
async def readiness():
    global is_ready, first_ready_deadline
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
        "sheet_id_present": bool(config.GOOGLE_SHEET_ID),
        "webhook_url": config.WEBHOOK_URL or "(none)",
        "reload_minutes": config.RELOAD_MINUTES,
    }

# ---------- Run ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("bot:app", host="0.0.0.0", port=config.PORT, workers=1)
