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
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, InputMediaVideo
)
from aiogram.filters import Command
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest
from fastapi import FastAPI, Request, HTTPException
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from cachetools import TTLCache
from collections import defaultdict, OrderedDict
import uvicorn
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

# ---------- .env для локальной отладки ----------
load_dotenv()

# ---------- Логирование ----------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logging.getLogger("aiogram.client.session").setLevel(logging.WARNING)

def _mask(s: Optional[str], keep_tail: int = 6) -> str:
    if not s:
        return "None"
    return "***" + s[-keep_tail:] if len(s) > keep_tail else "***"

# ---------- Токен: извлечение и санитизация ----------
TOKEN_RE = re.compile(r"\d+:[A-Za-z0-9_-]+")
def clean_token(raw: str) -> Optional[str]:
    if raw is None:
        return None
    s = raw.strip().strip('"').strip("'")
    m = TOKEN_RE.search(s)
    return m.group(0) if m else None

# ---------- Конфиг ----------
@dataclass
class Config:
    BOT_TOKEN: str
    GOOGLE_SERVICE_ACCOUNT_KEY: str
    SHEET_ID: str

    WEBHOOK_PATH: str = "/webhook"
    WEBHOOK_URL: str = ""
    RANGE_NAME: str = "Guides!A:C"
    ADMIN_ID: int = 6970816136  # остаётся для информации, но /reload теперь доступна всем
    PORT: int = int(os.getenv("PORT", "8000"))

    def __post_init__(self):
        public_url = os.getenv("PUBLIC_URL")
        if not public_url:
            domain = os.getenv("KOYEB_PUBLIC_DOMAIN") or os.getenv("KOYEB_EXTERNAL_HOSTNAME")
            if domain:
                public_url = f"https://{domain}"
        self.WEBHOOK_URL = f"{public_url}{self.WEBHOOK_PATH}" if public_url else ""

_raw_token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
_bot_token = clean_token(_raw_token)
_sheet_id = os.getenv("GOOGLE_SHEET_ID") or os.getenv("SHEET_ID")
_gsak = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")

config = Config(
    BOT_TOKEN=_bot_token,
    GOOGLE_SERVICE_ACCOUNT_KEY=_gsak,
    SHEET_ID=_sheet_id,
)

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

# --- Данные/кэш ---
main_buttons: List[str] = []
submenus: dict[str, List[str]] = {}
texts: dict[str, str] = {}
main_menu = None
last_modified_time = None
cache = TTLCache(maxsize=1, ttl=300)
rate_limit = defaultdict(list)
last_messages: dict[int, list[int]] = {}
scheduler: Optional[AsyncIOScheduler] = None

# ---------- Сессии доступа ----------
def init_db():
    conn = sqlite3.connect("sessions.db")
    conn.execute("CREATE TABLE IF NOT EXISTS sessions (user_id INTEGER PRIMARY KEY, expiry REAL)")
    conn.commit()
    conn.close()
    logging.info("SQLite инициализирован")

def has_access(user_id: int) -> bool:
    conn = sqlite3.connect("sessions.db")
    cursor = conn.execute("SELECT expiry FROM sessions WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    conn.close()
    return bool(row and row[0] > time.time())

async def grant_access(user_id: int):
    expiry = time.time() + 1800  # 30 минут
    conn = sqlite3.connect("sessions.db")
    conn.execute("INSERT OR REPLACE INTO sessions (user_id, expiry) VALUES (?, ?)", (user_id, expiry))
    conn.commit()
    conn.close()
    logging.info(f"Гостевой доступ {user_id} до {time.ctime(expiry)}")

def reset_all_sessions():
    conn = sqlite3.connect("sessions.db")
    conn.execute("DELETE FROM sessions")
    conn.commit()
    conn.close()
    logging.info("Все сессии сброшены; потребуется повторный ввод кода")

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
VIDEO_EXTS = {".mp4"}
ANIM_EXTS  = {".gif"}
DOC_EXTS   = {".pdf", ".svg"}

def extract_urls_ordered(text: str) -> List[str]:
    urls = URL_RE.findall(text or "")
    seen = OrderedDict()
    for u in urls:
        seen.setdefault(u, True)
    return list(seen.keys())

def ext_of(url: str) -> str:
    path = urlparse(url).path
    return os.path.splitext(path)[1].lower()

def split_media(urls: List[str]) -> Tuple[List[types.InputMedia], List[str], List[str]]:
    media_items: List[types.InputMedia] = []
    anims: List[str] = []
    docs: List[str] = []
    for url in urls:
        e = ext_of(url)
        if e in PHOTO_EXTS:
            media_items.append(InputMediaPhoto(media=url))
        elif e in VIDEO_EXTS:
            media_items.append(InputMediaVideo(media=url))
        elif e in ANIM_EXTS:
            anims.append(url)
        elif e in DOC_EXTS:
            docs.append(url)
        else:
            docs.append(url)
    return media_items[:10], anims, docs

async def send_album_and_text(chat_id: int, guide_text: str) -> List[int]:
    sent_ids: List[int] = []
    urls = extract_urls_ordered(guide_text)
    media, anims, docs = split_media(urls)

    if len(media) == 1:
        item = media[0]
        try:
            if isinstance(item, InputMediaPhoto):
                msg = await bot.send_photo(chat_id, item.media)
            elif isinstance(item, InputMediaVideo):
                msg = await bot.send_video(chat_id, item.media)
            else:
                msg = await bot.send_document(chat_id, item.media)
            sent_ids.append(msg.message_id)
        except Exception as e:
            logging.error(f"send single media failed: {e}")
    elif len(media) > 1:
        try:
            group = await bot.send_media_group(chat_id=chat_id, media=media)
            sent_ids.extend([m.message_id for m in group])
        except Exception as e:
            logging.error(f"send_media_group failed: {e}")
            for item in media:
                try:
                    if isinstance(item, InputMediaPhoto):
                        msg = await bot.send_photo(chat_id, item.media)
                    elif isinstance(item, InputMediaVideo):
                        msg = await bot.send_video(chat_id, item.media)
                    else:
                        msg = await bot.send_document(chat_id, item.media)
                    sent_ids.append(msg.message_id)
                    await asyncio.sleep(0.3)
                except Exception as e2:
                    logging.error(f"fallback single media failed: {e2}")

    for aurl in anims[:10]:
        try:
            msg = await bot.send_animation(chat_id, aurl)
            sent_ids.append(msg.message_id)
            await asyncio.sleep(0.2)
        except Exception as e:
            logging.error(f"send_animation failed: {e}")

    for durl in docs[:10]:
        try:
            msg = await bot.send_document(chat_id, durl)
            sent_ids.append(msg.message_id)
            await asyncio.sleep(0.2)
        except Exception as e:
            logging.error(f"send_document failed: {e}")

    text_without_urls = URL_RE.sub("", guide_text).strip()
    if text_without_urls:
        msg = await bot.send_message(chat_id, text_without_urls, reply_markup=main_menu)
        sent_ids.append(msg.message_id)
    else:
        msg = await bot.send_message(chat_id, "Выберите следующий раздел:", reply_markup=main_menu)
        sent_ids.append(msg.message_id)

    return sent_ids

# ---------- Google Sheets ----------
def sanitize_text(text: str, sanitize=True) -> str:
    if sanitize:
        return re.sub(r"[^\w\s-]", "", text.strip())[:100]
    return text.strip()[:100]

async def load_guides(force=False):
    """
    Подтягивает кнопки/тексты из Google Sheets.
    Транзиентные SSL-гличи ретраим.
    """
    global main_buttons, submenus, texts, main_menu, last_modified_time, SHEETS_SERVICE, DRIVE_SERVICE
    cache_key = "guides_data"
    if cache.get(cache_key) and not force:
        main_buttons, submenus, texts, main_menu = cache[cache_key]
        return

    if not SHEETS_SERVICE or not DRIVE_SERVICE:
        logging.error("Google services not initialized. Check GOOGLE_SERVICE_ACCOUNT_KEY.")
        return

    max_retries = 4
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            if not force:
                file_metadata = DRIVE_SERVICE.files().get(fileId=config.SHEET_ID, fields="modifiedTime").execute()
                current_modified_time = file_metadata.get("modifiedTime")
                if last_modified_time is not None and last_modified_time == current_modified_time:
                    return
                last_modified_time = current_modified_time

            result = SHEETS_SERVICE.spreadsheets().values().get(
                spreadsheetId=config.SHEET_ID, range=config.RANGE_NAME
            ).execute()
            values = result.get("values", [])
            if not values:
                logging.warning(f"No data found in range {config.RANGE_NAME}.")
                return
            if len(values[0]) < 3 or values[0][1].lower() == "button":
                values = values[1:]

            main_buttons = []
            submenus = {}
            texts = {}

            for row in values:
                if len(row) < 3:
                    continue
                parent = sanitize_text(row[0], sanitize=False) if row[0] else None
                button = sanitize_text(row[1], sanitize=False)
                text = (row[2] or "").strip() or "Текст не найден в Google Sheets."
                texts[button] = text
                if not parent:
                    main_buttons.append(button)
                else:
                    submenus.setdefault(parent, []).append(button)

            buttons = [[KeyboardButton(text=btn)] for btn in main_buttons]
            main_menu = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)

            cache[cache_key] = (main_buttons, submenus, texts, main_menu)
            logging.info(f"Guides loaded: {len(main_buttons)} main buttons, {sum(len(v) for v in submenus.values())} sub buttons")
            return

        except HttpError as e:
            if e.resp.status == 429:
                logging.warning(f"Rate limit, retrying: {e}")
                await asyncio.sleep(max(5.0, delay))
                delay = min(delay * 2, 10.0)
            else:
                logging.error(f"HTTP Error {e.resp.status}: {e}")
                if attempt == max_retries:
                    logging.error("Max retries reached; using cached data if available.")
                    if cache.get(cache_key):
                        main_buttons, submenus, texts, main_menu = cache[cache_key]
                    else:
                        main_menu = None
                    break
        except Exception as e:
            msg = str(e)
            if isinstance(e, ssl.SSLError) or "EOF occurred" in msg or "SSLError" in type(e).__name__ or "Broken pipe" in msg:
                logging.warning(f"Transient network error on load_guides (attempt {attempt}/{max_retries}): {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)
                continue
            logging.error(f"Unexpected error on load_guides (attempt {attempt}/{max_retries}): {e}")
            if attempt == max_retries:
                logging.error("Max retries reached; using cached data if available.")
                if cache.get(cache_key):
                    main_buttons, submenus, texts, main_menu = cache[cache_key]
                else:
                    main_menu = None
                break
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)

def build_submenu_inline(parent: str) -> Optional[InlineKeyboardMarkup]:
    if parent not in submenus:
        logging.info(f"No submenu found for parent='{parent}'")
        return None
    subs = submenus[parent]
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for btn in subs:
        # callback_data: суб|<текст> (если влезает), иначе sub#<sha1(текста)>
        direct = f"sub|{btn}"
        if len(direct.encode("utf-8")) <= 64:
            cb = direct
        else:
            cb = "sub#" + hashlib.sha1(btn.encode("utf-8")).hexdigest()[:32]  # sha1 для совместимости, но можно sha256
        kb.inline_keyboard.append([InlineKeyboardButton(text=btn, callback_data=cb)])
    logging.info(f"Built submenu for '{parent}' with {len(subs)} items")
    return kb

# ---------- Служебка ----------
async def clear_old_messages(message_or_callback: types.Message | types.CallbackQuery):
    user_id = message_or_callback.from_user.id
    current_message_id = (
        message_or_callback.message_id if isinstance(message_or_callback, types.Message)
        else message_or_callback.message.message_id
    )
    if user_id in last_messages and last_messages[user_id]:
        try:
            last_message_id = last_messages[user_id][-1]
            await bot.delete_message(user_id, last_message_id)
        except Exception as e:
            logging.debug(f"Failed to delete last message {last_message_id}: {e}")
        last_messages[user_id] = []
    try:
        await bot.delete_message(user_id, current_message_id)
    except Exception as e:
        logging.debug(f"Failed to delete triggering message {current_message_id}: {e}")

# ---------- Хендлеры ----------
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    rate_limit[user_id] = [t for t in rate_limit[user_id] if time.time() - t < 60]
    if len(rate_limit[user_id]) >= 10:
        await message.answer("Слишком много запросов. Попробуйте позже.")
        return
    rate_limit[user_id].append(time.time())

    await clear_old_messages(message)
    if has_access(user_id):
        if not main_menu:
            sent = await message.answer("Ошибка: Кнопки не загружены из Google Sheets. Попробуйте позже или используйте /reload.")
            last_messages[user_id] = [sent.message_id]
            return
        sent = await message.answer("Главное меню:", reply_markup=main_menu)
        last_messages[user_id] = [sent.message_id]
    else:
        sent = await message.answer("Введите код доступа.")
        last_messages[user_id] = [sent.message_id]

@dp.message(Command("reload"))
async def cmd_reload(message: types.Message):
    """
    Теперь доступна всем:
    - жёстко перезагружает гайды из таблицы
    - сбрасывает все сессии -> требуется повторный ввод кода
    """
    user_id = message.from_user.id
    rate_limit[user_id] = [t for t in rate_limit[user_id] if time.time() - t < 60]
    if len(rate_limit[user_id]) >= 10:
        await message.answer("Слишком много запросов. Попробуйте позже.")
        return
    rate_limit[user_id].append(time.time())

    await clear_old_messages(message)

    # 1) Обновим таблицу
    await load_guides(force=True)

    # 2) Сбросим все сессии доступа
    reset_all_sessions()

    # 3) Сообщим пользователю и вернём на ввод кода
    sent = await message.answer("Бот обновлён. Введите код доступа.")
    last_messages[user_id] = [sent.message_id]

@dp.message()
async def main_handler(message: types.Message):
    user_id = message.from_user.id
    rate_limit[user_id] = [t for t in rate_limit[user_id] if time.time() - t < 60]
    if len(rate_limit[user_id]) >= 10:
        await message.answer("Слишком много запросов. Попробуйте позже.")
        return
    rate_limit[user_id].append(time.time())

    if not hasattr(message, "text"):
        sent = await message.answer("Неизвестная команда. Используйте кнопки ⬇️", reply_markup=main_menu)
        last_messages[user_id] = [sent.message_id]
        return

    txt = message.text.strip()
    await clear_old_messages(message)
    sent_messages: list[int] = []

    if not has_access(user_id):
        if txt == "infobot":
            await grant_access(user_id)
            if not main_menu:
                sent = await message.answer("Ошибка: Кнопки не загружены из Google Sheets. Попробуйте позже или используйте /reload.", reply_markup=main_menu)
                sent_messages.append(sent.message_id)
            else:
                sent = await message.answer("Доступ предоставлен на 30 минут. Главное меню:", reply_markup=main_menu)
                sent_messages.append(sent.message_id)
            try:
                await bot.delete_message(user_id, message.message_id)
            except Exception:
                pass
    else:
        if txt in main_buttons:
            if txt in submenus:
                submenu = build_submenu_inline(txt)
                if submenu:
                    sent = await message.answer(f"Выберите опцию для {txt}:", reply_markup=submenu)
                    sent_messages.append(sent.message_id)
                else:
                    sent = await message.answer(f"Подменю для {txt} не найдено.", reply_markup=main_menu)
                    sent_messages.append(sent.message_id)
            else:
                guide_text = texts.get(txt, "Текст не найден в Google Sheets.").strip()
                sent_ids = await send_album_and_text(user_id, guide_text)
                sent_messages.extend(sent_ids)
        else:
            sent = await message.answer("Пожалуйста, используйте кнопки ⬇️", reply_markup=main_menu)
            sent_messages.append(sent.message_id)

    last_messages[user_id] = sent_messages

@dp.callback_query()
async def process_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    data = callback.data or ""
    try:
        # сразу гасим «крутилку»
        await callback.answer()
    except Exception:
        pass

    logging.info(f"Callback received from {user_id}: {data}")
    rate_limit[user_id] = [t for t in rate_limit[user_id] if time.time() - t < 60]
    if len(rate_limit[user_id]) >= 10:
        await callback.message.answer("Слишком много запросов. Попробуйте позже.")
        return
    rate_limit[user_id].append(time.time())

    if not has_access(user_id):
        await callback.message.answer("Доступ истек. Введите код доступа.", reply_markup=main_menu)
        return

    await clear_old_messages(callback)

    try:
        btn: Optional[str] = None
        if data.startswith("sub|"):
            btn = data.split("|", 1)[1]
        elif data.startswith("sub#"):
            h = data[4:]
            # поиск по всем клавишам — устойчиво к перезапускам
            for k in texts.keys():
                if hashlib.sha1(k.encode("utf-8")).hexdigest().startswith(h):
                    btn = k
                    break

        if not btn:
            logging.warning(f"Unknown callback data: {data}. keys_in_texts={len(texts)}")
            await callback.message.answer("Элемент не найден. Обновите меню (/reload).", reply_markup=main_menu)
            return

        guide_text = texts.get(btn)
        if guide_text is None:
            logging.warning(f"Button '{btn}' not found in texts; reloading guides")
            await load_guides(force=True)
            guide_text = texts.get(btn, "Текст не найден в Google Sheets.")

        sent_ids = await send_album_and_text(user_id, guide_text.strip())
        last_messages[user_id] = sent_ids

    except Exception as e:
        logging.error(f"Callback processing error: {e}")
        await callback.message.answer("Ошибка обработки. Попробуйте снова.", reply_markup=main_menu)

# ---------- Веб сервер ----------
@app.post(config.WEBHOOK_PATH if config.WEBHOOK_URL else "/webhook")
async def webhook_handler(request: Request):
    try:
        update = await request.json()
        up_type = "callback_query" if "callback_query" in update else "message" if "message" in update else list(update.keys())
        logging.debug(f"Incoming update type: {up_type}")
        if 'message' in update or 'callback_query' in update:
            asyncio.create_task(dp.feed_update(bot, types.Update(**update)))
        return {"ok": True}
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    if not SHEETS_SERVICE or not DRIVE_SERVICE or last_modified_time is None:
        raise HTTPException(status_code=503, detail="Google services unavailable or no successful guide load")
    return {"status": "healthy"}

@app.get("/debug/env")
async def debug_env():
    return {
        "bot_token_present": bool(config.BOT_TOKEN),
        "bot_token_len": len(config.BOT_TOKEN) if config.BOT_TOKEN else 0,
        "sheet_id_present": bool(config.SHEET_ID),
        "gsak_present": bool(config.GOOGLE_SERVICE_ACCOUNT_KEY),
        "webhook_url": config.WEBHOOK_URL or "(empty)",
    }

# ---------- Установка вебхука ----------
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
    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            await bot.set_webhook(url)
            logging.info(f"Webhook set to {url}")
            return
        except TelegramRetryAfter as e:
            wait = float(getattr(e, "retry_after", delay))
            logging.warning(f"set_webhook rate-limited, retrying in {wait}s (attempt {attempt}/{max_attempts})")
            await asyncio.sleep(wait)
        except TelegramBadRequest as e:
            logging.error(f"set_webhook bad request: {e}")
            raise
        except Exception as e:
            logging.warning(f"set_webhook attempt {attempt} failed: {e}")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)
    raise RuntimeError("Failed to set webhook after retries")

# ---------- Старт ----------
@app.on_event("startup")
async def on_startup():
    global bot, CREDS, SHEETS_SERVICE, DRIVE_SERVICE, last_modified_time, scheduler
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

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=config.PORT, workers=1)