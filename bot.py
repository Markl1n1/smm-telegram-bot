# bot.py
import os
import asyncio
import json
import time
import logging
import sqlite3
import re
import hashlib
from dataclasses import dataclass
from typing import Optional, List, Tuple
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, InputMediaVideo, InputMediaAnimation, InputMediaDocument
)
from aiogram.filters import Command
from fastapi import FastAPI, Request, HTTPException
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from cachetools import TTLCache
from collections import defaultdict, OrderedDict
import uvicorn
from dotenv import load_dotenv

# ---------- .env для локальной отладки ----------
load_dotenv()

# ---------- Логирование ----------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def _mask(s: Optional[str], keep_tail: int = 6) -> str:
    if not s:
        return "None"
    return "***" + s[-keep_tail:] if len(s) > keep_tail else "***"

# ---------- Токен: извлечение и санитизация ----------
TOKEN_RE = re.compile(r"\d+:[A-Za-z0-9_-]+")  # допустимый шаблон токена

def clean_token(raw: str) -> Optional[str]:
    if raw is None:
        return None
    s = raw.strip().strip('"').strip("'")
    m = TOKEN_RE.search(s)
    return m.group(0) if m else None

# ---------- Конфиг (Вариант A: без дефолтов -> с дефолтами) ----------
@dataclass
class Config:
    BOT_TOKEN: str
    GOOGLE_SERVICE_ACCOUNT_KEY: str
    SHEET_ID: str

    WEBHOOK_PATH: str = "/webhook"
    WEBHOOK_URL: str = ""
    RANGE_NAME: str = "Guides!A:C"
    ADMIN_ID: int = 6970816136
    PORT: int = int(os.getenv("PORT", "8000"))

    def __post_init__(self):
        public_url = os.getenv("PUBLIC_URL")
        if not public_url:
            domain = os.getenv("KOYEB_PUBLIC_DOMAIN") or os.getenv("KOYEB_EXTERNAL_HOSTNAME")
            if domain:
                public_url = f"https://{domain}"
        if not public_url:
            raise EnvironmentError(
                "Не задан PUBLIC_URL и нет KOYEB_PUBLIC_DOMAIN/KOYEB_EXTERNAL_HOSTNAME. "
                "Добавь PUBLIC_URL = https://{{KOYEB_PUBLIC_DOMAIN}} в переменные окружения."
            )
        self.WEBHOOK_URL = f"{public_url}{self.WEBHOOK_PATH}"

# --- Читаем окружение с алиасами имён и чистим токен ---
_raw_token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
_bot_token = clean_token(_raw_token)
_sheet_id = os.getenv("GOOGLE_SHEET_ID") or os.getenv("SHEET_ID")
_gsak = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")

logging.info(f"BOT_TOKEN: {_mask(_bot_token)} (len={len(_bot_token) if _bot_token else 0})")
logging.info(f"GOOGLE_SHEET_ID: {bool(_sheet_id)}")
logging.info(f"GOOGLE_SERVICE_ACCOUNT_KEY set: {bool(_gsak)}")
logging.info(f"KOYEB_PUBLIC_DOMAIN: {os.getenv('KOYEB_PUBLIC_DOMAIN')}")
logging.info(f"PUBLIC_URL: {os.getenv('PUBLIC_URL')}")
logging.info(f"PORT: {os.getenv('PORT')}")

config = Config(
    BOT_TOKEN=_bot_token,
    GOOGLE_SERVICE_ACCOUNT_KEY=_gsak,
    SHEET_ID=_sheet_id,
)

# --- Глобальные объекты (инициализируем в startup) ---
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
cache = TTLCache(maxsize=1, ttl=300)  # 5 минут
rate_limit = defaultdict(list)
last_messages: dict[int, list[int]] = {}

# --- Короткие callback_data (<=64 байт) ---
# mapping: token -> кнопка
cb_map: dict[str, str] = {}

def make_cb_token(parent: str, btn: str) -> str:
    raw = f"{parent}|{btn}"
    token = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:32]  # 32 символа
    return f"sub:{token}"

# --- SQLite для сессий ---
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

# --- Проверка окружения и токена ---
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

# --- Утилиты для ссылок и отправки медиа ---
URL_RE = re.compile(
    r'https?://(?:[a-zA-Z0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
)

PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
VIDEO_EXTS = {".mp4"}  # .mov лучше избегать — не всегда поддерживается
ANIM_EXTS  = {".gif"}
DOC_EXTS   = {".pdf", ".svg"}

def extract_urls_ordered(text: str) -> List[str]:
    """Достаёт URL в порядке появления + дедупликация (stable)."""
    urls = URL_RE.findall(text or "")
    seen = OrderedDict()
    for u in urls:
        seen.setdefault(u, True)
    return list(seen.keys())

def ext_of(url: str) -> str:
    """Расширение без query/fragment в нижнем регистре."""
    path = urlparse(url).path
    ext = os.path.splitext(path)[1].lower()
    return ext

def split_media(urls: List[str]) -> Tuple[List[types.InputMedia], List[str]]:
    """
    Разделяет ссылки на:
    - media_items (Photo/Video/Animation) — пойдут в send_media_group
    - docs (PDF/SVG/прочие) — отправим отдельно как документы
    Возвращает не более 10 media для альбома (Telegram лимит).
    """
    media_items: List[types.InputMedia] = []
    docs: List[str] = []
    for url in urls:
        e = ext_of(url)
        if e in PHOTO_EXTS:
            media_items.append(InputMediaPhoto(media=url))
        elif e in VIDEO_EXTS:
            media_items.append(InputMediaVideo(media=url))
        elif e in ANIM_EXTS:
            media_items.append(InputMediaAnimation(media=url))
        elif e in DOC_EXTS:
            docs.append(url)
        else:
            # неизвестное — лучше как документ, чтобы не ронять альбом
            docs.append(url)
    return media_items[:10], docs  # альбом максимум 10

async def send_album_and_text(chat_id: int, guide_text: str) -> List[int]:
    """
    Отправляет:
    1) альбом медиa (до 10);
    2) документы по очереди;
    3) текст без ссылок;
    4) служебное сообщение с клавиатурой (если в пункте 3 текст не отправляли).
    Возвращает список message_id отправленных сообщений.
    """
    sent_ids: List[int] = []
    urls = extract_urls_ordered(guide_text)
    media, docs = split_media(urls)

    # 1) Альбом
    if len(media) == 1:
        # одиночное фото/видео/гиф как отдельное сообщение
        item = media[0]
        try:
            if isinstance(item, InputMediaPhoto):
                msg = await bot.send_photo(chat_id, item.media)
            elif isinstance(item, InputMediaVideo):
                msg = await bot.send_video(chat_id, item.media)
            elif isinstance(item, InputMediaAnimation):
                msg = await bot.send_animation(chat_id, item.media)
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
            # fallback — попробуем по одному
            for item in media:
                try:
                    if isinstance(item, InputMediaPhoto):
                        msg = await bot.send_photo(chat_id, item.media)
                    elif isinstance(item, InputMediaVideo):
                        msg = await bot.send_video(chat_id, item.media)
                    elif isinstance(item, InputMediaAnimation):
                        msg = await bot.send_animation(chat_id, item.media)
                    else:
                        msg = await bot.send_document(chat_id, item.media)
                    sent_ids.append(msg.message_id)
                    await asyncio.sleep(0.3)
                except Exception as e2:
                    logging.error(f"fallback single media failed: {e2}")

    # 2) Документы
    # (не в альбоме; отправляем отдельными сообщениями)
    for durl in docs[:10]:  # не будем спамить слишком много
        try:
            msg = await bot.send_document(chat_id, durl)
            sent_ids.append(msg.message_id)
            await asyncio.sleep(0.2)
        except Exception as e:
            logging.error(f"send_document failed: {e}")

    # 3) Текст без ссылок
    text_without_urls = URL_RE.sub("", guide_text).strip()
    if text_without_urls:
        msg = await bot.send_message(chat_id, text_without_urls, reply_markup=main_menu)
        sent_ids.append(msg.message_id)
    else:
        # 4) Если текста нет — служебное сообщение с клавиатурой, чтобы меню не пропадало
        msg = await bot.send_message(chat_id, "Выберите следующий раздел:", reply_markup=main_menu)
        sent_ids.append(msg.message_id)

    return sent_ids

# --- Загрузка данных из Google Sheets ---
def sanitize_text(text: str, sanitize=True) -> str:
    if sanitize:
        return re.sub(r"[^\w\s-]", "", text.strip())[:100]
    return text.strip()[:100]

async def load_guides(force=False):
    global main_buttons, submenus, texts, main_menu, last_modified_time, SHEETS_SERVICE, DRIVE_SERVICE, cb_map
    cache_key = "guides_data"
    if cache.get(cache_key) and not force:
        main_buttons, submenus, texts, main_menu, cb_map = cache[cache_key]
        return

    if not SHEETS_SERVICE or not DRIVE_SERVICE:
        logging.error("Google services not initialized. Check GOOGLE_SERVICE_ACCOUNT_KEY.")
        return

    max_retries = 3
    for attempt in range(max_retries):
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
            cb_map = {}

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

            # Reply Keyboard
            buttons = [[KeyboardButton(text=btn)] for btn in main_buttons]
            main_menu = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)

            cache[cache_key] = (main_buttons, submenus, texts, main_menu, cb_map)
            logging.info(f"Guides loaded: {len(main_buttons)} main buttons, {sum(len(v) for v in submenus.values())} sub buttons")
            return
        except HttpError as e:
            if e.resp.status == 429:
                logging.warning(f"Rate limit, retrying: {e}")
                await asyncio.sleep(5 + 2 ** attempt)
            else:
                logging.error(f"HTTP Error {e.resp.status}: {e}")
                if attempt == max_retries - 1:
                    main_menu = None
                    break
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            if attempt == max_retries - 1:
                main_menu = None
                break
            await asyncio.sleep(5 + 2 ** attempt)

def build_submenu_inline(parent: str) -> Optional[InlineKeyboardMarkup]:
    if parent not in submenus:
        logging.info(f"No submenu found for parent='{parent}'")
        return None
    subs = submenus[parent]
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for btn in subs:
        token = make_cb_token(parent, btn)
        cb_map[token] = btn
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=btn, callback_data=token)])
    logging.info(f"Built submenu for '{parent}' with {len(subs)} items")
    return keyboard

# --- Управление сообщениями ---
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

# --- Периодическая перезагрузка из таблицы ---
async def periodic_reload():
    global last_modified_time, SHEETS_SERVICE, DRIVE_SERVICE
    while True:
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
                    SHEETS_SERVICE = build("sheets", "v4", credentials=creds)
                    DRIVE_SERVICE = build("drive", "v3", credentials=creds)
                    logging.info("Reinitialized Google services")
                except Exception as e:
                    logging.error(f"Reinit failed: {e}")
        except Exception as e:
            logging.error(f"Error in periodic reload: {e}")
        await asyncio.sleep(300)

# --- Хендлеры Telegram ---
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
    user_id = message.from_user.id
    rate_limit[user_id] = [t for t in rate_limit[user_id] if time.time() - t < 60]
    if len(rate_limit[user_id]) >= 10:
        await message.answer("Слишком много запросов. Попробуйте позже.")
        return
    rate_limit[user_id].append(time.time())

    if user_id != config.ADMIN_ID:
        sent = await message.answer("Доступ запрещен.")
        last_messages[user_id] = [sent.message_id]
        return
    await clear_old_messages(message)
    await load_guides(force=True)
    if main_menu:
        sent = await message.answer("Guides reloaded from Google Sheets.", reply_markup=main_menu)
        last_messages[user_id] = [sent.message_id]
    else:
        sent = await message.answer("Ошибка при перезагрузке guides. Проверьте настройки Google Sheets.")
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
            # Пытаемся отправить подменю (inline)
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
    logging.info(f"Callback received from {user_id}: {data}")
    rate_limit[user_id] = [t for t in rate_limit[user_id] if time.time() - t < 60]
    if len(rate_limit[user_id]) >= 10:
        await callback.message.answer("Слишком много запросов. Попробуйте позже.")
        await callback.answer()
        return
    rate_limit[user_id].append(time.time())

    if not has_access(user_id):
        await callback.message.answer("Доступ истек. Введите код доступа.", reply_markup=main_menu)
        await callback.answer()
        return

    await clear_old_messages(callback)

    try:
        if data.startswith("sub:"):
            btn = cb_map.get(data)
            if not btn:
                logging.warning(f"Unknown callback token: {data}. Known tokens: {len(cb_map)}")
                await callback.message.answer("Элемент не найден. Обновите меню (/reload).", reply_markup=main_menu)
                await callback.answer()
                return
            guide_text = texts.get(btn, "Текст не найден в Google Sheets.").strip()
            sent_ids = await send_album_and_text(user_id, guide_text)
            last_messages[user_id] = sent_ids
        else:
            logging.warning(f"Unexpected callback data: {data}")
            msg = await callback.message.answer("Некорректная кнопка. Попробуйте снова.", reply_markup=main_menu)
            last_messages[user_id] = [msg.message_id]

        await callback.answer()
    except Exception as e:
        logging.error(f"Callback processing error: {e}")
        await callback.message.answer("Ошибка обработки. Попробуйте снова.", reply_markup=main_menu)
        await callback.answer()

# ---------- Веб сервер ----------
@app.post(config.WEBHOOK_PATH)
async def webhook_handler(request: Request):
    try:
        update = await request.json()
        # Логируем тип апдейта для диагностики
        up_type = "callback_query" if "callback_query" in update else "message" if "message" in update else list(update.keys())
        logging.debug(f"Incoming update type: {up_type}")
        if 'message' in update or 'callback_query' in update:
            asyncio.create_task(dp.feed_update(bot, types.Update(**update)))
        return {"ok": True}
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    if not SHEETS_SERVICE or not DRIVE_SERVICE:
        raise HTTPException(status_code=503, detail="Google services unavailable")
    return {"status": "healthy"}

# Диагностика окружения (без утечек)
@app.get("/debug/env")
async def debug_env():
    return {
        "bot_token_present": bool(config.BOT_TOKEN),
        "bot_token_len": len(config.BOT_TOKEN) if config.BOT_TOKEN else 0,
        "sheet_id_present": bool(config.SHEET_ID),
        "gsak_present": bool(config.GOOGLE_SERVICE_ACCOUNT_KEY),
        "webhook_url": config.WEBHOOK_URL,
    }

# ---------- Старт приложения ----------
@app.on_event("startup")
async def on_startup():
    global bot, CREDS, SHEETS_SERVICE, DRIVE_SERVICE, last_modified_time

    validate_env_vars()
    bot = Bot(token=config.BOT_TOKEN)

    try:
        creds_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_KEY)
    except json.JSONDecodeError as e:
        raise EnvironmentError(f"GOOGLE_SERVICE_ACCOUNT_KEY не JSON: {e}")
    CREDS = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    SHEETS_SERVICE = build("sheets", "v4", credentials=CREDS)
    DRIVE_SERVICE = build("drive", "v3", credentials=CREDS)

    init_db()
    last_modified_time = None
    await bot.set_webhook(config.WEBHOOK_URL)
    logging.info(f"Webhook set to {config.WEBHOOK_URL}")

    await load_guides(force=True)
    if not main_menu:
        logging.warning("Guides not loaded on startup. Service may be limited.")

    asyncio.create_task(keep_alive())
    asyncio.create_task(periodic_reload())

async def keep_alive():
    while True:
        try:
            await bot.get_me()
        except Exception as e:
            logging.error(f"Keep-alive failed: {e}")
        await asyncio.sleep(300)

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=config.PORT)
