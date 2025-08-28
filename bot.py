# bot.py
import os
import asyncio
import json
import time
import logging
import sqlite3
import re
from dataclasses import dataclass
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
from aiogram.filters import Command
from fastapi import FastAPI, Request, HTTPException
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from cachetools import TTLCache
from collections import defaultdict
import uvicorn
from dotenv import load_dotenv

# Load .env file for local development
load_dotenv()

# ---------- Logging (stdout, без утечек секретов) ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _mask(s: str | None, keep_tail: int = 6) -> str:
    if not s:
        return "None"
    if len(s) <= keep_tail:
        return "***"
    return f"***{s[-keep_tail:]}"

logging.info(f"BOT_TOKEN: {_mask(os.getenv('BOT_TOKEN'))}")
logging.info(f"GOOGLE_SERVICE_ACCOUNT_KEY set: {bool(os.getenv('GOOGLE_SERVICE_ACCOUNT_KEY'))}")
logging.info(f"GOOGLE_SHEET_ID: {os.getenv('GOOGLE_SHEET_ID')}")
logging.info(f"KOYEB_PUBLIC_DOMAIN: {os.getenv('KOYEB_PUBLIC_DOMAIN')}")
logging.info(f"PUBLIC_URL: {os.getenv('PUBLIC_URL')}")
logging.info(f"PORT: {os.getenv('PORT')}")

# ---------- Config ----------
@dataclass
class Config:
    # Требуемые поля (без дефолтов) — сначала
    BOT_TOKEN: str
    GOOGLE_SERVICE_ACCOUNT_KEY: str
    SHEET_ID: str

    # Поля с дефолтами — после
    WEBHOOK_PATH: str = "/webhook"
    WEBHOOK_URL: str = ""
    PORT: int = int(os.getenv("PORT", "8000"))
    RANGE_NAME: str = "Guides!A:C"
    ADMIN_ID: int = 6970816136

    def __post_init__(self):
        """
        Строим публичный URL для вебхука:
        - Если задан PUBLIC_URL — используем его (на Koyeb удобно задать PUBLIC_URL=https://{{KOYEB_PUBLIC_DOMAIN}})
        - Иначе пробуем KOYEB_PUBLIC_DOMAIN -> https://<domain>
        - Иначе — падаем с понятной ошибкой.
        """
        public_url = os.getenv("PUBLIC_URL")
        if not public_url:
            public_domain = os.getenv("KOYEB_PUBLIC_DOMAIN")
            if public_domain:
                public_url = f"https://{public_domain}"
        if public_url:
            self.WEBHOOK_URL = f"{public_url}{self.WEBHOOK_PATH}"
            logging.info(f"WEBHOOK_URL resolved to: {self.WEBHOOK_URL}")
        else:
            logging.error("Neither PUBLIC_URL nor KOYEB_PUBLIC_DOMAIN is set.")
            raise EnvironmentError(
                "Set PUBLIC_URL (e.g. https://{{KOYEB_PUBLIC_DOMAIN}} in Koyeb) or KOYEB_PUBLIC_DOMAIN"
            )

# Initialize Config (исправленный порядок полей — см. выше)
config = Config(
    BOT_TOKEN=os.getenv("BOT_TOKEN"),
    GOOGLE_SERVICE_ACCOUNT_KEY=os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY"),
    SHEET_ID=os.getenv("GOOGLE_SHEET_ID"),
)

# --- Validate Environment Variables ---
def validate_env_vars():
    required_vars = {
        "BOT_TOKEN": config.BOT_TOKEN,
        "GOOGLE_SERVICE_ACCOUNT_KEY": config.GOOGLE_SERVICE_ACCOUNT_KEY,
        "GOOGLE_SHEET_ID": config.SHEET_ID,
    }
    missing = [key for key, value in required_vars.items() if not value]
    if missing:
        logging.error(f"Missing/invalid env vars: {', '.join(missing)}")
        raise EnvironmentError(f"Missing or invalid environment variables: {', '.join(missing)}")

# --- Bot and FastAPI Setup ---
bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()

# --- Google Sheets Setup ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]
try:
    creds_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_KEY)
    logging.info("GOOGLE_SERVICE_ACCOUNT_KEY parsed as JSON successfully")
except json.JSONDecodeError as e:
    logging.error(f"Invalid JSON in GOOGLE_SERVICE_ACCOUNT_KEY: {e}")
    creds_info = None

CREDS = Credentials.from_service_account_info(creds_info, scopes=SCOPES) if creds_info else None
SHEETS_SERVICE = build("sheets", "v4", credentials=CREDS) if CREDS else None
DRIVE_SERVICE = build("drive", "v3", credentials=CREDS) if CREDS else None

if not SHEETS_SERVICE or not DRIVE_SERVICE:
    logging.critical("Google services failed to initialize")

# --- Data Structures ---
main_buttons: list[str] = []
submenus: dict[str, list[str]] = {}
texts: dict[str, str] = {}
main_menu = None
last_modified_time = None
cache = TTLCache(maxsize=1, ttl=300)  # 5 минут
rate_limit = defaultdict(list)
last_messages: dict[int, list[int]] = {}

# --- SQLite for User Sessions ---
def init_db():
    conn = sqlite3.connect("sessions.db")
    conn.execute("CREATE TABLE IF NOT EXISTS sessions (user_id INTEGER PRIMARY KEY, expiry REAL)")
    conn.commit()
    conn.close()
    logging.info("SQLite database initialized")

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
    logging.info(f"Granted access to user {user_id} until {time.ctime(expiry)}")

# --- Google Sheets Data Loading ---
def sanitize_text(text: str, sanitize=True) -> str:
    if sanitize:
        return re.sub(r"[^\w\s-]", "", text.strip())[:100]
    return text.strip()[:100]

async def load_guides(force=False):
    global main_buttons, submenus, texts, main_menu, last_modified_time
    cache_key = "guides_data"
    if cache.get(cache_key) and not force:
        main_buttons, submenus, texts, main_menu = cache[cache_key]
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
                logging.warning(f"No data found in range {config.RANGE_NAME} of sheet {config.SHEET_ID}.")
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
                text = sanitize_text(row[2], sanitize=False) if row[2] else "Текст не найден в Google Sheets."
                texts[button] = text
                if not parent:
                    main_buttons.append(button)
                else:
                    submenus.setdefault(parent, []).append(button)
            buttons = [[KeyboardButton(text=btn)] for btn in main_buttons]
            main_menu = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)
            cache[cache_key] = (main_buttons, submenus, texts, main_menu)
            return
        except HttpError as e:
            if e.resp.status == 429:
                logging.warning(f"Rate limit hit, retrying after delay: {e._get_reason()}")
                await asyncio.sleep(5 + 2 ** attempt)
            else:
                logging.error(f"HTTP Error {e.resp.status} on attempt {attempt + 1}: {e._get_reason()}")
                if attempt == max_retries - 1:
                    main_menu = None
                    break
        except Exception as e:
            logging.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            if attempt == max_retries - 1:
                main_menu = None
                break
            await asyncio.sleep(5 + 2 ** attempt)

def build_submenu_inline(parent: str):
    if parent not in submenus:
        return None
    subs = submenus[parent]
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for btn in subs:
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=btn, callback_data=f"sub_{btn}")])
    return keyboard

# --- Message Management ---
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

# --- Periodic Reload ---
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
                if not SHEETS_SERVICE or not DRIVE_SERVICE:
                    try:
                        creds_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_KEY)
                        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
                        SHEETS_SERVICE = build("sheets", "v4", credentials=creds)
                        DRIVE_SERVICE = build("drive", "v3", credentials=creds)
                        logging.info("Reinitialized Google services")
                    except Exception as e:
                        logging.error(f"Failed to reinitialize Google services: {e}")
        except Exception as e:
            logging.error(f"Error in periodic reload: {e}")
        await asyncio.sleep(300)

# --- Telegram Handlers ---
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
            sent = await message.answer("Неверный код доступа. Введите код доступа.")
            sent_messages.append(sent.message_id)
    else:
        if txt in main_buttons:
            if txt in submenus:
                submenu = build_submenu_inline(txt)
                if submenu:
                    sent = await message.answer(f"Выберите опцию для {txt}:", reply_markup=submenu)
                    sent_messages.append(sent.message_id)
                else:
                    sent = await message.answer(f"Ошибка: Подменю для {txt} не найдено.", reply_markup=main_menu)
                    sent_messages.append(sent.message_id)
            else:
                guide_text = texts.get(txt, "Текст не найден в Google Sheets.").strip()
                urls = re.findall(r'https?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', guide_text)
                image_urls = [url for url in urls if any(url.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.svg', '.pdf', '.gif'])]
                max_retries = 2
                if image_urls:
                    media = [InputMediaPhoto(media=url) for url in image_urls]
                    for attempt in range(max_retries + 1):
                        try:
                            sent_group = await bot.send_media_group(chat_id=user_id, media=media)
                            sent_messages.extend([msg.message_id for msg in sent_group])
                            break
                        except Exception:
                            if attempt < max_retries:
                                await asyncio.sleep(1 + 2 ** attempt)
                            else:
                                for url in image_urls:
                                    try:
                                        sent = await message.answer_photo(url, reply_markup=main_menu)
                                        sent_messages.append(sent.message_id)
                                        await asyncio.sleep(0.5)
                                    except Exception:
                                        pass
                text_without_urls = re.sub(r'https?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', guide_text).strip()
                if text_without_urls:
                    sent = await message.answer(text_without_urls, reply_markup=main_menu)
                    sent_messages.append(sent.message_id)
        else:
            sent = await message.answer("Пожалуйста, используйте кнопки ⬇️", reply_markup=main_menu)
            sent_messages.append(sent.message_id)

    last_messages[user_id] = sent_messages

@dp.callback_query()
async def process_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
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
    data = callback.data
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            if data.startswith("sub_"):
                subbutton = data[4:]
                guide_text = texts.get(subbutton, "Текст не найден в Google Sheets.").strip()
                sent_messages: list[int] = []
                urls = re.findall(r'https?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', guide_text)
                image_urls = [url for url in urls if any(url.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.svg', '.pdf', '.gif'])]
                if image_urls:
                    media = [InputMediaPhoto(media=url) for url in image_urls]
                    for attempt2 in range(max_retries + 1):
                        try:
                            sent_group = await bot.send_media_group(chat_id=user_id, media=media)
                            sent_messages.extend([msg.message_id for msg in sent_group])
                            break
                        except Exception:
                            if attempt2 < max_retries:
                                await asyncio.sleep(1 + 2 ** attempt2)
                            else:
                                for url in image_urls:
                                    try:
                                        sent = await callback.message.answer_photo(url, reply_markup=main_menu)
                                        sent_messages.append(sent.message_id)
                                        await asyncio.sleep(0.5)
                                    except Exception:
                                        pass
                text_without_urls = re.sub(r'https?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', guide_text).strip()
                if text_without_urls:
                    sent = await callback.message.answer(text_without_urls, reply_markup=main_menu)
                    sent_messages.append(sent.message_id)
                last_messages[user_id] = sent_messages
            await callback.answer()
            break
        except Exception:
            if attempt < max_retries:
                await asyncio.sleep(1 + 2 ** attempt)
            else:
                await callback.message.answer("Ошибка обработки. Попробуйте снова.", reply_markup=main_menu)
                await callback.answer()

# --- Webhook Handler ---
@app.post(config.WEBHOOK_PATH)
async def webhook_handler(request: Request):
    try:
        update = await request.json()
        # В aiogram v3 корректно прокидываем Update словарём
        if 'message' in update or 'callback_query' in update:
            asyncio.create_task(dp.feed_update(bot, types.Update(**update)))
        return {"ok": True}
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Health Check ---
@app.get("/health")
async def health_check():
    if not SHEETS_SERVICE or not DRIVE_SERVICE:
        raise HTTPException(status_code=503, detail="Google services unavailable")
    return {"status": "healthy"}

# --- Startup Logic ---
@app.on_event("startup")
async def on_startup():
    global last_modified_time
    validate_env_vars()
    init_db()
    if not SHEETS_SERVICE or not DRIVE_SERVICE:
        logging.critical("Google services not initialized. Shutting down.")
        raise SystemExit("Google services initialization failed")

    last_modified_time = None
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logging.info(f"Setting webhook, attempt {attempt + 1}/{max_retries}")
            await bot.set_webhook(config.WEBHOOK_URL)
            logging.info(f"Webhook set to {config.WEBHOOK_URL}")
            break
        except Exception as e:
            logging.error(f"Failed to set webhook: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5 + 2 ** attempt)

    for attempt in range(max_retries):
        await load_guides(force=True)
        if main_menu:
            break
        logging.warning(f"Initial load failed, retrying ({attempt + 1}/3)...")
        await asyncio.sleep(5 + 2 ** attempt)

    if not main_menu:
        logging.error("Failed to load guides after retries. Service may be unstable.")

    asyncio.create_task(keep_alive())
    asyncio.create_task(periodic_reload())

async def keep_alive():
    while True:
        try:
            await bot.get_me()
        except Exception as e:
            logging.error(f"Keep-alive ping failed: {e}")
        await asyncio.sleep(300)

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=int(config.PORT))
