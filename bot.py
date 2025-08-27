import os
import asyncio
import json
import time
import logging
import sqlite3
import re
from dataclasses import dataclass
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.enums import ParseMode
from fastapi import FastAPI, Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from cachetools import TTLCache
from collections import defaultdict
import uvicorn

# --- Configuration ---
@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN")
    WEBHOOK_PATH: str = "/webhook"
    WEBHOOK_URL: str = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}{WEBHOOK_PATH}"
    PORT: int = int(os.getenv("PORT", 10000))
    GOOGLE_SERVICE_ACCOUNT_KEY: str = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
    SHEET_ID: str = os.getenv("GOOGLE_SHEET_ID")
    RANGE_NAME: str = "Guides!A:C"
    ADMIN_ID: int = 6970816136

config = Config()

# --- Validate Environment Variables ---
def validate_env_vars():
    required_vars = ["BOT_TOKEN", "GOOGLE_SERVICE_ACCOUNT_KEY", "SHEET_ID"]
    missing = [var for var in required_vars if not getattr(config, var)]
    if missing:
        logging.error(f"Missing environment variables: {', '.join(missing)}")
        raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")

# --- Logging Setup ---
logging.basicConfig(filename='bot.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Bot and FastAPI Setup ---
bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()

# --- Google Sheets Setup ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.metadata.readonly']
try:
    creds_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_KEY)
    logging.debug("Successfully parsed GOOGLE_SERVICE_ACCOUNT_KEY")
except json.JSONDecodeError as e:
    logging.error(f"Invalid JSON in GOOGLE_SERVICE_ACCOUNT_KEY: {e}")
    creds_info = None
CREDS = Credentials.from_service_account_info(creds_info, scopes=SCOPES) if creds_info else None
SHEETS_SERVICE = build('sheets', 'v4', credentials=CREDS) if CREDS else None
DRIVE_SERVICE = build('drive', 'v3', credentials=CREDS) if CREDS else None

if not SHEETS_SERVICE or not DRIVE_SERVICE:
    logging.critical("Google services failed to initialize")

# --- Data Structures ---
main_buttons = []
submenus = {}
texts = {}
main_menu = None
last_modified_time = None
cache = TTLCache(maxsize=1, ttl=300)  # Cache for 5 minutes
rate_limit = defaultdict(list)

# --- SQLite for User Sessions ---
def init_db():
    conn = sqlite3.connect("sessions.db")
    conn.execute("CREATE TABLE IF NOT EXISTS sessions (user_id INTEGER PRIMARY KEY, expiry REAL)")
    conn.commit()
    conn.close()
    logging.debug("SQLite database initialized")

def has_access(user_id):
    conn = sqlite3.connect("sessions.db")
    cursor = conn.execute("SELECT expiry FROM sessions WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    conn.close()
    if row and row[0] > time.time():
        logging.debug(f"User {user_id} has valid session")
        return True
    logging.debug(f"User {user_id} has no valid session")
    return False

async def grant_access(user_id):
    expiry = time.time() + 1800  # 30 minutes
    conn = sqlite3.connect("sessions.db")
    conn.execute("INSERT OR REPLACE INTO sessions (user_id, expiry) VALUES (?, ?)", (user_id, expiry))
    conn.commit()
    conn.close()
    logging.debug(f"Granted access to user {user_id} until {time.ctime(expiry)}")

# --- Google Sheets Data Loading ---
def sanitize_text(text: str, sanitize=True) -> str:
    if sanitize:
        return re.sub(r'[^\w\s-]', '', text.strip())[:100]  # Remove special chars, limit length
    return text.strip()[:100]

async def load_guides(force=False):
    global main_buttons, submenus, texts, main_menu, last_modified_time
    cache_key = "guides_data"
    if cache.get(cache_key) and not force:
        logging.info("Using cached Google Sheets data")
        main_buttons, submenus, texts, main_menu = cache[cache_key]
        logging.debug(f"Cached main_buttons: {main_buttons}, submenus: {submenus}")
        return

    if not SHEETS_SERVICE or not DRIVE_SERVICE:
        logging.error("Google services not initialized. Check GOOGLE_SERVICE_ACCOUNT_KEY.")
        return

    max_retries = 3
    for attempt in range(max_retries):
        try:
            logging.info(f"Attempt {attempt + 1}/{max_retries} to load guides for SHEET_ID: {config.SHEET_ID}")
            if not force:
                file_metadata = DRIVE_SERVICE.files().get(fileId=config.SHEET_ID, fields='modifiedTime').execute()
                current_modified_time = file_metadata.get('modifiedTime')
                logging.info(f"Current modified time: {current_modified_time}")
                if last_modified_time is not None and last_modified_time == current_modified_time:
                    logging.info("No changes detected in Google Sheets.")
                    return
                last_modified_time = current_modified_time

            result = SHEETS_SERVICE.spreadsheets().values().get(spreadsheetId=config.SHEET_ID, range=config.RANGE_NAME).execute()
            values = result.get('values', [])
            logging.debug(f"Raw Google Sheets data: {values}")
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
                    logging.debug(f"Skipping incomplete row: {row}")
                    continue
                parent = sanitize_text(row[0], sanitize=False) if row[0] else None
                button = sanitize_text(row[1], sanitize=False)
                text = sanitize_text(row[2], sanitize=False) if row[2] else "Текст не найден в Google Sheets."
                texts[button] = text
                if not parent:
                    main_buttons.append(button)
                else:
                    if parent not in submenus:
                        submenus[parent] = []
                    submenus[parent].append(button)
            buttons = [[KeyboardButton(text=btn)] for btn in main_buttons]
            main_menu = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)
            cache[cache_key] = (main_buttons, submenus, texts, main_menu)
            logging.info(f"Loaded main_buttons: {main_buttons}")
            logging.info(f"Loaded submenus: {submenus}")
            logging.info(f"Loaded texts: {texts}")
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

def build_submenu_inline(parent):
    if parent not in submenus:
        logging.debug(f"No submenu found for parent: {parent}")
        return None
    subs = submenus[parent]
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for btn in subs:
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=btn, callback_data=f"sub_{btn}")])
    logging.debug(f"Built submenu for {parent}: {subs}")
    return keyboard

# --- Message Management ---
last_messages = {}

async def clear_old_messages(message_or_callback: types.Message | types.CallbackQuery):
    user_id = message_or_callback.from_user.id
    if isinstance(message_or_callback, types.Message):
        current_message_id = message_or_callback.message_id
    else:
        current_message_id = message_or_callback.message.message_id
    if user_id in last_messages and last_messages[user_id]:
        try:
            last_message_id = last_messages[user_id][-1]  # Delete only the most recent
            await bot.delete_message(user_id, last_message_id)
            logging.debug(f"Deleted message {last_message_id} for user {user_id}")
        except Exception as e:
            logging.error(f"Failed to delete message {last_message_id}: {e}")
        last_messages[user_id] = []
    try:
        await bot.delete_message(user_id, current_message_id)
        logging.debug(f"Deleted triggering message {current_message_id} for user {user_id}")
    except Exception as e:
        logging.error(f"Failed to delete triggering message {current_message_id}: {e}")

# --- Periodic Reload ---
async def periodic_reload():
    global last_modified_time
    while True:
        if DRIVE_SERVICE and config.SHEET_ID:
            try:
                file_metadata = DRIVE_SERVICE.files().get(fileId=config.SHEET_ID, fields='modifiedTime').execute()
                current_modified_time = file_metadata.get('modifiedTime')
                if current_modified_time and (last_modified_time is None or last_modified_time != current_modified_time):
                    logging.info(f"Change detected at {current_modified_time}. Reloading guides...")
                    await load_guides(force=True)
                last_modified_time = current_modified_time
            except Exception as e:
                logging.error(f"Error checking modified time: {e}")
        await asyncio.sleep(300)  # Check every 5 minutes

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
            logging.debug(f"Sent error message to user {user_id}: No main menu loaded")
            return
        sent = await message.answer("Главное меню:", reply_markup=main_menu)
        last_messages[user_id] = [sent.message_id]
        logging.debug(f"Sent main menu to user {user_id}")
    else:
        sent = await message.answer("Введите код доступа.")
        last_messages[user_id] = [sent.message_id]
        logging.debug(f"Prompted user {user_id} for passcode")

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
        logging.debug(f"Denied reload for non-admin user {user_id}")
        return
    await clear_old_messages(message)
    await load_guides(force=True)
    if main_menu:
        sent = await message.answer("Guides reloaded from Google Sheets.", reply_markup=main_menu)
        last_messages[user_id] = [sent.message_id]
        logging.debug(f"Reloaded guides for admin {user_id}")
    else:
        sent = await message.answer("Ошибка при перезагрузке guides. Проверьте настройки Google Sheets.")
        last_messages[user_id] = [sent.message_id]
        logging.debug(f"Failed to reload guides for admin {user_id}")

@dp.message()
async def main_handler(message: types.Message):
    user_id = message.from_user.id
    rate_limit[user_id] = [t for t in rate_limit[user_id] if time.time() - t < 60]
    if len(rate_limit[user_id]) >= 10:
        await message.answer("Слишком много запросов. Попробуйте позже.")
        logging.debug(f"Rate limit exceeded for user {user_id}")
        return
    rate_limit[user_id].append(time.time())
    
    if not hasattr(message, 'text'):
        sent = await message.answer("Неизвестная команда. Используйте кнопки ⬇️", reply_markup=main_menu)
        last_messages[user_id] = [sent.message_id]
        logging.debug(f"Non-text message received from user {user_id}")
        return
    txt = message.text.strip()
    logging.debug(f"Received message from user {user_id}: {txt}, main_buttons: {main_buttons}")
    await clear_old_messages(message)
    sent_messages = []

    if not has_access(user_id):
        if txt == "infobot":
            await grant_access(user_id)
            if not main_menu:
                sent = await message.answer("Ошибка: Кнопки не загружены из Google Sheets. Попробуйте позже или используйте /reload.", reply_markup=main_menu)
                sent_messages.append(sent.message_id)
                logging.debug(f"Passcode accepted but no main menu for user {user_id}")
            else:
                sent = await message.answer("Доступ предоставлен на 30 минут. Главное меню:", reply_markup=main_menu)
                sent_messages.append(sent.message_id)
                logging.debug(f"Passcode accepted, sent main menu to user {user_id}")
            try:
                await bot.delete_message(user_id, message.message_id)
                logging.debug(f"Deleted passcode message for user {user_id}")
            except Exception as e:
                logging.error(f"Failed to delete passcode message from {user_id}: {e}")
        else:
            sent = await message.answer("Неверный код доступа. Введите код доступа.")
            sent_messages.append(sent.message_id)
            logging.debug(f"Invalid passcode from user {user_id}: {txt}")
    else:
        if txt in main_buttons:
            logging.debug(f"Valid button pressed by user {user_id}: {txt}")
            if txt in submenus:
                submenu = build_submenu_inline(txt)
                if submenu:
                    sent = await message.answer(f"Выберите опцию для {txt}:", reply_markup=submenu)
                    sent_messages.append(sent.message_id)
                    logging.debug(f"Sent submenu for {txt} to user {user_id}")
                else:
                    sent = await message.answer(f"Ошибка: Подменю для {txt} не найдено.", reply_markup=main_menu)
                    sent_messages.append(sent.message_id)
                    logging.debug(f"No submenu found for {txt} for user {user_id}")
            else:
                guide_text = texts.get(txt, "Текст не найден в Google Sheets.").strip()
                # Extract and send all image URLs
                max_retries = 2
                for url in re.findall(r'https?://[^\s]+', guide_text):
                    if any(url.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.svg', '.pdf', '.gif']):
                        for attempt in range(max_retries + 1):
                            try:
                                sent = await message.answer_photo(url, reply_markup=main_menu)
                                sent_messages.append(sent.message_id)
                                logging.debug(f"Sent image attachment {url} to user {user_id}")
                                break
                            except Exception as e:
                                logging.error(f"Failed to send image {url} to user {user_id}, attempt {attempt + 1}/{max_retries + 1}: {e}")
                                if attempt < max_retries:
                                    await asyncio.sleep(1 + 2 ** attempt)
                # Send remaining text if any
                text_without_urls = re.sub(r'https?://[^\s]+', '', guide_text).strip()
                if text_without_urls:
                    sent = await message.answer(text_without_urls, reply_markup=main_menu)
                    sent_messages.append(sent.message_id)
                    logging.debug(f"Sent remaining text to user {user_id}: {text_without_urls}")
        else:
            sent = await message.answer("Пожалуйста, используйте кнопки ⬇️", reply_markup=main_menu)
            sent_messages.append(sent.message_id)
            logging.debug(f"Invalid button {txt} from user {user_id}, main_buttons: {main_buttons}")
    last_messages[user_id] = sent_messages

@dp.callback_query()
async def process_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    rate_limit[user_id] = [t for t in rate_limit[user_id] if time.time() - t < 60]
    if len(rate_limit[user_id]) >= 10:
        await callback.message.answer("Слишком много запросов. Попробуйте позже.")
        logging.debug(f"Rate limit exceeded for user {user_id}")
        await callback.answer()
        return
    rate_limit[user_id].append(time.time())
    
    logging.info(f"Received callback from {user_id} with data: {callback.data}")
    if not has_access(user_id):
        await callback.message.answer("Доступ истек. Введите код доступа.", reply_markup=main_menu)
        logging.debug(f"Access expired for user {user_id}")
        await callback.answer()
        return
    await clear_old_messages(callback)
    data = callback.data
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            if data.startswith("sub_"):
                subbutton = data[4:]
                logging.info(f"Processing subbutton {subbutton} for user {user_id}")
                guide_text = texts.get(subbutton, "Текст не найден в Google Sheets.").strip()
                sent_messages = []
                # Extract and send all image URLs
                for url in re.findall(r'https?://[^\s]+', guide_text):
                    if any(url.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.svg', '.pdf', '.gif']):
                        for attempt in range(max_retries + 1):
                            try:
                                sent = await callback.message.answer_photo(url, reply_markup=main_menu)
                                sent_messages.append(sent.message_id)
                                logging.debug(f"Sent image attachment {url} for subbutton {subbutton} to user {user_id}")
                                break
                            except Exception as e:
                                logging.error(f"Failed to send image {url} to user {user_id}, attempt {attempt + 1}/{max_retries + 1}: {e}")
                                if attempt < max_retries:
                                    await asyncio.sleep(1 + 2 ** attempt)
                # Send remaining text if any
                text_without_urls = re.sub(r'https?://[^\s]+', '', guide_text).strip()
                if text_without_urls:
                    sent = await callback.message.answer(text_without_urls, reply_markup=main_menu)
                    sent_messages.append(sent.message_id)
                    logging.debug(f"Sent remaining text for subbutton {subbutton} to user {user_id}: {text_without_urls}")
                last_messages[user_id] = sent_messages
                logging.info(f"Processed callback for subbutton {subbutton} and sent response to {user_id}")
            await callback.answer()
            break
        except Exception as e:
            logging.error(f"Callback error for user {user_id}, attempt {attempt + 1}/{max_retries + 1}: {e}")
            if attempt < max_retries:
                await asyncio.sleep(1 + 2 ** attempt)
            else:
                await callback.message.answer("Ошибка обработки. Попробуйте снова.", reply_markup=main_menu)
                logging.debug(f"Callback error for user {user_id} after retries")
                await callback.answer()

# --- Webhook Handler ---
@app.post(config.WEBHOOK_PATH)
async def webhook_handler(request: Request):
    try:
        update = await request.json()
        logging.info(f"Received update at {time.strftime('%H:%M:%S')}: {update}")
        if 'message' in update or 'callback_query' in update:
            asyncio.create_task(dp.feed_update(bot, types.Update(**update)))
        else:
            logging.info("Update does not contain a message or callback_query field, skipping.")
        return {"ok": True}
    except Exception as e:
        logging.error(f"Error processing webhook at {time.strftime('%H:%M:%S')}: {e}")
        return {"ok": False, "error": str(e)}, 500

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
    asyncio.create_task(periodic_reload())

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=config.PORT)