import os
import asyncio
import json
import base64
import time
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from fastapi import FastAPI, Request
import uvicorn
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}{WEBHOOK_PATH}"
PORT = int(os.getenv("PORT", 10000))

bot = Bot(token=TOKEN)
dp = Dispatcher()
app = FastAPI()

# --- Google Sheets Setup ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.metadata.readonly']
SERVICE_ACCOUNT_KEY = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
try:
    creds_info = json.loads(SERVICE_ACCOUNT_KEY)  # Using raw JSON
except json.JSONDecodeError as e:
    print(f"Invalid JSON in GOOGLE_SERVICE_ACCOUNT_KEY: {e}")
    creds_info = None
CREDS = Credentials.from_service_account_info(creds_info, scopes=SCOPES) if creds_info else None
SHEETS_SERVICE = build('sheets', 'v4', credentials=CREDS) if CREDS else None
DRIVE_SERVICE = build('drive', 'v3', credentials=CREDS) if CREDS else None
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
RANGE_NAME = "Guides!A:C"  # Columns A (Parent), B (Button), C (Text)

print(f"CREDS: {CREDS}")
print(f"SHEETS_SERVICE: {SHEETS_SERVICE}")
print(f"DRIVE_SERVICE: {DRIVE_SERVICE}")
print(f"SHEET_ID: {SHEET_ID}")

# Data structures
main_buttons = []  # List of main button names
submenus = {}  # {parent: [subbutton names]}
texts = {}  # {button: text}
main_menu = None  # Dynamic main keyboard
last_modified_time = None  # Track last modified time of the sheet

async def load_guides(force=False):
    global main_buttons, submenus, texts, main_menu, last_modified_time
    if not SHEETS_SERVICE or not DRIVE_SERVICE:
        print("Google services not initialized. Check GOOGLE_SERVICE_ACCOUNT_KEY.")
        return

    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} to load guides for SHEET_ID: {SHEET_ID}")
            # Check last modified time using Drive API
            if not force:
                file_metadata = DRIVE_SERVICE.files().get(fileId=SHEET_ID, fields='modifiedTime').execute()
                current_modified_time = file_metadata.get('modifiedTime')
                print(f"Current modified time: {current_modified_time}")
                if last_modified_time and last_modified_time == current_modified_time:
                    print("No changes detected in Google Sheets.")
                    return
                last_modified_time = current_modified_time

            # Load data from Sheets API
            result = SHEETS_SERVICE.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=RANGE_NAME).execute()
            values = result.get('values', [])
            if not values:
                print(f"Warning: No data found in range {RANGE_NAME} of sheet {SHEET_ID}.")
                return
            if len(values[0]) < 3 or values[0][1].lower() == "button":
                values = values[1:]
            main_buttons = []
            submenus = {}
            texts = {}
            for row in values:
                if len(row) < 3:
                    continue
                parent = row[0].strip() if row[0] else None
                button = row[1].strip()
                text = row[2].strip() if row[2] else None
                texts[button] = text or "Текст не найден в Google Sheets."
                if not parent:
                    main_buttons.append(button)
                else:
                    if parent not in submenus:
                        submenus[parent] = []
                    submenus[parent].append(button)
            buttons = []
            row_buttons = []
            for btn in main_buttons:
                row_buttons.append(KeyboardButton(text=btn))
                if len(row_buttons) == 5:
                    buttons.append(row_buttons)
                    row_buttons = []
            if row_buttons:
                buttons.append(row_buttons)
            main_menu = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)
            print(f"Loaded main_buttons: {main_buttons}")
            print(f"Loaded submenus: {submenus}")
            print(f"Loaded texts: {texts}")
            return
        except HttpError as e:
            print(f"HTTP Error {e.resp.status} on attempt {attempt + 1}: {e._get_reason()}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5 + 2 ** attempt)
            else:
                print("Max retries reached. Failed to load guides.")
                main_menu = None
        except Exception as e:
            print(f"Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5 + 2 ** attempt)
            else:
                print("Max retries reached. Failed to load guides.")
                main_menu = None

# Build submenu keyboard for a parent
def build_submenu(parent):
    if parent not in submenus:
        return None
    subs = submenus[parent]
    buttons = []
    row_buttons = []
    for btn in subs:
        row_buttons.append(KeyboardButton(text=btn))
        if len(row_buttons) == 5:
            buttons.append(row_buttons)
            row_buttons = []
    if row_buttons:
        buttons.append(row_buttons)
    buttons.append([KeyboardButton(text="⬅️ Назад")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

# Store all bot message IDs for each user
last_bot_messages = {}

# User sessions: user_id: expiration_time
user_sessions = {}

ACCESS_CODE = "infobot"
SESSION_DURATION = 1800  # 30 minutes in seconds

def has_access(user_id):
    if user_id in user_sessions:
        if time.time() < user_sessions[user_id]:
            return True
        else:
            del user_sessions[user_id]
    return False

async def grant_access(user_id):
    user_sessions[user_id] = time.time() + SESSION_DURATION

async def clear_old_messages(message: types.Message):
    if not hasattr(message, 'from_user') or not hasattr(message.from_user, 'id'):
        print("Warning: No user_id available in message.")
        return
    user_id = message.from_user.id
    if user_id in last_bot_messages and last_bot_messages[user_id]:
        msg_ids = last_bot_messages[user_id].copy()
        for i, msg_id in enumerate(msg_ids):
            try:
                await bot.delete_message(user_id, msg_id)
                await asyncio.sleep(0.2 * (i % 5))
            except Exception as e:
                print(f"Failed to delete message {msg_id}: {e}")
        last_bot_messages[user_id] = []

async def periodic_reload():
    while True:
        await load_guides()
        await asyncio.sleep(10)

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    await clear_old_messages(message)
    if has_access(user_id):
        if not main_menu:
            await message.answer("Ошибка: Кнопки не загружены из Google Sheets. Попробуйте позже или используйте /reload.")
            return
        sent = await message.answer("Главное меню:", reply_markup=main_menu)
        last_bot_messages[user_id] = [sent.message_id]
    else:
        sent = await message.answer("Введите код доступа.")
        last_bot_messages[user_id] = [sent.message_id]

@dp.message(Command("reload"))
async def cmd_reload(message: types.Message):
    user_id = message.from_user.id
    ADMIN_ID = 123456789  # CHANGE TO YOUR ID
    if user_id != ADMIN_ID:
        await message.answer("Доступ запрещен.")
        return
    await load_guides(force=True)
    if main_menu:
        await message.answer("Guides reloaded from Google Sheets.")
    else:
        await message.answer("Ошибка при перезагрузке guides. Проверьте настройки Google Sheets.")

@dp.message()
async def main_handler(message: types.Message):
    if not hasattr(message, 'text'):
        await message.answer("Неизвестная команда. Используйте кнопки ⬇️", reply_markup=main_menu)
        return
    user_id = message.from_user.id
    txt = message.text.strip()
    await clear_old_messages(message)
    sent_messages = []

    if not has_access(user_id):
        if txt == ACCESS_CODE:
            await grant_access(user_id)
            if not main_menu:
                sent = await message.answer("Ошибка: Кнопки не загружены из Google Sheets. Попробуйте позже или используйте /reload.")
                sent_messages.append(sent.message_id)
            else:
                sent = await message.answer("Доступ предоставлен на 30 минут. Главное меню:", reply_markup=main_menu)
                sent_messages.append(sent.message_id)
        else:
            sent = await message.answer("Неверный код доступа. Введите код доступа.")
            sent_messages.append(sent.message_id)
    else:
        if txt == "⬅️ Назад":
            sent = await message.answer("Главное меню:", reply_markup=main_menu)
            sent_messages.append(sent.message_id)
        elif txt in main_buttons:
            if txt in submenus:
                submenu = build_submenu(txt)
                if submenu:
                    sent = await message.answer(f"Подменю для {txt}:", reply_markup=submenu)
                    sent_messages.append(sent.message_id)
                else:
                    sent = await message.answer(f"Ошибка: Подменю для {txt} не найдено.", reply_markup=main_menu)
                    sent_messages.append(sent.message_id)
            else:
                guide_text = texts.get(txt, "Текст не найден в Google Sheets.")
                sent = await message.answer(guide_text, reply_markup=main_menu)
                sent_messages.append(sent.message_id)
        elif any(txt in subs for subs in submenus.values()):
            guide_text = texts.get(txt, "Текст не найден в Google Sheets.")
            sent = await message.answer(guide_text, reply_markup=main_menu)
            sent_messages.append(sent.message_id)
        else:
            sent = await message.answer("Пожалуйста, используйте кнопки ⬇️", reply_markup=main_menu)
            sent_messages.append(sent.message_id)

    last_bot_messages[user_id] = sent_messages

# --- FastAPI webhook handler ---
@app.post(WEBHOOK_PATH)
async def webhook_handler(request: Request):
    try:
        update = await request.json()
        print(f"Received update: {update}")
        if 'message' in update:
            await dp.feed_update(bot, types.Update(**update))
        else:
            print("Update does not contain a message field, skipping.")
        return {"ok": True}
    except Exception as e:
        print(f"Error processing webhook: {e}")
        return {"ok": False, "error": str(e)}, 500

# --- Set webhook and start periodic reload on startup ---
@app.on_event("startup")
async def on_startup():
    await bot.set_webhook(WEBHOOK_URL)
    for attempt in range(3):
        await load_guides(force=True)
        if main_menu:
            break
        print(f"Initial load failed, retrying ({attempt + 1}/3)...")
        await asyncio.sleep(5 + 2 ** attempt)
    if not main_menu:
        print("Failed to load guides after retries. Service may be unstable.")
    asyncio.create_task(periodic_reload())

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT)