import os
import asyncio
import json
import time
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.enums import ParseMode
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
            error_details = e._get_reason()
            print(f"HTTP Error {e.resp.status} on attempt {attempt + 1}: {error_details}")
            print(f"Error details: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5 + 2 ** attempt)
            else:
                print("Max retries reached. Failed to load guides.")
                main_menu = None
        except Exception as e:
            print(f"Unexpected error on attempt {attempt + 1): {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5 + 2 ** attempt)
            else:
                print("Max retries reached. Failed to load guides.")
                main_menu = None

# Build inline keyboard for subbuttons
def build_submenu_inline(parent):
    if parent not in submenus:
        return None
    subs = submenus[parent]
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for btn in subs:
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=btn, callback_data=f"sub_{btn}")])
    return keyboard

# Store all bot and user message IDs for each user
last_messages = {}  # {user_id: [message_ids]}

async def clear_old_messages(message_or_callback: types.Message | types.CallbackQuery):
    if isinstance(message_or_callback, types.Message):
        user_id = message_or_callback.from_user.id
    else:  # CallbackQuery
        user_id = message_or_callback.from_user.id
    if user_id in last_messages and last_messages[user_id]:
        msg_ids = last_messages[user_id].copy()
        for i, msg_id in enumerate(msg_ids):
            try:
                await bot.delete_message(user_id, msg_id)
                await asyncio.sleep(0.2 * (i % 5))  # Dust-like animation
            except Exception as e:
                print(f"Failed to delete message {msg_id}: {e}")
        last_messages[user_id] = []
    # Clear user messages (approximate based on chat history)
    try:
        chat_history = await bot.get_chat_history(user_id, limit=10)
        for msg in chat_history:
            if msg.from_user and msg.from_user.is_bot:
                continue  # Skip bot messages (already handled)
            await bot.delete_message(user_id, msg.message_id)
    except Exception as e:
        print(f"Failed to clear user messages: {e}")

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
    ADMIN_ID = 123456789  # CHANGE TO YOUR ID
    if user_id != ADMIN_ID:
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
    if not hasattr(message, 'text'):
        sent = await message.answer("Неизвестная команда. Используйте кнопки ⬇️", reply_markup=main_menu)
        last_messages[message.from_user.id] = [sent.message_id]
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
        if txt in main_buttons:
            await clear_old_messages(message)
            if txt in submenus:  # Has subbuttons
                submenu = build_submenu_inline(txt)
                if submenu:
                    sent = await message.answer(f"Выберите опцию для {txt}:", reply_markup=submenu)
                    sent_messages.append(sent.message_id)
                else:
                    sent = await message.answer(f"Ошибка: Подменю для {txt} не найдено.", reply_markup=main_menu)
                    sent_messages.append(sent.message_id)
            else:  # No subs, send text or attachments
                guide_text = texts.get(txt, "Текст не найден в Google Sheets.").strip()
                lines = guide_text.split('\n')
                for line in lines:
                    line = line.strip()
                    if any(line.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.svg', '.pdf', '.gif']):
                        sent = await message.answer_document(line, caption="Attached file")
                        sent_messages.append(sent.message_id)
                    else:
                        if sent_messages:  # Append text after attachments
                            sent = await message.answer(line, reply_markup=main_menu)
                        else:  # First non-attachment line
                            sent = await message.answer(line, reply_markup=main_menu)
                        sent_messages.append(sent.message_id)
        else:
            sent = await message.answer("Пожалуйста, используйте кнопки ⬇️", reply_markup=main_menu)
            sent_messages.append(sent.message_id)

    last_messages[user_id] = sent_messages

# Handle inline button callbacks
@dp.callback_query()
async def process_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await clear_old_messages(callback)
    data = callback.data
    if data.startswith("sub_"):
        subbutton = data[4:]  # Extract subbutton text
        guide_text = texts.get(subbutton, "Текст не найден в Google Sheets.").strip()
        sent_messages = []
        lines = guide_text.split('\n')
        for line in lines:
            line = line.strip()
            if any(line.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.svg', '.pdf', '.gif']):
                sent = await callback.message.answer_document(line, caption="Attached file")
                sent_messages.append(sent.message_id)
            else:
                if sent_messages:  # Append text after attachments
                    sent = await callback.message.answer(line, reply_markup=main_menu)
                else:  # First non-attachment line
                    sent = await callback.message.answer(line, reply_markup=main_menu)
                sent_messages.append(sent.message_id)
        last_messages[user_id] = sent_messages
    await callback.answer()  # Acknowledge the callback

# --- FastAPI webhook handler ---
@app.post(WEBHOOK_PATH)
async def webhook_handler(request: Request):
    try:
        update = await request.json()
        print(f"Received update: {update}")
        if 'message' in update:
            await dp.feed_update(bot, types.Update(**update))
        elif 'callback_query' in update:
            await dp.feed_update(bot, types.Update(**update))
        else:
            print("Update does not contain a message or callback_query field, skipping.")
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

# --- Helper Functions ---
def has_access(user_id):
    if user_id in user_sessions:
        if time.time() < user_sessions[user_id]:
            return True
        else:
            del user_sessions[user_id]
    return False

async def grant_access(user_id):
    user_sessions[user_id] = time.time() + 1800  # 30 minutes in seconds

# Global session storage
user_sessions = {}