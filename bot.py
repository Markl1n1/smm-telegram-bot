import os
import asyncio
import json
import base64
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from fastapi import FastAPI, Request
import uvicorn
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}{WEBHOOK_PATH}"
PORT = int(os.getenv("PORT", 10000))

bot = Bot(token=TOKEN)
dp = Dispatcher()
app = FastAPI()

# --- Google Sheets Setup ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_KEY = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
try:
    # Assume base64 encoded JSON; decode and load
    creds_info = json.loads(base64.b64decode(SERVICE_ACCOUNT_KEY).decode('utf-8'))
except (base64.binascii.Error, ValueError):
    # Fallback: try direct JSON string
    creds_info = json.loads(SERVICE_ACCOUNT_KEY) if SERVICE_ACCOUNT_KEY else None
CREDS = Credentials.from_service_account_info(creds_info, scopes=SCOPES) if creds_info else None
SHEETS_SERVICE = build('sheets', 'v4', credentials=CREDS) if CREDS else None
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
RANGE_NAME = "Guides!A:B"  # Columns A (Button) and B (Text)

# Cache guides and buttons
guides = {}
buttons_list = []  # List of button names from Sheets
main_menu = None  # Will be built dynamically

async def load_guides():
    global guides, buttons_list, main_menu
    if SHEETS_SERVICE:
        try:
            sheet = SHEETS_SERVICE.spreadsheets()
            result = sheet.values().get(spreadsheetId=SHEET_ID, range=RANGE_NAME).execute()
            values = result.get('values', [])
            # Skip first row if headers (assume row[0] like "Button" or empty)
            if values and (not values[0] or values[0][0].lower() == "button"):
                values = values[1:]
            guides = {row[0]: row[1] for row in values if len(row) >= 2}
            buttons_list = list(guides.keys())
            # Build dynamic keyboard: 4 buttons per row
            buttons = []
            row_buttons = []
            for btn in buttons_list:
                row_buttons.append(KeyboardButton(text=btn))
                if len(row_buttons) == 4:
                    buttons.append(row_buttons)
                    row_buttons = []
            if row_buttons:
                buttons.append(row_buttons)
            main_menu = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)
            print(f"Loaded guides: {guides}")
            print(f"Loaded buttons: {buttons_list}")
        except Exception as e:
            print(f"Error loading guides: {e}")
    else:
        print("Google Sheets service not initialized.")

last_bot_messages = {}

async def clear_old_messages(message: types.Message):
    user_id = message.from_user.id
    if user_id in last_bot_messages:
        for msg_id in last_bot_messages[user_id]:
            try:
                await bot.delete_message(user_id, msg_id)
            except Exception as e:
                print(f"Failed to delete message: {e}")
        last_bot_messages[user_id] = []

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await clear_old_messages(message)
    if not main_menu:
        await message.answer("Ошибка: Кнопки не загружены из Google Sheets.")
        return
    sent = await message.answer("Главное меню:", reply_markup=main_menu)
    last_bot_messages[message.from_user.id] = [sent.message_id]

@dp.message(Command("reload"))
async def cmd_reload(message: types.Message):
    # Restrict to admin (replace with your Telegram ID)
    ADMIN_ID = 123456789  # Change to your ID
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещен.")
        return
    await load_guides()
    await message.answer("Guides reloaded from Google Sheets.")

@dp.message()
async def main_handler(message: types.Message):
    txt = message.text
    await clear_old_messages(message)
    sent_messages = []

    if txt in guides:
        guide_text = guides[txt]
        sent = await message.answer(guide_text, reply_markup=main_menu)
        sent_messages.append(sent.message_id)
    else:
        sent = await message.answer("Пожалуйста, используйте кнопки ⬇️", reply_markup=main_menu)
        sent_messages.append(sent.message_id)

    last_bot_messages[message.from_user.id] = sent_messages

# --- FastAPI webhook handler ---
@app.post(WEBHOOK_PATH)
async def webhook_handler(request: Request):
    try:
        update = await request.json()
        print(f"Received update: {update}")
        await dp.feed_update(bot, types.Update(**update))
        return {"ok": True}
    except Exception as e:
        print(f"Error processing webhook: {e}")
        return {"ok": False, "error": str(e)}, 500

# --- Set webhook and load guides on startup ---
@app.on_event("startup")
async def on_startup():
    await bot.set_webhook(WEBHOOK_URL)
    await load_guides()

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT)