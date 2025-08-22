import os
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from fastapi import FastAPI, Request
import uvicorn

TOKEN = os.getenv("BOT_TOKEN")  # токен берём из переменных окружения
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}{WEBHOOK_PATH}"  # Render подставит имя хоста
PORT = int(os.getenv("PORT", 10000))  # Use Render's PORT if set

bot = Bot(token=TOKEN)
dp = Dispatcher()
app = FastAPI()

# --- Главное меню (20 кнопок в сетке 5x4) ---
buttons = []
for row in range(5):  
    row_buttons = []
    for col in range(4):  
        num = row * 4 + col + 1
        row_buttons.append(KeyboardButton(text=f"Кнопка {num}"))
    buttons.append(row_buttons)

main_menu = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

# --- Подменю для кнопки 1 ---
submenu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Подкнопка 1.1"), KeyboardButton(text="Подкнопка 1.2")],
        [KeyboardButton(text="⬅️ Назад")]
    ],
    resize_keyboard=True
)

last_bot_messages = {}

async def clear_old_messages(message: types.Message):
    user_id = message.from_user.id
    if user_id in last_bot_messages:
        for msg_id in last_bot_messages[user_id]:
            try:
                await bot.delete_message(user_id, msg_id)
            except:
                pass
    last_bot_messages[user_id] = []

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await clear_old_messages(message)
    sent = await message.answer("Главное меню:", reply_markup=main_menu)
    last_bot_messages[message.from_user.id] = [sent.message_id]

@dp.message()
async def main_handler(message: types.Message):
    txt = message.text
    await clear_old_messages(message)
    sent_messages = []

    if txt == "Кнопка 1":
        sent = await message.answer(" ", reply_markup=submenu)
        sent_messages.append(sent.message_id)

    elif txt == "Подкнопка 1.1":
        sent = await message.answer("Текст для подкнопки 1.1")
        sent_messages.append(sent.message_id)

    elif txt == "Подкнопка 1.2":
        sent = await message.answer("Текст для подкнопки 1.2")
        sent_messages.append(sent.message_id)

    elif txt == "⬅️ Назад":
        sent = await message.answer(" ", reply_markup=main_menu)
        sent_messages.append(sent.message_id)

    elif txt == "Кнопка 2":
        media = [
            types.InputMediaPhoto("https://picsum.photos/200/300"),
            types.InputMediaPhoto("https://picsum.photos/300/300")
        ]
        msgs = await message.answer_media_group(media)
        sent_messages.extend([m.message_id for m in msgs])

    elif txt.startswith("Кнопка"):
        sent = await message.answer(f"Вы нажали на {txt}. Здесь будет текст/гайд.")
        sent_messages.append(sent.message_id)

    else:
        sent = await message.answer("Пожалуйста, используйте кнопки ⬇️")
        sent_messages.append(sent.message_id)

    last_bot_messages[message.from_user.id] = sent_messages

# --- FastAPI обработчик вебхуков ---
@app.post(WEBHOOK_PATH)
async def webhook_handler(request: Request):
    try:
        update = await request.json()
        print(f"Received update: {update}")  # Debug log
        await dp.feed_update(bot, types.Update(**update))
        return {"ok": True}
    except Exception as e:
        print(f"Error processing webhook: {e}")  # Error log
        return {"ok": False, "error": str(e)}, 500

# --- Установка вебхука при старте ---
@app.on_event("startup")
async def on_startup():
    await bot.set_webhook(WEBHOOK_URL)

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=PORT)