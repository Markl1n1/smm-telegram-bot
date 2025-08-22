import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command

TOKEN = "8213315181:AAGsNKktElZM_diFVqS_WXyeBWo22zQgdCQ"

bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- Главное меню (20 кнопок в сетке 5x4) ---
buttons = []
for row in range(5):  # 5 рядов
    row_buttons = []
    for col in range(4):  # 4 кнопки в ряду
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

# --- Хранилище ID последних сообщений (чтобы удалять) ---
last_bot_messages = {}

# --- Удаление старых сообщений ---
async def clear_old_messages(message: types.Message):
    user_id = message.from_user.id
    if user_id in last_bot_messages:
        for msg_id in last_bot_messages[user_id]:
            try:
                await bot.delete_message(user_id, msg_id)
            except:
                pass
    last_bot_messages[user_id] = []


# --- /start ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await clear_old_messages(message)
    sent = await message.answer("Главное меню:", reply_markup=main_menu)
    last_bot_messages[message.from_user.id] = [sent.message_id]


# --- Обработка кнопок ---
@dp.message()
async def main_handler(message: types.Message):
    txt = message.text
    await clear_old_messages(message)  # чистим предыдущие

    sent_messages = []

    # --- Кнопка 1: подменю ---
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

    # --- Кнопка 2: отправка двух фото ---
    elif txt == "Кнопка 2":
        media = [
            types.InputMediaPhoto("https://picsum.photos/200/300"),
            types.InputMediaPhoto("https://picsum.photos/300/300")
        ]
        msgs = await message.answer_media_group(media)
        sent_messages.extend([m.message_id for m in msgs])

    # --- Остальные кнопки (3–20): просто текст ---
    elif txt.startswith("Кнопка"):
        sent = await message.answer(f"Вы нажали на {txt}. Здесь будет текст/гайд.")
        sent_messages.append(sent.message_id)

    else:
        sent = await message.answer("Пожалуйста, используйте кнопки ⬇️")
        sent_messages.append(sent.message_id)

    # Сохраняем ID отправленных сообщений для последующего удаления
    last_bot_messages[message.from_user.id] = sent_messages


# --- Запуск ---
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
