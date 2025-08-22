import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command

TOKEN = "8213315181:AAGsNKktElZM_diFVqS_WXyeBWo22zQgdCQ"

bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- Главное меню (20 кнопок) ---
buttons = [[KeyboardButton(text=f"Кнопка {i}")] for i in range(1, 21)]
main_menu = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

# --- Подменю для кнопки 1 ---
submenu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Подкнопка 1.1")],
        [KeyboardButton(text="Подкнопка 1.2")],
        [KeyboardButton(text="⬅️ Назад")]
    ],
    resize_keyboard=True
)

# --- /start ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Привет! Вот главное меню:", reply_markup=main_menu)

# --- Обработка всех сообщений ---
@dp.message()
async def main_handler(message: types.Message):
    txt = message.text

    # --- Кнопка 1: подменю ---
    if txt == "Кнопка 1":
        await message.answer("Открываю подменю:", reply_markup=submenu)

    elif txt == "Подкнопка 1.1":
        await message.answer("Текст для подкнопки 1.1")

    elif txt == "Подкнопка 1.2":
        await message.answer("Текст для подкнопки 1.2")

    elif txt == "⬅️ Назад":
        await message.answer("Возврат в главное меню", reply_markup=main_menu)

    # --- Кнопка 2: отправка двух фото ---
    elif txt == "Кнопка 2":
        media = [
            types.InputMediaPhoto("https://picsum.photos/200/300"),
            types.InputMediaPhoto("https://picsum.photos/300/300")
        ]
        await message.answer_media_group(media)

    # --- Остальные кнопки (3–20): просто текст ---
    elif txt.startswith("Кнопка"):
        await message.answer(f"Вы нажали на {txt}. Здесь будет текст/гайд.")

    else:
        await message.answer("Пожалуйста, используйте кнопки ⬇️")

# --- Запуск ---
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
