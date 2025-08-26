import asyncio
import logging
import os
import re
import traceback
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse

from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.types import (Message, CallbackQuery, InlineKeyboardMarkup,
                           InlineKeyboardButton, ReplyKeyboardMarkup,
                           KeyboardButton)
from aiogram.filters import CommandStart
from aiogram.exceptions import TelegramRetryAfter
from aiogram import types

# =========================
# Config & Logging
# =========================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var is required")

# Render typically sets PORT; we don't need it explicitly for FastAPI here
WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH", "/webhook")
# (Optional) If you host behind a fixed external URL, you can set:
# WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # used when you want setWebhook elsewhere

# Access control (very simple demo)
ACCESS_TTL_MINUTES = int(os.environ.get("ACCESS_TTL_MINUTES", "1440"))  # 24h by default

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("bot")

# =========================
# Bot & App
# =========================

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()


# =========================
# Demo Data Layer (replace with your Google Sheets loader)
# =========================
"""
We strongly recommend using SHORT, STABLE IDs for callback_data.
Example "catalog" below: each item has ("id", "label", "text").
Replace `load_catalog()` with your actual Sheets/GSheets loader, but keep the
shape: Dict[id] = (label, text).
"""

def load_catalog() -> Dict[str, Tuple[str, str]]:
    # TODO: Replace with your real loader.
    # Example content:
    return {
        "intro": ("Введение", "Добро пожаловать!\nhttps://example.com/file.pdf"),
        "faq": ("FAQ", "Частые вопросы."),
        "help": ("Помощь", "Опишите вашу проблему, и мы поможем."),
    }

CATALOG: Dict[str, Tuple[str, str]] = load_catalog()

# texts: id -> text (so we never key by the visible label)
TEXTS: Dict[str, str] = {k: v[1] for k, v in CATALOG.items()}


# =========================
# Keyboards
# =========================

def build_main_menu() -> ReplyKeyboardMarkup:
    # Keep reply keyboard lightweight; show only when needed
    kb = [
        [KeyboardButton(text="Меню")],
    ]
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=kb)

def build_inline_menu() -> InlineKeyboardMarkup:
    # Build inline keyboard using short IDs to stay <64 bytes
    buttons: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for idx, (item_id, (label, _text)) in enumerate(CATALOG.items(), start=1):
        row.append(InlineKeyboardButton(text=label, callback_data=f"sub:{item_id}"))
        if idx % 2 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# =========================
# Helpers
# =========================

def is_http_url(s: str) -> bool:
    try:
        u = urlparse(s)
        return u.scheme in ("http", "https") and bool(u.netloc)
    except Exception:
        return False

def split_lines(text: str) -> List[str]:
    return [ln.strip() for ln in (text or "").splitlines() if ln.strip()]

async def safe_delete_message(chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception as e:
        # Ignore delete errors; just log
        log.debug(f"Delete failed for {chat_id}/{message_id}: {e}")

async def clear_old_messages(
    chat_id: int,
    message_ids: List[int],
    skip_ids: Optional[set] = None,
    throttle_seconds: float = 0.25,
) -> None:
    """
    Delete a list of messages, skipping those in skip_ids.
    Use small throttling to avoid flood limits.
    """
    skip_ids = skip_ids or set()
    for mid in message_ids:
        if mid in skip_ids:
            continue
        await safe_delete_message(chat_id, mid)
        await asyncio.sleep(throttle_seconds)


# Extremely simple "access" stub—replace with your real logic
# e.g., check a user record with an expiry timestamp
def has_access(user_id: int) -> bool:
    # Implement your real check
    return True


# =========================
# Handlers
# =========================

@dp.message(CommandStart())
async def on_start(message: Message):
    await message.answer(
        "Привет! Нажмите «Меню», чтобы открыть разделы.",
        reply_markup=build_main_menu(),
    )

@dp.message(F.text.casefold() == "меню")
async def on_menu(message: Message):
    await message.answer(
        "Выберите раздел:",
        reply_markup=build_main_menu(),
    )
    await message.answer(
        "Доступные разделы:",
        reply_markup=build_inline_menu(),  # show inline keyboard separately
    )

@dp.callback_query()
async def process_callback(callback: CallbackQuery):
    """
    Fixes applied:
    - We do NOT delete the keyboard message before responding.
    - We handle unknown/invalid callback_data with a friendly message.
    - We only treat real HTTP(S) links as attachable documents.
    - We attach the reply keyboard only when useful (not every time).
    - We log full tracebacks for debugging.
    """
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id if callback.message else None

    try:
        data = (callback.data or "").strip()
        # Always answer the callback to stop the "loading" animation
        # but only AFTER we know what we're doing if you want tighter UX.
        # Here we answer near the end as well.
        if not has_access(user_id):
            await callback.message.answer(
                "Доступ истек. Введите код доступа или свяжитесь с администратором.",
                reply_markup=build_main_menu(),
            )
            await callback.answer()
            return

        if not data.startswith("sub:"):
            await callback.message.answer(
                "Команда кнопки не распознана. Попробуйте ещё раз через «Меню».",
                reply_markup=build_main_menu(),
            )
            await callback.answer()
            return

        item_id = data.split("sub:", 1)[1]
        guide_text = TEXTS.get(item_id)
        if not guide_text:
            await callback.message.answer(
                "Эта кнопка пока ничего не делает. Попробуйте другой раздел.",
                reply_markup=build_main_menu(),
            )
            await callback.answer()
            return

        # Send the main text first
        sent_messages: List[int] = []
        main = await callback.message.answer(guide_text)
        sent_messages.append(main.message_id)

        # Optionally parse lines for auto-attachments (http/https only)
        for line in split_lines(guide_text):
            lower = line.lower()
            if lower.endswith((".png", ".jpg", ".jpeg", ".svg", ".pdf", ".gif")) and is_http_url(line):
                try:
                    doc = await callback.message.answer_document(line)
                    sent_messages.append(doc.message_id)
                except TelegramRetryAfter as e:
                    # honor floodwait
                    await asyncio.sleep(int(e.retry_after) + 1)
                    doc = await callback.message.answer_document(line)
                    sent_messages.append(doc.message_id)
                except Exception as e:
                    log.warning(f"Attachment send failed for URL '{line}': {e}")

        # Answer callback to stop loader
        await callback.answer()

        # Now it is SAFE to clean up older messages.
        # We will remove ONLY older bot messages in the chat that are adjacent to this flow.
        # If you maintain your own registry of sent IDs, use it here.
        # Below is a conservative cleanup: delete the message that contained the inline keyboard
        # *only after* we've successfully sent the response.
        if callback.message:
            try:
                await safe_delete_message(chat_id, callback.message.message_id)
            except Exception as e:
                log.debug(f"Could not delete keyboard message: {e}")

        # (Optional) If you keep track of previous bot messages for this user, delete them here.
        # Example stub: nothing else to delete.

    except Exception as e:
        log.error(f"Callback error for user {user_id}: {e}")
        traceback.print_exc()
        try:
            if callback.message:
                await callback.message.answer(
                    "Ошибка обработки запроса. Пожалуйста, попробуйте ещё раз.",
                    reply_markup=build_main_menu(),
                )
        finally:
            try:
                await callback.answer()
            except Exception:
                pass


# A generic text handler to gently guide users back to the menu
@dp.message()
async def on_fallback(message: Message):
    await message.answer(
        "Не понял запрос. Нажмите «Меню», чтобы выбрать раздел.",
        reply_markup=build_main_menu(),
    )
    await message.answer(
        "Доступные разделы:",
        reply_markup=build_inline_menu(),
    )


# =========================
# FastAPI Webhook
# =========================

@app.post(WEBHOOK_PATH)
async def webhook_handler(request: Request):
    """
    Simpler and safer: forward ALL updates to aiogram.
    """
    update_dict = await request.json()
    try:
        await dp.feed_update(bot, types.Update(**update_dict))
    except Exception as e:
        log.error(f"feed_update failed: {e}")
        traceback.print_exc()
    return {"ok": True}


# Optional: healthcheck root
@app.get("/")
async def root():
    return {"status": "ok"}


# =========================
# Local dev runner (polling)
# =========================
if __name__ == "__main__":
    # For local debugging only (Render will run via ASGI server)
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
