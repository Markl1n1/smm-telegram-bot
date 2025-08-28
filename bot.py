# api/webhook.py
import os
import asyncio
import json
import time
import logging
import re
import ssl
import hashlib
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, InputMediaVideo
)
from aiogram.filters import Command
from fastapi import FastAPI, Request, HTTPException
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from cachetools import TTLCache
from collections import OrderedDict
from dotenv import load_dotenv

# ---------- env ----------
load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logging.getLogger("aiogram.client.session").setLevel(logging.WARNING)

def _mask(s: Optional[str], keep_tail: int = 6) -> str:
    if not s:
        return "None"
    return "***" + s[-keep_tail:] if len(s) > keep_tail else "***"

TOKEN_RE = re.compile(r"\d+:[A-Za-z0-9_-]+")
def clean_token(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = raw.strip().strip('"').strip("'")
    m = TOKEN_RE.search(s)
    return m.group(0) if m else None

# ---------- Config ----------
@dataclass
class Config:
    BOT_TOKEN: str
    GOOGLE_SERVICE_ACCOUNT_KEY: str
    SHEET_ID: str

    RANGE_NAME: str = "Guides!A:C"
    ADMIN_ID: int = 6970816136  # информативно – теперь /reload для всех

_raw_token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
config = Config(
    BOT_TOKEN=clean_token(_raw_token) or "",
    GOOGLE_SERVICE_ACCOUNT_KEY=os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY") or "",
    SHEET_ID=(os.getenv("GOOGLE_SHEET_ID") or os.getenv("SHEET_ID") or "")
)

if not config.BOT_TOKEN or not config.GOOGLE_SERVICE_ACCOUNT_KEY or not config.SHEET_ID:
    raise RuntimeError("Missing required envs: BOT_TOKEN, GOOGLE_SERVICE_ACCOUNT_KEY, GOOGLE_SHEET_ID/SHEET_ID")

# ---------- Globals (жизнь “между” запросами, пока инстанс тёплый) ----------
bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]
CREDS: Optional[Credentials] = None
SHEETS_SERVICE = None
DRIVE_SERVICE = None

main_buttons: List[str] = []
submenus: Dict[str, List[str]] = {}
texts: Dict[str, str] = {}
main_menu: Optional[ReplyKeyboardMarkup] = None

# Кэш на 5 минут — чтобы меньше ходить в Sheets в рамках тёплого инстанса
guides_cache = TTLCache(maxsize=1, ttl=300)

# In-memory сессии (30 мин). Для продакшена лучше Redis.
sessions: Dict[int, float] = {}

# ---------- Utils ----------
URL_RE = re.compile(r'https?://(?:[a-zA-Z0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
VIDEO_EXTS = {".mp4"}
ANIM_EXTS  = {".gif"}
DOC_EXTS   = {".pdf", ".svg"}

def extract_urls_ordered(text: str) -> List[str]:
    urls = URL_RE.findall(text or "")
    seen = OrderedDict()
    for u in urls:
        seen.setdefault(u, True)
    return list(seen.keys())

def ext_of(url: str) -> str:
    path = urlparse(url).path
    return os.path.splitext(path)[1].lower()

def split_media(urls: List[str]):
    media_items: List[types.InputMedia] = []
    anims: List[str] = []
    docs: List[str] = []
    for url in urls:
        e = ext_of(url)
        if e in PHOTO_EXTS:
            media_items.append(InputMediaPhoto(media=url))
        elif e in VIDEO_EXTS:
            media_items.append(InputMediaVideo(media=url))
        elif e in ANIM_EXTS:
            anims.append(url)
        elif e in DOC_EXTS:
            docs.append(url)
        else:
            docs.append(url)
    return media_items[:10], anims, docs

def has_access(user_id: int) -> bool:
    now = time.time()
    # чистим протухшие
    expired = [u for u, t in sessions.items() if t < now]
    for u in expired:
        sessions.pop(u, None)
    return sessions.get(user_id, 0) > now

async def grant_access(user_id: int):
    sessions[user_id] = time.time() + 1800  # 30 минут

def reset_all_sessions():
    sessions.clear()

def sanitize_text(text: str, sanitize=True) -> str:
    if sanitize:
        return re.sub(r"[^\w\s-]", "", text.strip())[:100]
    return text.strip()[:100]

def make_cb_data(btn: str) -> str:
    direct = f"sub|{btn}"
    if len(direct.encode("utf-8")) <= 64:
        return direct
    return f"sub#{hashlib.sha1(btn.encode('utf-8')).hexdigest()[:32]}"

def resolve_btn_from_cb(data: str) -> Optional[str]:
    if data.startswith("sub|"):
        return data.split("|", 1)[1]
    if data.startswith("sub#"):
        h = data[4:]
        for k in texts.keys():
            if hashlib.sha1(k.encode("utf-8")).hexdigest().startswith(h):
                return k
    return None

# ---------- Google clients (ленивая инициализация) ----------
def ensure_google():
    global CREDS, SHEETS_SERVICE, DRIVE_SERVICE
    if SHEETS_SERVICE and DRIVE_SERVICE:
        return
    creds_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_KEY)
    CREDS = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    SHEETS_SERVICE = build("sheets", "v4", credentials=CREDS, cache_discovery=False)
    DRIVE_SERVICE  = build("drive",  "v3", credentials=CREDS, cache_discovery=False)

async def load_guides(force=False):
    global main_buttons, submenus, texts, main_menu
    if guides_cache.get("ok") and not force:
        return

    ensure_google()

    max_retries, delay = 4, 1.0
    for attempt in range(1, max_retries + 1):
        try:
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

            buttons = [[KeyboardButton(text=btn)] for btn in main_buttons]
            main_menu = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)

            guides_cache["ok"] = True
            logging.info(f"Guides loaded: {len(main_buttons)} main, {sum(len(v) for v in submenus.values())} sub")
            return

        except HttpError as e:
            if e.resp.status == 429:
                logging.warning(f"Rate limit, retrying: {e}")
                await asyncio.sleep(max(5.0, delay))
                delay = min(delay * 2, 10.0)
            else:
                logging.error(f"HTTP Error {e.resp.status}: {e}")
                break
        except Exception as e:
            if isinstance(e, ssl.SSLError) or "EOF occurred" in str(e):
                logging.warning(f"Transient SSL on load_guides (attempt {attempt}/{max_retries}): {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)
                continue
            logging.error(f"Unexpected load_guides error: {e}")
            break

async def send_album_and_text(chat_id: int, guide_text: str) -> List[int]:
    sent_ids: List[int] = []
    urls = extract_urls_ordered(guide_text)
    media, anims, docs = split_media(urls)

    if len(media) == 1:
        item = media[0]
        try:
            if isinstance(item, InputMediaPhoto):
                msg = await bot.send_photo(chat_id, item.media)
            elif isinstance(item, InputMediaVideo):
                msg = await bot.send_video(chat_id, item.media)
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
            for item in media:
                try:
                    if isinstance(item, InputMediaPhoto):
                        msg = await bot.send_photo(chat_id, item.media)
                    elif isinstance(item, InputMediaVideo):
                        msg = await bot.send_video(chat_id, item.media)
                    else:
                        msg = await bot.send_document(chat_id, item.media)
                    sent_ids.append(msg.message_id)
                    await asyncio.sleep(0.25)
                except Exception as e2:
                    logging.error(f"fallback single media failed: {e2}")

    for aurl in anims[:10]:
        try:
            msg = await bot.send_animation(chat_id, aurl)
            sent_ids.append(msg.message_id)
            await asyncio.sleep(0.15)
        except Exception as e:
            logging.error(f"send_animation failed: {e}")

    for durl in docs[:10]:
        try:
            msg = await bot.send_document(chat_id, durl)
            sent_ids.append(msg.message_id)
            await asyncio.sleep(0.15)
        except Exception as e:
            logging.error(f"send_document failed: {e}")

    text_without_urls = URL_RE.sub("", guide_text).strip()
    if text_without_urls:
        msg = await bot.send_message(chat_id, text_without_urls, reply_markup=main_menu)
    else:
        msg = await bot.send_message(chat_id, "Выберите следующий раздел:", reply_markup=main_menu)
    sent_ids.append(msg.message_id)
    return sent_ids

# ---------- Handlers ----------
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await load_guides()
    if has_access(message.from_user.id):
        await message.answer("Главное меню:", reply_markup=main_menu)
    else:
        await message.answer("Введите код доступа.")

@dp.message(Command("reload"))
async def cmd_reload(message: types.Message):
    # 1) Жёсткая перезагрузка данных
    guides_cache.clear()
    await load_guides(force=True)
    # 2) Сбросить доступы
    reset_all_sessions()
    await message.answer("Бот обновлён. Введите код доступа.")

@dp.message()
async def main_handler(message: types.Message):
    await load_guides()
    user_id = message.from_user.id
    if not hasattr(message, "text"):
        await message.answer("Неизвестная команда. Используйте кнопки ⬇️", reply_markup=main_menu)
        return

    txt = message.text.strip()
    if not has_access(user_id):
        if txt == "infobot":
            await grant_access(user_id)
            await message.answer("Доступ предоставлен на 30 минут. Главное меню:", reply_markup=main_menu)
        else:
            await message.answer("Введите код доступа.")
        return

    # авторизован
    if txt in main_buttons:
        if txt in submenus:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=b, callback_data=make_cb_data(b))]
                for b in submenus[txt]
            ])
            await message.answer(f"Выберите опцию для {txt}:", reply_markup=kb)
        else:
            guide_text = texts.get(txt, "Текст не найден в Google Sheets.").strip()
            await send_album_and_text(user_id, guide_text)
    else:
        await message.answer("Пожалуйста, используйте кнопки ⬇️", reply_markup=main_menu)

@dp.callback_query()
async def process_callback(callback: types.CallbackQuery):
    try:
        await callback.answer()
    except Exception:
        pass

    await load_guides()
    if not has_access(callback.from_user.id):
        await callback.message.answer("Доступ истек. Введите код доступа.", reply_markup=main_menu)
        return

    btn = resolve_btn_from_cb(callback.data or "")
    if not btn:
        logging.warning(f"Unknown callback data: {callback.data}. keys={len(texts)}")
        await callback.message.answer("Элемент не найден. Обновите меню (/reload).", reply_markup=main_menu)
        return

    guide_text = texts.get(btn)
    if guide_text is None:
        guides_cache.clear()
        await load_guides(force=True)
        guide_text = texts.get(btn, "Текст не найден в Google Sheets.")
    await send_album_and_text(callback.from_user.id, guide_text.strip())

# ---------- Vercel entry ----------
@app.post("/")
async def webhook_entry(request: Request):
    """
    Точка входа для Vercel: /api/webhook
    """
    try:
        update = types.Update(**(await request.json()))
        await dp.feed_update(bot, update)
        return {"ok": True}
    except Exception as e:
        logging.error(f"Error processing update: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# (опционально) простая проверка
@app.get("/")
async def ping():
    return {"status": "ok"}
