# phone.py
import os
import re
import json
from typing import Dict, Any, Optional

import requests

# --- API keys (env vars win; fall back to the keys you provided) ---
NUMLOOKUP_API_KEY = os.getenv(
    "NUMLOOKUP_API_KEY",
    "num_live_tcLFYRa5HmnTr5CgiClWzwTSu4qYT94aswZw1EWe",
)
SMSMOBILE_API_KEY = os.getenv(
    "SMSMOBILE_API_KEY",
    "2e83b76b788bd1fed079505f513a420b45b0ee9db85d372d",
)

NUMLOOKUP_ENDPOINT = "https://api.numlookupapi.com/v1/validate/{number}"
SMSMOBILE_ENDPOINT = "https://api.smsmobileapi.com/whatsapp/checknumber/"

HTTP_TIMEOUT = 15  # seconds


def normalize_number(user_input: str) -> str:
    """
    Normalize a phone number:
    - keep leading '+' if present, otherwise digits only
    - remove spaces, dashes, parentheses and other separators
    """
    s = user_input.strip()
    if not s:
        return ""

    if s.startswith("+"):
        # Keep leading '+' and strip all non-digits after it
        digits = re.sub(r"\D", "", s[1:])
        return f"+{digits}" if digits else "+"
    else:
        # Only digits
        return re.sub(r"\D", "", s)


def digits_only(number: str) -> str:
    """Digits only version (used by smsmobileapi's 'recipients')."""
    return re.sub(r"\D", "", number)


def _safe_get_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"raw_text": resp.text}


def query_numlookup(number: str) -> Dict[str, Any]:
    """
    Call NumlookupAPI with + or without + (API accepts both).
    Returns a dict with status and data/error.
    """
    url = NUMLOOKUP_ENDPOINT.format(number=number.lstrip("+"))
    params = {"apikey": NUMLOOKUP_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        data = _safe_get_json(r)
        ok = r.ok
        return {"ok": ok, "status": r.status_code, "data": data}
    except requests.RequestException as e:
        return {"ok": False, "status": None, "error": str(e)}


def query_smsmobile(number: str) -> Dict[str, Any]:
    """
    Call smsmobileapi.com WhatsApp checknumber endpoint.
    Their docs state two params: apikey (yours) and recipients (phone to verify).
    We send digits-only to be safe.
    """
    params = {
        "apikey": SMSMOBILE_API_KEY,
        "recipients": digits_only(number),
    }
    try:
        r = requests.get(SMSMOBILE_ENDPOINT, params=params, timeout=HTTP_TIMEOUT)
        data = _safe_get_json(r)
        ok = r.ok
        return {"ok": ok, "status": r.status_code, "data": data}
    except requests.RequestException as e:
        return {"ok": False, "status": None, "error": str(e)}


def check_phone(user_input: str) -> Dict[str, Any]:
    """
    Orchestrates both checks and returns a single structured result.
    """
    normalized = normalize_number(user_input)
    if not normalized or normalized in {"+", ""}:
        return {
            "ok": False,
            "error": "Empty or invalid phone number format.",
            "input": user_input,
        }

    numlookup = query_numlookup(normalized)
    smsmobile = query_smsmobile(normalized)

    return {
        "ok": True,
        "input": user_input,
        "normalized": normalized,
        "numlookupapi": numlookup,
        "smsmobileapi": smsmobile,
    }


def format_result_markdown(result: Dict[str, Any]) -> str:
    """
    Build a user-friendly HTML-formatted message to send via Telegram.
    (Aiogram is configured with ParseMode.HTML in your bot.)
    """
    if not result.get("ok"):
        return f"❌ <b>Ошибка</b>: {result.get('error', 'Unknown error')}"

    normalized = result.get("normalized", "")
    nl = result.get("numlookupapi", {})
    sm = result.get("smsmobileapi", {})

    # Extract some friendly fields from Numlookup if available
    nl_data = nl.get("data") or {}
    nl_valid = nl_data.get("valid")
    nl_international = nl_data.get("international_format")
    nl_country = nl_data.get("country_name")
    nl_carrier = nl_data.get("carrier")
    nl_line = nl_data.get("line_type")

    # Extract smsmobile whatsapp check
    sm_data = sm.get("data") or {}
    sm_found = None
    if isinstance(sm_data, dict):
        # documented example field: "contact_found_on_whatsapp": "yes"/"no"
        v = sm_data.get("contact_found_on_whatsapp")
        if isinstance(v, str):
            sm_found = v.lower() in {"yes", "true", "1"}

    # Build message
    parts = []
    parts.append(f"🔎 <b>Проверка номера</b>: <code>{normalized}</code>")

    # Numlookup
    parts.append("\n<b>NumlookupAPI</b>")
    if nl.get("ok"):
        items = []
        if nl_valid is not None:
            items.append(f"Valid: <b>{'✅' if nl_valid else '❌'}</b>")
        if nl_international:
            items.append(f"International: <code>{nl_international}</code>")
        if nl_country:
            items.append(f"Country: {nl_country}")
        if nl_carrier:
            items.append(f"Carrier: {nl_carrier}")
        if nl_line:
            items.append(f"Type: {nl_line}")
        if not items:
            items.append("Unknown")
        parts.extend(items)
    else:
        parts.append(f"❌ error (status={nl.get('status')})")

    # SMSMobile WhatsApp
    parts.append("\n<b>WhatsApp</b>")
    if sm.get("ok"):
        if sm_found is not None:
            parts.append(f"Whatsapp: <b>{'✅' if sm_found else '❌'}</b>")
        else:
            parts.append(f"Ответ: <code>{json.dumps(sm_data, ensure_ascii=False)}</code>")
    else:
        parts.append(f"❌ error (status={sm.get('status')})")

    return "\n".join(parts)
