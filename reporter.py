"""
Reporter: Sends analysis report via Telegram Bot.
"""
import logging
import sys

import requests

# Ensure stdout supports UTF-8 on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_THREAD_ID

logger = logging.getLogger(__name__)

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

# Telegram message limit
MAX_MESSAGE_LENGTH = 4096


def send_telegram(text: str) -> bool:
    """Send a message via Telegram Bot API."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Telegram credentials not configured!")
        print("=== REPORT (Telegram not configured) ===")
        print(text)
        print("=" * 40)
        return False

    url = TELEGRAM_API_URL.format(token=TELEGRAM_BOT_TOKEN)

    # Split long messages if needed
    chunks = _split_message(text)

    success = True
    for chunk in chunks:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if TELEGRAM_THREAD_ID:
            payload["message_thread_id"] = int(TELEGRAM_THREAD_ID)
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            result = resp.json()
            if not result.get("ok"):
                logger.error(f"Telegram API error: {result}")
                success = False
        except requests.RequestException as e:
            logger.error(f"Failed to send Telegram message: {e}")
            success = False

    return success


def _split_message(text: str) -> list[str]:
    """Split text into chunks that fit Telegram's message limit."""
    if len(text) <= MAX_MESSAGE_LENGTH:
        return [text]

    chunks = []
    lines = text.split("\n")
    current_chunk = ""

    for line in lines:
        if len(current_chunk) + len(line) + 1 > MAX_MESSAGE_LENGTH:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = line + "\n"
        else:
            current_chunk += line + "\n"

    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    return chunks


def send_report(report_text: str) -> bool:
    """Send the daily report. Falls back to console if Telegram fails."""
    logger.info("Sending report via Telegram...")
    sent = send_telegram(report_text)

    if not sent:
        logger.warning("Telegram send failed, printing to console as fallback.")
        print("\n" + "=" * 50)
        print(report_text)
        print("=" * 50 + "\n")

    return sent
