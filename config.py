"""
Configuration for CTP1 Daily Report Agent
"""
import os

# Superset
SUPERSET_URL = os.getenv("SUPERSET_URL", "https://bitool-hn2.zingplay.com")
SUPERSET_SESSION = os.getenv("SUPERSET_SESSION", "")
SUPERSET_ACCESS_TOKEN = os.getenv("SUPERSET_ACCESS_TOKEN", "")
SUPERSET_DB_ID = int(os.getenv("SUPERSET_DB_ID", "1"))

# Tracker (tracker.zingplay.com)
TRACKER_URL = os.getenv("TRACKER_URL", "https://tracker.zingplay.com")
TRACKER_SESSION = os.getenv("TRACKER_SESSION", "")
TRACKER_APP_NAME = os.getenv("TRACKER_APP_NAME", "cotyphu")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_THREAD_ID = os.getenv("TELEGRAM_THREAD_ID", "")  # topic/thread trong supergroup

# Claude API
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# Timezone
TIMEZONE = "Asia/Ho_Chi_Minh"

# Report settings
COMPARE_DAYS = [1, 2, 7]  # D-1 (yesterday), D-2, D-8 for WoW
AVG_WINDOW = 7  # rolling average window
