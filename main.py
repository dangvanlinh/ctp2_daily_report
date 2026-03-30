"""
CTP1 Daily Report Agent - Main Orchestrator
Runs daily at 7:00 AM (Asia/Ho_Chi_Minh).

Flow:
  1. Collect overall metrics from Tracker (tracker.zingplay.com)
  2. Analyze with Claude — highlight anomalies
  3. Send report via Telegram

ClickHouse (data_collector.py) is NOT used here.
It is used on-demand when investigating anomalies after the report.
"""
from dotenv import load_dotenv
load_dotenv(override=True)

import json
import logging
import sys
from datetime import datetime, timedelta

import pytz

from config import TIMEZONE
from tracker_collector import collect_tracker_metrics
from analyzer import analyze_with_claude, generate_fallback_report
from reporter import send_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("daily_report.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def run_daily_report(target_date=None):
    """
    Main flow:
    1. Determine report date (yesterday by default)
    2. Collect overall metrics from Tracker
    3. Send to Claude for analysis + anomaly detection
    4. Send report via Telegram
    """
    tz = pytz.timezone(TIMEZONE)

    if target_date is None:
        target_date = datetime.now(tz).date() - timedelta(days=1)

    logger.info(f"=== Starting daily report for {target_date} ===")

    # Step 1: Collect from Tracker
    logger.info("Step 1: Collecting metrics from Tracker...")
    try:
        data = collect_tracker_metrics(target_date)
    except Exception as e:
        logger.error(f"Tracker collection failed: {e}")
        send_report(f"🚨 CTP1 Daily Report FAILED\nDate: {target_date}\nError: {e}")
        return False

    if not data or not data.get("overall", {}).get("D-1"):
        logger.warning("No data returned from Tracker!")
        send_report(
            f"⚠️ CTP1 Daily Report - NO DATA\n"
            f"Date: {target_date}\n"
            f"Không lấy được dữ liệu từ Tracker. Kiểm tra TRACKER_SESSION."
        )
        return False

    # Step 2: Analyze with Claude
    logger.info("Step 2: Analyzing with Claude...")
    data_str = json.dumps(data, indent=2, ensure_ascii=False, default=str)

    try:
        report = analyze_with_claude(data_str)
    except Exception as e:
        logger.error(f"Claude analysis failed: {e}. Using fallback.")
        report = generate_fallback_report(data)

    if report.startswith("❌"):
        logger.warning(f"Claude returned error, using fallback.")
        fallback = generate_fallback_report(data)
        report = f"{fallback}\n\n---\n{report}"

    # Step 3: Send via Telegram
    logger.info("Step 3: Sending report...")
    success = send_report(report)

    if success:
        logger.info("✅ Daily report sent successfully!")
    else:
        logger.warning("⚠️ Telegram delivery may have failed.")

    return success


if __name__ == "__main__":
    if len(sys.argv) > 1:
        from datetime import date as date_cls
        target = date_cls.fromisoformat(sys.argv[1])
        run_daily_report(target)
    else:
        run_daily_report()
