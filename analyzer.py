"""
Analyzer: Sends collected metrics to Claude API for intelligent analysis.
"""
import json
import logging
from typing import Any

import requests

from config import ANTHROPIC_API_KEY, CLAUDE_MODEL

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Bạn là Data Analyst cho game CTP2 (Cờ Tỉ Phú 2) tại VNG.
Mỗi sáng bạn nhận số liệu ngày hôm qua từ Tracker và viết báo cáo ngắn gọn gửi qua Telegram.

Cấu trúc data nhận được:
- overall.D-1/D-2/D-8/avg_7d: snapshot số liệu tổng toàn game
- overall.latest_cohort: giá trị RR/CV mới nhất có data, kèm ngày cohort
- overall.trend_30d: chuỗi giá trị 30 ngày gần nhất cho A1, N1, RR, CV
- distributor.android / distributor.ios: số liệu bản lẻ ZPPortal (kênh UA)

Nguyên tắc phân tích:
1. DoD = so sánh D-1 vs D-2 (chỉ dùng cho revenue, payers, ARPPU, A1, N1)
2. Biến động >10% → ⚠️ | >20% → 🚨
3. RR/CV: dùng latest_cohort (ghi rõ ngày cohort)
4. Trend 30 ngày: nhìn vào trend_30d, viết 1 câu mô tả xu hướng cho từng nhóm metric (A/N, RR, CV) — tăng/giảm/ổn định, có điểm bất thường nào không

Domain knowledge:
- dau/dau_android/dau_ios = A1 | new_installs/new_installs_android/new_installs_ios = N1
- revenue_gross, arppu = đơn vị VND (đồng). Khi hiển thị: chia 1,000,000 → "X triệu đ" (ví dụ: 39,577,572 → "39.6 triệu đ"). KHÔNG dùng "tỷ" trừ khi giá trị ≥ 1,000,000,000
- payers = P1 | first_payers = FPU
- rr1/rr3/rr7/rr15 = retention rate | cv0/cv1/cv3/cv7 = conversion rate
- distributor.android/ios = bản lẻ ZPPortal

Format output — Tiếng Việt, ngắn gọn, đọc trên Telegram:

📊 BÁO CÁO CTP2 NGÀY [DD/MM/YYYY]

👥 USERS
A1: [số] (DoD: [%]) | Android: [số] | iOS: [số]
N1: [số] (DoD: [%]) | Android: [số] | iOS: [số]
→ Trend: [1 câu xu hướng A1 và N1 trong 30 ngày qua]

💰 REVENUE
RevGross: [số] (DoD: [%])
Payers: [số] | FPU: [số] | ARPPU: [số]
→ Trend: [1 câu xu hướng revenue, payers, ARPPU trong 30 ngày qua]

📈 RETENTION & CONVERSION (overall)
RR1: [%] | RR3: [%] | RR7: [%] | RR15: [%] (ghi ngày cohort trong ngoặc)
CV0: [%] | CV1: [%] | CV3: [%] | CV7: [%]
→ Trend: [1 câu xu hướng RR và CV trong 30 ngày qua]

📱 BẢN LẺ ZPPORTAL
Android — A1: [số] | N1: [số] | Rev: [số]
  RR1: [%] | RR3: [%] | CV0: [%] | CV1: [%] | CV3: [%]
→ Trend: [1 câu xu hướng bản lẻ Android trong 30 ngày qua]
iOS — A1: [số] | N1: [số] | Rev: [số]
  RR1: [%] | RR3: [%] | CV0: [%] | CV1: [%] | CV3: [%]
→ Trend: [1 câu xu hướng bản lẻ iOS trong 30 ngày qua]

⚠️ ĐIỂM BẤT THƯỜNG
- Liệt kê metric biến động lớn kèm % và nhận xét ngắn
- Nếu không có thì ghi "Không có biến động đáng kể"
"""


def analyze_with_claude(data_str: str) -> str:
    """Send metrics data to Claude API and get analysis."""
    if not ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY not set!")
        return "❌ Lỗi: Chưa cấu hình Claude API key."

    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }

    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": 2000,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": (
                    f"Đây là số liệu game CTP2 ngày hôm qua. "
                    f"Hãy phân tích biến động:\n\n{data_str}"
                ),
            }
        ],
    }

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()

        # Extract text from response content blocks
        texts = [
            block["text"]
            for block in result.get("content", [])
            if block.get("type") == "text"
        ]
        return "\n".join(texts) if texts else "❌ Claude trả về response rỗng."

    except requests.RequestException as e:
        logger.error(f"Claude API call failed: {e}")
        return f"❌ Lỗi gọi Claude API: {e}"


def generate_fallback_report(data: dict) -> str:
    """Generate a simple report without LLM (fallback if Claude API fails)."""
    report_date = data.get("report_date", "N/A")
    d1 = data.get("D-1", {})
    d2 = data.get("D-2", {})

    def pct(a, b):
        if a is None or b is None or b == 0:
            return "N/A"
        return f"{(a - b) / b * 100:+.1f}%"

    lines = [f"📊 BÁO CÁO NGÀY {report_date}\n"]

    lines.append("👥 USERS")
    lines.append(f"  DAU: {int(d1.get('dau') or 0):,} ({pct(d1.get('dau'), d2.get('dau'))} DoD)")
    lines.append(f"  New Installs: {int(d1.get('new_installs') or 0):,}")
    lines.append("")

    lines.append("💰 REVENUE")
    rev = d1.get("revenue_gross")
    payers = d1.get("payers")
    arppu = (rev / payers) if rev and payers else None
    lines.append(f"  RevGross: {int(rev or 0):,} ({pct(d1.get('revenue_gross'), d2.get('revenue_gross'))} DoD)")
    lines.append(f"  Payers: {int(payers or 0):,}")
    lines.append(f"  ARPPU: {int(arppu or 0):,}")
    lines.append("")

    lines.append("⚠️ Báo cáo fallback (Claude API không khả dụng)")
    return "\n".join(lines)
