# CTP1 Daily Report Agent

AI-powered daily metrics report cho game Cờ Tỉ Phú (CTP2).

## Flow

```
7:00 AM daily (cron)
    │
    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  ClickHouse  │───▶│  Claude API  │───▶│   Telegram   │
│  Query Data  │    │  Analyze     │    │   Send Report│
└──────────────┘    └──────────────┘    └──────────────┘
```

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your actual credentials
```

### 3. Load env vars (add to your shell profile or use python-dotenv)
```bash
export $(cat .env | xargs)
```

### 4. Test run
```bash
# Run for yesterday
python main.py

# Run for a specific date
python main.py 2026-03-27
```

### 5. Setup cron (chạy 7h sáng hàng ngày)
```bash
crontab -e
# Add this line:
0 7 * * * cd /path/to/ctp1-daily-agent && export $(cat .env | xargs) && /usr/bin/python3 main.py >> cron.log 2>&1
```

## Files

| File | Role |
|------|------|
| `config.py` | Settings (ClickHouse, Telegram, Claude API) |
| `queries.py` | SQL queries cho từng nhóm metric |
| `data_collector.py` | Chạy queries, thu thập data cho D-1, D-2, D-8, avg7d |
| `analyzer.py` | Gửi data cho Claude API phân tích, có fallback report |
| `reporter.py` | Gửi báo cáo qua Telegram (tự split nếu dài) |
| `main.py` | Orchestrator chạy toàn bộ flow |

## Adding New Metrics

1. Thêm SQL query vào `queries.py`
2. Register trong `QUERY_REGISTRY`
3. Thêm query name vào `metric_names` list trong `data_collector.py`
4. Update `SYSTEM_PROMPT` trong `analyzer.py` nếu cần context mới
