"""
Data Collector: Queries ClickHouse via Superset SQL API and aggregates metrics for comparison.
"""
import json
import logging
import uuid
from datetime import date, timedelta
from typing import Any

import requests

from config import SUPERSET_URL, SUPERSET_SESSION, SUPERSET_DB_ID, TIMEZONE, SUPERSET_ACCESS_TOKEN
from queries import QUERY_REGISTRY, _date_filter, _date_range_filter

logger = logging.getLogger(__name__)


def _make_session() -> requests.Session:
    """Create a requests session with session cookie."""
    s = requests.Session()
    token = SUPERSET_ACCESS_TOKEN or SUPERSET_SESSION
    if token:
        s.headers.update({"Cookie": f"session={token}"})
    return s


def _get_csrf_token(session: requests.Session) -> str:
    """Fetch CSRF token from Superset (required for POST requests)."""
    resp = session.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/",
        timeout=10,
        verify=False,
    )
    resp.raise_for_status()
    return resp.json().get("result", "")


def query_superset(sql: str) -> list[dict]:
    """Execute a SQL query via Superset SQL Lab API and return rows as list of dicts."""
    if not SUPERSET_ACCESS_TOKEN and not SUPERSET_SESSION:
        logger.error("No auth configured (SUPERSET_ACCESS_TOKEN or SUPERSET_SESSION)!")
        return []

    session = _make_session()

    try:
        csrf_token = _get_csrf_token(session)
    except Exception as e:
        logger.error(f"Failed to get CSRF token: {e}")
        return []

    payload = {
        "database_id": SUPERSET_DB_ID,
        "sql": sql,
        "client_id": str(uuid.uuid4())[:10],
        "queryLimit": 10000,
        "runAsync": False,
    }

    try:
        resp = session.post(
            f"{SUPERSET_URL}/api/v1/sqllab/execute/",
            json=payload,
            headers={
                "X-CSRFToken": csrf_token,
                "Referer": SUPERSET_URL,
            },
            timeout=60,
            verify=False,
        )
        resp.raise_for_status()
        result = resp.json()

        columns = [col["name"] for col in result.get("columns", [])]
        rows = result.get("data", [])

        # Convert list of values to list of dicts
        if rows and isinstance(rows[0], list):
            return [dict(zip(columns, row)) for row in rows]

        # Already dicts
        return rows

    except requests.RequestException as e:
        logger.error(f"Superset query failed: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.debug(f"Response: {e.response.text[:500]}")
        return []


def collect_metric(query_name: str, target_date: date) -> list[dict]:
    """Run a named query for a specific date."""
    template = QUERY_REGISTRY.get(query_name)
    if not template:
        logger.error(f"Unknown query: {query_name}")
        return []

    date_str = target_date.strftime("%Y-%m-%d")
    sql = template.format(date_filter=_date_filter(date_str))
    return query_superset(sql)


def collect_metric_range(query_name: str, start_date: date, end_date: date) -> list[dict]:
    """Run a named query for a date range (for rolling averages)."""
    template = QUERY_REGISTRY.get(query_name)
    if not template:
        logger.error(f"Unknown query: {query_name}")
        return []

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    sql = template.format(
        date_filter=_date_range_filter(start_str, end_str)
    )
    return query_superset(sql)


def collect_all_metrics(report_date: date) -> dict[str, Any]:
    """
    Collect all metrics for the report date and comparison dates.

    Returns a structured dict:
    {
        "report_date": "2026-03-27",
        "metrics": {
            "user_metrics": {
                "D-1": [...],   # yesterday (report_date)
                "D-2": [...],   # day before
                "D-8": [...],   # same day last week
                "avg_7d": [...] # rolling 7-day average
            },
            ...
        }
    }
    """
    metric_names = [
        "user_metrics",
        "revenue_metrics",
        "revenue_by_source",
    ]

    results = {
        "report_date": report_date.strftime("%Y-%m-%d"),
        "metrics": {},
    }

    for name in metric_names:
        logger.info(f"Collecting {name}...")
        metric_data = {}

        metric_data["D-1"] = collect_metric(name, report_date)
        metric_data["D-2"] = collect_metric(name, report_date - timedelta(days=1))
        metric_data["D-8"] = collect_metric(name, report_date - timedelta(days=7))
        metric_data["avg_7d"] = collect_metric_range(
            name,
            report_date - timedelta(days=7),
            report_date - timedelta(days=1),
        )

        results["metrics"][name] = metric_data

    return results


def format_data_for_llm(data: dict) -> str:
    """Format collected data as a readable string for Claude to analyze."""
    return json.dumps(data, indent=2, ensure_ascii=False, default=str)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from datetime import datetime
    import pytz

    tz = pytz.timezone(TIMEZONE)
    yesterday = datetime.now(tz).date() - timedelta(days=1)

    print(f"Collecting metrics for {yesterday}...")
    data = collect_all_metrics(yesterday)
    print(format_data_for_llm(data))
