"""
SQL Queries for CTP1 Daily Metrics
All queries use parameterized date filtering via {date_filter} placeholder.
"""


def _date_filter(date_str: str) -> str:
    """Generate ClickHouse date filter for a specific date."""
    return f"toDate(log_time, 'Asia/Ho_Chi_Minh') = '{date_str}'"


def _date_range_filter(start_date: str, end_date: str) -> str:
    """Generate ClickHouse date range filter."""
    return (
        f"toDate(log_time, 'Asia/Ho_Chi_Minh') >= '{start_date}' "
        f"AND toDate(log_time, 'Asia/Ho_Chi_Minh') <= '{end_date}'"
    )


# =============================================================================
# USER METRICS
# =============================================================================

USER_METRICS_SINGLE_DAY = """
SELECT
    toDate(log_time, 'Asia/Ho_Chi_Minh') AS report_date,
    COUNT(DISTINCT user_id)                                       AS dau,
    COUNT(DISTINCT CASE WHEN user_age = 0 THEN user_id END)       AS new_users,
    COUNT(DISTINCT CASE WHEN user_age > 0 THEN user_id END)       AS returning_users
FROM ctp_2025_db.stg_raw_log
WHERE log_action = 'Login'
    AND {date_filter}
GROUP BY report_date
ORDER BY report_date
"""

# =============================================================================
# REVENUE METRICS
# =============================================================================

REVENUE_METRICS_SINGLE_DAY = """
SELECT
    toDate(log_time, 'Asia/Ho_Chi_Minh') AS report_date,
    SUM(toFloat64OrZero(extra_2)) AS total_revenue,
    COUNT(DISTINCT user_id)       AS paying_user_count,
    COUNT(*)                      AS transaction_count
FROM ctp_2025_db.stg_raw_log
WHERE log_action = 'Payment'
    AND {date_filter}
GROUP BY report_date
ORDER BY report_date
"""

REVENUE_BY_SOURCE = """
SELECT
    toDate(log_time, 'Asia/Ho_Chi_Minh') AS report_date,
    extra_4                       AS rev_from,
    SUM(toFloat64OrZero(extra_2)) AS revenue,
    COUNT(DISTINCT user_id)       AS payers,
    COUNT(*)                      AS transactions
FROM ctp_2025_db.stg_raw_log
WHERE log_action = 'Payment'
    AND {date_filter}
GROUP BY report_date, rev_from
ORDER BY report_date, revenue DESC
"""

# =============================================================================
# QUERY REGISTRY - maps metric names to query templates
# =============================================================================

QUERY_REGISTRY = {
    "user_metrics": USER_METRICS_SINGLE_DAY,
    "revenue_metrics": REVENUE_METRICS_SINGLE_DAY,
    "revenue_by_source": REVENUE_BY_SOURCE,
}
