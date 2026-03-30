"""
Tracker Collector: Scrapes daily metrics from tracker.zingplay.com
- /gsnreport/accactive  — overall metrics
- /gsnreport/accdistributor — metrics by distributor (UA channels)
"""
import json
import logging
import re
import warnings
from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

from config import TRACKER_URL, TRACKER_SESSION, TRACKER_APP_NAME, TIMEZONE

logger = logging.getLogger(__name__)

# ── accactive: metrics to extract ────────────────────────────────────────────
SERIES_MAP = {
    # Users
    "A1":           "dau",
    "A1_Android":   "dau_android",
    "A1_iOS":       "dau_ios",
    # New installs
    "N1":           "new_installs",
    "N1_Android":   "new_installs_android",
    "N1_iOS":       "new_installs_ios",
    # Revenue
    "RevGross":     "revenue_gross",
    "Rev_Age0":     "rev_age0",
    "Rev_Age1-3":   "rev_age1_3",
    "Rev_Age4-15":  "rev_age4_15",
    "%Rev_Age0":    "pct_rev_age0",
    "%Rev_Age1-3":  "pct_rev_age1_3",
    "%Rev_Age4-15": "pct_rev_age4_15",
    "Total_New_Rev":"total_new_rev",
    "FPU":          "first_payers",
    "P1":           "payers",
    "ARPPU":        "arppu",
    # Conversion
    "CV0":          "cv0",
    "CV1":          "cv1",
    "CV3":          "cv3",
    "CV7":          "cv7",
    "CV15":         "cv15",
    # Retention
    "RR1":          "rr1",
    "RR3":          "rr3",
    "RR7":          "rr7",
    "RR15":         "rr15",
}

# ── accdistributor: charts to extract + target series ─────────────────────────
# Chart ID on page → field name in output
DISTRIBUTOR_CHARTS = {
    "chartA1":    "a1",
    "chartN1":    "n1",
    "chartRev":   "revenue_gross",
    "chartRR1":   "rr1",
    "chartRR3":   "rr3",
    "chartRR7":   "rr7",
    "chartCV0":   "cv0",
    "chartCV1":   "cv1",
    "chartCV3":   "cv3",
    "chartCV7":   "cv7",
}

# Distributor series names — use full names including app suffix
DISTRIBUTOR_TARGETS = {
    "and_ZPPortal_ctp2_vn": "android",
    "ios_ZPPortal_ctp2_vn": "ios",
}


def _make_session() -> requests.Session:
    s = requests.Session()
    s.cookies.set("PHPSESSID", TRACKER_SESSION,
                  domain="tracker.zingplay.com", path="/")
    s.verify = False
    return s


def _fetch_page(session: requests.Session, endpoint: str, app_name: str,
                country: str, from_date: str, to_date: str) -> str:
    """GET csrf token then POST filters to an accXxx endpoint."""
    url = f"{TRACKER_URL}/{endpoint}"
    r = session.get(url, timeout=15)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    token_input = soup.find("input", {"name": "_token"})
    if not token_input:
        raise ValueError(f"CSRF token not found on {endpoint}")
    csrf = token_input["value"]

    r2 = session.post(url, data={
        "_token":   csrf,
        "Country":  country,
        "AppName":  app_name,
        "FromDate": from_date,
        "ToDate":   to_date,
    }, headers={"Referer": url}, timeout=30)
    r2.raise_for_status()
    return r2.text


def _parse_series(html: str) -> tuple[list[str], dict[str, list]]:
    """Extract categories (dates) and all series from a Highcharts page."""
    cats = re.findall(r"categories\s*:\s*(\[[^\]]+\])", html)
    dates = json.loads(cats[0]) if cats else []

    series_raw = re.findall(
        r'\{"name":"([^"]+)","id":"[^"]+","data":\[([^\]]+)\]', html
    )
    series = {}
    for name, data_str in series_raw:
        vals = []
        for v in data_str.split(","):
            v = v.strip()
            try:
                vals.append(float(v) if v != "null" else None)
            except ValueError:
                vals.append(None)
        series[name] = vals

    return dates, series


def _parse_distributor_charts(html: str) -> dict[str, dict]:
    """
    Parse accdistributor page by splitting on each chart's JS init block.
    Returns {chart_id: {"dates": [...], "series": {series_name: [values]}}}
    """
    # Each chart is initialized as: $('#chartXXX').highcharts({...})
    # Split the HTML on these markers
    parts = re.split(r"\$\('#(chart\w+)'\)\.highcharts\(", html)
    # parts = [preamble, chart_id1, content1, chart_id2, content2, ...]

    result = {}
    for i in range(1, len(parts), 2):
        chart_id = parts[i]
        if chart_id not in DISTRIBUTOR_CHARTS:
            continue
        content = parts[i + 1] if i + 1 < len(parts) else ""

        cats = re.findall(r"categories:\s*(\[[^\]]+\])", content)
        dates = json.loads(cats[0]) if cats else []

        series_data = {}
        for series_name in DISTRIBUTOR_TARGETS:
            pattern = (r'\{"name":"' + re.escape(series_name)
                       + r'","id":"[^"]*","data":\[([^\]]+)\]')
            m = re.search(pattern, content)
            if m:
                vals = []
                for v in m.group(1).split(","):
                    v = v.strip()
                    try:
                        vals.append(float(v) if v != "null" else None)
                    except ValueError:
                        vals.append(None)
                series_data[series_name] = vals

        result[chart_id] = {"dates": dates, "series": series_data}

    return result


def _build_snapshot(dates: list[str], series: dict[str, list],
                    target_date_str: str,
                    series_map: dict[str, str]) -> dict:
    """Extract values for a specific date from series dict."""
    target_short = target_date_str[5:]  # "2026-03-28" -> "03-28"
    idx = dates.index(target_short) if target_short in dates else len(dates) - 1

    snapshot = {"date": dates[idx] if dates else target_short}
    for series_name, field_name in series_map.items():
        vals = series.get(series_name, [])
        snapshot[field_name] = vals[idx] if idx < len(vals) else None
    return snapshot


COHORT_METRICS = {
    "RR1": "rr1", "RR3": "rr3", "RR7": "rr7", "RR15": "rr15",
    "CV0": "cv0", "CV1": "cv1", "CV3": "cv3", "CV7": "cv7", "CV15": "cv15",
}


def _build_latest_cohort(dates: list[str], series: dict[str, list],
                         report_date: date) -> dict:
    """
    For each cohort metric (RR/CV), scan backwards to find the most recent
    non-null value. Returns {field: {"value": v, "cohort_date": "MM-DD"}}.
    """
    result = {}
    for series_name, field_name in COHORT_METRICS.items():
        vals = series.get(series_name, [])
        # Scan backwards from report_date (D-1) — CV0 is same-day, available immediately
        for offset in range(0, 60):
            d = report_date - timedelta(days=offset)
            short = d.strftime("%m-%d")
            if short in dates:
                idx = dates.index(short)
                v = vals[idx] if idx < len(vals) else None
                if v is not None:
                    result[field_name] = {"value": v, "cohort_date": short}
                    break
        if field_name not in result:
            result[field_name] = {"value": None, "cohort_date": None}
    return result


TREND_METRICS = {
    "A1": "dau", "N1": "new_installs",
    "RevGross": "revenue_gross", "P1": "payers", "ARPPU": "arppu",
    "RR1": "rr1", "RR3": "rr3", "RR7": "rr7",
    "CV0": "cv0", "CV1": "cv1", "CV3": "cv3",
}


def _build_trend_30d(dates: list[str], series: dict[str, list],
                     report_date: date) -> dict:
    """
    Build last-30-days daily values for key metrics (date → value).
    Used by Claude to summarize trend in a sentence.
    """
    result = {}
    for series_name, field_name in TREND_METRICS.items():
        vals = series.get(series_name, [])
        history = {}
        for offset in range(29, -1, -1):
            d = report_date - timedelta(days=offset)
            short = d.strftime("%m-%d")
            if short in dates:
                idx = dates.index(short)
                v = vals[idx] if idx < len(vals) else None
                history[short] = v
        result[field_name] = history
    return result


def _build_avg(dates: list[str], series: dict[str, list],
               report_date: date, series_map: dict[str, str]) -> dict:
    """Compute 7-day rolling average (D-7 to D-1) for each metric."""
    avg = {}
    for series_name, field_name in series_map.items():
        vals_7d = []
        for offset in range(1, 8):
            d = report_date - timedelta(days=offset)
            short = d.strftime("%m-%d")
            if short in dates:
                idx = dates.index(short)
                v = series.get(series_name, [])
                val = v[idx] if idx < len(v) else None
                if val is not None:
                    vals_7d.append(val)
        avg[field_name] = round(sum(vals_7d) / len(vals_7d), 2) if vals_7d else None
    return avg


def _build_distributor_snapshot(chart_data: dict, target_date_str: str,
                                 report_date: date) -> dict:
    """
    Build D-1/D-2/D-8/avg_7d snapshots for each distributor from chart data.
    Returns {distributor_key: {"D-1": {...}, "D-2": {...}, ...}}
    """
    result = {}
    for dist_series, dist_key in DISTRIBUTOR_TARGETS.items():
        d1 = target_date_str
        d2 = (report_date - timedelta(days=1)).strftime("%Y-%m-%d")
        d8 = (report_date - timedelta(days=7)).strftime("%Y-%m-%d")

        snap_d1, snap_d2, snap_d8 = {}, {}, {}
        avg = {}

        for chart_id, field_name in DISTRIBUTOR_CHARTS.items():
            if chart_id not in chart_data:
                continue
            dates = chart_data[chart_id]["dates"]
            vals = chart_data[chart_id]["series"].get(dist_series, [])

            def get_val(date_str):
                short = date_str[5:]
                idx = dates.index(short) if short in dates else len(dates) - 1
                return vals[idx] if idx < len(vals) else None

            snap_d1[field_name] = get_val(d1)
            snap_d2[field_name] = get_val(d2)
            snap_d8[field_name] = get_val(d8)

            # avg_7d
            vals_7d = []
            for offset in range(1, 8):
                d = report_date - timedelta(days=offset)
                short = d.strftime("%m-%d")
                if short in dates:
                    idx = dates.index(short)
                    v = vals[idx] if idx < len(vals) else None
                    if v is not None:
                        vals_7d.append(v)
            avg[field_name] = round(sum(vals_7d) / len(vals_7d), 2) if vals_7d else None

        # trend_30d per distributor
        trend = {}
        for chart_id, field_name in DISTRIBUTOR_CHARTS.items():
            if chart_id not in chart_data:
                continue
            dates = chart_data[chart_id]["dates"]
            vals = chart_data[chart_id]["series"].get(dist_series, [])
            history = {}
            for offset in range(29, -1, -1):
                d = report_date - timedelta(days=offset)
                short = d.strftime("%m-%d")
                if short in dates:
                    idx = dates.index(short)
                    history[short] = vals[idx] if idx < len(vals) else None
            trend[field_name] = history

        result[dist_key] = {
            "D-1": snap_d1,
            "D-2": snap_d2,
            "D-8": snap_d8,
            "avg_7d": avg,
            "trend_30d": trend,
        }

    return result


def collect_tracker_metrics(report_date: date,
                            app_name: str = None,
                            country: str = "") -> dict:
    """
    Collect metrics from both accactive and accdistributor.
    Returns combined dict with 'overall' and 'distributor' sections.
    """
    if app_name is None:
        app_name = TRACKER_APP_NAME

    if not TRACKER_SESSION:
        logger.error("TRACKER_SESSION not set!")
        return {}

    from_date = (report_date - timedelta(days=35)).strftime("%Y-%m-%d")
    to_date = report_date.strftime("%Y-%m-%d")
    d1_str = report_date.strftime("%Y-%m-%d")
    d2_str = (report_date - timedelta(days=1)).strftime("%Y-%m-%d")
    d8_str = (report_date - timedelta(days=7)).strftime("%Y-%m-%d")

    session = _make_session()

    # ── 1. accactive ──────────────────────────────────────────────────────────
    try:
        html_active = _fetch_page(session, "gsnreport/accactive",
                                  app_name, country, from_date, to_date)
    except Exception as e:
        logger.error(f"Failed to fetch accactive: {e}")
        return {}

    dates, series = _parse_series(html_active)
    logger.info(f"accactive: {len(series)} series, {len(dates)} dates "
                f"({dates[0] if dates else '?'} … {dates[-1] if dates else '?'})")

    overall = {
        "D-1":           _build_snapshot(dates, series, d1_str, SERIES_MAP),
        "D-2":           _build_snapshot(dates, series, d2_str, SERIES_MAP),
        "D-8":           _build_snapshot(dates, series, d8_str, SERIES_MAP),
        "avg_7d":        _build_avg(dates, series, report_date, SERIES_MAP),
        "latest_cohort": _build_latest_cohort(dates, series, report_date),
        "trend_30d":     _build_trend_30d(dates, series, report_date),
    }

    # ── 2. accdistributor ─────────────────────────────────────────────────────
    distributor = {}
    try:
        html_dist = _fetch_page(session, "gsnreport/accdistributor",
                                app_name, country, from_date, to_date)
        chart_data = _parse_distributor_charts(html_dist)
        charts_found = [c for c in DISTRIBUTOR_CHARTS if c in chart_data]
        logger.info(f"accdistributor: {len(charts_found)}/{len(DISTRIBUTOR_CHARTS)} charts parsed")
        distributor = _build_distributor_snapshot(chart_data, d1_str, report_date)
    except Exception as e:
        logger.error(f"Failed to fetch accdistributor: {e}")

    return {
        "source":      "tracker",
        "report_date": report_date.strftime("%Y-%m-%d"),
        "app_name":    app_name,
        "overall":     overall,
        "distributor": distributor,
    }


if __name__ == "__main__":
    import sys, io, importlib, os
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    import pytz
    from datetime import datetime
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")

    from dotenv import load_dotenv
    load_dotenv(override=True)

    # Reload config so env vars are picked up
    import config as _cfg
    importlib.reload(_cfg)
    from config import TRACKER_URL, TRACKER_SESSION, TRACKER_APP_NAME  # noqa: F811
    import tracker_collector as _self
    _self.TRACKER_SESSION = os.getenv("TRACKER_SESSION", "")
    _self.TRACKER_URL = os.getenv("TRACKER_URL", "https://tracker.zingplay.com")
    _self.TRACKER_APP_NAME = os.getenv("TRACKER_APP_NAME", "cotyphu")

    tz = pytz.timezone(TIMEZONE)
    report_date = datetime.now(tz).date() - timedelta(days=1)

    print(f"Fetching tracker metrics for {report_date}...")
    data = collect_tracker_metrics(report_date)
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
