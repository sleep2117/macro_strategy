"""
KRX index data utilities (prices + fundamentals) via pykrx

This module mirrors the storage format used in world_indices.py:
- Daily OHLCV CSVs under data/daily/<symbol>.csv with columns
  [Open, High, Low, Close, Adj Close?, Volume] (Adj Close omitted if N/A)
- Daily valuation snapshots (PE, PB, DividendYield) under data/valuations/<symbol>.csv

Symbols here use a filesystem-safe prefix: KRX_IDX_<ticker>
e.g., KOSPI (1001) -> data/daily/KRX_IDX_1001.csv
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import pandas as pd

from pykrx import stock

# --------------------
# Paths and helpers
# --------------------

BASE_DIR = Path(__file__).resolve().parent
DAILY_DIR = BASE_DIR / "data" / "daily"
VAL_DIR = BASE_DIR / "data" / "valuations"
DAILY_DIR.mkdir(parents=True, exist_ok=True)
VAL_DIR.mkdir(parents=True, exist_ok=True)


def _symbol_for_krx_index(ticker: str) -> str:
    """Create a filesystem-safe symbol for a KRX index code."""
    return f"KRX_IDX_{str(ticker).strip()}"


def _daily_csv_path(ticker: str) -> Path:
    return DAILY_DIR / f"{_symbol_for_krx_index(ticker)}.csv"


def _valuation_csv_path(ticker: str) -> Path:
    return VAL_DIR / f"{_symbol_for_krx_index(ticker)}.csv"


def _to_yyyymmdd(dt: datetime | str | None) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, str):
        # Accept 'YYYY-MM-DD' or 'YYYYMMDD'
        s = dt.replace("-", "")
        if len(s) == 8:
            return s
        raise ValueError(f"Invalid date string: {dt}")
    return dt.strftime("%Y%m%d")


def list_krx_index_tickers(market: str = "KOSPI", date: str | None = None) -> pd.DataFrame:
    """Return DataFrame of available index tickers and names for a market.

    - market: 'KOSPI' | 'KOSDAQ' | 'KRX'
    - date: optional 'YYYYMMDD' or 'YYYY-MM-DD' (defaults to today)
    """
    if date is None:
        date = datetime.today().strftime("%Y%m%d")
    else:
        date = _to_yyyymmdd(date)
    codes = stock.get_index_ticker_list(date=date, market=market)
    rows = [{"ticker": c, "name": stock.get_index_ticker_name(c)} for c in codes]
    return pd.DataFrame(rows)


# --------------------
# Price (OHLCV)
# --------------------

def fetch_index_ohlcv(ticker: str, start: str | datetime | None = None, end: str | datetime | None = None, freq: str = "d") -> pd.DataFrame:
    """Fetch KRX index OHLCV from pykrx and normalize columns to English.

    Returns a DataFrame indexed by Timestamp with columns [Open, High, Low, Close, Volume].
    """
    if end is None:
        end = datetime.today()
    if start is None:
        # pykrx supports long history; pick early default
        start = datetime(1990, 1, 1)
    s = _to_yyyymmdd(start)
    e = _to_yyyymmdd(end)
    df = stock.get_index_ohlcv_by_date(s, e, str(ticker), freq=freq)
    if df is None or df.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"]).astype({})
    # Normalize headers
    rename_map = {
        "시가": "Open",
        "고가": "High",
        "저가": "Low",
        "종가": "Close",
        "거래량": "Volume",
    }
    out = df.rename(columns=rename_map).copy()
    # Ensure we keep only expected columns
    keep = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in out.columns]
    out = out[keep]
    # Index normalization
    out.index.name = "Date"
    out = out.sort_index()
    return out


def _load_existing_daily(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=["Date"], index_col="Date")
        return df.sort_index()
    except Exception:
        return None


def update_index_daily_csv(ticker: str) -> tuple[Path, int]:
    """Create or incrementally update daily OHLCV CSV for a KRX index.

    Returns (path, rows_added)
    """
    path = _daily_csv_path(ticker)
    existing = _load_existing_daily(path)
    if existing is None or existing.empty:
        df = fetch_index_ohlcv(ticker)
        if df.empty:
            # write placeholder with schema
            pd.DataFrame(columns=["Open","High","Low","Close","Volume"]).to_csv(path, index_label="Date")
            return path, 0
        tmp = path.with_suffix(".csv.tmp")
        df.to_csv(tmp, index_label="Date")
        os.replace(tmp, path)
        return path, len(df)
    # Incremental
    last = existing.index.max()
    start = last + timedelta(days=1)
    new = fetch_index_ohlcv(ticker, start=start)
    if new is None or new.empty:
        return path, 0
    combined = pd.concat([existing, new])
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    tmp = path.with_suffix(".csv.tmp")
    combined.to_csv(tmp, index_label="Date")
    os.replace(tmp, path)
    return path, len(new)


def update_index_daily_csv_quick(ticker: str, years: int = 3) -> tuple[Path, int]:
    """Write or update daily OHLCV but limit initial backfill to recent years.

    - If CSV exists: delegate to incremental `update_index_daily_csv`.
    - If not: fetch from (today - years) to today to keep test runs fast.
    Returns (path, rows_added or total rows if new file).
    """
    path = _daily_csv_path(ticker)
    existing = _load_existing_daily(path)
    if existing is not None and not existing.empty:
        return update_index_daily_csv(ticker)
    end = datetime.today()
    start = end - timedelta(days=int(years * 365))
    df = fetch_index_ohlcv(ticker, start=start, end=end)
    if df is None or df.empty:
        # ensure schema file
        pd.DataFrame(columns=["Open","High","Low","Close","Volume"]).to_csv(path, index_label="Date")
        return path, 0
    tmp = path.with_suffix(".csv.tmp")
    df.to_csv(tmp, index_label="Date")
    os.replace(tmp, path)
    return path, len(df)


# --------------------
# Fundamentals (PE/PB/Dividend Yield)
# --------------------

VAL_FIELDS = [
    "trailingPE",                # from PER
    "priceToBook",               # from PBR
    "trailingAnnualDividendYield"  # from 배당수익률 (as decimal)
]


def fetch_index_fundamentals(ticker: str, start: str | datetime, end: str | datetime) -> pd.DataFrame:
    """Fetch KRX index fundamentals between dates; map to world_indices valuation schema.

    Returns DataFrame with columns [Date, symbol, currency, quoteType, trailingPE, priceToBook, trailingAnnualDividendYield]
    """
    s = _to_yyyymmdd(start)
    e = _to_yyyymmdd(end)
    df = stock.get_index_fundamental_by_date(s, e, str(ticker))
    if df is None or df.empty:
        return pd.DataFrame(columns=["Date","symbol","currency","quoteType",*VAL_FIELDS])

    # Rename columns to English and support both 'DIV' and '배당수익률'
    rename_map = {
        "PER": "trailingPE",
        "PBR": "priceToBook",
        "배당수익률": "trailingAnnualDividendYield",
        "DIV": "trailingAnnualDividendYield",
    }
    out = df.rename(columns=rename_map).copy()
    # Numeric conversions
    for c in ["trailingPE", "priceToBook", "trailingAnnualDividendYield"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    # Dividend yield from percentage to decimal if looks like percentage
    if "trailingAnnualDividendYield" in out.columns:
        ser = out["trailingAnnualDividendYield"]
        if ser.dropna().gt(1.0).mean() > 0.5:
            out["trailingAnnualDividendYield"] = ser / 100.0
    # Treat rows with missing/placeholder as invalid: drop where both PE and PB are <=0 or NaN
    pe = out.get("trailingPE")
    pb = out.get("priceToBook")
    if pe is not None and pb is not None:
        mask_valid = (~pe.fillna(0).le(0)) | (~pb.fillna(0).le(0))
        out = out[mask_valid]
    # Keep desired cols in standard order
    keep_metrics = [c for c in ["trailingPE","priceToBook","trailingAnnualDividendYield"] if c in out.columns]
    out = out[keep_metrics]
    out.index.name = "Date"
    out = out.sort_index().reset_index()
    # Normalize Date to ISO string
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.date.astype(str)
    # Append metadata
    out["symbol"] = _symbol_for_krx_index(ticker)
    out["currency"] = "KRW"
    out["quoteType"] = "INDEX"
    # Final column order
    columns = ["Date", *keep_metrics, "symbol", "currency", "quoteType"]
    out = out[columns]
    return out


def update_index_valuation_csv(ticker: str, mode: str = "append_today") -> tuple[Path, int]:
    """Update valuation CSV for a KRX index.

    - mode='append_today': append only today's row if missing
    - mode='backfill': if file missing/empty, write full history; else append from last+1 day
    Returns (path, rows_added)
    """
    path = _valuation_csv_path(ticker)
    # Use KST for KRX-related dating and summaries
    today_str = datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat()

    canonical_cols = ["Date", "trailingPE", "priceToBook", "trailingAnnualDividendYield", "symbol", "currency", "quoteType"]

    def _ensure_canonical(path: Path):
        try:
            df0 = pd.read_csv(path)
            # Add missing cols
            for col in canonical_cols:
                if col not in df0.columns:
                    df0[col] = pd.NA
            df0 = df0[canonical_cols]
            df0 = df0.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
            tmp2 = path.with_suffix(".csv.tmp")
            df0.to_csv(tmp2, index=False)
            os.replace(tmp2, path)
        except Exception:
            pass

    if mode == "append_today":
        # Guard against same-day zeros by looking back a few days
        lookback_days = int(os.environ.get("KRX_VAL_LOOKBACK", "7"))
        start_dt = datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=lookback_days)
        df = fetch_index_fundamentals(ticker, start_dt, today_str)
        if df is None or df.empty:
            # Ensure file exists
            if not path.exists():
                pd.DataFrame(columns=["Date","symbol","currency","quoteType",*VAL_FIELDS]).to_csv(path, index=False)
            _ensure_canonical(path)
            return path, 0
        if path.exists():
            try:
                cur = pd.read_csv(path)
                # Normalize Date to ISO string and numeric metrics
                if "Date" in cur.columns:
                    cur["Date"] = pd.to_datetime(cur["Date"], errors="coerce").dt.date.astype(str)
                for c in ["trailingPE", "priceToBook"]:
                    if c in cur.columns:
                        cur[c] = pd.to_numeric(cur[c], errors="coerce")
                # Determine last row validity
                if cur.empty:
                    last_date = "1990-01-01"
                    invalid_last = False
                else:
                    last_date = cur["Date"].max()
                    last_row = cur[cur["Date"] == last_date].tail(1)
                    pe_ok = not (last_row.get("trailingPE").fillna(0) <= 0).all() if "trailingPE" in last_row.columns else False
                    pb_ok = not (last_row.get("priceToBook").fillna(0) <= 0).all() if "priceToBook" in last_row.columns else False
                    invalid_last = not (pe_ok or pb_ok)

                appended_df = None
                if invalid_last:
                    # Replace the invalid last row with the latest valid row from df (if any)
                    latest_valid = df.tail(1)
                    if latest_valid.empty:
                        _ensure_canonical(path)
                        return path, 0
                    cur = cur[cur["Date"] != last_date]
                    appended_df = latest_valid
                else:
                    # Append any newer valid rows
                    df_new = df[df["Date"] > last_date]
                    if df_new.empty:
                        _ensure_canonical(path)
                        return path, 0
                    appended_df = df_new
                new_df = pd.concat([cur, appended_df], ignore_index=True)
            except Exception:
                new_df = df
        else:
            # New file: take only the latest valid row from lookback window (if any)
            new_df = df.tail(1)
        # Reorder to canonical schema and sort
        for col in canonical_cols:
            if col not in new_df.columns:
                new_df[col] = pd.NA
        # Ensure Date as ISO string before sorting/dedup
        if "Date" in new_df.columns:
            new_df["Date"] = pd.to_datetime(new_df["Date"], errors="coerce").dt.date.astype(str)
        new_df = new_df[canonical_cols]
        new_df = new_df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
        tmp = path.with_suffix(".csv.tmp")
        new_df.to_csv(tmp, index=False)
        os.replace(tmp, path)
        # Compute appended row count if possible
        try:
            if 'appended_df' in locals() and isinstance(appended_df, pd.DataFrame):
                added_count = len(appended_df)
            elif 'df_new' in locals() and isinstance(df_new, pd.DataFrame):
                added_count = len(df_new)
            else:
                added_count = len(new_df)
        except Exception:
            added_count = 0
        return path, added_count

    # backfill mode: always fetch full history and merge with existing
    try:
        cur = pd.read_csv(path) if path.exists() and os.path.getsize(path) > 0 else None
        if cur is not None and not cur.empty and "Date" in cur.columns:
            cur["Date"] = pd.to_datetime(cur["Date"], errors="coerce").dt.date.astype(str)
    except Exception:
        cur = None
    full_df = fetch_index_fundamentals(ticker, "19900101", datetime.today())
    if full_df is None or full_df.empty:
        if not path.exists():
            pd.DataFrame(columns=["Date","symbol","currency","quoteType",*VAL_FIELDS]).to_csv(path, index=False)
        _ensure_canonical(path)
        return path, 0
    # Canonical order
    for col in canonical_cols:
        if col not in full_df.columns:
            full_df[col] = pd.NA
    full_df = full_df[canonical_cols]
    if cur is None or cur.empty:
        new_df = full_df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
        added = len(new_df)
    else:
        merged = pd.concat([cur, full_df], ignore_index=True)
        merged = merged.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
        added = max(0, len(merged) - len(cur))
        new_df = merged
    tmp = path.with_suffix(".csv.tmp")
    new_df.to_csv(tmp, index=False)
    os.replace(tmp, path)
    return path, added


# --------------------
# Quick manual test
# --------------------

# Test batch: user-provided KRX indices (code -> name)
KRX_TEST_INDICES: dict[str, str] = {
    "1001": "코스피",
    "1002": "코스피 대형주",
    "1003": "코스피 중형주",
    "1004": "코스피 소형주",
    "1005": "음식료·담배",
    "1006": "섬유·의류",
    "1007": "종이·목재",
    "1008": "화학",
    "1009": "제약",
    "1010": "비금속",
    "1011": "금속",
    "1012": "기계·장비",
    "1013": "전기전자",
    "1014": "의료·정밀기기",
    "1015": "운송장비·부품",
    "1016": "유통",
    "1017": "전기·가스",
    "1018": "건설",
    "1019": "운송·창고",
    "1020": "통신",
    "1021": "금융",
    "1024": "증권",
    "1025": "보험",
    "1026": "일반서비스",
    "1027": "제조",
    "1028": "코스피 200",
    "1034": "코스피 100",
    "1035": "코스피 50",
    "1045": "부동산",
    "1046": "IT 서비스",
    "1047": "오락·문화",
    "1150": "코스피 200 커뮤니케이션서비스",
    "1151": "코스피 200 건설",
    "1152": "코스피 200 중공업",
    "1153": "코스피 200 철강/소재",
    "1154": "코스피 200 에너지/화학",
    "1155": "코스피 200 정보기술",
    "1156": "코스피 200 금융",
    "1157": "코스피 200 생활소비재",
    "1158": "코스피 200 경기소비재",
    "1159": "코스피 200 산업재",
    "1160": "코스피 200 헬스케어",
    "1167": "코스피 200 중소형주",
    "1182": "코스피 200 초대형제외 지수",
    "1224": "코스피 200 비중상한 30%",
    "1227": "코스피 200 비중상한 25%",
    "1232": "코스피 200 비중상한 20%",
    "1244": "코스피200제외 코스피지수",
    "1894": "코스피 200 TOP 10",
}


def batch_update_indices(index_map: dict[str, str] | None = None, valuation_mode: str = "append_today") -> pd.DataFrame:
    """Run price and valuation updates for a batch of indices.

    Returns a summary DataFrame with columns:
    [ticker, name, price_path, price_rows_added, valuation_path, valuation_rows_added, status]
    """
    if index_map is None:
        index_map = KRX_TEST_INDICES
    rows: list[dict] = []
    run_at = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    for code, name in index_map.items():
        entry = {
            "ticker": code,
            "name": name,
            "price_path": None,
            "price_rows_added": 0,
            "price_updated": False,
            "price_status": None,
            "price_reason": None,
            "valuation_path": None,
            "valuation_rows_added": 0,
            "valuation_updated": False,
            "valuation_status": None,
            "valuation_reason": None,
            "status": "ok",
            "run_at": run_at,
        }
        try:
            price_mode = os.environ.get("KRX_PRICE_MODE", "quick").lower()
            if price_mode == "full":
                p_path, p_added = update_index_daily_csv(code)
            else:
                # quick mode limits initial backfill for faster tests
                years = int(os.environ.get("KRX_PRICE_YEARS", "3"))
                p_path, p_added = update_index_daily_csv_quick(code, years=years)
            entry["price_path"] = str(p_path)
            entry["price_rows_added"] = int(p_added)
            if p_added > 0:
                entry["price_updated"] = True
                entry["price_status"] = "updated"
                entry["price_reason"] = f"appended {p_added}"
            else:
                entry["price_status"] = "no_change"
                entry["price_reason"] = "up_to_date or no_new_rows"
        except Exception as e:
            entry["status"] = f"price_error: {e}"[:200]
            entry["price_status"] = "error"
            entry["price_reason"] = str(e)[:200]
        try:
            v_path, v_added = update_index_valuation_csv(code, mode=valuation_mode)
            entry["valuation_path"] = str(v_path)
            entry["valuation_rows_added"] = int(v_added)
            if v_added > 0:
                entry["valuation_updated"] = True
                entry["valuation_status"] = "updated"
                entry["valuation_reason"] = f"appended {v_added}"
            else:
                entry["valuation_status"] = "no_change"
                entry["valuation_reason"] = "up_to_date or no_new_rows"
            if entry["status"] == "ok":
                entry["status"] = "ok"
        except Exception as e:
            # Preserve prior error if any
            if entry["status"] == "ok":
                entry["status"] = f"valuation_error: {e}"[:200]
            else:
                entry["status"] = (entry["status"] + f"; valuation_error: {e}")[:200]
            entry["valuation_status"] = "error"
            entry["valuation_reason"] = str(e)[:200]
        rows.append(entry)
        # Minimal progress print for long runs
        print(f"[{code} {name}] price+val done -> status={entry['status']}")
    return pd.DataFrame(rows)

if __name__ == "__main__":
    # If KRX_INDEX is set to 'ALL' or 'TEST_ALL', run batch for provided list.
    env_code = os.environ.get("KRX_INDEX", "1001")
    mode = os.environ.get("KRX_VAL_MODE", "append_today")  # or 'backfill'

    if env_code.upper() in {"ALL", "TEST_ALL"}:
        print("Running batch update for KRX test indices...")
        summary = batch_update_indices(KRX_TEST_INDICES, valuation_mode=mode)
        # Write a small run summary next to data dir for quick inspection
        out_csv = BASE_DIR / "data" / "krx_batch_summary.csv"
        summary.to_csv(out_csv, index=False)
        print(f"Batch summary saved: {out_csv}")
        # Print compact status counts
        print(summary["status"].value_counts(dropna=False))
    else:
        # Single index mode (default 1001)
        code = env_code
        print(f"Updating daily OHLCV for KRX index {code}...")
        p, added = update_index_daily_csv(code)
        print(f"Saved: {p} (+{added} rows)")

        print(f"Updating valuation snapshots for {code} (mode={mode})...")
        vp, vadded = update_index_valuation_csv(code, mode=mode)
        print(f"Saved: {vp} (+{vadded} rows)")
