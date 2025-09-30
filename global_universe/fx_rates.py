"""
Collect FX rates for all currencies used in investment_universe
and produce per-currency daily conversion factors to USD and KRW.

- Sources: Yahoo Finance chart API via curl to avoid blocking
- Output: CSV files under global_universe/data/fx/<CURRENCY>.csv
  Columns: Date,toUSD,toKRW,currency

Conventions:
- Yahoo 'USD{CCY}=X' represents {CCY} per USD. For example, USDJPY=X ≈ 150.
  Hence conversion factors are:
    CCY->USD = 1 / (USD{CCY})
    CCY->KRW = (USDKRW) / (USD{CCY})
- Special subunit mapping: 'GBp' is treated as 0.01 GBP

Usage:
  python -m global_universe.fx_rates [--range 10y|max] [--force]
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple
from datetime import datetime, timedelta, timezone

import pandas as pd

THIS_DIR = Path(__file__).resolve().parent
UNIVERSE_FILE = THIS_DIR / "world_indices.py"
OUT_DIR = THIS_DIR / "data" / "fx"


def load_investment_universe_literal(path: Path) -> Dict[str, Any]:
    import ast
    text = path.read_text(encoding="utf-8")
    marker = "investment_universe ="
    idx = text.find(marker)
    if idx == -1:
        raise KeyError("investment_universe assignment not found")
    brace_idx = text.find("{", idx)
    if brace_idx == -1:
        raise ValueError("Could not find opening '{' for investment_universe")
    depth = 0
    end_idx = None
    for i in range(brace_idx, len(text)):
        ch = text[i]
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                end_idx = i + 1
                break
    if end_idx is None:
        raise ValueError("Failed to match braces for investment_universe")
    literal = text[brace_idx:end_idx]
    return ast.literal_eval(literal)


def gather_currencies(universe: Dict[str, Any]) -> List[str]:
    cur: set[str] = set()
    for _, cdata in universe.items():
        # Include country-level default currency if present
        ctry_curr = cdata.get("currency")
        if ctry_curr:
            cur.add(str(ctry_curr))
        for kind in ("sectors", "factors", "themes"):
            for _, entry in (cdata.get(kind, {}) or {}).items():
                code = entry.get("currency")
                if code:
                    cur.add(str(code))
    # Always include USD and KRW for reference
    cur.update({"USD", "KRW"})
    # Stable order: USD first, then KRW, then alpha
    ordered = ["USD", "KRW"] + sorted([c for c in cur if c not in {"USD", "KRW"}])
    return ordered


def normalize_currency(code: str) -> Tuple[str, float]:
    """Return (base_code, scale_per_unit).
    E.g., GBp -> (GBP, 0.01).
    """
    if code == "GBp":
        return "GBP", 0.01
    if code == "GBX":
        return "GBP", 0.01
    return code, 1.0


def fetch_chart_series(symbol: str, range_: str = "10y", interval: str = "1d") -> pd.Series:
    """Fetch Yahoo chart series via range/interval. May be downsampled by Yahoo for long ranges."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    cmd = [
        "curl",
        "-sS",
        "-H",
        f"User-Agent: {headers['User-Agent']}",
        "-H",
        f"Accept: {headers['Accept']}",
        "-H",
        f"Accept-Language: {headers['Accept-Language']}",
        f"{url}?range={range_}&interval={interval}",
    ]
    out = subprocess.check_output(cmd, timeout=30)
    data = json.loads(out.decode("utf-8", errors="ignore"))
    result = (data or {}).get("chart", {}).get("result")
    if not result:
        raise RuntimeError(f"No chart result for {symbol}")
    res = result[0]
    ts = res.get("timestamp") or []
    quote = (res.get("indicators") or {}).get("quote") or []
    closes = (quote[0].get("close") if quote else None) or []
    # Convert to series
    if not ts or not closes:
        raise RuntimeError(f"Empty series for {symbol}")
    # Some closes may be None; zip and drop
    pairs = [(t, c) for t, c in zip(ts, closes) if c is not None]
    if not pairs:
        raise RuntimeError(f"No valid close values for {symbol}")
    idx = pd.to_datetime([p[0] for p in pairs], unit="s", utc=True).date
    ser = pd.Series([p[1] for p in pairs], index=pd.Index(idx, name="Date"), name=symbol)
    return ser


def fetch_chart_series_daily(symbol: str, start_epoch: int = 0, end_epoch: int | None = None) -> pd.Series:
    """Fetch Yahoo chart series using explicit period1/period2 and interval=1d to enforce daily granularity."""
    if end_epoch is None:
        end_epoch = int(time.time())
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    cmd = [
        "curl",
        "-sS",
        "-H",
        f"User-Agent: {headers['User-Agent']}",
        "-H",
        f"Accept: {headers['Accept']}",
        "-H",
        f"Accept-Language: {headers['Accept-Language']}",
        f"{url}?period1={start_epoch}&period2={end_epoch}&interval=1d",
    ]
    out = subprocess.check_output(cmd, timeout=45)
    data = json.loads(out.decode("utf-8", errors="ignore"))
    result = (data or {}).get("chart", {}).get("result")
    if not result:
        raise RuntimeError(f"No chart result for {symbol}")
    res = result[0]
    ts = res.get("timestamp") or []
    quote = (res.get("indicators") or {}).get("quote") or []
    closes = (quote[0].get("close") if quote else None) or []
    if not ts or not closes:
        raise RuntimeError(f"Empty series for {symbol}")
    pairs = [(t, c) for t, c in zip(ts, closes) if c is not None]
    if not pairs:
        raise RuntimeError(f"No valid close values for {symbol}")
    idx = pd.to_datetime([p[0] for p in pairs], unit="s", utc=True).date
    ser = pd.Series([p[1] for p in pairs], index=pd.Index(idx, name="Date"), name=symbol)
    return ser


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_currency_csv(code: str, df: pd.DataFrame) -> Path:
    ensure_dir(OUT_DIR)
    out = OUT_DIR / f"{code}.csv"
    df_sorted = df.sort_index()
    df_sorted.to_csv(out, index_label="Date")
    return out


def _epoch(dt) -> int:
    return int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp())


def build_fx_rates(range_: str = "10y", force: bool = False, recent_days: int | None = None, buffer_days: int = 3) -> List[Path]:
    universe = load_investment_universe_literal(UNIVERSE_FILE)
    currencies = gather_currencies(universe)

    # Determine start epoch for incremental fetch
    now = datetime.now(timezone.utc)
    # Default start for full backfill
    start_epoch_usdkrw = 0
    existing_krw = OUT_DIR / "KRW.csv"
    if recent_days is not None and existing_krw.exists():
        try:
            df_old = pd.read_csv(existing_krw, parse_dates=["Date"], index_col="Date")
            last = df_old.index.max()
            if pd.notna(last):
                start_dt = (last.to_pydatetime().replace(tzinfo=timezone.utc) - timedelta(days=buffer_days))
                start_epoch_usdkrw = int(start_dt.timestamp())
        except Exception:
            # Fall back to 0 if parsing fails
            start_epoch_usdkrw = 0

    # Fetch USDKRW once with enforced daily
    usdk_rw = fetch_chart_series_daily("USDKRW=X", start_epoch=start_epoch_usdkrw)

    # Cache for USD{CCY}
    usd_per_ccy: Dict[str, pd.Series] = {}

    for code in currencies:
        base, _ = normalize_currency(code)
        if base in {"USD", "KRW"}:
            continue
        if base in usd_per_ccy:
            continue
        sym = f"USD{base}=X"
        try:
            # Determine start epoch per currency using its existing file if recent mode
            start_epoch_ccy = 0
            dest_ccy = OUT_DIR / f"{base}.csv"
            if recent_days is not None and dest_ccy.exists():
                try:
                    df_old = pd.read_csv(dest_ccy, parse_dates=["Date"], index_col="Date")
                    last = df_old.index.max()
                    if pd.notna(last):
                        start_dt = (last.to_pydatetime().replace(tzinfo=timezone.utc) - timedelta(days=buffer_days))
                        start_epoch_ccy = int(start_dt.timestamp())
                except Exception:
                    start_epoch_ccy = 0
            ser = fetch_chart_series_daily(sym, start_epoch=start_epoch_ccy)
        except Exception as e:
            # Fallback to legacy symbol (e.g., JPY=X which is USDJPY)
            legacy = f"{base}=X"
            try:
                start_epoch_ccy = 0
                dest_ccy = OUT_DIR / f"{base}.csv"
                if recent_days is not None and dest_ccy.exists():
                    try:
                        df_old = pd.read_csv(dest_ccy, parse_dates=["Date"], index_col="Date")
                        last = df_old.index.max()
                        if pd.notna(last):
                            start_dt = (last.to_pydatetime().replace(tzinfo=timezone.utc) - timedelta(days=buffer_days))
                            start_epoch_ccy = int(start_dt.timestamp())
                    except Exception:
                        start_epoch_ccy = 0
                ser = fetch_chart_series_daily(legacy, start_epoch=start_epoch_ccy)
            except Exception as e2:
                raise RuntimeError(f"Failed to fetch FX for {base}: {sym} and fallback {legacy} failed: {e}; {e2}")
        usd_per_ccy[base] = ser

    # Build daily index for the period fetched (incremental or full)
    start_date = min([min(usdk_rw.index)] + [min(s.index) for s in usd_per_ccy.values() if len(s) > 0])
    end_date = max([max(usdk_rw.index)] + [max(s.index) for s in usd_per_ccy.values() if len(s) > 0])
    idx = pd.date_range(start=start_date, end=end_date, freq="D")

    # Prepare USDKRW aligned series (daily, forward-filled)
    usdk_rw_full = pd.Series(index=idx, dtype=float)
    usdk_rw_full.loc[usdk_rw.index] = usdk_rw.values
    usdk_rw_full = usdk_rw_full.sort_index().ffill()

    written: List[Path] = []

    for code in currencies:
        base, scale = normalize_currency(code)
        # Build series for toUSD and toKRW
        if base == "USD":
            to_usd = pd.Series(1.0, index=idx)
            to_krw = usdk_rw_full.copy()
        elif base == "KRW":
            to_usd = 1.0 / usdk_rw_full
            to_krw = pd.Series(1.0, index=idx)
        else:
            c_per_usd = pd.Series(index=idx, dtype=float)
            ser = usd_per_ccy.get(base)
            if ser is None:
                raise RuntimeError(f"Missing USD{base} series")
            c_per_usd.loc[ser.index] = ser.values
            c_per_usd = c_per_usd.sort_index().ffill()
            to_usd = 1.0 / c_per_usd
            to_krw = usdk_rw_full / c_per_usd

        if scale != 1.0:
            to_usd = to_usd * scale
            to_krw = to_krw * scale

        df = pd.DataFrame({
            "toUSD": to_usd,
            "toKRW": to_krw,
            "currency": code,
        }, index=idx)

        # Drop rows where both numeric conversions are missing (ignore constant 'currency' col)
        df = df.dropna(how="all", subset=["toUSD", "toKRW"])

        dest = OUT_DIR / f"{code}.csv"
        if dest.exists():
            try:
                old = pd.read_csv(dest, parse_dates=["Date"], index_col="Date")
                merged = pd.concat([old, df])
                merged = merged[~merged.index.duplicated(keep="last")].sort_index()
                merged = merged.dropna(how="all", subset=["toUSD", "toKRW"])
                if not merged.equals(old):
                    merged.to_csv(dest, index_label="Date")
                    written.append(dest)
                else:
                    # No change for this currency
                    pass
            except Exception:
                # If read fails, just write new df
                written.append(write_currency_csv(code, df))
        else:
            # Bootstrap case: write new
            written.append(write_currency_csv(code, df))

    return written


def main(argv: List[str] | None = None) -> int:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--range", default="10y", help="Deprecated: kept for compatibility; ignored in daily mode")
    p.add_argument("--force", action="store_true", help="Rewrite even if output exists (full mode)")
    p.add_argument("--recent", type=int, default=None, help="Incremental mode: only fetch last N days and merge")
    p.add_argument("--buffer-days", type=int, default=3, help="Backfill cushion when merging (default: 3)")
    args = p.parse_args(argv)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    written = build_fx_rates(range_=args.range, force=args.force, recent_days=args.recent, buffer_days=args.buffer_days)
    if written:
        print("Wrote/updated:")
        for pth in written:
            print(f"- {pth}")
    else:
        print("FX files already up-to-date (or nothing to write)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
