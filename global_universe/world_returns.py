"""
World indices return analysis utilities.

This module builds on global_universe.world_indices to:
- Ensure price data exists for a curated set of country benchmarks
- Compute local-currency and USD-converted returns for flexible periods
- Optionally compute daily-vol based Sharpe ratios

Key entry points:
- build_default_index_specs() -> list[IndexSpec]
- compute_returns_table(...)

The module intentionally keeps side effects small; it writes/reads daily price
CSVs under global_universe/data/daily via world_indices helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

# Local import (same package)
from . import world_indices as wi
from . import fx_rates as fxmod


# ----------------------------
# Specs and defaults
# ----------------------------

@dataclass(frozen=True)
class IndexSpec:
    """Specification for a benchmark.

    - name: display name
    - local_symbol: price series in local currency (index preferred)
    - local_currency: 3-letter currency code; if None, resolved via Yahoo metadata
    - usd_proxy: optional USD-quoted ETF to compare with FX-converted series
    """

    name: str
    local_symbol: str
    local_currency: Optional[str] = None
    usd_proxy: Optional[str] = None


def build_default_index_specs() -> list[IndexSpec]:
    """Return the default world benchmark set similar to the sample image.

    Notes
    - Local symbols prioritize country indices where possible for native currency.
    - usd_proxy uses a liquid USD ETF when available (for a sanity check vs FX conversion).
    """
    return [
        IndexSpec("S&P 500", "^GSPC", "USD", None),
        IndexSpec("Nasdaq 100", "^NDX", "USD", None),
        IndexSpec("Russell 2000", "^RUT", "USD", None),
        IndexSpec("S&P 600 Small-cap", "^SP600", "USD", None),
        IndexSpec("S&P 400 Mid-cap", "^SP400", "USD", None),
        IndexSpec("Canadian S&P/TSX", "^GSPTSE", "CAD", "EWC"),
        IndexSpec("Hang Seng Index", "^HSI", "HKD", "EWH"),
        IndexSpec("Chinese CSI 300", "000300.SS", "CNY", "ASHR"),
        IndexSpec("EuroStoxx 50", "^STOXX50E", "EUR", "FEZ"),
        IndexSpec("Japanese Nikkei 225", "^N225", "JPY", "EWJ"),
        IndexSpec("Brazil Ibovespa", "^BVSP", "BRL", "EWZ"),
        IndexSpec("MSCI World ex-US", "ACWX", "USD", None),  # USD-quoted composite
    ]


# ----------------------------
# IO helpers
# ----------------------------

@lru_cache(maxsize=256)
def _read_price_series(symbol: str) -> pd.Series:
    """Return daily close series for symbol from the local CSV cache.

    Prefers 'Adj Close' when present; falls back to 'Close'.
    Ensures the CSV exists by backfilling via world_indices if missing.
    """
    # Ensure CSV exists (quick backfill if empty)
    p = wi._csv_path_for(symbol)
    if not p.exists():
        try:
            wi.backfill_symbol_csv(symbol)
        except Exception:
            pass
    try:
        df = pd.read_csv(p)
    except Exception:
        return pd.Series(dtype=float)
    if df is None or df.empty or "Date" not in df.columns:
        return pd.Series(dtype=float)
    # Parse with utc=True to handle mixed offsets (-05:00/-04:00 across DST)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
    # Convert to tz-naive for clean indexing
    try:
        df.index = df.index.tz_convert(None)
    except Exception:
        pass
    col = "Adj Close" if "Adj Close" in df.columns else ("Close" if "Close" in df.columns else None)
    if col is None:
        return pd.Series(dtype=float)
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    s.name = symbol
    return s


def _first_on_or_after(s: pd.Series, dt: pd.Timestamp) -> Optional[pd.Timestamp]:
    try:
        idx = s.index
        pos = idx.searchsorted(dt, side="left")
        if pos >= len(idx):
            return None
        return idx[pos]
    except Exception:
        return None


def _last_on_or_before(s: pd.Series, dt: pd.Timestamp) -> Optional[pd.Timestamp]:
    try:
        idx = s.index
        pos = idx.searchsorted(dt, side="right") - 1
        if pos < 0:
            return None
        return idx[pos]
    except Exception:
        return None


def _ensure_series(symbols: Iterable[str]) -> None:
    """Ensure daily price CSVs exist for the given symbols (quick pass)."""
    for sym in symbols:
        try:
            wi.backfill_symbol_csv(sym)
        except Exception:
            # Best-effort; analysis can still proceed for available symbols
            pass


# ----------------------------
# FX helpers
# ----------------------------

@lru_cache(maxsize=64)
def _fx_usd_per_local(currency: str) -> pd.Series:
    """Return a series of USD per 1 unit of local `currency`.

    Strategy:
    - Try direct pair `{CUR}USD=X` (e.g., EURUSD=X). If available, use as-is.
    - Else try `USD{CUR}=X` (e.g., USDJPY=X) and invert.
    - Else try `{CUR}=X` which Yahoo often maps to USD/{CUR}, and invert.
    - If currency is USD, return a series of ones aligned to S&P 500 dates.
    """
    cur = (currency or "").upper()
    if cur in ("", "USD"):
        # Build a unity series aligned to SPX for convenience
        base = _read_price_series("^GSPC")
        return pd.Series(1.0, index=base.index)

    candidates = [f"{cur}USD=X", f"USD{cur}=X", f"{cur}=X"]
    for sym in candidates:
        s = _read_price_series(sym)
        if s.empty:
            continue
        if sym.startswith("USD") or sym == f"{cur}=X":
            # Series is local per USD -> invert to USD per local
            with np.errstate(divide="ignore", invalid="ignore"):
                inv = 1.0 / s.astype(float)
                inv = inv.replace([np.inf, -np.inf], np.nan).dropna()
            inv.name = f"USD/{cur} (derived)"
            return inv
        else:
            s.name = f"USD/{cur}"
            return s
    # Fallback: unity (acts like USD) to avoid crashing; caller can detect
    base = _read_price_series("^GSPC")
    return pd.Series(1.0, index=base.index)


@lru_cache(maxsize=64)
def _fx_usd_from_cache(currency: str) -> pd.Series:
    """Load daily USD per currency series from fx_rates module cache, if available.

    Returns an empty series if not found.
    """
    code = (currency or "").upper()
    # Map sub-units (GBp/GBX) to GBP like fx_rates
    if code in {"GBP", "GBP.", "GBp", "GBX"}:
        code = "GBP"
    fx_dir = fxmod.OUT_DIR
    path = fx_dir / f"{code}.csv"
    if not path.exists():
        # Do not trigger a full FX rebuild here; it is expensive and blocks UI.
        # Fallback to Yahoo FX pairs path handled by _fx_usd_per_local.
        return pd.Series(dtype=float)
    try:
        df = pd.read_csv(path, parse_dates=["Date"], index_col="Date")
        if df.empty or "toUSD" not in df.columns:
            return pd.Series(dtype=float)
        s = pd.to_numeric(df["toUSD"], errors="coerce").dropna()
        s.index = pd.to_datetime(s.index).normalize()
        s.name = f"USD/{code} (cache)"
        return s
    except Exception:
        return pd.Series(dtype=float)


def convert_local_to_usd(local_prices: pd.Series, currency: str) -> pd.Series:
    """Convert a local-currency price series into USD using FX daily closes."""
    # Prefer high-quality daily series from cache; fallback to pair fetch
    fx = _fx_usd_from_cache(currency)
    if fx.empty:
        fx = _fx_usd_per_local(currency)
    if fx.empty or local_prices.empty:
        return pd.Series(dtype=float)
    # Align by calendar date (time components can differ by market)
    lp = local_prices.copy()
    fx2 = fx.copy()
    try:
        lp.index = pd.to_datetime(lp.index, errors="coerce").normalize()
        fx2.index = pd.to_datetime(fx2.index, errors="coerce").normalize()
    except Exception:
        pass
    idx = lp.index.intersection(fx2.index)
    if len(idx) == 0:
        return pd.Series(dtype=float)
    usd = lp.loc[idx] * fx2.loc[idx]
    usd.name = f"{local_prices.name}_USD"
    return usd


# ----------------------------
# Return metrics
# ----------------------------

def _period_bounds(period: str | None, start: Optional[date | datetime], end: Optional[date | datetime]) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Resolve start/end bounds.

    - period: 'ytd' | '1y' | '3y' | '5y' | None
    - If period is None or 'custom', uses provided start/end with sensible defaults.
    """
    today = pd.Timestamp.today().normalize()
    if end is None:
        end_ts = today
    else:
        end_ts = pd.Timestamp(end).normalize()

    if period is not None:
        p = period.lower()
        if p in {"1m", "1mo", "30d"}:
            start_ts = end_ts - pd.DateOffset(months=1)
        elif p == "ytd":
            start_ts = pd.Timestamp(year=end_ts.year, month=1, day=1)
        elif p == "1y":
            start_ts = end_ts - pd.DateOffset(years=1)
        elif p == "3y":
            start_ts = end_ts - pd.DateOffset(years=3)
        elif p == "5y":
            start_ts = end_ts - pd.DateOffset(years=5)
        elif p == "10y":
            start_ts = end_ts - pd.DateOffset(years=10)
        else:
            # treat as custom if unrecognized
            start_ts = pd.Timestamp(start or (end_ts - pd.DateOffset(years=1)))
    else:
        start_ts = pd.Timestamp(start or (end_ts - pd.DateOffset(years=1)))
    return start_ts.normalize(), end_ts


def compute_point_to_point_return(series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> Optional[float]:
    """Compute cumulative return between first price on/after start and last on/before end.

    Returns decimal return (e.g., 0.123 for +12.3%) or None if insufficient data.
    """
    if series is None or series.empty:
        return None
    t0 = _first_on_or_after(series, start)
    t1 = _last_on_or_before(series, end)
    if t0 is None or t1 is None or t1 <= t0:
        return None
    p0 = float(series.loc[t0])
    p1 = float(series.loc[t1])
    if not np.isfinite(p0) or not np.isfinite(p1) or p0 <= 0:
        return None
    return (p1 / p0) - 1.0


def compute_sharpe(series: pd.Series, start: pd.Timestamp, end: pd.Timestamp, rf_annual: float = 0.0) -> Optional[float]:
    """Compute simple annualized Sharpe using daily closes in [start, end].

    - rf_annual: annualized risk-free rate (decimal). If 0, Sharpe = CAGR / vol.
    """
    if series is None or series.empty:
        return None
    t0 = _first_on_or_after(series, start)
    t1 = _last_on_or_before(series, end)
    if t0 is None or t1 is None or t1 <= t0:
        return None
    s = series.loc[t0:t1]
    rets = s.pct_change().dropna()
    if rets.empty:
        return None
    # Infer observation frequency to avoid mis-annualization for weekly/monthly series
    obs_per_year = _infer_obs_per_year(s.index)
    if obs_per_year is None or len(rets) < 20:
        return None
    avg = rets.mean()
    std = rets.std()
    if std == 0 or not np.isfinite(std):
        return None
    ann_ret = (1 + avg) ** obs_per_year - 1
    ann_vol = std * np.sqrt(obs_per_year)
    excess = ann_ret - rf_annual
    return float(excess / ann_vol)

def compute_sortino(series: pd.Series, start: pd.Timestamp, end: pd.Timestamp, rf_annual: float = 0.0) -> Optional[float]:
    """Compute annualized Sortino ratio using downside deviation of daily returns.

    - Threshold = daily risk-free rate (rf_annual / 252). Defaults to 0.
    - Returns None if not enough data.
    """
    if series is None or series.empty:
        return None
    t0 = _first_on_or_after(series, start)
    t1 = _last_on_or_before(series, end)
    if t0 is None or t1 is None or t1 <= t0:
        return None
    s = series.loc[t0:t1]
    rets = s.pct_change().dropna()
    if rets.empty:
        return None
    # Infer observation frequency
    obs_per_year = _infer_obs_per_year(s.index)
    if obs_per_year is None or len(rets) < 20:
        return None
    rf_daily = rf_annual / float(obs_per_year)
    # Downside deviation relative to rf_daily
    downside = rets - rf_daily
    downside = downside[downside < 0.0]
    if downside.empty:
        return None
    dd = downside.std()
    if dd == 0 or not np.isfinite(dd):
        return None
    # Annualize return and downside deviation similar to Sharpe
    avg = rets.mean()
    ann_ret = (1 + avg) ** obs_per_year - 1
    ann_dd = dd * np.sqrt(obs_per_year)
    excess = ann_ret - rf_annual
    return float(excess / ann_dd)


def _infer_obs_per_year(index: pd.DatetimeIndex) -> Optional[int]:
    """Infer observations per year from median day spacing.

    Returns 252 for daily, 52 for weekly, 12 for monthly, else a rounded estimate.
    """
    try:
        if not isinstance(index, pd.DatetimeIndex) or len(index) < 3:
            return None
        d = pd.Series(index).diff().dropna().dt.days.astype(float)
        if d.empty:
            return None
        med = float(np.nanmedian(d.values))
        if not np.isfinite(med) or med <= 0:
            return None
        if med <= 2.0:
            return 252
        if med <= 10.0:
            return 52
        if med <= 45.0:
            return 12
        # Fallback heuristic
        return max(1, int(round(365.0 / med)))
    except Exception:
        return None

def compute_returns_table(
    specs: Iterable[IndexSpec] | None = None,
    period: str | None = "ytd",
    start: Optional[date | datetime] = None,
    end: Optional[date | datetime] = None,
    calc_sharpe: bool = True,
    calc_sortino: bool = True,
    use_usd_proxy_if_available: bool = False,
    ensure_data: bool = False,
) -> pd.DataFrame:
    """Compute returns and optional Sharpe metrics for the given benchmarks.

    Columns:
    - name, local_symbol, usd_symbol, local_return, usd_return, diff_vs_sp500_local, diff_vs_sp500_usd
    - sharpe_local, sharpe_usd (if calc_sharpe)
    """
    spec_list = list(specs or build_default_index_specs())
    # Ensure inputs
    all_syms: list[str] = []
    for s in spec_list:
        all_syms.append(s.local_symbol)
        if s.usd_proxy:
            all_syms.append(s.usd_proxy)
    # Optionally ensure local CSVs exist (network). Default False for UI speed.
    if ensure_data:
        _ensure_series(all_syms)

    start_ts, end_ts = _period_bounds(period, start, end)

    # Preload price series
    price_cache: dict[str, pd.Series] = {sym: _read_price_series(sym) for sym in set(all_syms)}

    # Resolve currency helper
    def _cur_for(sym: str, exp: Optional[str]) -> Optional[str]:
        try:
            return wi.resolve_currency(sym, exp)
        except Exception:
            return exp

    # Compute SP500 baseline (USD both local/usd identical)
    spx = _read_price_series("^GSPC")
    spx_ret = compute_point_to_point_return(spx, start_ts, end_ts)
    spx_sharpe = compute_sharpe(spx, start_ts, end_ts) if calc_sharpe else None

    rows: list[dict] = []
    for spec in spec_list:
        s_loc = price_cache.get(spec.local_symbol)
        if s_loc is None or s_loc.empty:
            s_loc = _read_price_series(spec.local_symbol)
        cur = _cur_for(spec.local_symbol, spec.local_currency)
        usd_series = convert_local_to_usd(s_loc, cur or "USD") if cur != "USD" else s_loc

        # Optionally compare with USD proxy (ETF)
        usd_sym_used = None
        if use_usd_proxy_if_available and spec.usd_proxy:
            usd_proxy_series = price_cache.get(spec.usd_proxy)
            if usd_proxy_series is None or usd_proxy_series.empty:
                usd_proxy_series = _read_price_series(spec.usd_proxy)
            # choose the longer series for robustness
            if not usd_series.empty and not usd_proxy_series.empty:
                if len(usd_proxy_series) > len(usd_series):
                    usd_series = usd_proxy_series
                    usd_sym_used = spec.usd_proxy
            elif not usd_proxy_series.empty:
                usd_series = usd_proxy_series
                usd_sym_used = spec.usd_proxy

        r_loc = compute_point_to_point_return(s_loc, start_ts, end_ts)
        r_usd = compute_point_to_point_return(usd_series, start_ts, end_ts)

        sh_loc = compute_sharpe(s_loc, start_ts, end_ts) if calc_sharpe else None
        sh_usd = compute_sharpe(usd_series, start_ts, end_ts) if calc_sharpe else None
        so_loc = compute_sortino(s_loc, start_ts, end_ts) if calc_sortino else None
        so_usd = compute_sortino(usd_series, start_ts, end_ts) if calc_sortino else None

        rows.append({
            "name": spec.name,
            "local_symbol": spec.local_symbol,
            "local_currency": cur,
            "usd_symbol": usd_sym_used or (spec.usd_proxy if (use_usd_proxy_if_available and spec.usd_proxy) else (spec.local_symbol if (cur == "USD") else None)),
            "local_return": r_loc,
            "usd_return": r_usd,
            "diff_vs_sp500_local": (None if (r_loc is None or spx_ret is None) else (r_loc - spx_ret)),
            "diff_vs_sp500_usd": (None if (r_usd is None or spx_ret is None) else (r_usd - spx_ret)),
            "sharpe_local": sh_loc,
            "sharpe_usd": sh_usd,
            "sortino_local": so_loc,
            "sortino_usd": so_usd,
        })

    df = pd.DataFrame(rows)
    # Sorting/display helpers
    def _pct_fmt(x: Optional[float]) -> str:
        if x is None or not np.isfinite(x):
            return ""
        return f"{x*100:+.2f}"

    def _sh_fmt(x: Optional[float]) -> str:
        if x is None or not np.isfinite(x):
            return ""
        return f"{x:.2f}"

    out = df.copy()
    # Add formatted strings for quick human viewing (kept alongside raw cols)
    out["local_return_%"] = df["local_return"].apply(_pct_fmt)
    out["usd_return_%"] = df["usd_return"].apply(_pct_fmt)
    out["diff_vs_sp500_local_%"] = df["diff_vs_sp500_local"].apply(_pct_fmt)
    out["diff_vs_sp500_usd_%"] = df["diff_vs_sp500_usd"].apply(_pct_fmt)
    out["sharpe_local_str"] = df["sharpe_local"].apply(_sh_fmt)
    out["sharpe_usd_str"] = df["sharpe_usd"].apply(_sh_fmt)
    out["sortino_local_str"] = df["sortino_local"].apply(_sh_fmt)
    out["sortino_usd_str"] = df["sortino_usd"].apply(_sh_fmt)

    out.attrs["period"] = period
    out.attrs["start"] = str(start_ts.date())
    out.attrs["end"] = str(end_ts.date())
    return out


def clear_caches() -> None:
    """Clear memoized readers (for app's refresh button)."""
    try:
        _read_price_series.cache_clear()
        _fx_usd_from_cache.cache_clear()
        _fx_usd_per_local.cache_clear()
    except Exception:
        pass


def save_returns_report(
    df: pd.DataFrame,
    out_dir: Path | str | None = None,
    basename: str = "world_returns",
) -> Path:
    """Save a CSV report under data/reports and return the path."""
    if out_dir is None:
        out_dir = Path(wi._BASE_DIR) / "data" / "reports"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    period = df.attrs.get("period") or "custom"
    start = df.attrs.get("start")
    end = df.attrs.get("end")
    fname = f"{basename}_{period}_{start}_to_{end}.csv".replace(" ", "_")
    path = out_dir / fname
    tmp = path.with_suffix(".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)
    return path


if __name__ == "__main__":
    # Minimal CLI for quick checks
    import argparse

    ap = argparse.ArgumentParser(description="Compute world index returns.")
    ap.add_argument("--period", default="ytd", help="ytd|1y|3y|5y|10y|custom")
    ap.add_argument("--start", default=None, help="YYYY-MM-DD (for custom)")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (optional)")
    ap.add_argument("--no-sharpe", action="store_true", help="Disable Sharpe calculation")
    ap.add_argument("--use-usd-proxy", action="store_true", help="Prefer USD ETF for USD series when available")
    args = ap.parse_args()

    start = pd.to_datetime(args.start).date() if args.start else None
    end = pd.to_datetime(args.end).date() if args.end else None
    df = compute_returns_table(period=(None if args.period == "custom" else args.period), start=start, end=end, calc_sharpe=not args.no_sharpe, use_usd_proxy_if_available=args.use_usd_proxy)
    path = save_returns_report(df)
    print(f"Saved report: {path}")
