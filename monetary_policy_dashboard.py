"""Streamlit dashboard for Fed monetary plumbing and policy data.

Data are pulled directly from FRED CSV endpoints (no API key required):
- Balance sheet and SOMA (WALCL, WSHOSHO)
- ON RRP usage and TGA balance (RRPONTSYD, WTREGEN)
- Reserve balances (RESBALNS)
- Policy rates and reference rates (IORB, SOFR, DFEDTARU, DFEDTARL, EFFR)
"""

from __future__ import annotations

import io
from io import BytesIO
from datetime import date, datetime, timedelta
from pathlib import Path
from uuid import uuid4
from typing import Any, Dict, Tuple

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

FRED_BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
NYFED_BASE_URL = "https://markets.newyorkfed.org/api"
DEFAULT_START = date(2018, 1, 1)
DEFAULT_END: date | None = None
MATURITY_BUCKETS = [(0, 1), (1, 3), (3, 5), (5, 7), (7, 10), (10, 20), (20, 200)]
EXPORT_DIR = Path(__file__).resolve().parent / "data_exports"

SERIES: Dict[str, Dict[str, Any]] = {
    "walcl": {"id": "WALCL", "label": "Fed balance sheet (WALCL)", "unit": "millions"},
    "soma": {"id": "WSHOSHO", "label": "SOMA holdings (WSHOSHO)", "unit": "millions"},
    # WALCL/WSHOSHO/RESBALNS/WTREGEN are millions of USD per FRED
    "rrp": {"id": "RRPONTSYD", "label": "ON RRP usage", "unit": "billions"},  # FRED reports billions
    "tga": {"id": "WTREGEN", "label": "Treasury General Account (TGA)", "unit": "millions"},
    "reserves": {"id": "RESBALNS", "label": "Reserve balances", "unit": "millions"},
    "iorb": {"id": "IORB", "label": "IORB", "unit": "percent"},
    "sofr": {"id": "SOFR", "label": "SOFR", "unit": "percent"},
    "ff_upper": {"id": "DFEDTARU", "label": "Fed funds target upper", "unit": "percent"},
    "ff_lower": {"id": "DFEDTARL", "label": "Fed funds target lower", "unit": "percent"},
    "effr": {"id": "EFFR", "label": "Effective fed funds rate (EFFR)", "unit": "percent"},
    "onrrp_rate": {"id": "RRPONTSYAWARD", "label": "ON RRP rate", "unit": "percent"},
    "discount_rate": {"id": "DPCREDIT", "label": "Discount window rate", "unit": "percent"},
    "t_bill_1m": {"id": "DGS1MO", "label": "1M T-bill", "unit": "percent"},
    "srf_rate": {"id": "SRFTSYD", "label": "SRF rate", "unit": "percent"},
    "currency": {"id": "CURRCIR", "label": "Currency in circulation", "unit": "billions"},
    "primary_credit": {
        "id": "WLCFLPCL",
        "label": "Primary credit",
        "unit": "millions",
    },
    "secondary_credit": {
        "id": "WLCFLSCL",
        "label": "Secondary credit",
        "unit": "millions",
    },
    "seasonal_credit": {
        "id": "WLCFLSECL",
        "label": "Seasonal credit",
        "unit": "millions",
    },
    "ppplf": {
        "id": "H41RESPPALDJNWW",
        "label": "PPP Liquidity Facility (net)",
        "unit": "millions",
    },
    "btfp": {
        "id": "H41RESPPALDKNWW",
        "label": "Bank Term Funding Program (net)",
        "unit": "millions",
    },
    "other_credit": {
        "id": "WLCFOCEL",
        "label": "Other credit extensions",
        "unit": "millions",
    },
}

SECURED_RATES_LABEL = {
    "sofr": "SOFR",
    "tgcr": "TGCR",
    "bgcr": "BGCR",
    "sofrai": "SOFR avg (index)",
}

UNSECURED_RATES_LABEL = {
    "effr": "EFFR",
    "obfr": "OBFR",
}

FCI_FRED_SERIES = {
    "NFCI": "NFCI",
    "ANFCI": "ANFCI",
    "STLFSI4": "STLFSI4",
    "KCFSI": "KCFSI",
}


def _as_date_string(value: date | datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return value.strftime("%Y-%m-%d")


def _export_csv(df: pd.DataFrame, filename: str) -> None:
    try:
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(EXPORT_DIR / filename)
    except OSError:
        # Silent fail to keep UI responsive
        pass


def _to_trillions(series: pd.Series, unit: str) -> pd.Series:
    if unit == "millions":
        return series / 1_000_000
    if unit == "billions":
        return series / 1_000
    if unit == "dollars":
        return series / 1_000_000_000_000
    return series


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fred_series(key: str, cfg: Dict[str, Any], start_date: date | datetime | None) -> pd.DataFrame:
    params = {"id": cfg["id"]}
    start_as_str = _as_date_string(start_date)
    if start_as_str:
        params["cosd"] = start_as_str

    resp = requests.get(FRED_BASE_URL, params=params, timeout=15)
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text))
    if df.empty:
        return pd.DataFrame(columns=[key])

    # FRED sometimes uses DATE, sometimes observation_date
    lower_map = {c.lower(): c for c in df.columns}
    date_col = lower_map.get("date") or lower_map.get("observation_date")
    if not date_col:
        return pd.DataFrame(columns=[key])

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.rename(columns={cfg["id"]: key, date_col: "DATE"})
    df[key] = pd.to_numeric(df[key], errors="coerce")
    df = df.set_index("DATE").sort_index()
    return df[[key]]


@st.cache_data(ttl=1800, show_spinner=False)
def load_all_series(start_date: date | datetime) -> Tuple[pd.DataFrame, Dict[str, str]]:
    frames: list[pd.DataFrame] = []
    errors: Dict[str, str] = {}
    for key, cfg in SERIES.items():
        try:
            df = fetch_fred_series(key, cfg, start_date)
        except Exception as exc:  # noqa: BLE001
            errors[key] = str(exc)
            continue
        if df.empty or df[key].dropna().empty:
            errors[key] = "no data returned"
            continue
        frames.append(df)

    if not frames:
        return pd.DataFrame(), errors

    combined = pd.concat(frames, axis=1).sort_index()
    combined = combined.loc[combined.index >= pd.to_datetime(start_date)]
    combined = combined.ffill()

    if {"walcl", "rrp", "tga"}.issubset(combined.columns):
        # Normalize units to millions for net liquidity
        rrp_mn = combined["rrp"] * (1_000 if SERIES.get("rrp", {}).get("unit") == "billions" else 1)
        tga_mn = combined["tga"] * (1_000 if SERIES.get("tga", {}).get("unit") == "billions" else 1)
        combined["net_liquidity"] = combined["walcl"] - rrp_mn - tga_mn

    _export_csv(combined, "fred_liquidity_rates.csv")
    return combined, errors


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_nyfed_reference_rates(
    ratetype: str,
    secured: bool,
    start_date: date | datetime,
    end_date: date | datetime | None,
) -> pd.DataFrame:
    segment = "secured" if secured else "unsecured"
    path = f"{NYFED_BASE_URL}/rates/{segment}/{ratetype}/search.json"
    params: Dict[str, str] = {}
    start_str = _as_date_string(start_date)
    end_str = _as_date_string(end_date)
    if start_str:
        params["startdate"] = start_str
    if end_str:
        params["enddate"] = end_str

    resp = requests.get(path, params=params, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    rows = payload.get("refRates", []) if isinstance(payload, dict) else []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "effectiveDate" not in df.columns:
        return pd.DataFrame()

    df["effectiveDate"] = pd.to_datetime(df["effectiveDate"])
    rename_map = {
        "percentRate": "rate",
        "volumeInBillions": "volume_billions",
        "type": "type",
    }
    for src, dst in rename_map.items():
        if src in df.columns:
            df = df.rename(columns={src: dst})
    df = df.sort_values("effectiveDate").set_index("effectiveDate")
    numeric_cols = ["rate", "volume_billions", "percentPercentile1", "percentPercentile25", "percentPercentile75", "percentPercentile99"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=900, show_spinner=False)
def load_nyfed_reference_rates(
    start_date: date | datetime,
    end_date: date | datetime | None,
) -> Dict[str, pd.DataFrame]:
    data: Dict[str, pd.DataFrame] = {}
    for rt in SECURED_RATES_LABEL:
        try:
            data[rt] = fetch_nyfed_reference_rates(rt, secured=True, start_date=start_date, end_date=end_date)
        except Exception as exc:  # noqa: BLE001
            data[rt] = pd.DataFrame({"__error__": [str(exc)]})
    for rt in UNSECURED_RATES_LABEL:
        try:
            data[rt] = fetch_nyfed_reference_rates(rt, secured=False, start_date=start_date, end_date=end_date)
        except Exception as exc:  # noqa: BLE001
            data[rt] = pd.DataFrame({"__error__": [str(exc)]})
    return data


@st.cache_data(ttl=900, show_spinner=False)
def fetch_repo_operations(operation_type: str, last_n: int) -> pd.DataFrame:
    path = f"{NYFED_BASE_URL}/rp/{operation_type}/all/results/last/{last_n}.json"
    resp = requests.get(path, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    repo = payload.get("repo", {}) if isinstance(payload, dict) else {}
    ops = repo.get("operations", [])
    if not ops:
        return pd.DataFrame()
    df = pd.DataFrame(ops)
    df["operationDate"] = pd.to_datetime(df["operationDate"])
    df["maturityDate"] = pd.to_datetime(df.get("maturityDate"))
    df["operation_type"] = operation_type
    for col in ["totalAmtAccepted", "totalAmtSubmitted", "operationLimit"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values("operationDate")


@st.cache_data(ttl=900, show_spinner=False)
def load_repo_operations(last_n: int) -> Dict[str, pd.DataFrame]:
    data: Dict[str, pd.DataFrame] = {}
    for optype in ("reverserepo", "repo"):
        try:
            data[optype] = fetch_repo_operations(optype, last_n)
        except Exception as exc:  # noqa: BLE001
            data[optype] = pd.DataFrame({"__error__": [str(exc)]})
    return data


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_soma_summary() -> pd.DataFrame:
    path = f"{NYFED_BASE_URL}/soma/summary.json"
    resp = requests.get(path, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    soma = payload.get("soma", {}) if isinstance(payload, dict) else {}
    summary = soma.get("summary", [])
    if not summary:
        return pd.DataFrame()
    df = pd.DataFrame(summary)
    df["asOfDate"] = pd.to_datetime(df["asOfDate"])
    numeric_cols = [
        "mbs",
        "cmbs",
        "tips",
        "frn",
        "tipsInflationCompensation",
        "notesbonds",
        "bills",
        "agencies",
        "total",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ust_total"] = df.get("notesbonds", 0) + df.get("bills", 0) + df.get("tips", 0) + df.get("frn", 0) + df.get("tipsInflationCompensation", 0)
    df["mbs_total"] = df.get("mbs", 0) + df.get("cmbs", 0)
    df = df.sort_values("asOfDate").set_index("asOfDate")
    return df


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_soma_tsy_details(as_of: str) -> pd.DataFrame:
    """Fetch SOMA Treasury holdings (all types) for a given as-of date."""
    path = f"{NYFED_BASE_URL}/soma/tsy/get/asof/{as_of}.json"
    resp = requests.get(path, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    soma = payload.get("soma", {}) if isinstance(payload, dict) else {}
    holdings = soma.get("holdings", [])
    if not holdings:
        return pd.DataFrame()
    df = pd.DataFrame(holdings)
    for col in ["asOfDate", "maturityDate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ["parValue", "inflationCompensation", "changeFromPriorWeek", "changeFromPriorYear", "percentOutstanding"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _bucketize_maturity(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "maturityDate" not in df or "asOfDate" not in df:
        return pd.DataFrame()
    today_ref = df["asOfDate"].max()
    df = df.copy()
    df["years_to_maturity"] = (df["maturityDate"] - today_ref).dt.days / 365.25
    buckets = []
    labels = []
    for low, high in MATURITY_BUCKETS:
        mask = (df["years_to_maturity"] >= low) & (df["years_to_maturity"] < high)
        buckets.append(df.loc[mask, "parValue"].sum())
        labels.append(f"{low}-{high}Y" if high < 200 else f"{low}+Y")
    return pd.DataFrame({"bucket": labels, "parValue": buckets})


def _weighted_average_maturity_years(df: pd.DataFrame) -> float:
    if df.empty or "parValue" not in df or "maturityDate" not in df or "asOfDate" not in df:
        return float("nan")
    today_ref = df["asOfDate"].max()
    years = (df["maturityDate"] - today_ref).dt.days / 365.25
    weights = df["parValue"]
    valid = weights > 0
    if not valid.any():
        return float("nan")
    return float((years[valid] * weights[valid]).sum() / weights[valid].sum())


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_soma_tsy_monthly() -> pd.DataFrame:
    """Fetch monthly Treasury holdings (CUSIP-level) and aggregate to maturity buckets per as-of date."""
    path = f"{NYFED_BASE_URL}/soma/tsy/get/monthly.json"
    resp = requests.get(path, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    soma = payload.get("soma", {}) if isinstance(payload, dict) else {}
    holdings = soma.get("holdings", [])
    if not holdings:
        return pd.DataFrame()
    df = pd.DataFrame(holdings)
    for col in ["asOfDate", "maturityDate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ["parValue"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["asOfDate", "maturityDate", "parValue"])
    df["years_to_maturity"] = (df["maturityDate"] - df["asOfDate"]).dt.days / 365.25
    # More granular buckets: 0-1,1-3,3-5,5-7,7-10,10-20,20+
    bins = [0, 1, 3, 5, 7, 10, 20, 200]
    labels = ["0-1Y", "1-3Y", "3-5Y", "5-7Y", "7-10Y", "10-20Y", "20+Y"]
    df["bucket"] = pd.cut(df["years_to_maturity"], bins=bins, labels=labels, right=False)
    grouped = df.groupby(["asOfDate", "bucket"])["parValue"].sum().unstack("bucket").sort_index()
    return grouped


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_soma_tsy_wam_series() -> pd.Series:
    """Compute weighted average maturity (years) by as-of date using monthly SOMA Treasury detail."""
    path = f"{NYFED_BASE_URL}/soma/tsy/get/monthly.json"
    resp = requests.get(path, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    soma = payload.get("soma", {}) if isinstance(payload, dict) else {}
    holdings = soma.get("holdings", [])
    if not holdings:
        return pd.Series(dtype=float)
    df = pd.DataFrame(holdings)
    for col in ["asOfDate", "maturityDate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ["parValue"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["asOfDate", "maturityDate", "parValue"])
    df["years_to_maturity"] = (df["maturityDate"] - df["asOfDate"]).dt.days / 365.25
    grouped = df.groupby("asOfDate")
    wam = grouped.apply(lambda g: (g["parValue"] * g["years_to_maturity"]).sum() / g["parValue"].sum())
    wam = wam.groupby(pd.Grouper(freq="MS")).mean().sort_index()
    return wam


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_reserve_demand_elasticity() -> pd.DataFrame:
    """Download Reserve Demand Elasticity Excel and return tidy DataFrame."""
    url = "https://www.newyorkfed.org/medialibrary/Research/Interactives/Data/elasticity/download-data"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    excel = pd.ExcelFile(BytesIO(resp.content))
    df = excel.parse("chart data", skiprows=4)
    if df.empty:
        return pd.DataFrame()
    df = df.rename(
        columns={
            df.columns[0]: "date",
            "Elasticity - 50th percentile (main)": "p50",
            "Elasticity - 2.5th percentile": "p2_5",
            "Elasticity - 97.5th percentile": "p97_5",
            "Elasticity - 16th percentile": "p16",
            "Elasticity - 84th percentile": "p84",
        }
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    num_cols = ["p50", "p2_5", "p97_5", "p16", "p84"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.set_index("date").sort_index()
    return df[num_cols]


def _extract_srf_usage(combined: pd.DataFrame) -> pd.DataFrame:
    """
    Extract Standing Repo Facility usage from repo operations.
    Heuristic: filter repo operations whose note or operationId contains 'SRF' (case-insensitive).
    """
    if combined.empty:
        return pd.DataFrame()
    df = combined.copy()
    if "kind" in df.columns:
        df = df[df["kind"] == "repo"]
    pattern_cols = []
    for col in ["note", "operationId", "operationId".lower()]:
        if col in df.columns:
            pattern_cols.append(col)
    if not pattern_cols:
        return pd.DataFrame()
    mask = False
    for col in pattern_cols:
        mask = mask | df[col].astype(str).str.contains("SRF", case=False, na=False)
    df = df[mask]
    if df.empty or "operationDate" not in df or "totalAmtAccepted" not in df:
        return pd.DataFrame()
    daily = (
        df.groupby(pd.to_datetime(df["operationDate"]))["totalAmtAccepted"]
        .sum()
        .sort_index()
        / 1_000_000_000
    )
    return daily.to_frame("srf_billion")


def render_balance_sheet_stacks(df: pd.DataFrame, soma_df: pd.DataFrame) -> None:
    st.subheader("Federal Reserve Assets & Liabilities (stacked, $ trillions)")
    if df.empty:
        st.info("No FRED data available for balance sheet charts.")
        return

    # Assets
    walcl_tr = _to_trillions(df.get("walcl", pd.Series(dtype=float)), "millions")
    ust_tr = pd.Series(dtype=float)
    mbs_tr = pd.Series(dtype=float)
    if not soma_df.empty:
        ust_tr = soma_df.get("ust_total", pd.Series(dtype=float)) / 1_000_000_000_000
        mbs_tr = soma_df.get("mbs_total", pd.Series(dtype=float)) / 1_000_000_000_000

    assets_df = pd.concat(
        {
            "treasury": ust_tr,
            "agency_mbs": mbs_tr,
            "walcl": walcl_tr,
        },
        axis=1,
    ).sort_index()
    assets_df = assets_df.ffill()
    if "walcl" in assets_df:
        assets_df["other_assets"] = assets_df["walcl"] - assets_df["treasury"].fillna(0) - assets_df["agency_mbs"].fillna(0)
    else:
        assets_df["other_assets"] = pd.Series(dtype=float)

    # Liabilities
    liabilities_df = pd.concat(
        {
            "walcl": walcl_tr,
            "currency": _to_trillions(df.get("currency", pd.Series(dtype=float)), SERIES.get("currency", {}).get("unit", "")),
            "reserves": _to_trillions(df.get("reserves", pd.Series(dtype=float)), "millions"),
            "tga": _to_trillions(df.get("tga", pd.Series(dtype=float)), "millions"),
            "onrrp": _to_trillions(df.get("rrp", pd.Series(dtype=float)), "millions"),
        },
        axis=1,
    ).sort_index()
    liabilities_df = liabilities_df.ffill()
    liabilities_df["other_liabs"] = (
        liabilities_df["walcl"]
        - liabilities_df[["currency", "reserves", "tga", "onrrp"]].fillna(0).sum(axis=1)
    )

    # Asset stack plot
    if not assets_df.empty:
        fig_assets = go.Figure()
        for col, name, color in [
            ("other_assets", "Other assets", "#9e9e9e"),
            ("agency_mbs", "Agency MBS", "#4fb3ff"),
            ("treasury", "Treasury securities", "#1e88e5"),
        ]:
            if col not in assets_df:
                continue
            fig_assets.add_trace(
                go.Scatter(
                    x=assets_df.index,
                    y=assets_df[col],
                    mode="lines",
                    name=name,
                    stackgroup="assets",
                    line=dict(width=0.5, color=color),
                )
            )
        for vline in [pd.Timestamp("2020-03-11"), pd.Timestamp("2022-06-01")]:
            fig_assets.add_vline(x=vline, line=dict(color="#616161", dash="dash"))
        fig_assets.update_layout(
            height=420,
            yaxis_title="Trillions USD",
            margin=dict(l=10, r=10, t=30, b=10),
            legend_title_text="Assets",
        )
        st.plotly_chart(
            fig_assets,
            use_container_width=True,
            theme="streamlit",
            key=f"bs_assets_{uuid4()}",
        )
        st.download_button(
            "Download asset stack data (CSV)",
            assets_df[["treasury", "agency_mbs", "other_assets"]].to_csv().encode("utf-8"),
            file_name="balance_sheet_assets.csv",
            mime="text/csv",
            key=f"bs_assets_csv_{uuid4()}",
        )
    else:
        st.info("No asset data available.")

    # Liability stack plot
    if not liabilities_df.empty:
        fig_liab = go.Figure()
        for col, name, color in [
            ("other_liabs", "Other liabilities & capital", "#b07b2c"),
            ("currency", "Currency in circulation", "#c28e35"),
            ("reserves", "Reserves", "#5b8c61"),
            ("tga", "Treasury General Account", "#7b9c6b"),
            ("onrrp", "ON RRP", "#f4a261"),
        ]:
            if col not in liabilities_df:
                continue
            fig_liab.add_trace(
                go.Scatter(
                    x=liabilities_df.index,
                    y=liabilities_df[col],
                    mode="lines",
                    name=name,
                    stackgroup="liabs",
                    line=dict(width=0.5, color=color),
                )
            )
        for vline in [pd.Timestamp("2020-03-11"), pd.Timestamp("2022-06-01")]:
            fig_liab.add_vline(x=vline, line=dict(color="#616161", dash="dash"))
        fig_liab.update_layout(
            height=420,
            yaxis_title="Trillions USD",
            margin=dict(l=10, r=10, t=30, b=10),
            legend_title_text="Liabilities",
        )
        st.plotly_chart(
            fig_liab,
            use_container_width=True,
            theme="streamlit",
            key=f"bs_liabs_{uuid4()}",
        )
        st.download_button(
            "Download liability stack data (CSV)",
            liabilities_df[["currency", "reserves", "tga", "onrrp", "other_liabs"]].to_csv().encode("utf-8"),
            file_name="balance_sheet_liabilities.csv",
            mime="text/csv",
            key=f"bs_liabs_csv_{uuid4()}",
        )
    else:
        st.info("No liability data available.")


def render_tsy_maturity_stack(df: pd.DataFrame, soma_df: pd.DataFrame) -> None:
    st.subheader("SOMA Treasury holdings by maturity buckets (stacked, $ trillions)")
    tsy_buckets = fetch_soma_tsy_monthly()
    if tsy_buckets.empty:
        st.info("No SOMA Treasury detail data returned.")
        return

    # Monthly alignment and convert to trillions
    tsy_tr = tsy_buckets.groupby(pd.Grouper(freq="MS")).sum() / 1_000_000_000_000

    # MBS from summary (weekly) resampled to monthly mean
    mbs_tr = pd.Series(dtype=float)
    if not soma_df.empty and "mbs_total" in soma_df.columns:
        mbs_tr = soma_df["mbs_total"].groupby(pd.Grouper(freq="MS")).mean() / 1_000_000_000_000
        mbs_tr = mbs_tr.reindex(tsy_tr.index, method="ffill")

    aligned = pd.concat([tsy_tr, mbs_tr.rename("Agency MBS")], axis=1).sort_index().ffill()

    fig = go.Figure()
    colors = {
        "0-1Y": "#bbdefb",
        "1-3Y": "#90caf9",
        "3-5Y": "#64b5f6",
        "5-7Y": "#42a5f5",
        "7-10Y": "#2196f3",
        "10-20Y": "#1565c0",
        "20+Y": "#0d47a1",
        "Agency MBS": "#9fa8da",
    }
    for col in ["Agency MBS", "20+Y", "10-20Y", "7-10Y", "5-7Y", "3-5Y", "1-3Y", "0-1Y"]:
        if col not in aligned:
            continue
        fig.add_trace(
            go.Scatter(
                x=aligned.index,
                y=aligned[col],
                mode="lines",
                name=col,
                stackgroup="tsy",
                line=dict(width=0.5, color=colors.get(col, None)),
            )
        )
    for vline in [pd.Timestamp("2020-03-11"), pd.Timestamp("2022-06-01")]:
        fig.add_vline(x=vline, line=dict(color="#616161", dash="dash"))
    fig.update_layout(
        height=420,
        yaxis_title="Trillions USD",
        margin=dict(l=10, r=10, t=30, b=10),
        legend_title_text="Assets",
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        theme="streamlit",
        key=f"tsy_maturity_stack_{uuid4()}",
    )
    st.download_button(
        "Download UST bucket stack data (CSV)",
        aligned.to_csv().encode("utf-8"),
        file_name="soma_treasury_maturity_stack.csv",
        mime="text/csv",
        key=f"tsy_bucket_csv_{uuid4()}",
    )


def render_tsy_maturity_buckets_latest() -> None:
    """Render latest maturity buckets (single as-of) as bar chart using detailed SOMA Treasury holdings."""
    monthly = fetch_soma_tsy_monthly()
    if monthly.empty:
        st.info("No SOMA Treasury detail available for maturity buckets.")
        return
    monthly_grouped = monthly.groupby(pd.Grouper(freq="MS")).sum()
    latest_date = monthly_grouped.index.max()
    latest = monthly_grouped.loc[latest_date]
    bucketed = latest / 1_000_000_000_000  # to trillions

    fig = go.Figure(
        data=[go.Bar(x=bucketed.index, y=bucketed.values, marker_color="#4caf50", name="Par value (billions USD)")]
    )
    fig.update_layout(
        height=320,
        yaxis_title="Par value (billions USD)",
        xaxis_title="Years to maturity",
        margin=dict(l=10, r=10, t=30, b=40),
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit", key=f"tsy_bucket_bar_{uuid4()}")
    st.download_button(
        "Download latest maturity buckets (CSV)",
        bucketed.to_csv().encode("utf-8"),
        file_name="soma_treasury_maturity_latest.csv",
        mime="text/csv",
        key=f"tsy_bucket_bar_csv_{uuid4()}",
    )


def render_tsy_wam_series() -> None:
    wam_series = fetch_soma_tsy_wam_series()
    if wam_series.empty:
        st.info("No SOMA Treasury WAM data available.")
        return
    fig = go.Figure(
        data=[
            go.Scatter(
                x=wam_series.index,
                y=wam_series,
                mode="lines",
                name="WAM (years)",
                line=dict(color="#2e7d32"),
            )
        ]
    )
    for vline in [pd.Timestamp("2020-03-11"), pd.Timestamp("2022-06-01")]:
        fig.add_vline(x=vline, line=dict(color="#616161", dash="dash"))
    fig.update_layout(
        height=300,
        yaxis_title="Years",
        margin=dict(l=10, r=10, t=30, b=10),
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit", key=f"tsy_wam_{uuid4()}")
    st.download_button(
        "Download WAM series (CSV)",
        wam_series.to_csv().encode("utf-8"),
        file_name="soma_treasury_wam.csv",
        mime="text/csv",
        key=f"tsy_wam_csv_{uuid4()}",
    )


def render_reserve_demand_elasticity() -> None:
    st.subheader("Reserve Demand Elasticity (percentile bands)")
    df = fetch_reserve_demand_elasticity()
    if df.empty:
        st.info("No Reserve Demand Elasticity data available.")
        return
    df = df.dropna(how="all")
    fig = go.Figure()
    # 2.5-97.5 band
    fig.add_trace(go.Scatter(x=df.index, y=df["p2_5"], line=dict(color="#90caf9", width=1), name="2.5th percentile", showlegend=False))
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["p97_5"],
            line=dict(color="#90caf9", width=1),
            fill="tonexty",
            fillcolor="rgba(144,202,249,0.2)",
            name="97.5th percentile",
        )
    )
    # 16-84 band
    fig.add_trace(go.Scatter(x=df.index, y=df["p16"], line=dict(color="#9e9e9e", width=1), name="16th percentile", showlegend=False))
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["p84"],
            line=dict(color="#9e9e9e", width=1),
            fill="tonexty",
            fillcolor="rgba(158,158,158,0.2)",
            name="84th percentile",
        )
    )
    # median
    fig.add_trace(go.Scatter(x=df.index, y=df["p50"], line=dict(color="#c62828", width=2), name="50th percentile"))
    fig.update_layout(
        height=420,
        yaxis_title="Basis points / percentage points",
        margin=dict(l=10, r=10, t=30, b=10),
        legend_title_text="",
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit", key=f"reserve_elasticity_{uuid4()}")
    st.download_button(
        "Download Reserve Demand Elasticity (CSV)",
        df.to_csv().encode("utf-8"),
        file_name="reserve_demand_elasticity.csv",
        mime="text/csv",
        key=f"reserve_elasticity_csv_{uuid4()}",
    )


def render_spreads_monitor(df: pd.DataFrame, ny_rates: Dict[str, pd.DataFrame]) -> None:
    st.subheader("Money Market Spreads Monitor")

    def _get_rate(series_name: str) -> pd.Series:
        if series_name in df:
            return df[series_name]
        rate_df = ny_rates.get(series_name)
        if rate_df is not None and not rate_df.empty and "rate" in rate_df:
            return rate_df["rate"]
        return pd.Series(dtype=float)

    # Build spreads
    spreads: Dict[str, Dict[str, str | pd.Series]] = {
        "Private repo demand (TGCR - ON RRP)": {
            "series": _get_rate("tgcr") - _get_rate("onrrp_rate"),
            "desc": "Demand for cash vs collateral; higher spread indicates excess collateral.",
        },
        "Bank repos (SOFR - IORB)": {
            "series": _get_rate("sofr") - _get_rate("iorb"),
            "desc": "Above zero implies banks deploy reserves into repos consistently.",
        },
        "Reserve demand (EFFR - IORB)": {
            "series": _get_rate("effr") - _get_rate("iorb"),
            "desc": "Scarcity vs abundance of reserves; positive indicates scarcity.",
        },
        "FHLB repo demand (SOFR - EFFR)": {
            "series": _get_rate("sofr") - _get_rate("effr"),
            "desc": "Suggests where FHLBs invest liquidity portfolios.",
        },
    }

    any_data = False
    for title, payload in spreads.items():
        ser = payload["series"].dropna()
        if ser.empty:
            st.info(f"{title}: data unavailable.")
            continue
        any_data = True
        fig = go.Figure(
            data=[
                go.Scatter(
                    x=ser.index,
                    y=ser,
                    mode="lines",
                    name=title,
                    line=dict(color="#26a69a"),
                )
            ]
        )
        fig.update_layout(
            height=260,
            yaxis_title="Spread (pp)",
            margin=dict(l=10, r=10, t=30, b=10),
        )
        st.markdown(f"**{title}** – {payload['desc']}")
        st.plotly_chart(fig, use_container_width=True, theme="streamlit", key=f"spread_{uuid4()}")
        st.download_button(
            f"Download {title} (CSV)",
            ser.to_csv().encode("utf-8"),
            file_name=title.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_") + ".csv",
            mime="text/csv",
            key=f"spread_csv_{uuid4()}",
        )

    if not any_data:
        st.info("No spreads could be computed with available data.")


def render_stress_monitor(df: pd.DataFrame, ny_rates: Dict[str, pd.DataFrame], repo_ops: Dict[str, pd.DataFrame]) -> None:
    st.subheader("Money Market Stress Monitor")

    def _series(name: str) -> pd.Series:
        if name in df:
            return df[name]
        rate_df = ny_rates.get(name)
        if rate_df is not None and not rate_df.empty and "rate" in rate_df:
            return rate_df["rate"]
        return pd.Series(dtype=float)

    # Reserve scarcity: EFFR - IORB
    effr = _series("effr")
    iorb = _series("iorb")
    reserve_spread = (effr - iorb).dropna()

    # Bank activity in repo: SOFR - IORB
    sofr = _series("sofr")
    repo_spread = (sofr - iorb).dropna()

    # Comfortable reserve levels: approximate using RESBALNS (millions -> trillions)
    reserve_level = df.get("reserves", pd.Series(dtype=float)) / 1_000_000

    # SRF activity: repo operations filtered for SRF
    srf_daily = _extract_srf_usage(
        pd.concat([repo_ops.get("repo", pd.DataFrame()), repo_ops.get("reverserepo", pd.DataFrame())], ignore_index=True)
    )

    # Charts
    if not reserve_spread.empty:
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=reserve_spread.index, y=reserve_spread, mode="lines", name="EFFR - IORB"))
        fig1.add_hline(y=0, line=dict(color="red", dash="dash"))
        fig1.update_layout(height=220, yaxis_title="pp", margin=dict(l=10, r=10, t=30, b=10))
        st.markdown("**Reserve scarcity (EFFR - IORB)**")
        st.plotly_chart(fig1, use_container_width=True, theme="streamlit", key=f"stress_reserve_{uuid4()}")

    if not repo_spread.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=repo_spread.index, y=repo_spread, mode="lines", name="SOFR - IORB"))
        fig2.add_hline(y=0, line=dict(color="red", dash="dash"))
        fig2.update_layout(height=220, yaxis_title="pp", margin=dict(l=10, r=10, t=30, b=10))
        st.markdown("**Bank activity in repo (SOFR - IORB)**")
        st.plotly_chart(fig2, use_container_width=True, theme="streamlit", key=f"stress_repo_{uuid4()}")

    if not reserve_level.empty:
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(x=reserve_level.index, y=reserve_level, mode="lines", name="Reserves (trn)"))
        fig3.update_layout(height=220, yaxis_title="Trillions USD", margin=dict(l=10, r=10, t=30, b=10))
        st.markdown("**Comfortable reserve levels (RESBALNS)**")
        st.plotly_chart(fig3, use_container_width=True, theme="streamlit", key=f"stress_reserves_{uuid4()}")

    if not srf_daily.empty:
        fig4 = go.Figure()
        fig4.add_trace(go.Bar(x=srf_daily.index, y=srf_daily["srf_billion"], name="SRF accepted (bln)", marker_color="#d32f2f"))
        fig4.update_layout(height=220, yaxis_title="Billions USD", margin=dict(l=10, r=10, t=30, b=10))
        st.markdown("**SRF activity (daily accepted)**")
        st.plotly_chart(fig4, use_container_width=True, theme="streamlit", key=f"stress_srf_{uuid4()}")



def render_benchmark_monitor(ny_rates: Dict[str, pd.DataFrame]) -> None:
    st.subheader("Fed Rate Benchmark Monitor")
    mapping = {
        "sofr": ("SOFR", "Secured overnight (Tri-party, GCF, DVP)"),
        "bgcr": ("BGCR", "Broad GC (GCF + tri-party)"),
        "tgcr": ("TGCR", "Tri-party GC (uncleared)"),
        "obfr": ("OBFR", "Overnight bank funding (Eurodollar + fed funds)"),
        "effr": ("EFFR", "Effective fed funds"),
    }
    fig = go.Figure()
    labels = []
    for key, (name, _) in mapping.items():
        df = ny_rates.get(key)
        if df is None or df.empty or "rate" not in df:
            continue
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df["rate"],
                mode="lines",
                name=name,
            )
        )
        labels.append(name)
    if not labels:
        st.info("No benchmark rate data available.")
        return
    fig.update_layout(
        height=420,
        yaxis_title="Percent",
        margin=dict(l=10, r=10, t=30, b=10),
        legend_title_text="Benchmarks",
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit", key=f"benchmark_monitor_{uuid4()}")
    st.download_button(
        "Download benchmark rates (CSV)",
        pd.concat({k: ny_rates[k]["rate"] for k in mapping.keys() if k in ny_rates and "rate" in ny_rates[k]}, axis=1)
        .to_csv()
        .encode("utf-8"),
        file_name="benchmark_rates.csv",
        mime="text/csv",
        key=f"benchmark_monitor_csv_{uuid4()}",
    )
    st.markdown(
        "- **SOFR**: volume-weighted secured o/n; sources = tri-party, GCF, bilateral DVP repos\n"
        "- **BGCR**: secured GC across GCF + tri-party\n"
        "- **TGCR**: secured GC tri-party (uncleared)\n"
        "- **OBFR**: unsecured o/n bank funding (Eurodollar + fed funds)\n"
        "- **EFFR**: unsecured fed funds transactions",
    )


def render_money_market_complex(df: pd.DataFrame) -> None:
    st.subheader("Money Market Rates Complex")
    cols = [
        ("discount_rate", "Discount (window)", "#b71c1c"),
        ("onrrp_rate", "ON RRP rate", "#455a64"),
        ("srf_rate", "SRF rate", "#6a1b9a"),
        ("iorb", "IORB", "#2e7d32"),
        ("effr", "EFFR", "#8e24aa"),
        ("sofr", "SOFR", "#1e88e5"),
        ("t_bill_1m", "1M T-bill", "#fbc02d"),
    ]
    available = [(c, n, col) for c, n, col in cols if c in df.columns]
    if not available:
        st.info("No money market rate data available.")
        return
    fig = go.Figure()
    for c, n, col in available:
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[c],
                mode="lines",
                name=n,
                line=dict(color=col),
            )
        )
    fig.update_layout(
        height=420,
        yaxis_title="Percent",
        margin=dict(l=10, r=10, t=30, b=10),
        legend_title_text="",
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit", key=f"money_complex_{uuid4()}")
    st.download_button(
        "Download money market rates (CSV)",
        df[[c for c, _, _ in available]].to_csv().encode("utf-8"),
        file_name="money_market_rates.csv",
        mime="text/csv",
        key=f"money_complex_csv_{uuid4()}",
    )

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fcig_series(start_date: date | datetime) -> pd.DataFrame:
    url = "https://www.federalreserve.gov/econres/notes/feds-notes/fci_g_public_monthly_3yr.csv"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    if "date" not in df.columns:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df = df.rename(columns={"FCI-G Index (baseline)": "FCI-G"})
    df = df.loc[df.index >= pd.to_datetime(start_date)]
    _export_csv(df, "fci_g.csv")
    return df


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_fred_generic(series_id: str, key: str, start_date: date | datetime) -> pd.DataFrame:
    params = {"id": series_id}
    start_as_str = _as_date_string(start_date)
    if start_as_str:
        params["cosd"] = start_as_str
    resp = requests.get(FRED_BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    if df.empty:
        return pd.DataFrame(columns=[key])
    lower_map = {c.lower(): c for c in df.columns}
    date_col = lower_map.get("date") or lower_map.get("observation_date")
    if not date_col:
        return pd.DataFrame(columns=[key])
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.rename(columns={series_id: key, date_col: "DATE"})
    df[key] = pd.to_numeric(df[key], errors="coerce")
    df = df.set_index("DATE").sort_index()
    return df[[key]]


@st.cache_data(ttl=1800, show_spinner=False)
def load_fci_indices(start_date: date | datetime) -> Dict[str, pd.DataFrame]:
    data: Dict[str, pd.DataFrame] = {}
    data["FCI-G"] = fetch_fcig_series(start_date)
    for key, sid in FCI_FRED_SERIES.items():
        try:
            data[key] = fetch_fred_generic(sid, key, start_date)
        except Exception as exc:  # noqa: BLE001
            data[key] = pd.DataFrame({"__error__": [str(exc)]})
    return data


def latest_with_delta(series: pd.Series, delta_days: int = 30) -> Tuple[pd.Timestamp, float, float | None]:
    cleaned = series.dropna()
    if cleaned.empty:
        return pd.NaT, float("nan"), None
    last_date = cleaned.index[-1]
    last_value = cleaned.iloc[-1]

    cutoff = last_date - timedelta(days=delta_days)
    earlier = cleaned[cleaned.index <= cutoff]
    delta = None
    if not earlier.empty:
        delta = last_value - earlier.iloc[-1]
    return last_date, float(last_value), delta


def format_billions(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value:.1f}B"


def format_percent(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value:.2f}%"


def render_liquidity_section(df: pd.DataFrame, delta_days: int) -> None:
    st.subheader("Liquidity and balances (billions USD)")
    liq_cols = ["walcl", "soma", "reserves", "rrp", "tga", "net_liquidity"]
    facility_cols = [
        "primary_credit",
        "secondary_credit",
        "seasonal_credit",
        "ppplf",
        "btfp",
        "other_credit",
    ]
    display_names = {
        "walcl": "Total assets",
        "soma": "SOMA holdings",
        "reserves": "Reserve balances",
        "rrp": "ON RRP",
        "tga": "TGA",
        "net_liquidity": "Net liquidity (assets - RRP - TGA)",
        "primary_credit": "Primary credit",
        "secondary_credit": "Secondary credit",
        "seasonal_credit": "Seasonal credit",
        "ppplf": "PPP Liquidity Facility",
        "btfp": "Bank Term Funding Program",
        "other_credit": "Other credit extensions",
    }
    available_cols = [c for c in liq_cols if c in df.columns]
    available_facility_cols = [c for c in facility_cols if c in df.columns]

    metrics = []
    for col in available_cols:
        last_date, last_val, delta = latest_with_delta(df[col], delta_days)
        unit = SERIES.get(col, {}).get("unit", "millions")
        factor = 1_000 if unit == "millions" else 1  # convert to billions for display
        metrics.append(
            (
                display_names.get(col, col),
                last_val / factor,
                (delta / factor) if delta is not None else None,
                last_date,
            ),
        )

    if metrics:
        cols = st.columns(len(metrics))
        for idx, (label, value, delta, last_date) in enumerate(metrics):
            delta_label = None
            if delta is not None:
                delta_label = f"{format_billions(delta)} vs {delta_days}d ago"
            cols[idx].metric(
                label,
                format_billions(value),
                delta_label,
                help=f"Last: {last_date.date() if pd.notnull(last_date) else 'n/a'}",
            )

    fig = go.Figure()
    for col, name in [
        ("walcl", "Total assets"),
        ("soma", "SOMA holdings"),
        ("reserves", "Reserve balances"),
        ("rrp", "ON RRP"),
        ("tga", "TGA"),
        ("net_liquidity", "Net liquidity"),
    ]:
        if col not in df:
            continue
        unit = SERIES.get(col, {}).get("unit", "millions")
        scale = 1_000 if unit == "millions" else 1  # convert to billions for chart
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[col] / scale,
                mode="lines",
                name=name,
            )
        )
    fig.update_layout(
        height=480,
        legend_title_text="",
        yaxis_title="Billions USD (millions / 1,000)",
        margin=dict(l=10, r=10, t=40, b=10),
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")
    st.download_button(
        "Download liquidity data (CSV)",
        df[available_cols].to_csv(index=True).encode("utf-8"),
        file_name="liquidity_series.csv",
        mime="text/csv",
        help="Includes WALCL, SOMA, reserves, RRP, TGA, net_liquidity (raw units per series; index = date).",
    )

    if available_facility_cols:
        st.subheader("Fed credit facilities (billions USD)")
        fig_fac = go.Figure()
        for col in available_facility_cols:
            fig_fac.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df[col] / 1_000,
                    mode="lines",
                    name=display_names.get(col, col),
                )
            )
        fig_fac.update_layout(
            height=380,
            legend_title_text="",
            yaxis_title="Billions USD (millions / 1,000)",
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig_fac, use_container_width=True, theme="streamlit")
        st.download_button(
            "Download facilities data (CSV)",
            df[available_facility_cols].to_csv(index=True).encode("utf-8"),
            file_name="credit_facilities.csv",
            mime="text/csv",
            help="Primary/secondary/seasonal credit, PPP facility, BTFP, other extensions (millions; index=date).",
        )


def render_rate_section(df: pd.DataFrame, delta_days: int) -> None:
    st.subheader("Policy and money market rates (%)")
    rate_cols = ["iorb", "sofr", "effr", "onrrp_rate", "ff_lower", "ff_upper"]
    rate_cols_available = [c for c in rate_cols if c in df.columns]
    display = {
        "iorb": "IORB",
        "sofr": "SOFR",
        "effr": "EFFR",
        "onrrp_rate": "ON RRP rate",
        "ff_upper": "FF target upper",
        "ff_lower": "FF target lower",
    }

    metrics = []
    for col in rate_cols_available:
        last_date, last_val, delta = latest_with_delta(df[col], delta_days)
        metrics.append((display.get(col, col), last_val, delta, last_date))

    if metrics:
        cols = st.columns(len(metrics))
        for idx, (label, value, delta, last_date) in enumerate(metrics):
            delta_label = None
            if delta is not None:
                delta_label = f"{delta:.02f}pp vs {delta_days}d ago"
            cols[idx].metric(
                label,
                format_percent(value),
                delta_label,
                help=f"Last: {last_date.date() if pd.notnull(last_date) else 'n/a'}",
            )

    fig = go.Figure()
    if {"ff_lower", "ff_upper"}.issubset(df.columns):
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df["ff_lower"],
                name="FF target lower",
                line=dict(color="rgba(33, 150, 243, 0.5)", dash="dash"),
                showlegend=True,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df["ff_upper"],
                name="FF target upper",
                line=dict(color="rgba(33, 150, 243, 0.5)", dash="dash"),
                fill="tonexty",
                fillcolor="rgba(33, 150, 243, 0.12)",
                showlegend=True,
            )
        )

    for col, name, color in [
        ("iorb", "IORB", "#ef6c00"),
        ("sofr", "SOFR", "#26a69a"),
        ("effr", "EFFR", "#8e24aa"),
        ("onrrp_rate", "ON RRP rate", "#607d8b"),
    ]:
        if col not in df:
            continue
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[col],
                mode="lines",
                name=name,
                line=dict(color=color),
            )
        )

    fig.update_layout(
        height=420,
        legend_title_text="",
        yaxis_title="Percent",
        margin=dict(l=10, r=10, t=40, b=10),
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")

    if {"sofr", "iorb"}.issubset(df.columns):
        spread = df["sofr"] - df["iorb"]
        fig_spread = go.Figure()
        fig_spread.add_trace(
            go.Scatter(
                x=spread.index,
                y=spread,
                mode="lines",
                name="SOFR - IORB",
                line=dict(color="#1e88e5"),
            )
        )
        fig_spread.update_layout(
            height=300,
            yaxis_title="Percentage points",
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig_spread, use_container_width=True, theme="streamlit")

    st.download_button(
        "Download policy rate data (CSV)",
        df[rate_cols_available].to_csv(index=True).encode("utf-8"),
        file_name="policy_rates.csv",
        mime="text/csv",
        help="Includes IORB, SOFR, EFFR, FF target band in percent (index = date).",
    )


def render_data_table(df: pd.DataFrame) -> None:
    st.subheader("Latest data")
    latest_row = df.dropna(how="all").iloc[[-1]].copy()
    latest_row.index = latest_row.index.date

    pretty_cols = []
    for col in latest_row.columns:
        cfg = SERIES.get(col, {})
        if cfg.get("unit") == "percent":
            latest_row[col] = latest_row[col].map(lambda v: f"{v:.2f}%")
        elif cfg.get("unit") == "millions":
            latest_row[col] = latest_row[col].map(lambda v: format_billions(v))
        pretty_cols.append(cfg.get("label", col))

    latest_row.columns = pretty_cols
    st.dataframe(latest_row, use_container_width=True)
    _export_csv(df, "fred_latest_row_source.csv")


def render_fci_section(fci_data: Dict[str, pd.DataFrame], start_date: date | datetime) -> None:
    st.subheader("Financial Conditions (FCI-G + regional indices)")
    errors = [
        f"{k}: {df['__error__'].iloc[0]}"
        for k, df in fci_data.items()
        if not df.empty and "__error__" in df.columns
    ]
    if errors:
        st.warning("FCI fetch errors: " + "; ".join(errors))

    frames = []
    for key, df in fci_data.items():
        if df.empty or "__error__" in df.columns:
            continue
        frames.append(df.rename(columns={df.columns[0]: key}) if df.shape[1] == 1 else df[[key]] if key in df.columns else df)

    merged = pd.DataFrame()
    if frames:
        merged = pd.concat(frames, axis=1).sort_index()
        merged = merged.loc[merged.index >= pd.to_datetime(start_date)]
        merged = merged.ffill()

    if not merged.empty:
        latest_values = {}
        for col in merged.columns:
            series_clean = merged[col].dropna()
            if not series_clean.empty:
                latest_values[col] = series_clean.iloc[-1]
        cols = st.columns(min(len(latest_values), 4) or 1)
        for idx, (k, v) in enumerate(latest_values.items()):
            cols[idx % len(cols)].metric(k, f"{v:.3f}")

        fig = go.Figure()
        for col in merged.columns:
            fig.add_trace(go.Scatter(x=merged.index, y=merged[col], mode="lines", name=col))
        fig.update_layout(
            height=420,
            yaxis_title="Index level",
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig, use_container_width=True, theme="streamlit")

        st.download_button(
            "Download FCI series (CSV)",
            merged.to_csv(index=True).encode("utf-8"),
            file_name="fci_series.csv",
            mime="text/csv",
        )
        _export_csv(merged, "fci_series.csv")
    else:
        st.info("No FCI data available.")


def _build_rate_frame(data: Dict[str, pd.DataFrame], column: str, labels: Dict[str, str]) -> pd.DataFrame:
    frames = []
    for key, df in data.items():
        if df.empty or column not in df:
            continue
        frames.append(df[[column]].rename(columns={column: labels.get(key, key)}))
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, axis=1).sort_index()
    return merged


def render_nyfed_rates_section(
    rates_data: Dict[str, pd.DataFrame],
    start_date: date | datetime,
    end_date: date | datetime | None,
) -> None:
    st.subheader("NY Fed Markets API – Secured/Unsecured Rates")
    rate_errors = [
        f"{key}: {df['__error__'].iloc[0]}"
        for key, df in rates_data.items()
        if not df.empty and "__error__" in df.columns
    ]
    if rate_errors:
        st.warning("Rate fetch errors: " + "; ".join(rate_errors))
    sec_frame = _build_rate_frame(rates_data, "rate", {**SECURED_RATES_LABEL, **UNSECURED_RATES_LABEL})
    vol_frame = _build_rate_frame(rates_data, "volume_billions", {**SECURED_RATES_LABEL, **UNSECURED_RATES_LABEL})
    date_filter_start = pd.to_datetime(start_date)
    date_filter_end = pd.to_datetime(end_date) if end_date else None
    if not sec_frame.empty:
        sec_frame = sec_frame.loc[sec_frame.index >= date_filter_start]
        if date_filter_end is not None:
            sec_frame = sec_frame.loc[sec_frame.index <= date_filter_end]
        fig = go.Figure()
        for col in sec_frame.columns:
            fig.add_trace(go.Scatter(x=sec_frame.index, y=sec_frame[col], mode="lines", name=col))
        fig.update_layout(
            height=420,
            legend_title_text="Rates",
            yaxis_title="Percent",
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig, use_container_width=True, theme="streamlit")

    if not vol_frame.empty:
        vol_frame = vol_frame.loc[vol_frame.index >= date_filter_start]
        if date_filter_end is not None:
            vol_frame = vol_frame.loc[vol_frame.index <= date_filter_end]
        # Only keep columns that have any non-null volume
        vol_frame = vol_frame.loc[:, vol_frame.notna().any()]
        if not vol_frame.empty:
            fig_vol = go.Figure()
            for col in vol_frame.columns:
                fig_vol.add_trace(
                    go.Bar(
                        x=vol_frame.index,
                        y=vol_frame[col],
                        name=f"{col} volume (billions)",
                        opacity=0.75,
                    )
                )
            fig_vol.update_layout(
                barmode="group",
                height=360,
                yaxis_title="Volume (billions USD)",
                margin=dict(l=10, r=10, t=40, b=10),
            )
            st.plotly_chart(fig_vol, use_container_width=True, theme="streamlit")

    if not sec_frame.empty:
        st.download_button(
            "Download NY Fed rates (CSV)",
            sec_frame.to_csv(index=True).encode("utf-8"),
            file_name="nyfed_reference_rates.csv",
            mime="text/csv",
            help="SOFR/TGCR/BGCR/SOFRAI/EFFR/OBFR rates from NY Fed Markets API.",
        )
    if not vol_frame.empty:
        st.download_button(
            "Download NY Fed volumes (CSV)",
            vol_frame.to_csv(index=True).encode("utf-8"),
            file_name="nyfed_reference_volumes.csv",
            mime="text/csv",
            help="Transaction volumes (billions USD) from NY Fed Markets API.",
        )
    if not sec_frame.empty:
        _export_csv(sec_frame, "nyfed_reference_rates.csv")
    if not vol_frame.empty:
        _export_csv(vol_frame, "nyfed_reference_volumes.csv")


def render_repo_section(repo_data: Dict[str, pd.DataFrame], last_n: int) -> None:
    st.subheader(f"Repo / Reverse Repo operations (last {last_n} operations)")
    combined_frames = []
    repo_errors = [
        f"{key}: {df['__error__'].iloc[0]}"
        for key, df in repo_data.items()
        if not df.empty and "__error__" in df.columns
    ]
    if repo_errors:
        st.warning("Repo fetch errors: " + "; ".join(repo_errors))

    for key, df in repo_data.items():
        if df.empty or "__error__" in df.columns:
            continue
        combined_frames.append(df.assign(kind=key))
    if not combined_frames:
        st.info("No repo data returned from NY Fed API.")
        return
    combined = pd.concat(combined_frames, ignore_index=True)
    daily = (
        combined.groupby(["operationDate", "kind"])["totalAmtAccepted"]
        .sum()
        .unstack("kind")
        .sort_index()
        / 1_000_000_000
    )
    srf_daily = _extract_srf_usage(combined)
    fig = go.Figure()
    for col, color in [("reverserepo", "#1e88e5"), ("repo", "#6a1b9a")]:
        if col not in daily.columns:
            continue
        fig.add_trace(go.Bar(x=daily.index, y=daily[col], name=col, marker_color=color))
    fig.update_layout(
        barmode="stack",
        height=420,
        yaxis_title="Accepted (billions USD)",
        margin=dict(l=10, r=10, t=40, b=10),
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")

    latest_rows = combined.sort_values("operationDate").tail(5)
    show_cols = ["operationDate", "operationId", "operation_type", "operationMethod", "term", "totalAmtAccepted", "participatingCpty", "acceptedCpty"]
    for col in show_cols:
        if col in latest_rows.columns and pd.api.types.is_numeric_dtype(latest_rows[col]):
            latest_rows[col] = latest_rows[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "")
    available_cols = [col for col in show_cols if col in latest_rows.columns]
    st.dataframe(latest_rows[available_cols], use_container_width=True, hide_index=True)

    if not srf_daily.empty:
        srf_latest = srf_daily["srf_billion"].iloc[-1]
        st.metric("Latest SRF accepted (billions)", f"{srf_latest:.2f}")
        fig_srf = go.Figure(
            data=[go.Bar(x=srf_daily.index, y=srf_daily["srf_billion"], marker_color="#1976d2", name="SRF")]
        )
        fig_srf.update_layout(
            height=280,
            yaxis_title="Billions USD",
            margin=dict(l=10, r=10, t=30, b=10),
        )
        st.plotly_chart(fig_srf, use_container_width=True, theme="streamlit")

    st.download_button(
        "Download repo operations (CSV)",
        combined.to_csv(index=False).encode("utf-8"),
        file_name="repo_operations_raw.csv",
        mime="text/csv",
        help="Raw repo/reverse repo operations (NY Fed Markets API).",
    )
    st.download_button(
        "Download daily totals (CSV)",
        daily.to_csv(index=True).encode("utf-8"),
        file_name="repo_operations_daily_totals.csv",
        mime="text/csv",
        help="Daily accepted totals (billions USD) by repo/reverserepo.",
    )
    _export_csv(combined, "repo_operations_raw.csv")
    _export_csv(daily, "repo_operations_daily_totals.csv")
    if not srf_daily.empty:
        st.download_button(
            "Download SRF usage (CSV)",
            srf_daily.to_csv(index=True).encode("utf-8"),
            file_name="srf_usage_daily.csv",
            mime="text/csv",
            help="Daily SRF accepted amounts inferred from repo operations.",
        )
        _export_csv(srf_daily, "srf_usage_daily.csv")


def render_soma_section(soma_df: pd.DataFrame, start_date: date | datetime, end_date: date | datetime | None) -> None:
    st.subheader("SOMA summary holdings (NY Fed Markets API)")
    if soma_df.empty:
        st.info("No SOMA data returned.")
        return
    filtered = soma_df.copy()
    filtered = filtered.loc[filtered.index >= pd.to_datetime(start_date)]
    if end_date:
        filtered = filtered.loc[filtered.index <= pd.to_datetime(end_date)]
    latest = filtered.iloc[-1]
    metrics = {
        "Total": latest.get("total", float("nan")),
        "UST (bills+notes/bonds+TIPS)": latest.get("ust_total", float("nan")),
        "MBS+CMBS": latest.get("mbs_total", float("nan")),
    }
    cols = st.columns(len(metrics))
    for idx, (label, val) in enumerate(metrics.items()):
        if pd.isna(val):
            cols[idx].metric(label, "n/a")
        else:
            cols[idx].metric(label, f"{val / 1_000_000_000:,.1f}B")

    fig = go.Figure()
    for col, name in [
        ("total", "Total"),
        ("ust_total", "UST aggregate"),
        ("mbs_total", "MBS+CMBS"),
    ]:
        if col not in filtered:
            continue
        fig.add_trace(go.Scatter(x=filtered.index, y=filtered[col] / 1_000_000_000, mode="lines", name=name))
    fig.update_layout(
        height=420,
        yaxis_title="Trillions USD",
        margin=dict(l=10, r=10, t=40, b=10),
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")

    st.download_button(
        "Download SOMA summary (CSV)",
        filtered.to_csv(index=True).encode("utf-8"),
        file_name="soma_summary.csv",
        mime="text/csv",
        help="Weekly SOMA totals by asset class from NY Fed Markets API.",
    )

    # Detailed Treasury holdings (latest as-of)
    latest_date = filtered.index.max()
    if pd.notnull(latest_date):
        as_of_str = latest_date.strftime("%Y-%m-%d")
        detail_df = fetch_soma_tsy_details(as_of_str)
        if not detail_df.empty:
            type_summary = (
                detail_df.groupby("securityType")["parValue"]
                .sum()
                .sort_values(ascending=False)
                / 1_000_000_000
            )
            st.markdown(f"**Latest Treasury holdings by type (as of {as_of_str})**")
            st.bar_chart(type_summary)

            bucketed = _bucketize_maturity(detail_df)
            if not bucketed.empty:
                st.markdown("**Maturity buckets (Treasury SOMA)**")
                fig_bucket = go.Figure(
                    data=[
                        go.Bar(
                            x=bucketed["bucket"],
                            y=bucketed["parValue"] / 1_000_000_000,
                            marker_color="#4caf50",
                        )
                    ]
                )
                fig_bucket.update_layout(
                    height=320,
                    yaxis_title="Par value (billions USD)",
                    margin=dict(l=10, r=10, t=30, b=10),
                )
                st.plotly_chart(fig_bucket, use_container_width=True, theme="streamlit")

            st.download_button(
                f"Download SOMA Treasury details ({as_of_str})",
                detail_df.to_csv(index=False).encode("utf-8"),
                file_name=f"soma_treasury_{as_of_str}.csv",
                mime="text/csv",
                help="CUSIP-level SOMA Treasury holdings with maturity and par value.",
            )
            _export_csv(detail_df, f"soma_treasury_{as_of_str}.csv")

            wam_years = _weighted_average_maturity_years(detail_df)
            if not pd.isna(wam_years):
                st.metric("Treasury WAM (years)", f"{wam_years:.2f}")


def main() -> None:
    st.set_page_config(
        page_title="Fed monetary plumbing dashboard",
        layout="wide",
    )
    st.title("Fed monetary plumbing dashboard")
    st.caption(
        "FRED CSV endpoints (no API key) for WALCL, WSHOSHO, RRPONTSYD, WTREGEN, "
        "RESBALNS, IORB, SOFR, DFEDTARU/L, and EFFR, plus NY Fed Markets API for SOFR/TGCR/BGCR, "
        "EFFR/OBFR, repo/RRP operations, and SOMA summary. Units: millions USD for balances, percent for rates.",
    )

    with st.sidebar:
        start_date = st.date_input("Start date", value=DEFAULT_START, help="Applies to all series requested from FRED.")
        end_date = st.date_input("End date", value=date.today())
        repo_window = st.slider("Repo lookback (operations)", min_value=30, max_value=180, value=90, step=10)
        delta_days = st.slider("Delta window (days)", min_value=7, max_value=120, value=30, step=1)
        if st.button("Refresh data", use_container_width=True):
            fetch_fred_series.clear()
            load_all_series.clear()
            fetch_nyfed_reference_rates.clear()
            load_nyfed_reference_rates.clear()
            fetch_repo_operations.clear()
            load_repo_operations.clear()
            fetch_soma_summary.clear()

    df, errors = load_all_series(start_date)
    if errors:
        st.warning(
            "One or more series failed to load: "
            + ", ".join(f"{key} ({msg})" for key, msg in errors.items())
        )

    if df.empty:
        st.error("No data returned from FRED. Try a different start date.")
        return

    nyfed_rates = load_nyfed_reference_rates(start_date, end_date)
    repo_ops = load_repo_operations(repo_window)
    soma_df = fetch_soma_summary()
    fci_data = load_fci_indices(start_date)

    tab_liq, tab_rates, tab_ny_rates, tab_repo, tab_soma, tab_fci, tab_spreads = st.tabs(
        ["Liquidity", "Policy rates", "NY Fed reference rates", "Repo Ops", "SOMA", "Financial Conditions", "Spreads"],
    )
    with tab_liq:
        render_liquidity_section(df, delta_days)
        st.divider()
        render_balance_sheet_stacks(df, soma_df)
        render_tsy_maturity_stack(df, soma_df)
        st.subheader("Maturity buckets (latest, Treasury SOMA)")
        render_tsy_maturity_buckets_latest()
        st.subheader("SOMA Treasury WAM (years)")
        render_tsy_wam_series()
        st.subheader("Reserve Demand Elasticity")
        render_reserve_demand_elasticity()
    with tab_rates:
        render_rate_section(df, delta_days)
        st.divider()
        render_money_market_complex(df)
    with tab_ny_rates:
        render_nyfed_rates_section(nyfed_rates, start_date, end_date)
        st.subheader("Benchmark rates (SOFR/BGCR/TGCR/OBFR/EFFR)")
        render_benchmark_monitor(nyfed_rates)
    with tab_repo:
        render_repo_section(repo_ops, repo_window)
    with tab_soma:
        render_soma_section(soma_df, start_date, end_date)
    with tab_fci:
        render_fci_section(fci_data, start_date)
    with tab_spreads:
        render_spreads_monitor(df, nyfed_rates)
        st.divider()
        render_stress_monitor(df, nyfed_rates, repo_ops)
    # Add balance sheet stacks inside Liquidity for convenience
    with tab_liq:
        st.divider()
        render_balance_sheet_stacks(df, soma_df)

    render_data_table(df)


if __name__ == "__main__":
    main()
