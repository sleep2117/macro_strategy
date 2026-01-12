"""Supply/Demand PCE inflation decomposition (FRBSF Excel)."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import pandas as pd

try:
    from .us_eco_utils import load_data_from_csv  # type: ignore
    from .excel_source_utils import load_excel_sheets, parse_time_month, tidy_excel_frame  # type: ignore
except ImportError:
    from us_eco_utils import load_data_from_csv  # type: ignore
    from excel_source_utils import load_excel_sheets, parse_time_month, tidy_excel_frame  # type: ignore


DATA_URL = "https://www.frbsf.org/wp-content/uploads/supply-demand-pce-inflation.xlsx"
DATA_SHEET = "Data"
DATE_COLUMN = "time_month"

CSV_FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    "data",
    "supply_demand_pce_inflation_data.csv",
)

COLUMN_MAP: dict[str, str] = {
    "Demand-driven Inflation (core, y/y)": "core_demand_yoy",
    "Ambiguous (core, y/y)": "core_ambiguous_yoy",
    "Supply-driven Inflation (core, y/y)": "core_supply_yoy",
    "Demand-driven Inflation (core, m/m)": "core_demand_mom",
    "Ambiguous (core, m/m)": "core_ambiguous_mom",
    "Supply-driven Inflation (core, m/m)": "core_supply_mom",
    "Demand-driven Inflation (headline, y/y)": "headline_demand_yoy",
    "Ambiguous (headline, y/y)": "headline_ambiguous_yoy",
    "Supply-driven Inflation (headline, y/y)": "headline_supply_yoy",
    "Demand-driven Inflation (headline, m/m)": "headline_demand_mom",
    "Ambiguous (headline, m/m)": "headline_ambiguous_mom",
    "Supply-driven Inflation (headline, m/m)": "headline_supply_mom",
}

SUPPLY_DEMAND_PCE_SERIES: dict[str, dict[str, str]] = {
    key: {"unit": "%"} for key in COLUMN_MAP.values()
}

SUPPLY_DEMAND_PCE_KOREAN_NAMES: dict[str, str] = {
    "core_demand_yoy": "\ucf54\uc5b4 \uc218\uc694 \uc8fc\ub3c4 \uc778\ud50c\ub808\uc774\uc158 (YoY)",
    "core_ambiguous_yoy": "\ucf54\uc5b4 \ud63c\ud569 \uc694\uc778 \uc778\ud50c\ub808\uc774\uc158 (YoY)",
    "core_supply_yoy": "\ucf54\uc5b4 \uacf5\uae09 \uc8fc\ub3c4 \uc778\ud50c\ub808\uc774\uc158 (YoY)",
    "core_demand_mom": "\ucf54\uc5b4 \uc218\uc694 \uc8fc\ub3c4 \uc778\ud50c\ub808\uc774\uc158 (MoM)",
    "core_ambiguous_mom": "\ucf54\uc5b4 \ud63c\ud569 \uc694\uc778 \uc778\ud50c\ub808\uc774\uc158 (MoM)",
    "core_supply_mom": "\ucf54\uc5b4 \uacf5\uae09 \uc8fc\ub3c4 \uc778\ud50c\ub808\uc774\uc158 (MoM)",
    "headline_demand_yoy": "\ud5e4\ub4dc\ub77c\uc778 \uc218\uc694 \uc8fc\ub3c4 \uc778\ud50c\ub808\uc774\uc158 (YoY)",
    "headline_ambiguous_yoy": "\ud5e4\ub4dc\ub77c\uc778 \ud63c\ud569 \uc694\uc778 \uc778\ud50c\ub808\uc774\uc158 (YoY)",
    "headline_supply_yoy": "\ud5e4\ub4dc\ub77c\uc778 \uacf5\uae09 \uc8fc\ub3c4 \uc778\ud50c\ub808\uc774\uc158 (YoY)",
    "headline_demand_mom": "\ud5e4\ub4dc\ub77c\uc778 \uc218\uc694 \uc8fc\ub3c4 \uc778\ud50c\ub808\uc774\uc158 (MoM)",
    "headline_ambiguous_mom": "\ud5e4\ub4dc\ub77c\uc778 \ud63c\ud569 \uc694\uc778 \uc778\ud50c\ub808\uc774\uc158 (MoM)",
    "headline_supply_mom": "\ud5e4\ub4dc\ub77c\uc778 \uacf5\uae09 \uc8fc\ub3c4 \uc778\ud50c\ub808\uc774\uc158 (MoM)",
}

SUPPLY_DEMAND_PCE_DATA: dict[str, Any] = {
    "raw_data": pd.DataFrame(),
    "mom_data": pd.DataFrame(),
    "mom_change": pd.DataFrame(),
    "yoy_data": pd.DataFrame(),
    "yoy_change": pd.DataFrame(),
    "latest_values": {},
    "load_info": {
        "loaded": False,
        "load_time": None,
        "start_date": None,
        "series_count": 0,
        "data_points": 0,
        "source": None,
    },
}


def update_supply_demand_pce_inflation_csv(
    url: str = DATA_URL,
    output_path: str = CSV_FILE_PATH,
) -> pd.DataFrame:
    sheets = load_excel_sheets(url, sheet_names=[DATA_SHEET])
    raw_df = sheets.get(DATA_SHEET)
    if raw_df is None:
        raise ValueError(f"Missing {DATA_SHEET} in source data.")
    cleaned = tidy_excel_frame(
        raw_df,
        date_column=DATE_COLUMN,
        rename_map=COLUMN_MAP,
        keep_columns=list(COLUMN_MAP.values()),
        date_parser=parse_time_month,
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    cleaned.to_csv(output_path, index_label="date")
    return cleaned


def _build_latest_values(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    if df is None or df.empty:
        return latest
    for column in df.columns:
        series = pd.to_numeric(df[column], errors="coerce").dropna()
        if series.empty:
            continue
        last_timestamp = series.index[-1]
        latest[column] = {
            "value": float(series.iloc[-1]),
            "date": last_timestamp.strftime("%Y-%m-%d"),
        }
    return latest


def load_supply_demand_pce_inflation_data(
    force_reload: bool = False,
) -> dict[str, Any] | None:
    if force_reload or not os.path.exists(CSV_FILE_PATH):
        update_supply_demand_pce_inflation_csv()

    df = load_data_from_csv(CSV_FILE_PATH)
    if df is None or df.empty:
        return None

    SUPPLY_DEMAND_PCE_DATA["raw_data"] = df.copy()
    SUPPLY_DEMAND_PCE_DATA["mom_data"] = pd.DataFrame()
    SUPPLY_DEMAND_PCE_DATA["mom_change"] = pd.DataFrame()
    SUPPLY_DEMAND_PCE_DATA["yoy_data"] = pd.DataFrame()
    SUPPLY_DEMAND_PCE_DATA["yoy_change"] = pd.DataFrame()
    SUPPLY_DEMAND_PCE_DATA["latest_values"] = _build_latest_values(df)
    SUPPLY_DEMAND_PCE_DATA["korean_names"] = dict(SUPPLY_DEMAND_PCE_KOREAN_NAMES)
    SUPPLY_DEMAND_PCE_DATA["load_info"].update(
        {
            "loaded": True,
            "load_time": datetime.now(),
            "start_date": df.index.min().strftime("%Y-%m-%d"),
            "series_count": df.shape[1],
            "data_points": df.shape[0],
            "source": "FRBSF Excel",
        }
    )

    return SUPPLY_DEMAND_PCE_DATA


if __name__ == "__main__":
    update_supply_demand_pce_inflation_csv()
