"""PCE inflation contribution data (FRBSF Excel)."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import pandas as pd

try:
    from .us_eco_utils import load_data_from_csv  # type: ignore
    from .excel_source_utils import load_excel_sheets, tidy_excel_frame  # type: ignore
except ImportError:
    from us_eco_utils import load_data_from_csv  # type: ignore
    from excel_source_utils import load_excel_sheets, tidy_excel_frame  # type: ignore


DATA_URL = "https://www.frbsf.org/wp-content/uploads/pce-contributions-data.xlsx?2026-01-09"

CSV_FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    "data",
    "pce_contributions_data.csv",
)

SHEET_CONFIGS = [
    {
        "sheet": "chart1_headlinePCEPI_YoY",
        "rename_map": {
            "Total: Headline PCEPI, YoY": "headline_yoy_total",
            "Energy": "headline_yoy_energy",
            "Food": "headline_yoy_food",
            "Core Goods": "headline_yoy_core_goods",
            "Core Services exc. Housing": "headline_yoy_core_services_ex_housing",
            "Housing": "headline_yoy_housing",
        },
    },
    {
        "sheet": "chart2_headlinePCEPI_MoM",
        "rename_map": {
            "Total: Headline PCEPI, MoM": "headline_mom_total",
            "Energy": "headline_mom_energy",
            "Food": "headline_mom_food",
            "Core Goods": "headline_mom_core_goods",
            "Core Services exc. Housing": "headline_mom_core_services_ex_housing",
            "Housing": "headline_mom_housing",
        },
    },
    {
        "sheet": "chart3_corePCEPI_YoY",
        "rename_map": {
            "Total: Core PCEPI, YoY": "core_yoy_total",
            "Core Goods": "core_yoy_core_goods",
            "Core Services exc. Housing": "core_yoy_core_services_ex_housing",
            "Housing": "core_yoy_housing",
        },
    },
    {
        "sheet": "extra_corePCEPI_MoM",
        "rename_map": {
            "Total: Core PCEPI, MoM": "core_mom_total",
            "Core Goods": "core_mom_core_goods",
            "Core Services exc. Housing": "core_mom_core_services_ex_housing",
            "Housing": "core_mom_housing",
        },
    },
    {
        "sheet": "chart4_supercorePCEPI_YoY",
        "rename_map": {
            "Total: Supercore Services PCEPI, YoY": "supercore_yoy_total",
            "Health Care": "supercore_yoy_health_care",
            "Financial & Insurance": "supercore_yoy_financial_insurance",
            "Food Services & Accommodations": "supercore_yoy_food_services_accommodations",
            "Transportation Services": "supercore_yoy_transportation_services",
            "Other Services": "supercore_yoy_other_services",
        },
    },
    {
        "sheet": "extra_supercorePCEPI_MoM",
        "rename_map": {
            "Total: Supercore Services PCEPI, MoM": "supercore_mom_total",
            "Health Care": "supercore_mom_health_care",
            "Financial Services & Insurance": "supercore_mom_financial_insurance",
            "Food Services & Accommodations": "supercore_mom_food_services_accommodations",
            "Transportation Services": "supercore_mom_transportation_services",
            "Other Services": "supercore_mom_other_services",
        },
    },
]

COLUMN_ORDER: list[str] = []
for config in SHEET_CONFIGS:
    COLUMN_ORDER.extend(list(config["rename_map"].values()))

PCE_CONTRIBUTIONS_SERIES: dict[str, dict[str, str]] = {
    key: {"unit": "pp"} for key in COLUMN_ORDER
}

PCE_CONTRIBUTIONS_KOREAN_NAMES: dict[str, str] = {
    "headline_yoy_total": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \ucd1d\ud572",
    "headline_yoy_energy": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \uc5d0\ub108\uc9c0",
    "headline_yoy_food": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \uc2f4\ud488",
    "headline_yoy_core_goods": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \ucf54\uc5b4 \uc7ac\ud654",
    "headline_yoy_core_services_ex_housing": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \ucf54\uc5b4 \uc11c\ube44\uc2a4(\uc8fc\uac70 \uc81c\uc678)",
    "headline_yoy_housing": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \uc8fc\uac70",
    "headline_mom_total": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \ucd1d\ud572",
    "headline_mom_energy": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \uc5d0\ub108\uc9c0",
    "headline_mom_food": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \uc2f4\ud488",
    "headline_mom_core_goods": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \ucf54\uc5b4 \uc7ac\ud654",
    "headline_mom_core_services_ex_housing": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \ucf54\uc5b4 \uc11c\ube44\uc2a4(\uc8fc\uac70 \uc81c\uc678)",
    "headline_mom_housing": "\ud5e4\ub4dc\ub77c\uc778 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \uc8fc\uac70",
    "core_yoy_total": "\ucf54\uc5b4 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \ucd1d\ud572",
    "core_yoy_core_goods": "\ucf54\uc5b4 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \ucf54\uc5b4 \uc7ac\ud654",
    "core_yoy_core_services_ex_housing": "\ucf54\uc5b4 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \ucf54\uc5b4 \uc11c\ube44\uc2a4(\uc8fc\uac70 \uc81c\uc678)",
    "core_yoy_housing": "\ucf54\uc5b4 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \uc8fc\uac70",
    "core_mom_total": "\ucf54\uc5b4 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \ucd1d\ud572",
    "core_mom_core_goods": "\ucf54\uc5b4 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \ucf54\uc5b4 \uc7ac\ud654",
    "core_mom_core_services_ex_housing": "\ucf54\uc5b4 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \ucf54\uc5b4 \uc11c\ube44\uc2a4(\uc8fc\uac70 \uc81c\uc678)",
    "core_mom_housing": "\ucf54\uc5b4 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \uc8fc\uac70",
    "supercore_yoy_total": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \ucd1d\ud572",
    "supercore_yoy_health_care": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \ud5ec\uc2a4\ucf00\uc5b4",
    "supercore_yoy_financial_insurance": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \uae08\uc735\xb7\ubcf4\ud5d8",
    "supercore_yoy_food_services_accommodations": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \uc74c\uc2f4\xb7\uc230\ubc2c",
    "supercore_yoy_transportation_services": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \uc6b4\uc1a1 \uc11c\ube44\uc2a4",
    "supercore_yoy_other_services": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\ub144\ube44) - \uae30\ud0c0 \uc11c\ube44\uc2a4",
    "supercore_mom_total": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \ucd1d\ud572",
    "supercore_mom_health_care": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \ud5ec\uc2a4\ucf00\uc5b4",
    "supercore_mom_financial_insurance": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \uae08\uc735\xb7\ubcf4\ud5d8",
    "supercore_mom_food_services_accommodations": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \uc74c\uc2f4\xb7\uc230\ubc2c",
    "supercore_mom_transportation_services": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \uc6b4\uc1a1 \uc11c\ube44\uc2a4",
    "supercore_mom_other_services": "\uc288\ud37c\ucf54\uc5b4 \uc11c\ube44\uc2a4 PCE \uae30\uc5ec\ub3c4(\uc804\uc6d4\ube44) - \uae30\ud0c0 \uc11c\ube44\uc2a4",
}

PCE_CONTRIBUTIONS_DATA: dict[str, Any] = {
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


def update_pce_contributions_csv(
    url: str = DATA_URL,
    output_path: str = CSV_FILE_PATH,
) -> pd.DataFrame:
    sheet_names = [config["sheet"] for config in SHEET_CONFIGS]
    sheets = load_excel_sheets(url, sheet_names=sheet_names)

    frames: list[pd.DataFrame] = []
    for config in SHEET_CONFIGS:
        sheet = config["sheet"]
        raw_df = sheets.get(sheet)
        if raw_df is None:
            continue
        rename_map = config["rename_map"]
        cleaned = tidy_excel_frame(
            raw_df,
            date_column="date",
            rename_map=rename_map,
            keep_columns=list(rename_map.values()),
            drop_columns=["notes"],
        )
        frames.append(cleaned)

    if not frames:
        raise ValueError("No sheets parsed from source data.")

    combined = pd.concat(frames, axis=1, join="outer").sort_index()
    ordered_cols = [col for col in COLUMN_ORDER if col in combined.columns]
    combined = combined[ordered_cols]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined.to_csv(output_path, index_label="date")
    return combined


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


def load_pce_contributions_data(
    force_reload: bool = False,
) -> dict[str, Any] | None:
    if force_reload or not os.path.exists(CSV_FILE_PATH):
        update_pce_contributions_csv()

    df = load_data_from_csv(CSV_FILE_PATH)
    if df is None or df.empty:
        return None

    PCE_CONTRIBUTIONS_DATA["raw_data"] = df.copy()
    PCE_CONTRIBUTIONS_DATA["mom_data"] = pd.DataFrame()
    PCE_CONTRIBUTIONS_DATA["mom_change"] = pd.DataFrame()
    PCE_CONTRIBUTIONS_DATA["yoy_data"] = pd.DataFrame()
    PCE_CONTRIBUTIONS_DATA["yoy_change"] = pd.DataFrame()
    PCE_CONTRIBUTIONS_DATA["latest_values"] = _build_latest_values(df)
    PCE_CONTRIBUTIONS_DATA["korean_names"] = dict(PCE_CONTRIBUTIONS_KOREAN_NAMES)
    PCE_CONTRIBUTIONS_DATA["load_info"].update(
        {
            "loaded": True,
            "load_time": datetime.now(),
            "start_date": df.index.min().strftime("%Y-%m-%d"),
            "series_count": df.shape[1],
            "data_points": df.shape[0],
            "source": "FRBSF Excel",
        }
    )

    return PCE_CONTRIBUTIONS_DATA


if __name__ == "__main__":
    update_pce_contributions_csv()
