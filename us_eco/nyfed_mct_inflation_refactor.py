"""NY Fed MCT inflation data (Excel with rolling monthly sheets)."""

from __future__ import annotations

import os
import re
from datetime import datetime
from io import BytesIO
from typing import Any

import pandas as pd

try:
    from .us_eco_utils import load_data_from_csv  # type: ignore
    from .excel_source_utils import download_excel_bytes, tidy_excel_frame  # type: ignore
except ImportError:
    from us_eco_utils import load_data_from_csv  # type: ignore
    from excel_source_utils import download_excel_bytes, tidy_excel_frame  # type: ignore


DATA_URL = "https://www.newyorkfed.org/medialibrary/Research/Interactives/mct/downloads/NYFed_MCT-Inflation_data"

CSV_FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    "data",
    "nyfed_mct_inflation_data.csv",
)

SHEET_PATTERN = re.compile(r"^Charts(\d{6})$")
LAST_SHEET_NAME: str | None = None

COLUMN_RENAME = {
    "Headline (12m)": "headline_12m",
    "Core (12m)": "core_12m",
    "16th percentile": "percentile_16",
    "Median": "median",
    "84th percentile": "percentile_84",
    "Normalized": "normalized",
    "Goods": "goods",
    "Services ex. Housing": "services_ex_housing",
    "Housing": "housing",
    "Goods: Common": "goods_common",
    "Goods: Sector-specific": "goods_sector_specific",
    "Services ex. housing: Common": "services_ex_housing_common",
    "Services ex. housing: Sector-specific": "services_ex_housing_sector_specific",
    "Housing: Common": "housing_common",
    "Housing: Sector-specific": "housing_sector_specific",
}

COLUMN_ORDER = list(COLUMN_RENAME.values())

NYFED_MCT_SERIES: dict[str, dict[str, str]] = {
    key: {"unit": "%"} for key in COLUMN_ORDER
}

NYFED_MCT_KOREAN_NAMES: dict[str, str] = {
    "headline_12m": "\ud5e4\ub4dc\ub77c\uc778 PCE(12M)",
    "core_12m": "\ucf54\uc5b4 PCE(12M)",
    "percentile_16": "MCT 16\ubd84\uc704",
    "median": "MCT \uc911\uc559\uac12",
    "percentile_84": "MCT 84\ubd84\uc704",
    "normalized": "MCT \uc815\uaddc\ud654",
    "goods": "\uc7ac\ud654",
    "services_ex_housing": "\uc11c\ube44\uc2a4(\uc8fc\uac70 \uc81c\uc678)",
    "housing": "\uc8fc\uac70",
    "goods_common": "\uc7ac\ud654: \uacf5\ud1b5",
    "goods_sector_specific": "\uc7ac\ud654: \uc139\ud130\ubcc4",
    "services_ex_housing_common": "\uc11c\ube44\uc2a4(\uc8fc\uac70 \uc81c\uc678): \uacf5\ud1b5",
    "services_ex_housing_sector_specific": "\uc11c\ube44\uc2a4(\uc8fc\uac70 \uc81c\uc678): \uc139\ud130\ubcc4",
    "housing_common": "\uc8fc\uac70: \uacf5\ud1b5",
    "housing_sector_specific": "\uc8fc\uac70: \uc139\ud130\ubcc4",
}

NYFED_MCT_DATA: dict[str, Any] = {
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


def _select_latest_chart_sheet(sheet_names: list[str]) -> str:
    candidates: list[tuple[int, str]] = []
    for name in sheet_names:
        match = SHEET_PATTERN.match(name)
        if match:
            candidates.append((int(match.group(1)), name))
    if not candidates:
        raise ValueError("No chart sheets found in NY Fed MCT workbook.")
    return max(candidates, key=lambda item: item[0])[1]


def update_nyfed_mct_inflation_csv(
    url: str = DATA_URL,
    output_path: str = CSV_FILE_PATH,
) -> pd.DataFrame:
    global LAST_SHEET_NAME

    content = download_excel_bytes(url)
    workbook = pd.ExcelFile(BytesIO(content))
    sheet_name = _select_latest_chart_sheet(workbook.sheet_names)
    LAST_SHEET_NAME = sheet_name

    raw_df = pd.read_excel(workbook, sheet_name=sheet_name, header=5)
    cleaned = tidy_excel_frame(
        raw_df,
        date_column="Date",
        rename_map=COLUMN_RENAME,
        keep_columns=COLUMN_ORDER,
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


def load_nyfed_mct_inflation_data(
    force_reload: bool = False,
) -> dict[str, Any] | None:
    if force_reload or not os.path.exists(CSV_FILE_PATH):
        update_nyfed_mct_inflation_csv()

    df = load_data_from_csv(CSV_FILE_PATH)
    if df is None or df.empty:
        return None

    NYFED_MCT_DATA["raw_data"] = df.copy()
    NYFED_MCT_DATA["mom_data"] = pd.DataFrame()
    NYFED_MCT_DATA["mom_change"] = pd.DataFrame()
    NYFED_MCT_DATA["yoy_data"] = pd.DataFrame()
    NYFED_MCT_DATA["yoy_change"] = pd.DataFrame()
    NYFED_MCT_DATA["latest_values"] = _build_latest_values(df)
    NYFED_MCT_DATA["korean_names"] = dict(NYFED_MCT_KOREAN_NAMES)

    source_label = "NY Fed Excel"
    if LAST_SHEET_NAME:
        source_label = f"NY Fed Excel ({LAST_SHEET_NAME})"

    NYFED_MCT_DATA["load_info"].update(
        {
            "loaded": True,
            "load_time": datetime.now(),
            "start_date": df.index.min().strftime("%Y-%m-%d"),
            "series_count": df.shape[1],
            "data_points": df.shape[0],
            "source": source_label,
        }
    )

    return NYFED_MCT_DATA


if __name__ == "__main__":
    update_nyfed_mct_inflation_csv()
