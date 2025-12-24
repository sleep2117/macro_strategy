from __future__ import annotations

import json
import importlib
import importlib.util
import inspect
import os
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

import dash
from dash import dcc, html, dash_table, Input, Output, State, no_update

try:
    from . import us_eco_utils as utils_module
except ImportError:
    sys.path.append(os.path.dirname(__file__))
    import us_eco_utils as utils_module

from us_eco_utils import (
    calculate_mom_percent,
    calculate_mom_change,
    calculate_yoy_percent,
    calculate_yoy_change,
)

try:
    from kpds_fig_format_enhanced import (
        get_kpds_color,
        format_date_ticks,
        FONT_SIZE_GENERAL,
        FONT_SIZE_LEGEND,
        FONT_SIZE_ANNOTATION,
        calculate_title_position,
        create_five_year_comparison_chart,
    )
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from kpds_fig_format_enhanced import (
        get_kpds_color,
        format_date_ticks,
        FONT_SIZE_GENERAL,
        FONT_SIZE_LEGEND,
        FONT_SIZE_ANNOTATION,
        calculate_title_position,
        create_five_year_comparison_chart,
    )


PX_PER_CM = 37.7952755906
PRESETS_FILE_PATH = Path(__file__).with_name("dashboard_presets.json")

CHART_TYPE_LABELS: dict[str, str] = {
    "Multi line": "multi_line",
    "Single line": "single_line",
    "Dual axis": "dual_axis",
    "Horizontal bar": "horizontal_bar",
    "Vertical bar": "vertical_bar",
    "Five year": "five_year",
}

STANDARD_DATA_KEYS = [
    ("raw_data", "Raw"),
    ("mom_data", "MoM %"),
    ("mom_change", "MoM change"),
    ("yoy_data", "YoY %"),
    ("yoy_change", "YoY change"),
]

DEFAULT_CHART_WIDTH_CM = 19.5
DEFAULT_CHART_HEIGHT_CM = 10.0

FRIENDLY_TITLES = {
    "CPI_analysis_refactor": "CPI",
    "PPI_analysis_refactor": "PPI",
    "CES_employ_refactor": "CES Employment",
    "CPS_employ_refactor": "CPS Employment",
    "ADP_employ_refactor": "ADP Employment",
    "industrial_production_refactor": "Industrial Production",
    "retail_sales_refactor": "Retail Sales",
    "construction_spending_refactor": "Construction",
    "durable_goods_refactor": "Durable Goods",
    "new_residential_construction_refactor": "Housing Starts",
    "atlanta_wage_growth_refactor": "Atlanta Wage",
    "house_price_refactor": "House Prices",
    "house_sales_stock_refactor": "Housing Sales",
    "personal_income_refactor": "Personal Income",
    "pce_analysis_refactor": "PCE",
    "import_price_refactor_v2": "Import Prices",
    "int_trade_refactor": "Trade",
    "ism_pmi_refactor": "ISM PMI",
    "fed_pmi_refactor": "Fed PMI",
    "fed_balance_sheet_refactor": "Fed Balance Sheet",
    "realtor_housing_inventory_refactor": "Realtor Inventory",
    "unemployment_claims_analysis": "Unemployment Claims",
    "phillips_curve_enhanced": "Phillips Curve",
    "beveridge_curve_enhanced": "Beveridge Curve",
    "gdp_analysis_refactor": "GDP",
    "JOLTS_employ_refactor": "JOLTS",
    "misc_fred_series_refactor": "Misc FRED",
}

CATEGORY_MAP: dict[str, list[str]] = {
    "Inflation": [
        "CPI_analysis_refactor",
        "PPI_analysis_refactor",
        "pce_analysis_refactor",
        "import_price_refactor_v2",
        "misc_fred_series_refactor",
    ],
    "Employment": [
        "CES_employ_refactor",
        "CPS_employ_refactor",
        "ADP_employ_refactor",
        "JOLTS_employ_refactor",
        "atlanta_wage_growth_refactor",
        "unemployment_claims_analysis",
        "phillips_curve_enhanced",
        "beveridge_curve_enhanced",
    ],
    "Housing": [
        "house_price_refactor",
        "house_sales_stock_refactor",
        "realtor_housing_inventory_refactor",
        "construction_spending_refactor",
        "new_residential_construction_refactor",
    ],
    "Industry": [
        "industrial_production_refactor",
        "retail_sales_refactor",
        "ism_pmi_refactor",
        "fed_pmi_refactor",
    ],
}

DEFAULT_MODULE_FOR_SELECTION = "CPI_analysis_refactor"


def load_cached_dataframe(csv_path: str) -> pd.DataFrame | None:
    if not csv_path or not os.path.exists(csv_path):
        return None
    try:
        df = pd.read_csv(csv_path, index_col=0)
        df.index = pd.to_datetime(df.index)
        return df.sort_index()
    except Exception as exc:
        print(f"CSV load failed ({csv_path}): {exc}")
        return None


def load_dashboard_presets() -> dict[str, Any]:
    if not PRESETS_FILE_PATH.exists():
        return {}
    try:
        with PRESETS_FILE_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Preset load failed: {exc}")
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def save_dashboard_presets(presets: dict[str, Any]) -> None:
    try:
        PRESETS_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with PRESETS_FILE_PATH.open("w", encoding="utf-8") as handle:
            json.dump(presets, handle, ensure_ascii=False, indent=2)
    except OSError as exc:
        print(f"Preset save failed: {exc}")


def build_data_dict_from_raw(
    raw_df: pd.DataFrame,
    rename_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    if raw_df is None or raw_df.empty:
        return {}

    renamed = raw_df.sort_index().rename(columns=lambda c: rename_map.get(c, c) if rename_map else c)

    numeric_cols = []
    extra_cols = []
    for col in renamed.columns:
        converted = pd.to_numeric(renamed[col], errors="coerce")
        if converted.notna().any():
            renamed[col] = converted
            numeric_cols.append(col)
        else:
            extra_cols.append(col)

    raw_numeric = renamed[numeric_cols].dropna(how="all") if numeric_cols else pd.DataFrame(index=renamed.index)
    extra_df = renamed[extra_cols] if extra_cols else pd.DataFrame(index=renamed.index)

    mom = calculate_mom_percent(raw_numeric) if not raw_numeric.empty else pd.DataFrame()
    mom_change = calculate_mom_change(raw_numeric) if not raw_numeric.empty else pd.DataFrame()
    yoy = calculate_yoy_percent(raw_numeric) if not raw_numeric.empty else pd.DataFrame()
    yoy_change = calculate_yoy_change(raw_numeric) if not raw_numeric.empty else pd.DataFrame()

    return {
        "raw_data": raw_numeric,
        "extra_data": extra_df,
        "mom_data": mom,
        "mom_change": mom_change,
        "yoy_data": yoy,
        "yoy_change": yoy_change,
        "load_info": {
            "loaded": True,
            "load_time": datetime.now(),
            "start_date": raw_numeric.index.min().strftime("%Y-%m-%d") if not raw_numeric.empty else None,
            "series_count": raw_numeric.shape[1],
            "data_points": raw_numeric.shape[0],
            "source": "CSV (cache)",
        },
    }


def _sanitize_plotly_figure(fig):
    if fig is None or not hasattr(fig, "layout"):
        return fig

    if hasattr(fig.layout, "title"):
        text = getattr(fig.layout.title, "text", None)
        if text is None or str(text).strip().lower() == "undefined":
            fig.layout.title.text = ""

    for axis_name in ("yaxis", "yaxis2"):
        axis = getattr(fig.layout, axis_name, None)
        if axis is not None and hasattr(axis, "title"):
            text = getattr(axis.title, "text", None)
            if text is None or str(text).strip().lower() == "undefined":
                axis.title.text = ""

    annotations = []
    for ann in getattr(fig.layout, "annotations", []):
        text = getattr(ann, "text", None)
        if text is None or str(text).strip().lower() != "undefined":
            annotations.append(ann)
    if annotations or getattr(fig.layout, "annotations", None):
        fig.update_layout(annotations=annotations)
    return fig


def _data_dict_ready(data_dict: dict[str, Any] | None) -> bool:
    if not data_dict or "raw_data" not in data_dict:
        return False
    raw = data_dict.get("raw_data")
    return isinstance(raw, pd.DataFrame) and not raw.empty


def _apply_rename_map(data_dict: dict[str, Any], rename_map: dict[str, str] | None) -> dict[str, Any]:
    if not rename_map:
        return data_dict
    for key, df in list(data_dict.items()):
        if isinstance(df, pd.DataFrame):
            renamed = df.rename(columns=lambda c: rename_map.get(c, c))
            if key != "extra_data":
                numeric_cols = renamed.columns.difference(["period", "detailed_period"])
                renamed[numeric_cols] = renamed[numeric_cols].apply(pd.to_numeric, errors="coerce")
            data_dict[key] = renamed
    return data_dict


def _update_special_module_data(meta: dict[str, Any], data_dict: dict[str, Any]) -> dict[str, Any]:
    module = meta["module"]
    cached_df = meta.get("cached_df")

    if hasattr(module, "PHILLIPS_DATA") and isinstance(cached_df, pd.DataFrame):
        df_cached = cached_df.copy()
        numeric_cols = df_cached.columns.difference(["period", "detailed_period"])
        df_cached[numeric_cols] = df_cached[numeric_cols].apply(pd.to_numeric, errors="coerce")
        module.PHILLIPS_DATA["processed_data"] = df_cached
        module.PHILLIPS_DATA["combined_data"] = df_cached
        module.PHILLIPS_DATA["load_info"] = {
            "loaded": True,
            "load_time": datetime.now(),
            "start_date": df_cached.index.min().strftime("%Y-%m-%d") if not df_cached.empty else None,
            "series_count": df_cached.shape[1],
            "data_points": df_cached.shape[0],
            "source": "CSV (cache)",
        }
        latest_values = {}
        for col in df_cached.columns:
            if col not in ["period", "detailed_period"] and not df_cached[col].empty:
                latest_values[col] = {
                    "value": df_cached[col].iloc[-1],
                    "date": df_cached.index[-1].strftime("%Y-%m"),
                }
        module.PHILLIPS_DATA["latest_values"] = latest_values

    if hasattr(module, "BEVERIDGE_DATA") and isinstance(cached_df, pd.DataFrame):
        df_cached = cached_df.copy()
        numeric_cols = df_cached.columns.difference(["period", "detailed_period"])
        df_cached[numeric_cols] = df_cached[numeric_cols].apply(pd.to_numeric, errors="coerce")
        module.BEVERIDGE_DATA["raw_data"] = df_cached
        module.BEVERIDGE_DATA["combined_data"] = df_cached.dropna()
        module.BEVERIDGE_DATA["load_info"] = {
            "loaded": True,
            "load_time": datetime.now(),
            "start_date": df_cached.index.min().strftime("%Y-%m-%d") if not df_cached.empty else None,
            "series_count": df_cached.shape[1],
            "data_points": df_cached.shape[0],
            "source": "CSV (cache)",
        }
        module.BEVERIDGE_DATA["latest_values"] = {
            col: {"value": df_cached[col].iloc[-1], "date": df_cached.index[-1].strftime("%Y-%m")}
            for col in df_cached.columns
            if not df_cached[col].empty
        }

    return data_dict

def discover_modules() -> list[dict[str, Any]]:
    module_dir = Path(__file__).parent
    exclude = {
        "us_eco_utils",
        "us_eco_dashboard",
        "us_eco_dashboard_dash",
        "run_batch",
        "cpi_complete_all_series",
    }
    modules: list[dict[str, Any]] = []
    for path in sorted(module_dir.glob("*.py")):
        stem = path.stem
        if stem.startswith("__") or stem in exclude or "dashboard" in stem:
            continue
        modules.append({"stem": stem, "file_path": path})
    return modules


def _restore_module_loaders(module) -> None:
    real_loader = getattr(utils_module, "load_economic_data", None)
    if callable(real_loader):
        setattr(module, "load_economic_data", real_loader)
    real_group_loader = getattr(utils_module, "load_economic_data_grouped", None)
    if callable(real_group_loader):
        setattr(module, "load_economic_data_grouped", real_group_loader)


def load_module_from_path(name: str, file_path: Path, use_stub: bool = True):
    if name in sys.modules and use_stub:
        module_cached = sys.modules[name]
        _restore_module_loaders(module_cached)
        return module_cached
    module_dir = os.path.dirname(file_path)
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)
    spec = importlib.util.spec_from_file_location(name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"spec not found for {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    original_load = getattr(utils_module, "load_economic_data", None)
    original_group = getattr(utils_module, "load_economic_data_grouped", None)

    def stub_load(series_dict, data_source="BLS", csv_file_path=None, **kwargs):
        cached = load_cached_dataframe(csv_file_path) if csv_file_path else None
        if cached is not None:
            return build_data_dict_from_raw(cached)
        return {
            "raw_data": pd.DataFrame(),
            "mom_data": pd.DataFrame(),
            "mom_change": pd.DataFrame(),
            "yoy_data": pd.DataFrame(),
            "yoy_change": pd.DataFrame(),
            "load_info": {
                "loaded": False,
                "load_time": datetime.now(),
                "start_date": None,
                "series_count": 0,
                "data_points": 0,
                "source": "stub",
            },
        }

    def stub_group(series_groups, data_source="FRED", csv_file_path=None, **kwargs):
        return {}

    if use_stub and original_load is not None:
        utils_module.load_economic_data = stub_load  # type: ignore[assignment]
    if use_stub and original_group is not None:
        utils_module.load_economic_data_grouped = stub_group  # type: ignore[assignment]

    plotly_show_original = None
    plotly_io_show_original = None
    plotly_io_module = None
    mpl_pyplot = None
    mpl_pyplot_show_original = None
    mpl_figure_cls = None
    mpl_figure_show_original = None

    def _silent_plotly_show(self, *args, **kwargs):
        return self

    def _silent_callable(*args, **kwargs):
        return None

    try:
        plotly_show_original = go.Figure.show  # type: ignore[attr-defined]
        go.Figure.show = _silent_plotly_show  # type: ignore[assignment]
    except AttributeError:
        plotly_show_original = None

    try:
        import plotly.io as pio  # type: ignore

        plotly_io_module = pio
        plotly_io_show_original = pio.show  # type: ignore[attr-defined]
        pio.show = _silent_callable  # type: ignore[assignment]
    except Exception:
        plotly_io_module = None
        plotly_io_show_original = None

    try:
        import matplotlib.pyplot as plt  # type: ignore
        from matplotlib.figure import Figure as MPLFigure  # type: ignore

        mpl_pyplot = plt
        mpl_pyplot_show_original = plt.show  # type: ignore[attr-defined]
        plt.show = _silent_callable  # type: ignore[assignment]

        mpl_figure_cls = MPLFigure
        mpl_figure_show_original = MPLFigure.show  # type: ignore[attr-defined]
        MPLFigure.show = _silent_callable  # type: ignore[assignment]
    except Exception:
        mpl_pyplot = None
        mpl_pyplot_show_original = None
        mpl_figure_cls = None
        mpl_figure_show_original = None

    try:
        spec.loader.exec_module(module)  # type: ignore[arg-type]
    except Exception as exc:
        print(f"Module load error {file_path}: {exc}")
    finally:
        if plotly_show_original is not None:
            go.Figure.show = plotly_show_original  # type: ignore[assignment]
        if plotly_io_module is not None and plotly_io_show_original is not None:
            plotly_io_module.show = plotly_io_show_original  # type: ignore[assignment]
        if mpl_pyplot is not None and mpl_pyplot_show_original is not None:
            mpl_pyplot.show = mpl_pyplot_show_original  # type: ignore[assignment]
        if mpl_figure_cls is not None and mpl_figure_show_original is not None:
            mpl_figure_cls.show = mpl_figure_show_original  # type: ignore[assignment]
        if use_stub and original_load is not None:
            utils_module.load_economic_data = original_load  # type: ignore[assignment]
        if use_stub and original_group is not None:
            utils_module.load_economic_data_grouped = original_group  # type: ignore[assignment]
        _restore_module_loaders(module)
    return module


def load_module_metadata(info: dict[str, Any]) -> dict[str, Any] | None:
    module_name = f"us_eco_dynamic_{info['stem']}"
    module = load_module_from_path(module_name, info["file_path"])

    load_fn_name = None
    for name, obj in inspect.getmembers(module, inspect.isfunction):
        if obj.__module__ != module.__name__:
            continue
        if name.startswith("load_") and name.endswith("_data"):
            load_fn_name = name
            break
    if load_fn_name is None:
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            if name.startswith("load_") and name.endswith("_data"):
                load_fn_name = name
                break
    if load_fn_name is None:
        return None

    data_attr_name = None
    for name in dir(module):
        if name.endswith("_DATA"):
            candidate = getattr(module, name)
            if isinstance(candidate, dict) and "raw_data" in candidate:
                data_attr_name = name
                break
    if data_attr_name is None:
        return None

    korean_name_attr = None
    for name in dir(module):
        if name.endswith("KOREAN_NAMES"):
            korean_name_attr = name
            break

    csv_path = getattr(module, "CSV_FILE_PATH", None)

    series_defs: dict[str, str] = {}
    for name in dir(module):
        if name.endswith("_SERIES"):
            candidate = getattr(module, name)
            if isinstance(candidate, dict) and candidate:
                series_defs.update(candidate)

    id_map = {v: k for k, v in series_defs.items() if isinstance(v, str)}

    data_dict = getattr(module, data_attr_name, None)
    if data_dict and id_map:
        for key, df in list(data_dict.items()):
            if isinstance(df, pd.DataFrame):
                renamed = df.rename(columns=lambda c: id_map.get(c, c))
                data_dict[key] = renamed

    doc = inspect.getdoc(module) or ""
    title = doc.splitlines()[0].strip() if doc else info["stem"].upper()
    short_title = title.split()[0]
    friendly_title = FRIENDLY_TITLES.get(info["stem"], short_title)

    return {
        "title": title,
        "short_title": short_title,
        "friendly_title": friendly_title,
        "module": module,
        "module_path": module_name,
        "load_fn_name": load_fn_name,
        "data_attr_name": data_attr_name,
        "korean_name_attr": korean_name_attr,
        "csv_path": csv_path,
        "rename_map": id_map,
        "series_defs": series_defs,
        "file_path": str(info["file_path"]),
    }


def ensure_module_data(meta: dict[str, Any]) -> dict[str, Any] | None:
    module = meta["module"]
    data_attr_name = meta["data_attr_name"]
    csv_path = meta.get("csv_path")

    data_dict = getattr(module, data_attr_name, None)
    if _data_dict_ready(data_dict):
        return data_dict

    cached_df = meta.get("cached_df")
    if cached_df is None and csv_path:
        cached = load_cached_dataframe(csv_path)
        if cached is not None:
            rename_map = meta.get("rename_map")
            cached_named = cached.rename(columns=lambda c: rename_map.get(c, c) if rename_map else c)
            numeric_cols = cached_named.columns.difference(["period", "detailed_period"])
            cached_named[numeric_cols] = cached_named[numeric_cols].apply(pd.to_numeric, errors="coerce")
            meta["cached_df"] = cached_named
        else:
            meta["cached_df"] = None

    cached_named = meta.get("cached_df")
    if cached_named is not None:
        rename_map = meta.get("rename_map")
        data_dict_from_csv = build_data_dict_from_raw(cached_named, rename_map=rename_map)
        setattr(module, data_attr_name, data_dict_from_csv)
        data_dict = data_dict_from_csv
    else:
        data_dict = getattr(module, data_attr_name, None)

    if not _data_dict_ready(data_dict):
        return None

    rename_map = meta.get("rename_map")
    data_dict = _apply_rename_map(data_dict, rename_map)
    setattr(module, data_attr_name, data_dict)

    data_dict = _update_special_module_data(meta, data_dict)

    meta["data_dict"] = data_dict
    return data_dict


def _determine_category(stem: str) -> str:
    for category, stems in CATEGORY_MAP.items():
        if stem in stems:
            return category
    return "Other"


def build_series_registry(metas: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    registry: dict[str, dict[str, Any]] = {}
    default_checked: list[str] = []

    for meta in metas:
        stem = meta.get("stem") or meta.get("module_path", "").split("_dynamic_")[-1]
        module_label = meta.get("friendly_title", stem)
        module = meta["module"]
        korean_name_attr = meta.get("korean_name_attr")
        korean_names = getattr(module, korean_name_attr, {}) if korean_name_attr else {}
        if korean_names:
            korean_names = dict(korean_names)
        data_dict = meta.get("data_dict") or ensure_module_data(meta)
        if not _data_dict_ready(data_dict):
            continue

        raw_df = data_dict.get("raw_data")
        if raw_df is None or raw_df.empty:
            continue

        category = _determine_category(stem)

        series_defs = meta.get("series_defs", {}) or {}
        rename_map = meta.get("rename_map") or {}

        if korean_names and rename_map:
            for raw_key, alias_key in rename_map.items():
                if alias_key not in korean_names and raw_key in korean_names:
                    korean_names[alias_key] = korean_names[raw_key]

        for series in raw_df.columns:
            resolved_key = rename_map.get(series, series)
            column_candidates = {series, resolved_key}
            available_types: list[str] = []
            for dtype_key, _ in STANDARD_DATA_KEYS:
                df_candidate = data_dict.get(dtype_key)
                if isinstance(df_candidate, pd.DataFrame) and any(col in df_candidate.columns for col in column_candidates):
                    available_types.append(dtype_key)
            if not available_types:
                continue

            base_label = (
                korean_names.get(resolved_key)
                or korean_names.get(series)
                or resolved_key
            )
            display_label = f"{module_label} - {base_label}"
            series_key = f"{stem}::{series}"

            series_def = series_defs.get(series)
            if not series_def and series_defs:
                series_def = series_defs.get(resolved_key, {})
            unit = ""
            if isinstance(series_def, dict):
                unit = str(series_def.get("unit", "")).strip()

            registry[series_key] = {
                "key": series_key,
                "stem": stem,
                "category": category,
                "module_label": module_label,
                "series_name": series,
                "series_alias": resolved_key,
                "series_label": base_label,
                "display_label": display_label,
                "available_types": available_types,
                "unit": unit,
                "meta": meta,
            }

            if not default_checked and stem == DEFAULT_MODULE_FOR_SELECTION:
                default_checked.append(series_key)

    return registry, default_checked

def compute_axis_label(series_infos: list[dict[str, Any]], dtype_map: dict[str, str]) -> str:
    units: list[str] = []
    for info in series_infos:
        dtype_key = dtype_map.get(info["key"]) or ""
        if dtype_key in {"mom_data", "yoy_data"}:
            units.append("%")
        else:
            unit = info.get("unit")
            if unit:
                units.append(unit)
    clean = sorted({u for u in units if u})
    return ", ".join(clean)


def build_combined_dataframe(selected_infos: list[dict[str, Any]], dtype_map: dict[str, str]) -> tuple[pd.DataFrame, dict[str, str]]:
    frames: list[pd.Series] = []
    label_dtype: dict[str, str] = {}
    for info in selected_infos:
        meta = info["meta"]
        data_dict = meta.get("data_dict") or ensure_module_data(meta)
        if not _data_dict_ready(data_dict):
            continue
        dtype_key = dtype_map.get(info["key"])
        if not dtype_key:
            continue
        source_df = data_dict.get(dtype_key)
        if not isinstance(source_df, pd.DataFrame):
            continue
        series_name = info["series_name"]
        if series_name not in source_df.columns:
            continue
        series_data = pd.to_numeric(source_df[series_name], errors="coerce").rename(info["display_label"])
        frames.append(series_data)
        label_dtype[info["display_label"]] = dtype_key
    if not frames:
        return pd.DataFrame(), label_dtype
    combined = pd.concat(frames, axis=1, join="outer").sort_index()
    return combined, label_dtype


def _dtype_key_to_axis(dtype_key: str | None) -> str:
    if not dtype_key:
        return ""
    return dtype_key[:-5] if dtype_key.endswith("_data") else dtype_key


def _default_dtype_for_series(available_types: list[str]) -> str:
    if not available_types:
        return ""
    for dtype_key, _ in STANDARD_DATA_KEYS:
        if dtype_key in available_types:
            return dtype_key
    return available_types[0]


def _create_single_axis_line_chart(
    df: pd.DataFrame,
    axis_title: str,
    chart_width: int,
    chart_height: int,
    zero_line: bool,
    connect_map: dict[str, str] | None = None,
) -> go.Figure | None:
    if df is None or df.empty:
        return None

    fig = go.Figure()
    for idx, column in enumerate(df.columns):
        connect_flag = True
        if connect_map is not None and connect_map.get(column) == "week":
            connect_flag = False
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[column],
                name=column,
                line=dict(color=get_kpds_color(idx), dash="solid"),
                connectgaps=connect_flag,
            )
        )

    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        width=chart_width,
        height=chart_height,
        font=dict(family="NanumGothic", size=FONT_SIZE_GENERAL, color="black"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(family="NanumGothic", size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=60, r=40, t=40, b=60),
    )

    fig.update_xaxes(
        title_text="",
        showline=True,
        linewidth=1.3,
        linecolor="lightgrey",
        tickwidth=1.3,
        tickcolor="lightgrey",
        ticks="outside",
        showgrid=False,
    )

    fig.update_yaxes(
        title_text="",
        tickformat=",",
        showline=False,
        tickcolor="white",
        showgrid=False,
    )

    fig = format_date_ticks(fig, "%b-%y", "auto", df.index)

    if zero_line:
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    cleaned_title = (axis_title or "").strip()
    if cleaned_title:
        _apply_custom_axis_title(fig, "y", cleaned_title)

    return _sanitize_plotly_figure(fig)


def _create_horizontal_bar_chart_simple(
    latest_values: pd.Series,
    axis_title: str,
    chart_width: int,
    chart_height: int,
) -> go.Figure | None:
    if latest_values is None or latest_values.empty:
        return None

    series_sorted = latest_values.dropna().sort_values()
    if series_sorted.empty:
        return None

    colors = [get_kpds_color(idx) for idx in range(len(series_sorted))]
    texts = [f"{value:,.2f}" for value in series_sorted.values]

    fig = go.Figure(
        go.Bar(
            y=series_sorted.index.tolist(),
            x=series_sorted.values.tolist(),
            orientation="h",
            marker_color=colors,
            text=texts,
            textposition="outside",
            hovertemplate="%{y}: %{x:,.2f}<extra></extra>",
            showlegend=False,
        )
    )

    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        width=chart_width,
        height=max(chart_height, 300),
        font=dict(family="NanumGothic", size=FONT_SIZE_GENERAL, color="black"),
        margin=dict(l=120, r=40, t=40, b=60),
    )

    fig.add_vline(x=0, line_width=1, line_color="black", opacity=0.5)

    cleaned_title = (axis_title or "").strip()
    if cleaned_title:
        _apply_custom_axis_title(fig, "x", cleaned_title)

    return _sanitize_plotly_figure(fig)


def _create_vertical_bar_chart_simple(
    df: pd.DataFrame,
    axis_title: str,
    chart_width: int,
    chart_height: int,
) -> go.Figure | None:
    if df is None or df.empty:
        return None

    fig = go.Figure()
    for idx, column in enumerate(df.columns):
        fig.add_trace(
            go.Bar(
                x=df.index,
                y=df[column],
                name=column,
                marker_color=get_kpds_color(idx),
            )
        )

    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        width=chart_width,
        height=chart_height,
        font=dict(family="NanumGothic", size=FONT_SIZE_GENERAL, color="black"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(family="NanumGothic", size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=60, r=40, t=40, b=60),
        barmode="group",
    )

    fig.update_xaxes(
        title_text="",
        showline=True,
        linewidth=1.3,
        linecolor="lightgrey",
        tickwidth=1.3,
        tickcolor="lightgrey",
        ticks="outside",
        showgrid=False,
    )

    fig.update_yaxes(
        title_text="",
        tickformat=",",
        showline=False,
        tickcolor="white",
        showgrid=False,
    )

    fig = format_date_ticks(fig, "%b-%y", "auto", df.index)
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    cleaned_title = (axis_title or "").strip()
    if cleaned_title:
        _apply_custom_axis_title(fig, "y", cleaned_title)

    return _sanitize_plotly_figure(fig)


def _normalize_series(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.dropna()


def _infer_series_period(series: pd.Series) -> str:
    if series is None or series.empty:
        return "month"
    if len(series) < 3:
        return "month"
    diffs = series.index.to_series().diff().dropna()
    if diffs.empty:
        return "month"
    median_delta = diffs.median()
    if pd.isna(median_delta):
        return "month"
    delta_days = median_delta / pd.Timedelta(days=1)
    if delta_days <= 9:
        return "week"
    return "month"


def _make_periodic_five_year_format(
    series: pd.Series, history_years: int = 5
) -> tuple[pd.DataFrame | None, str]:
    normalized = _normalize_series(series)
    if normalized.empty:
        return None, "month"
    normalized.index = pd.to_datetime(normalized.index)
    normalized = normalized[~normalized.index.duplicated(keep="last")].sort_index()

    period_type = _infer_series_period(normalized)

    last_year = int(normalized.index.year.max())
    earliest = int(normalized.index.year.min())
    start_year = max(earliest, last_year - history_years + 1)
    selected = normalized[normalized.index.year >= start_year]
    if selected.empty:
        return None, period_type

    if period_type == "week":
        temp = selected.to_frame("value")
        temp["year"] = temp.index.year
        iso = temp.index.isocalendar()
        temp["period"] = iso.week.astype(int)
        temp = temp.groupby(["year", "period"], as_index=False)["value"].mean()
        pivot = temp.pivot(index="period", columns="year", values="value").sort_index()
        pivot = pivot.reindex(range(1, 54))
    else:
        temp = selected.to_frame("value")
        temp["year"] = temp.index.year
        temp["period"] = temp.index.month
        temp = temp.groupby(["year", "period"], as_index=False)["value"].mean()
        pivot = temp.pivot(index="period", columns="year", values="value").sort_index()
        pivot = pivot.reindex(range(1, 13))

    pivot.columns = [str(col) for col in pivot.columns]
    numeric = pivot.astype(float)
    stats = pd.DataFrame(index=pivot.index)
    stats["Avg"] = numeric.mean(axis=1, skipna=True)
    stats["Min"] = numeric.min(axis=1, skipna=True)
    stats["Max"] = numeric.max(axis=1, skipna=True)
    return pd.concat([numeric, stats], axis=1), period_type


def _compute_axis_range(series_list: list[pd.Series]) -> list[float] | None:
    combined = pd.concat([s.dropna() for s in series_list], axis=0) if series_list else pd.Series(dtype=float)
    if combined.empty:
        return None
    min_val = combined.min()
    max_val = combined.max()
    if pd.isna(min_val) or pd.isna(max_val):
        return None
    if min_val == max_val:
        pad = abs(min_val) * 0.05 if min_val != 0 else 1.0
        return [min_val - pad, max_val + pad]
    span = max_val - min_val
    pad = span * 0.05
    return [min_val - pad, max_val + pad]


def _axis_title_for(data_type: str, keys: list[str], series_defs: dict[str, dict]) -> str:
    if not keys:
        return ""
    if data_type in {"mom", "yoy"}:
        return "%"
    units: list[str] = []
    for key in keys:
        spec = series_defs.get(key, {}) if series_defs else {}
        unit_code = str(spec.get("unit", "")).upper().strip()
        if unit_code:
            units.append(unit_code)
    return ", ".join(sorted(set(units)))


def _apply_custom_axis_title(fig: go.Figure | None, axis: str, title_text: str) -> None:
    if fig is None or not title_text:
        return
    cleaned = title_text.strip()
    if not cleaned:
        return
    axis = axis.lower()
    if axis == "x":
        fig.update_xaxes(title_text=cleaned)
        return

    existing_annotations = list(getattr(fig.layout, "annotations", []) or [])
    new_annotations = []
    found = False
    for ann in existing_annotations:
        ann_dict = ann.to_plotly_json() if hasattr(ann, "to_plotly_json") else dict(ann)
        xref = ann_dict.get("xref")
        yref = ann_dict.get("yref")
        x_val = ann_dict.get("x")
        y_val = ann_dict.get("y")
        is_left_axis = (
            xref == "paper"
            and yref == "paper"
            and y_val is not None
            and abs(float(y_val) - 1.1) < 0.25
            and (x_val is None or float(x_val) <= 0.5)
        )
        if axis == "y" and is_left_axis:
            ann_dict["text"] = cleaned
            new_annotations.append(ann_dict)
            found = True
        else:
            new_annotations.append(ann_dict)

    if axis == "y" and not found:
        new_annotations.append(
            dict(
                text=cleaned,
                xref="paper",
                yref="paper",
                x=calculate_title_position(cleaned, "left"),
                y=1.1,
                showarrow=False,
                font=dict(family="NanumGothic", size=FONT_SIZE_ANNOTATION, color="black"),
                align="left",
            )
        )

    fig.update_layout(annotations=new_annotations)


def _create_dual_axis_chart(
    df: pd.DataFrame,
    axis_allocation: dict[str, list[str]],
    label_map: dict[str, str],
    data_type: str,
    series_defs: dict[str, dict],
    chart_width: int,
    chart_height: int,
    left_title_offset: float = 0.0,
    right_title_offset: float = 0.0,
    left_axis_data_type: str | None = None,
    right_axis_data_type: str | None = None,
    left_title_override: str | None = None,
    right_title_override: str | None = None,
    left_axis_range_override: list[float] | None = None,
    right_axis_range_override: list[float] | None = None,
    connect_map: dict[str, str] | None = None,
):
    left_cols = [col for col in axis_allocation.get("left", []) if col in df.columns]
    right_cols = [col for col in axis_allocation.get("right", []) if col in df.columns and col not in left_cols]
    if not left_cols or not right_cols:
        return None
    working_df = df[left_cols + right_cols].apply(pd.to_numeric, errors="coerce").dropna(how="all")
    if working_df.empty:
        return None

    left_dtype = left_axis_data_type or data_type
    right_dtype = right_axis_data_type or data_type
    left_title_default = _axis_title_for(left_dtype, left_cols, series_defs)
    right_title_default = _axis_title_for(right_dtype, right_cols, series_defs)
    left_title = left_title_default if left_title_override is None else left_title_override
    right_title = right_title_default if right_title_override is None else right_title_override
    left_range = _compute_axis_range([working_df[col] for col in left_cols])
    right_range = _compute_axis_range([working_df[col] for col in right_cols])

    if left_axis_range_override and len(left_axis_range_override) == 2:
        if left_axis_range_override[0] < left_axis_range_override[1]:
            left_range = list(left_axis_range_override)
    if right_axis_range_override and len(right_axis_range_override) == 2:
        if right_axis_range_override[0] < right_axis_range_override[1]:
            right_range = list(right_axis_range_override)

    zero_line_needed = False
    if left_range and left_range[0] <= 0 <= left_range[1]:
        zero_line_needed = True
    if right_range and right_range[0] <= 0 <= right_range[1]:
        zero_line_needed = True

    font_family = "NanumGothic"
    fig = go.Figure()

    for idx, col in enumerate(left_cols):
        connect_flag = True
        if connect_map is not None and connect_map.get(label_map.get(col, col)) == "week":
            connect_flag = False
        fig.add_trace(
            go.Scatter(
                x=working_df.index,
                y=working_df[col],
                name=label_map.get(col, col),
                line=dict(color=get_kpds_color(idx), dash="solid"),
                yaxis="y",
                connectgaps=connect_flag,
            )
        )

    for idx, col in enumerate(right_cols):
        color_idx = len(left_cols) + idx
        connect_flag = True
        if connect_map is not None and connect_map.get(label_map.get(col, col)) == "week":
            connect_flag = False
        fig.add_trace(
            go.Scatter(
                x=working_df.index,
                y=working_df[col],
                name=label_map.get(col, col),
                line=dict(color=get_kpds_color(color_idx), dash="solid"),
                yaxis="y2",
                connectgaps=connect_flag,
            )
        )

    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        width=chart_width,
        height=chart_height,
        font=dict(family=font_family, size=FONT_SIZE_GENERAL, color="black"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(family=font_family, size=FONT_SIZE_LEGEND),
            borderwidth=0,
            bordercolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=60, r=60, t=40, b=60),
    )

    fig.update_xaxes(
        title_text="",
        showline=True,
        linewidth=1.3,
        linecolor="lightgrey",
        tickwidth=1.3,
        tickcolor="lightgrey",
        ticks="outside",
        showgrid=False,
    )

    fig.update_yaxes(
        range=left_range,
        tickformat=",",
        showline=False,
        tickcolor="white",
        showgrid=False,
        title_text="",
        zeroline=False,
    )

    fig.update_layout(
        yaxis2=dict(
            range=right_range,
            tickformat=",",
            showline=False,
            tickcolor="white",
            anchor="x",
            overlaying="y",
            side="right",
            showgrid=False,
            title_text="",
            zeroline=False,
        )
    )

    fig = format_date_ticks(fig, "%b-%y", "auto", working_df.index)

    if zero_line_needed:
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)

    if left_title:
        pos = (calculate_title_position(left_title, "left") or -0.03) + left_title_offset
        fig.add_annotation(
            text=left_title,
            xref="paper",
            yref="paper",
            x=pos,
            y=1.1,
            showarrow=False,
            xanchor="left",
            yanchor="bottom",
            font=dict(family=font_family, size=FONT_SIZE_ANNOTATION, color="black"),
        )

    if right_title:
        pos = (calculate_title_position(right_title, "right") or 1.03) + right_title_offset
        fig.add_annotation(
            text=right_title,
            xref="paper",
            yref="paper",
            x=pos,
            y=1.1,
            showarrow=False,
            xanchor="right",
            yanchor="bottom",
            font=dict(family=font_family, size=FONT_SIZE_ANNOTATION, color="black"),
        )

    return _sanitize_plotly_figure(fig)


def _create_five_year_chart(
    series: pd.Series,
    series_name: str,
    unit_label: str,
    recent_years: int,
    chart_width: int,
    chart_height: int,
):
    history_years = max(recent_years, 5)
    formatted, period_type = _make_periodic_five_year_format(series, history_years)
    if formatted is None or formatted.dropna(how="all").empty:
        return None, None, period_type
    fig = create_five_year_comparison_chart(
        formatted,
        title=series_name,
        y_title=unit_label,
        x_axis_type="week" if period_type == "week" else "month",
        recent_years=recent_years,
    )
    if fig is not None:
        fig.update_layout(width=chart_width, height=chart_height)
    return _sanitize_plotly_figure(fig), formatted, period_type


def _make_empty_figure(message: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        width=int(DEFAULT_CHART_WIDTH_CM * PX_PER_CM),
        height=int(DEFAULT_CHART_HEIGHT_CM * PX_PER_CM),
        margin=dict(l=40, r=40, t=40, b=40),
    )
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=14, color="#444"),
    )
    return fig

FREQUENCY_OPTIONS = [
    {"label": "Original", "value": "original"},
    {"label": "Weekly -> Month end", "value": "weekly_to_month_end"},
]

DATA_TYPE_LABELS = {key: label for key, label in STANDARD_DATA_KEYS}
CHART_TYPE_OPTIONS = [{"label": label, "value": code} for label, code in CHART_TYPE_LABELS.items()]

KOREAN_FREQ_ORIGINAL = "\uc6d0\ubcf8 \uc720\uc9c0"
KOREAN_FREQ_MONTH_END = "\uc8fc\uac04 \u2192 \uc6d0\ub9d0 \ubcc0\ud658"


def _get_preset_options(presets: dict[str, Any]) -> list[dict[str, str]]:
    return [{"label": name, "value": name} for name in sorted(presets.keys())]


def _normalize_frequency_option(value: str | None) -> str:
    if not value:
        return "original"
    if value in ("original", "weekly_to_month_end"):
        return value
    if value == KOREAN_FREQ_ORIGINAL:
        return "original"
    if value == KOREAN_FREQ_MONTH_END:
        return "weekly_to_month_end"
    text = str(value).lower()
    if "week" in text:
        return "weekly_to_month_end"
    return "original"


def _series_options_from_registry(registry: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    entries = sorted(
        registry.values(),
        key=lambda info: (
            info.get("category", ""),
            info.get("module_label", ""),
            info.get("series_label", ""),
        ),
    )
    options: list[dict[str, str]] = []
    for info in entries:
        category = info.get("category", "Other")
        module_label = info.get("module_label", "")
        series_label = info.get("series_label", "")
        options.append(
            {
                "label": f"[{category}] {module_label} - {series_label}",
                "value": info["key"],
            }
        )
    return options


def _collect_selected_infos(
    selected_keys: list[str] | None,
    registry: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    infos: list[dict[str, Any]] = []
    for key in selected_keys or []:
        info = registry.get(key)
        if info:
            infos.append(dict(info))
    return infos



def _build_long_dataframe(selected_infos: list[dict[str, Any]], dtype_key: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for info in selected_infos:
        meta = info["meta"]
        data_dict = meta.get("data_dict") or ensure_module_data(meta)
        if not _data_dict_ready(data_dict):
            continue
        source_df = data_dict.get(dtype_key)
        if not isinstance(source_df, pd.DataFrame):
            continue
        series_name = info["series_name"]
        if series_name not in source_df.columns:
            continue
        series_data = pd.to_numeric(source_df[series_name], errors="coerce")
        index = source_df.index
        if not isinstance(index, pd.DatetimeIndex):
            index = pd.to_datetime(index, errors="coerce")
        frame = pd.DataFrame(
            {
                "date": index,
                "value": series_data.values,
                "series": series_name,
                "series_label": info.get("series_label", series_name),
                "display_label": info.get("display_label", series_name),
                "module": info.get("module_label", info.get("stem", "")),
                "category": info.get("category", ""),
                "data_type": dtype_key.replace("_data", ""),
                "unit": info.get("unit", ""),
            }
        )
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["date"])
    return combined


def _bi_field_panel(df: pd.DataFrame) -> html.Div:
    if df is None or df.empty:
        return html.Div("No fields available.")
    rows = [{"field": col, "dtype": str(df[col].dtype)} for col in df.columns]
    return html.Div(
        dash_table.DataTable(
            data=rows,
            columns=[
                {"name": "Field", "id": "field"},
                {"name": "Type", "id": "dtype"},
            ],
            page_size=10,
            style_table={"overflowY": "auto", "maxHeight": "260px"},
            style_cell={"textAlign": "left", "padding": "6px"},
            style_header={"fontWeight": "600"},
        )
    )


def _bi_field_options(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    if df is None or df.empty:
        return [], []
    fields = [col for col in df.columns]
    numeric_fields = [
        col for col in fields if pd.api.types.is_numeric_dtype(df[col])
    ]
    return fields, numeric_fields


def _build_table_rows(
    selected_infos: list[dict[str, Any]],
    settings: dict[str, dict[str, str]] | None,
    global_dtype: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for info in selected_infos:
        key = info["key"]
        stored = settings.get(key, {}) if settings else {}
        base_label = info.get("display_label", "")
        label = stored.get("label") or base_label
        stored_dtype = stored.get("dtype") or ""
        override_flag = stored.get("dtype_override")
        if override_flag is None:
            override_flag = bool(stored_dtype and stored_dtype != global_dtype)
        if override_flag and stored_dtype:
            dtype = stored_dtype
        else:
            dtype = global_dtype
        if dtype not in info.get("available_types", []):
            dtype = _default_dtype_for_series(info.get("available_types", []))
        axis = stored.get("axis") or "left"
        rows.append(
            {
                "key": key,
                "series": base_label,
                "label": str(label) if label is not None else base_label,
                "dtype": dtype,
                "axis": axis,
            }
        )
    return rows


def _settings_from_rows(
    rows: list[dict[str, Any]] | None,
    existing: dict[str, dict[str, str]] | None = None,
    global_dtype: str | None = None,
) -> dict[str, dict[str, str]]:
    settings = dict(existing or {})
    normalized_global = str(global_dtype).strip() if global_dtype else None
    for row in rows or []:
        key = row.get("key")
        if not key:
            continue
        label = str(row.get("label") or "").strip()
        dtype = str(row.get("dtype") or "").strip()
        dtype_override = False
        if normalized_global:
            dtype_override = dtype != "" and dtype != normalized_global
            if not dtype_override:
                dtype = ""
        axis = str(row.get("axis") or "left").strip() or "left"
        settings[key] = {
            "label": label,
            "dtype": dtype,
            "axis": axis,
            "dtype_override": dtype_override,
        }
    return settings


def _prepare_data_table(
    df: pd.DataFrame | None,
    max_rows: int = 200,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if df is None or df.empty:
        return [], []
    display_df = df.copy()
    if isinstance(display_df.index, pd.DatetimeIndex):
        display_df = display_df.sort_index()
    if isinstance(display_df.index, pd.DatetimeIndex):
        display_df.insert(0, "Date", display_df.index.strftime("%Y-%m-%d"))
    else:
        display_df.insert(0, "Index", display_df.index.astype(str))
    display_df = display_df.tail(max_rows).reset_index(drop=True)
    columns = [{"name": col, "id": col} for col in display_df.columns]
    return display_df.to_dict("records"), columns


def _extract_axis_range(
    enabled: bool,
    min_val: Any,
    max_val: Any,
) -> list[float] | None:
    if min_val is None or max_val is None:
        return None
    try:
        min_f = float(min_val)
        max_f = float(max_val)
    except (TypeError, ValueError):
        return None
    if min_f >= max_f:
        return None
    return [min_f, max_f]


def _build_figure(
    selected_infos: list[dict[str, Any]],
    settings: dict[str, dict[str, str]] | None,
    chart_type: str,
    global_dtype: str,
    per_series_override: bool,
    frequency_option: str,
    chart_width_cm: float | None,
    chart_height_cm: float | None,
    axis_titles: dict[str, str],
    axis_offsets: dict[str, float],
    manual_ranges: dict[str, list[float] | None],
    five_year_recent: int | None,
    zero_line: bool,
) -> tuple[go.Figure, pd.DataFrame | None, list[str]]:
    messages: list[str] = []
    settings = settings or {}

    if not selected_infos:
        return _make_empty_figure("Select one or more series."), None, messages

    dtype_map: dict[str, str] = {}
    for info in selected_infos:
        key = info["key"]
        available = info.get("available_types", [])
        dtype_choice = settings.get(key, {}).get("dtype") if per_series_override else None
        if not dtype_choice:
            dtype_choice = global_dtype
        if dtype_choice not in available:
            dtype_choice = _default_dtype_for_series(available)
        if not dtype_choice:
            continue
        dtype_map[key] = dtype_choice

    if not dtype_map:
        return _make_empty_figure("No valid data types for selected series."), None, messages

    combined_df, _ = build_combined_dataframe(selected_infos, dtype_map)
    if combined_df.empty:
        return _make_empty_figure("No data found for selected series."), None, messages

    label_map: dict[str, str] = {}
    for info in selected_infos:
        key = info["key"]
        base_label = info.get("display_label", "")
        custom_label = settings.get(key, {}).get("label")
        if isinstance(custom_label, str):
            custom_label = custom_label.strip()
        if not custom_label:
            custom_label = base_label
        label_map[base_label] = custom_label
        info["effective_label"] = custom_label

    combined_df = combined_df.rename(columns=label_map)
    combined_df = combined_df.dropna(how="all").dropna(axis=1, how="all")
    if combined_df.empty:
        return _make_empty_figure("No data after label mapping."), None, messages

    if frequency_option == "weekly_to_month_end":
        raw_map = {info["key"]: "raw_data" for info in selected_infos}
        raw_df, _ = build_combined_dataframe(selected_infos, raw_map)
        if raw_df is not None and not raw_df.empty:
            raw_df = raw_df.rename(columns=label_map)
            raw_df = raw_df.resample("M").last()
            raw_df = raw_df.dropna(how="all").dropna(axis=1, how="all")
            if raw_df.empty:
                return _make_empty_figure("No data after monthly conversion."), None, messages
            mom_df = calculate_mom_percent(raw_df)
            mom_change_df = calculate_mom_change(raw_df)
            yoy_df = calculate_yoy_percent(raw_df)
            yoy_change_df = calculate_yoy_change(raw_df)
            converted = pd.DataFrame(index=raw_df.index)
            for info in selected_infos:
                label = info.get("effective_label", info.get("display_label", ""))
                dtype_choice = dtype_map.get(info["key"])
                if dtype_choice == "raw_data":
                    converted[label] = raw_df.get(label)
                elif dtype_choice == "mom_data":
                    converted[label] = mom_df.get(label)
                elif dtype_choice == "mom_change":
                    converted[label] = mom_change_df.get(label)
                elif dtype_choice == "yoy_data":
                    converted[label] = yoy_df.get(label)
                elif dtype_choice == "yoy_change":
                    converted[label] = yoy_change_df.get(label)
            combined_df = converted.dropna(how="all").dropna(axis=1, how="all")
            if combined_df.empty:
                return _make_empty_figure("No data after monthly conversion."), None, messages
        else:
            messages.append("Monthly conversion skipped; raw data is empty.")

    frequency_map: dict[str, str] = {}
    for label in combined_df.columns:
        series = combined_df[label]
        if series is None:
            continue
        frequency_map[label] = _infer_series_period(series.dropna())

    axis_label_default = compute_axis_label(selected_infos, dtype_map)
    if not axis_label_default:
        dtype_set = {dtype_map.get(info["key"]) for info in selected_infos if dtype_map.get(info["key"])}
        if len(dtype_set) == 1:
            axis_label_default = DATA_TYPE_LABELS.get(next(iter(dtype_set)), "")

    chart_width = int((chart_width_cm or DEFAULT_CHART_WIDTH_CM) * PX_PER_CM)
    chart_height = int((chart_height_cm or DEFAULT_CHART_HEIGHT_CM) * PX_PER_CM)

    table_df = combined_df.copy()
    fig: go.Figure | None = None

    single_axis_title = (axis_titles.get("single") or axis_label_default or "").strip()
    left_axis_title = (axis_titles.get("left") or "").strip()
    right_axis_title = (axis_titles.get("right") or "").strip()

    if chart_type == "single_line":
        if combined_df.shape[1] > 1:
            messages.append("Single line chart uses the first series only.")
        if not combined_df.empty:
            fig = _create_single_axis_line_chart(
                combined_df[[combined_df.columns[0]]],
                single_axis_title,
                chart_width,
                chart_height,
                zero_line,
                frequency_map,
            )
    elif chart_type == "multi_line":
        fig = _create_single_axis_line_chart(
            combined_df,
            single_axis_title,
            chart_width,
            chart_height,
            zero_line,
            frequency_map,
        )
    elif chart_type == "horizontal_bar":
        latest = combined_df.apply(lambda s: s.dropna().iloc[-1] if not s.dropna().empty else None)
        fig = _create_horizontal_bar_chart_simple(
            latest.dropna(),
            single_axis_title,
            chart_width,
            chart_height,
        )
    elif chart_type == "vertical_bar":
        fig = _create_vertical_bar_chart_simple(
            combined_df,
            single_axis_title,
            chart_width,
            chart_height,
        )
    elif chart_type == "dual_axis":
        labels = list(combined_df.columns)
        axis_allocation = {"left": [], "right": []}
        for info in selected_infos:
            label = info.get("effective_label", info.get("display_label", ""))
            axis = settings.get(info["key"], {}).get("axis", "left")
            target = "right" if axis == "right" else "left"
            axis_allocation[target].append(label)
        if not axis_allocation["left"] or not axis_allocation["right"]:
            if len(labels) >= 2:
                axis_allocation["left"] = labels[:-1]
                axis_allocation["right"] = labels[-1:]
            else:
                messages.append("Dual axis requires at least two series.")
                return _make_empty_figure("Dual axis requires at least two series."), table_df, messages
        series_type_map = {
            info.get("effective_label", info.get("display_label", "")): dtype_map.get(info["key"], "")
            for info in selected_infos
        }
        left_dtype = {series_type_map.get(label) for label in axis_allocation["left"] if series_type_map.get(label)}
        right_dtype = {series_type_map.get(label) for label in axis_allocation["right"] if series_type_map.get(label)}
        dtype_set = {series_type_map.get(label) for label in labels if series_type_map.get(label)}
        base_dtype = _dtype_key_to_axis(next(iter(dtype_set))) if len(dtype_set) == 1 else "raw"
        series_defs = {
            info.get("effective_label", info.get("display_label", "")): {"unit": info.get("unit", "")}
            for info in selected_infos
        }
        fig = _create_dual_axis_chart(
            df=combined_df,
            axis_allocation=axis_allocation,
            label_map={label: label for label in labels},
            data_type=base_dtype,
            series_defs=series_defs,
            chart_width=chart_width,
            chart_height=chart_height,
            left_title_offset=axis_offsets.get("left", 0.0),
            right_title_offset=axis_offsets.get("right", 0.0),
            left_axis_data_type=_dtype_key_to_axis(next(iter(left_dtype))) if len(left_dtype) == 1 else None,
            right_axis_data_type=_dtype_key_to_axis(next(iter(right_dtype))) if len(right_dtype) == 1 else None,
            left_title_override=left_axis_title or None,
            right_title_override=right_axis_title or None,
            left_axis_range_override=manual_ranges.get("left"),
            right_axis_range_override=manual_ranges.get("right"),
            connect_map=frequency_map,
        )
    elif chart_type == "five_year":
        if len(selected_infos) > 1:
            messages.append("Five-year chart uses the first series only.")
        info = selected_infos[0]
        if dtype_map.get(info["key"]) != "raw_data":
            messages.append("Five-year chart uses raw data.")
        meta = info["meta"]
        data_dict = meta.get("data_dict") or ensure_module_data(meta)
        if not _data_dict_ready(data_dict):
            return _make_empty_figure("Raw data not available for five-year chart."), table_df, messages
        raw_df = data_dict.get("raw_data") if isinstance(data_dict, dict) else None
        series = None
        if isinstance(raw_df, pd.DataFrame) and info["series_name"] in raw_df.columns:
            series = raw_df[info["series_name"]]
        if series is None or series.dropna().empty:
            return _make_empty_figure("Five-year chart data is empty."), table_df, messages
        label = info.get("effective_label", info.get("display_label", ""))
        unit_label = info.get("unit", "")
        recent_years = five_year_recent or 5
        fig, formatted, _ = _create_five_year_chart(
            series,
            label,
            unit_label,
            recent_years,
            chart_width,
            chart_height,
        )
        if formatted is not None:
            table_df = formatted
    else:
        return _make_empty_figure("Unsupported chart type."), table_df, messages

    if fig is None:
        return _make_empty_figure("Unable to build chart."), table_df, messages

    single_axis_range = manual_ranges.get("single")
    if single_axis_range and chart_type != "dual_axis":
        if chart_type == "horizontal_bar":
            fig.update_xaxes(range=single_axis_range)
        else:
            fig.update_yaxes(range=single_axis_range)

    return fig, table_df, messages

MODULE_INFOS = discover_modules()
MODULE_METAS: list[dict[str, Any]] = []
METADATA_WARNINGS: list[str] = []
for info in MODULE_INFOS:
    meta = load_module_metadata(info)
    if meta is None:
        METADATA_WARNINGS.append(f"Metadata load failed: {info['stem']}")
        continue
    meta["stem"] = info["stem"]
    MODULE_METAS.append(meta)

SERIES_REGISTRY, DEFAULT_SERIES_SELECTION = build_series_registry(MODULE_METAS)
SERIES_OPTIONS = _series_options_from_registry(SERIES_REGISTRY)
BI_MODULE_OPTIONS = [
    {"label": meta.get("friendly_title", meta.get("stem", "")), "value": meta.get("stem", "")}
    for meta in MODULE_METAS
    if meta.get("stem")
]
PRESET_CACHE: dict[str, Any] = load_dashboard_presets()


app = dash.Dash(__name__, assets_folder=str(Path(__file__).parent / "assets"))
app.title = "US Eco Dashboard (Dash)"

warning_block = None
if METADATA_WARNINGS:
    warning_block = html.Details(
        [
            html.Summary("Metadata warnings"),
            html.Ul([html.Li(msg) for msg in METADATA_WARNINGS]),
        ],
        className="warning-block",
    )

app.layout = html.Div(
    className="app-shell",
    children=[
        html.Div(
            className="app-header",
            children=[
                html.Div(
                    children=[
                        html.Div("KPDS Macro Lab", className="brand-kicker"),
                        html.H2("US Eco Dashboard", className="app-title"),
                        html.P(
                            "Report-ready charts with presets and fast axis tuning.",
                            className="app-subtitle",
                        ),
                    ],
                ),
            ],
        ),
        warning_block if warning_block else html.Div(),
        html.Div(id="status-message", className="status-message"),
        html.Div(
            className="app-body",
            children=[
                html.Div(
                    className="side-panel card",
                    children=[
                        html.Div("Controls", className="card-title"),
                        html.Label("Series"),
                        dcc.Dropdown(
                            id="series-select",
                            options=SERIES_OPTIONS,
                            value=DEFAULT_SERIES_SELECTION,
                            multi=True,
                            placeholder="Select series",
                        ),
                        html.Label("Default data type", style={"marginTop": "10px"}),
                        dcc.Dropdown(
                            id="global-dtype",
                            options=[
                                {"label": label, "value": key} for key, label in STANDARD_DATA_KEYS
                            ],
                            value="raw_data",
                            clearable=False,
                        ),
                        html.Div(
                            "Data type is set in the series table.",
                            className="helper-text",
                        ),
                        html.Label("Chart type", style={"marginTop": "10px"}),
                        dcc.Dropdown(
                            id="chart-type",
                            options=CHART_TYPE_OPTIONS,
                            value="multi_line",
                            clearable=False,
                        ),
                        html.Label("Frequency", style={"marginTop": "10px"}),
                        dcc.Dropdown(
                            id="frequency-option",
                            options=FREQUENCY_OPTIONS,
                            value="original",
                            clearable=False,
                        ),
                        html.Label("Chart width (cm)", style={"marginTop": "10px"}),
                        dcc.Input(
                            id="chart-width",
                            type="number",
                            value=DEFAULT_CHART_WIDTH_CM,
                            min=10,
                            max=45,
                            step=0.5,
                        ),
                        html.Label("Chart height (cm)", style={"marginTop": "10px"}),
                        dcc.Input(
                            id="chart-height",
                            type="number",
                            value=DEFAULT_CHART_HEIGHT_CM,
                            min=8,
                            max=25,
                            step=0.5,
                        ),
                        html.Hr(className="divider"),
                        html.Details(
                            [
                                html.Summary("Five-year chart"),
                                dcc.Slider(
                                    id="five-year-recent",
                                    min=3,
                                    max=6,
                                    step=1,
                                    value=5,
                                    marks={i: str(i) for i in range(3, 7)},
                                ),
                            ],
                            open=False,
                        ),
                        html.Details(
                            [
                                html.Summary("Chart extras"),
                                dcc.Checklist(
                                    id="zero-line",
                                    options=[{"label": "Show zero line", "value": "on"}],
                                    value=[],
                                ),
                            ],
                            open=False,
                        ),
                        html.Hr(className="divider"),
                        html.Div("Presets", className="card-title"),
                        dcc.Dropdown(
                            id="preset-load",
                            options=_get_preset_options(PRESET_CACHE),
                            placeholder="Select preset",
                        ),
                        html.Div(
                            [
                                html.Button("Load", id="preset-load-btn", className="btn btn-secondary"),
                                html.Button("Delete", id="preset-delete-btn", className="btn btn-ghost"),
                            ],
                            className="button-row",
                        ),
                        html.Label("Preset name", style={"marginTop": "10px"}),
                        dcc.Input(id="preset-name-input", type="text", value=""),
                        dcc.Checklist(
                            id="preset-overwrite",
                            options=[{"label": "Allow overwrite", "value": "on"}],
                            value=[],
                            className="inline-check",
                            labelStyle={"display": "flex", "alignItems": "center", "gap": "6px"},
                            inputStyle={"marginRight": "6px"},
                            style={"marginTop": "6px"},
                        ),
                        html.Button("Save", id="preset-save-btn", className="btn btn-primary"),
                        html.Div(id="preset-message", className="preset-message"),
                        html.Hr(className="divider"),
                        html.Div("BI Builder", className="card-title"),
                        html.Label("BI modules"),
                        dcc.Dropdown(
                            id="bi-module-select",
                            options=BI_MODULE_OPTIONS,
                            value=[DEFAULT_MODULE_FOR_SELECTION] if DEFAULT_MODULE_FOR_SELECTION else [],
                            multi=True,
                            placeholder="Select modules",
                        ),
                        html.Label("BI series", style={"marginTop": "10px"}),
                        dcc.Dropdown(
                            id="bi-series-select",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="Select series",
                        ),
                        html.Label("BI data type", style={"marginTop": "10px"}),
                        dcc.Dropdown(
                            id="bi-dtype-select",
                            options=[
                                {"label": label, "value": key} for key, label in STANDARD_DATA_KEYS
                            ],
                            value="raw_data",
                            clearable=False,
                        ),
                        html.Label("Lookback (months)", style={"marginTop": "10px"}),
                        dcc.Slider(
                            id="bi-lookback",
                            min=6,
                            max=120,
                            step=6,
                            value=60,
                            marks={i: str(i) for i in range(12, 121, 12)},
                        ),
                        html.Label("BI chart", style={"marginTop": "10px"}),
                        dcc.Dropdown(
                            id="bi-chart-type",
                            options=[
                                {"label": "Line", "value": "line"},
                                {"label": "Bar", "value": "bar"},
                                {"label": "Scatter", "value": "scatter"},
                                {"label": "Area", "value": "area"},
                                {"label": "Box", "value": "box"},
                            ],
                            value="line",
                            clearable=False,
                        ),
                        html.Label("Aggregation", style={"marginTop": "10px"}),
                        dcc.Dropdown(
                            id="bi-agg",
                            options=[
                                {"label": "None", "value": "none"},
                                {"label": "Mean", "value": "mean"},
                                {"label": "Sum", "value": "sum"},
                                {"label": "Median", "value": "median"},
                                {"label": "Last", "value": "last"},
                            ],
                            value="none",
                            clearable=False,
                        ),

                    ],
                ),
                html.Div(
                    className="main-panel",
                    children=[
                        html.Div(
                            className="card chart-card",
                            children=[
                                dcc.Graph(
                                    id="chart",
                                    config={
                                        "displaylogo": False,
                                        "modeBarButtonsToAdd": [],
                                        "toImageButtonOptions": {
                                            "format": "png",
                                            "scale": 2,
                                        },
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Button(
                                            "Copy chart to clipboard",
                                            id="copy-chart-btn",
                                            className="btn btn-primary",
                                        ),
                                        html.Div(id="copy-status", className="copy-status"),
                                    ],
                                    className="chart-actions",
                                ),
                            ],
                        ),
                        html.Div(
                            className="card axis-card",
                            children=[
                                html.Div("Axis settings", className="card-title"),
                                html.Div(
                                    className="axis-panel",
                                    children=[
                                        html.Details(
                                            [
                                                html.Summary("Axis titles"),
                                                html.Div(
                                                    className="axis-fields",
                                                    children=[
                                                        html.Div(
                                                            [
                                                                html.Label("Single axis title"),
                                                                dcc.Input(
                                                                    id="single-axis-title",
                                                                    type="text",
                                                                    value="",
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("Left axis title"),
                                                                dcc.Input(
                                                                    id="left-axis-title",
                                                                    type="text",
                                                                    value="",
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("Right axis title"),
                                                                dcc.Input(
                                                                    id="right-axis-title",
                                                                    type="text",
                                                                    value="",
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("Left title offset"),
                                                                dcc.Input(
                                                                    id="left-title-offset",
                                                                    type="number",
                                                                    value=0.0,
                                                                    step=0.01,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("Right title offset"),
                                                                dcc.Input(
                                                                    id="right-title-offset",
                                                                    type="number",
                                                                    value=0.0,
                                                                    step=0.01,
                                                                ),
                                                            ]
                                                        ),
                                                    ],
                                                ),
                                            ],
                                            open=True,
                                            className="axis-box",
                                        ),
                                        html.Details(
                                            [
                                                html.Summary("Axis ranges"),
                                                html.Div(
                                                    className="range-grid",
                                                    children=[
                                                        html.Div(
                                                            [
                                                                dcc.Checklist(
                                                                    id="single-axis-manual",
                                                                    options=[
                                                                        {"label": "Single manual", "value": "on"}
                                                                    ],
                                                                    value=[],
                                                                )
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("Single axis min"),
                                                                dcc.Input(
                                                                    id="single-axis-min",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("Single axis max"),
                                                                dcc.Input(
                                                                    id="single-axis-max",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                dcc.Checklist(
                                                                    id="left-axis-manual",
                                                                    options=[
                                                                        {"label": "Left manual", "value": "on"}
                                                                    ],
                                                                    value=[],
                                                                )
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("Left axis min"),
                                                                dcc.Input(
                                                                    id="left-axis-min",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("Left axis max"),
                                                                dcc.Input(
                                                                    id="left-axis-max",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                dcc.Checklist(
                                                                    id="right-axis-manual",
                                                                    options=[
                                                                        {"label": "Right manual", "value": "on"}
                                                                    ],
                                                                    value=[],
                                                                )
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("Right axis min"),
                                                                dcc.Input(
                                                                    id="right-axis-min",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                        html.Div(
                                                            [
                                                                html.Label("Right axis max"),
                                                                dcc.Input(
                                                                    id="right-axis-max",
                                                                    type="number",
                                                                    step=0.1,
                                                                ),
                                                            ]
                                                        ),
                                                    ],
                                                ),
                                            ],
                                            open=True,
                                            className="axis-box",
                                        ),
                                    ],
                                ),
                            ],
                        ),
                        html.Div(
                            className="card table-card",
                            children=[
                                html.Div("Selected series", className="card-title"),
                                dash_table.DataTable(
                                    id="series-table",
                                    data=[],
                                    columns=[],
                                    editable=True,
                                    hidden_columns=["key"],
                                    dropdown={},
                                    dropdown_conditional=[],
                                    tooltip_data=[],
                                    tooltip_delay=0,
                                    tooltip_duration=None,
                                    style_table={
                                        "overflowX": "auto",
                                        "height": "240px",
                                        "overflowY": "auto",
                                        "tableLayout": "fixed",
                                    },
                                    style_cell={
                                        "textAlign": "left",
                                        "padding": "6px",
                                        "whiteSpace": "normal",
                                        "wordBreak": "break-word",
                                        "lineHeight": "1.2",
                                        "width": "120px",
                                        "minWidth": "120px",
                                        "maxWidth": "120px",
                                    },
                                    style_cell_conditional=[
                                        {
                                            "if": {"column_id": "series"},
                                            "width": "260px",
                                            "minWidth": "260px",
                                            "maxWidth": "260px",
                                        },
                                        {
                                            "if": {"column_id": "label"},
                                            "width": "260px",
                                            "minWidth": "260px",
                                            "maxWidth": "260px",
                                        },
                                        {
                                            "if": {"column_id": "dtype"},
                                            "width": "120px",
                                            "minWidth": "120px",
                                            "maxWidth": "120px",
                                        },
                                        {
                                            "if": {"column_id": "axis"},
                                            "width": "100px",
                                            "minWidth": "100px",
                                            "maxWidth": "100px",
                                        },
                                    ],
                                    style_data={
                                        "whiteSpace": "normal",
                                        "height": "auto",
                                        "lineHeight": "1.2",
                                    },
                                    style_data_conditional=[
                                        {"if": {"row_index": "odd"}, "backgroundColor": "#f6f2ea"}
                                    ],
                                    style_header={
                                        "fontWeight": "600",
                                        "backgroundColor": "#efe9dd",
                                    },
                                    css=[
                                        {
                                            "selector": ".dash-spreadsheet-container table",
                                            "rule": "table-layout: fixed; width: 100%;",
                                        },
                                    ],
                                ),
                            ],
                        ),
                        html.Div(
                            className="card table-card",
                            children=[
                                html.Details(
                                    open=False,
                                    className="table-details",
                                    children=[
                                        html.Summary("Data preview", className="card-title"),
                                        html.Div(
                                            [
                                                html.Button(
                                                    "Download CSV",
                                                    id="download-csv-btn",
                                                    className="btn btn-secondary",
                                                )
                                            ],
                                            className="button-row",
                                        ),
                                        dash_table.DataTable(
                                            id="data-table",
                                            data=[],
                                            columns=[],
                                            page_size=10,
                                            style_table={
                                                "overflowX": "auto",
                                                "height": "240px",
                                                "overflowY": "auto",
                                            },
                                            style_cell={
                                                "textAlign": "left",
                                                "padding": "6px",
                                                "minWidth": "90px",
                                            },
                                            style_data_conditional=[
                                                {"if": {"row_index": "odd"}, "backgroundColor": "#f6f2ea"}
                                            ],
                                            style_header={
                                                "fontWeight": "600",
                                                "backgroundColor": "#efe9dd",
                                            },
                                        ),
                                    ],
                                ),
                            ],
                        ),
                        html.Div(
                            className="card bi-card",
                            children=[
                                html.Div("BI Builder", className="card-title"),
                                html.Div(id="bi-status", className="status-message"),
                                html.Div(
                                    className="bi-layout",
                                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
                                    children=[
                                        html.Div(
                                            className="bi-field-panel",
                                            children=[
                                                html.Div("Field panel", className="card-title"),
                                                html.Div(id="bi-field-panel"),
                                            ],
                                        ),
                                        html.Div(
                                            className="bi-drop-panel",
                                            children=[
                                                html.Div("Drop zones", className="card-title"),
                                                html.Label("X"),
                                                dcc.Dropdown(id="bi-x", options=[], value=None, clearable=False),
                                                html.Label("Y", style={"marginTop": "8px"}),
                                                dcc.Dropdown(id="bi-y", options=[], value=None, clearable=False),
                                                html.Label("Color", style={"marginTop": "8px"}),
                                                dcc.Dropdown(id="bi-color", options=[], value=None, clearable=True),
                                                html.Label("Size", style={"marginTop": "8px"}),
                                                dcc.Dropdown(id="bi-size", options=[], value=None, clearable=True),
                                                html.Label("Facet row", style={"marginTop": "8px"}),
                                                dcc.Dropdown(id="bi-facet-row", options=[], value=None, clearable=True),
                                                html.Label("Facet col", style={"marginTop": "8px"}),
                                                dcc.Dropdown(id="bi-facet-col", options=[], value=None, clearable=True),
                                            ],
                                        ),
                                    ],
                                ),
                                dcc.Graph(
                                    id="bi-chart",
                                    config={"displaylogo": False},
                                ),
                                html.Details(
                                    open=False,
                                    children=[
                                        html.Summary("BI data preview", className="card-title"),
                                        dash_table.DataTable(
                                            id="bi-table",
                                            data=[],
                                            columns=[],
                                            page_size=10,
                                            style_table={
                                                "overflowX": "auto",
                                                "height": "240px",
                                                "overflowY": "auto",
                                            },
                                            style_cell={
                                                "textAlign": "left",
                                                "padding": "6px",
                                                "minWidth": "90px",
                                            },
                                            style_data_conditional=[
                                                {"if": {"row_index": "odd"}, "backgroundColor": "#f6f2ea"}
                                            ],
                                            style_header={
                                                "fontWeight": "600",
                                                "backgroundColor": "#efe9dd",
                                            },
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
        dcc.Store(id="series-settings-store", data={}),
        dcc.Store(id="full-data-store", data=""),
        dcc.Download(id="download-data"),
    ],
)


@app.callback(
    Output("series-table", "data"),
    Output("series-table", "columns"),
    Output("series-table", "dropdown"),
    Output("series-table", "dropdown_conditional"),
    Output("series-table", "tooltip_data"),
    Input("series-select", "value"),
    Input("global-dtype", "value"),
    State("series-settings-store", "data"),
)
def update_series_table(selected_keys, global_dtype, settings):
    selected_infos = _collect_selected_infos(selected_keys, SERIES_REGISTRY)
    rows = _build_table_rows(selected_infos, settings, global_dtype or "raw_data")

    columns = [
        {"name": "Key", "id": "key"},
        {"name": "Series", "id": "series", "editable": False},
        {"name": "Label", "id": "label", "editable": True},
        {"name": "Data type", "id": "dtype", "editable": True, "presentation": "dropdown"},
        {"name": "Axis", "id": "axis", "editable": True, "presentation": "dropdown"},
    ]

    dropdown = {
        "axis": {
            "options": [
                {"label": "Left", "value": "left"},
                {"label": "Right", "value": "right"},
            ]
        },
    }

    dropdown_conditional = []
    for info in selected_infos:
        dtype_options = [
            {"label": DATA_TYPE_LABELS.get(key, key), "value": key}
            for key in info.get("available_types", [])
        ]
        if dtype_options:
            dropdown_conditional.append(
                {
                    "if": {"column_id": "dtype", "filter_query": f'{{key}} eq "{info["key"]}"'},
                    "options": dtype_options,
                }
            )

    tooltips = []
    for row in rows:
        tooltips.append(
            {
                "series": {"value": str(row.get("series") or ""), "type": "text"},
                "label": {"value": str(row.get("label") or ""), "type": "text"},
            }
        )

    return rows, columns, dropdown, dropdown_conditional, tooltips


@app.callback(
    Output("series-settings-store", "data", allow_duplicate=True),
    Input("series-table", "data"),
    State("series-settings-store", "data"),
    State("global-dtype", "value"),
    prevent_initial_call=True,
)
def update_series_settings(table_data, existing, global_dtype):
    return _settings_from_rows(table_data, existing, global_dtype)


@app.callback(
    Output("chart", "figure"),
    Output("data-table", "data"),
    Output("data-table", "columns"),
    Output("full-data-store", "data"),
    Output("status-message", "children"),
    Input("series-select", "value"),
    Input("series-table", "data"),
    Input("chart-type", "value"),
    Input("global-dtype", "value"),
    Input("frequency-option", "value"),
    Input("chart-width", "value"),
    Input("chart-height", "value"),
    Input("single-axis-title", "value"),
    Input("left-axis-title", "value"),
    Input("right-axis-title", "value"),
    Input("left-title-offset", "value"),
    Input("right-title-offset", "value"),
    Input("single-axis-manual", "value"),
    Input("single-axis-min", "value"),
    Input("single-axis-max", "value"),
    Input("left-axis-manual", "value"),
    Input("left-axis-min", "value"),
    Input("left-axis-max", "value"),
    Input("right-axis-manual", "value"),
    Input("right-axis-min", "value"),
    Input("right-axis-max", "value"),
    Input("five-year-recent", "value"),
    Input("zero-line", "value"),
)
def update_chart(
    selected_keys,
    table_data,
    chart_type,
    global_dtype,
    frequency_option,
    chart_width_cm,
    chart_height_cm,
    single_axis_title,
    left_axis_title,
    right_axis_title,
    left_title_offset,
    right_title_offset,
    single_axis_manual,
    single_axis_min,
    single_axis_max,
    left_axis_manual,
    left_axis_min,
    left_axis_max,
    right_axis_manual,
    right_axis_min,
    right_axis_max,
    five_year_recent,
    zero_line_value,
):
    per_series_override = True
    settings = _settings_from_rows(table_data, global_dtype=global_dtype)
    selected_infos = _collect_selected_infos(selected_keys, SERIES_REGISTRY)

    axis_titles = {
        "single": single_axis_title or "",
        "left": left_axis_title or "",
        "right": right_axis_title or "",
    }
    axis_offsets = {
        "left": float(left_title_offset) if left_title_offset is not None else 0.0,
        "right": float(right_title_offset) if right_title_offset is not None else 0.0,
    }
    manual_ranges = {
        "single": _extract_axis_range("on" in (single_axis_manual or []), single_axis_min, single_axis_max),
        "left": _extract_axis_range("on" in (left_axis_manual or []), left_axis_min, left_axis_max),
        "right": _extract_axis_range("on" in (right_axis_manual or []), right_axis_min, right_axis_max),
    }
    zero_line = "on" in (zero_line_value or [])

    fig, table_df, messages = _build_figure(
        selected_infos=selected_infos,
        settings=settings,
        chart_type=chart_type or "multi_line",
        global_dtype=global_dtype or "raw_data",
        per_series_override=per_series_override,
        frequency_option=_normalize_frequency_option(frequency_option),
        chart_width_cm=chart_width_cm,
        chart_height_cm=chart_height_cm,
        axis_titles=axis_titles,
        axis_offsets=axis_offsets,
        manual_ranges=manual_ranges,
        five_year_recent=five_year_recent,
        zero_line=zero_line,
    )

    data_records, data_columns = _prepare_data_table(table_df)
    full_data_json = ""
    if isinstance(table_df, pd.DataFrame) and not table_df.empty:
        full_data_json = table_df.to_json(orient="split", date_format="iso")

    status = ""
    if messages:
        status = html.Ul([html.Li(msg) for msg in messages])

    return fig, data_records, data_columns, full_data_json, status


@app.callback(
    Output("download-data", "data"),
    Input("download-csv-btn", "n_clicks"),
    State("full-data-store", "data"),
    prevent_initial_call=True,
)
def download_csv(n_clicks, json_payload):
    if not n_clicks or not json_payload:
        return no_update
    try:
        df = pd.read_json(StringIO(json_payload), orient="split")
    except ValueError:
        return no_update
    filename = f"us_eco_dashboard_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    return dcc.send_data_frame(df.to_csv, filename, index=True, encoding="utf-8-sig")

@app.callback(
    Output("preset-load", "options"),
    Output("preset-load", "value"),
    Output("series-select", "value"),
    Output("global-dtype", "value"),
    Output("chart-type", "value"),
    Output("frequency-option", "value"),
    Output("chart-width", "value"),
    Output("chart-height", "value"),
    Output("single-axis-title", "value"),
    Output("left-axis-title", "value"),
    Output("right-axis-title", "value"),
    Output("left-title-offset", "value"),
    Output("right-title-offset", "value"),
    Output("five-year-recent", "value"),
    Output("single-axis-manual", "value"),
    Output("single-axis-min", "value"),
    Output("single-axis-max", "value"),
    Output("left-axis-manual", "value"),
    Output("left-axis-min", "value"),
    Output("left-axis-max", "value"),
    Output("right-axis-manual", "value"),
    Output("right-axis-min", "value"),
    Output("right-axis-max", "value"),
    Output("series-settings-store", "data"),
    Output("preset-message", "children"),
    Output("preset-name-input", "value"),
    Input("preset-load-btn", "n_clicks"),
    Input("preset-save-btn", "n_clicks"),
    Input("preset-delete-btn", "n_clicks"),
    State("preset-load", "value"),
    State("preset-name-input", "value"),
    State("preset-overwrite", "value"),
    State("series-select", "value"),
    State("series-table", "data"),
    State("chart-type", "value"),
    State("global-dtype", "value"),
    State("frequency-option", "value"),
    State("chart-width", "value"),
    State("chart-height", "value"),
    State("single-axis-title", "value"),
    State("left-axis-title", "value"),
    State("right-axis-title", "value"),
    State("left-title-offset", "value"),
    State("right-title-offset", "value"),
    State("five-year-recent", "value"),
    State("single-axis-manual", "value"),
    State("single-axis-min", "value"),
    State("single-axis-max", "value"),
    State("left-axis-manual", "value"),
    State("left-axis-min", "value"),
    State("left-axis-max", "value"),
    State("right-axis-manual", "value"),
    State("right-axis-min", "value"),
    State("right-axis-max", "value"),
)
def handle_presets(
    load_clicks,
    save_clicks,
    delete_clicks,
    preset_value,
    preset_name,
    overwrite_value,
    selected_keys,
    table_data,
    chart_type,
    global_dtype,
    frequency_option,
    chart_width_cm,
    chart_height_cm,
    single_axis_title,
    left_axis_title,
    right_axis_title,
    left_title_offset,
    right_title_offset,
    five_year_recent,
    single_axis_manual,
    single_axis_min,
    single_axis_max,
    left_axis_manual,
    left_axis_min,
    left_axis_max,
    right_axis_manual,
    right_axis_min,
    right_axis_max,
):
    ctx = dash.callback_context
    triggered = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None

    options = _get_preset_options(PRESET_CACHE)

    defaults = [
        options,
        preset_value,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
    ]

    if triggered == "preset-delete-btn":
        if not preset_value:
            defaults[24] = "Select a preset to delete."
            return defaults
        if preset_value in PRESET_CACHE:
            del PRESET_CACHE[preset_value]
            save_dashboard_presets(PRESET_CACHE)
            options = _get_preset_options(PRESET_CACHE)
            defaults[0] = options
            defaults[1] = None
            defaults[24] = f"Deleted preset: {preset_value}"
            return defaults
        defaults[24] = "Preset not found."
        return defaults

    if triggered == "preset-save-btn":
        if not preset_name:
            defaults[24] = "Enter a preset name before saving."
            return defaults
        allow_overwrite = "on" in (overwrite_value or [])
        if preset_name in PRESET_CACHE and not allow_overwrite:
            defaults[24] = "Preset already exists. Enable overwrite to replace it."
            return defaults

        settings = _settings_from_rows(table_data, global_dtype=global_dtype)
        selected_infos = _collect_selected_infos(selected_keys, SERIES_REGISTRY)
        if not selected_infos:
            defaults[24] = "Select at least one series before saving."
            return defaults
        per_series_override = True
        dtype_map: dict[str, str] = {}
        custom_labels: dict[str, str] = {}
        axis_allocation = {"left": [], "right": []}

        for info in selected_infos:
            key = info["key"]
            base_label = info.get("display_label", "")
            label = settings.get(key, {}).get("label") or base_label
            label = str(label).strip() if label else base_label
            custom_labels[key] = label
            axis = settings.get(key, {}).get("axis", "left")
            axis_allocation["right" if axis == "right" else "left"].append(label)

            dtype_choice = settings.get(key, {}).get("dtype") if per_series_override else None
            if not dtype_choice:
                dtype_choice = global_dtype
            if dtype_choice not in info.get("available_types", []):
                dtype_choice = _default_dtype_for_series(info.get("available_types", []))
            dtype_map[key] = dtype_choice

        display_labels = [custom_labels.get(info["key"], info.get("display_label", "")) for info in selected_infos]
        series_labels = {info["key"]: info.get("display_label", "") for info in selected_infos}

        axis_label_default = compute_axis_label(selected_infos, dtype_map)
        snapshot = {
            "series_keys": list(selected_keys or []),
            "series_labels": series_labels,
            "custom_labels": custom_labels,
            "global_dtype_key": global_dtype,
            "per_series_override": per_series_override,
            "dtype_map": dtype_map,
            "chart_type": chart_type,
            "chart_type_label": next(
                (label for label, code in CHART_TYPE_LABELS.items() if code == chart_type),
                chart_type,
            ),
            "chart_width_cm": float(chart_width_cm) if chart_width_cm is not None else DEFAULT_CHART_WIDTH_CM,
            "chart_height_cm": float(chart_height_cm) if chart_height_cm is not None else DEFAULT_CHART_HEIGHT_CM,
            "single_axis_title": single_axis_title or "",
            "left_axis_title": left_axis_title or "",
            "right_axis_title": right_axis_title or "",
            "left_title_offset": float(left_title_offset) if left_title_offset is not None else 0.0,
            "right_title_offset": float(right_title_offset) if right_title_offset is not None else 0.0,
            "dual_axis_left": list(axis_allocation.get("left", [])),
            "dual_axis_right": list(axis_allocation.get("right", [])),
            "five_year_recent_years": five_year_recent,
            "active_series_keys": list(selected_keys or []),
            "display_labels": display_labels,
            "axis_label_default": axis_label_default,
            "frequency_option": frequency_option,
            "single_axis_manual_range": False,
            "left_axis_manual_range": False,
            "right_axis_manual_range": False,
            "single_axis_range": None,
            "left_axis_range": None,
            "right_axis_range": None,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "name": preset_name,
        }

        single_range = _extract_axis_range(True, single_axis_min, single_axis_max)
        left_range = _extract_axis_range(True, left_axis_min, left_axis_max)
        right_range = _extract_axis_range(True, right_axis_min, right_axis_max)

        snapshot["single_axis_manual_range"] = (
            "on" in (single_axis_manual or []) or single_range is not None
        )
        snapshot["left_axis_manual_range"] = (
            "on" in (left_axis_manual or []) or left_range is not None
        )
        snapshot["right_axis_manual_range"] = (
            "on" in (right_axis_manual or []) or right_range is not None
        )

        if single_range:
            snapshot["single_axis_range"] = single_range
        if left_range:
            snapshot["left_axis_range"] = left_range
        if right_range:
            snapshot["right_axis_range"] = right_range

        PRESET_CACHE[preset_name] = snapshot
        save_dashboard_presets(PRESET_CACHE)
        options = _get_preset_options(PRESET_CACHE)
        defaults[0] = options
        defaults[1] = preset_name
        defaults[24] = f"Saved preset: {preset_name}"
        defaults[25] = preset_name
        return defaults

    if triggered == "preset-load-btn":
        if not preset_value:
            defaults[24] = "Select a preset to load."
            return defaults
        preset = PRESET_CACHE.get(preset_value)
        if not preset:
            defaults[24] = "Preset not found."
            return defaults

        series_keys = preset.get("active_series_keys") or preset.get("series_keys") or []
        available_keys = [key for key in series_keys if key in SERIES_REGISTRY]
        if not available_keys:
            defaults[24] = "No available series found in the preset."
            return defaults

        global_dtype_key = preset.get("global_dtype_key") or "raw_data"
        chart_type_value = preset.get("chart_type") or "multi_line"
        frequency_value = _normalize_frequency_option(preset.get("frequency_option"))

        custom_labels = preset.get("custom_labels") or {}
        series_labels = preset.get("series_labels") or {}
        dtype_map = preset.get("dtype_map") or {}
        left_labels = set(preset.get("dual_axis_left") or [])
        right_labels = set(preset.get("dual_axis_right") or [])

        settings: dict[str, dict[str, str]] = {}
        for key in available_keys:
            base_label = series_labels.get(key) or SERIES_REGISTRY[key].get("display_label", "")
            label = custom_labels.get(key) or base_label
            axis = "left"
            if label in right_labels:
                axis = "right"
            elif label in left_labels:
                axis = "left"
            dtype_value = dtype_map.get(key, "")
            dtype_override = bool(dtype_value and dtype_value != global_dtype_key)
            settings[key] = {
                "label": label,
                "dtype": dtype_value if dtype_override else "",
                "axis": axis,
                "dtype_override": dtype_override,
            }

        defaults = [
            options,
            preset_value,
            available_keys,
            global_dtype_key,
            chart_type_value,
            frequency_value,
            preset.get("chart_width_cm", DEFAULT_CHART_WIDTH_CM),
            preset.get("chart_height_cm", DEFAULT_CHART_HEIGHT_CM),
            preset.get("single_axis_title", ""),
            preset.get("left_axis_title", ""),
            preset.get("right_axis_title", ""),
            preset.get("left_title_offset", 0.0),
            preset.get("right_title_offset", 0.0),
            preset.get("five_year_recent_years", 5),
            ["on"] if preset.get("single_axis_manual_range") else [],
            (preset.get("single_axis_range") or [None, None])[0],
            (preset.get("single_axis_range") or [None, None])[1],
            ["on"] if preset.get("left_axis_manual_range") else [],
            (preset.get("left_axis_range") or [None, None])[0],
            (preset.get("left_axis_range") or [None, None])[1],
            ["on"] if preset.get("right_axis_manual_range") else [],
            (preset.get("right_axis_range") or [None, None])[0],
            (preset.get("right_axis_range") or [None, None])[1],
            settings,
            f"Loaded preset: {preset_value}",
            preset_value,
        ]
        return defaults

    return defaults


@app.callback(
    Output("bi-series-select", "options"),
    Output("bi-series-select", "value"),
    Input("bi-module-select", "value"),
    State("bi-series-select", "value"),
)
def update_bi_series_options(selected_modules, current_values):
    if not selected_modules:
        return [], []
    options = []
    for key, info in SERIES_REGISTRY.items():
        if info.get("stem") in selected_modules:
            options.append({"label": info.get("display_label", key), "value": key})
    options = sorted(options, key=lambda item: item["label"])
    allowed = {opt["value"] for opt in options}
    current_values = current_values or []
    next_values = [val for val in current_values if val in allowed]
    if not next_values:
        defaults = [key for key in DEFAULT_SERIES_SELECTION if key in allowed]
        if defaults:
            next_values = defaults
        elif options:
            next_values = [options[0]["value"]]
    return options, next_values


@app.callback(
    Output("bi-field-panel", "children"),
    Output("bi-x", "options"),
    Output("bi-x", "value"),
    Output("bi-y", "options"),
    Output("bi-y", "value"),
    Output("bi-color", "options"),
    Output("bi-color", "value"),
    Output("bi-size", "options"),
    Output("bi-size", "value"),
    Output("bi-facet-row", "options"),
    Output("bi-facet-row", "value"),
    Output("bi-facet-col", "options"),
    Output("bi-facet-col", "value"),
    Input("bi-series-select", "value"),
    Input("bi-dtype-select", "value"),
    State("bi-x", "value"),
    State("bi-y", "value"),
    State("bi-color", "value"),
    State("bi-size", "value"),
    State("bi-facet-row", "value"),
    State("bi-facet-col", "value"),
)
def update_bi_fields(selected_keys, dtype_key, x_value, y_value, color_value, size_value, facet_row_value, facet_col_value):
    selected_infos = _collect_selected_infos(selected_keys, SERIES_REGISTRY)
    dtype_key = dtype_key or "raw_data"
    df_long = _build_long_dataframe(selected_infos, dtype_key)
    if df_long.empty:
        empty_panel = html.Div("No data available.")
        return empty_panel, [], None, [], None, [], None, [], None, [], None, [], None

    panel = _bi_field_panel(df_long)
    fields, numeric_fields = _bi_field_options(df_long)
    field_options = [{"label": col, "value": col} for col in fields]
    numeric_options = [{"label": col, "value": col} for col in numeric_fields]

    x_next = x_value if x_value in fields else ("date" if "date" in fields else fields[0])
    y_next = y_value if y_value in numeric_fields else ("value" if "value" in numeric_fields else numeric_fields[0])
    color_next = color_value if color_value in fields else ("display_label" if "display_label" in fields else None)
    size_next = size_value if size_value in numeric_fields else None
    facet_row_next = facet_row_value if facet_row_value in fields else None
    facet_col_next = facet_col_value if facet_col_value in fields else None

    return (
        panel,
        field_options,
        x_next,
        numeric_options if numeric_options else field_options,
        y_next,
        field_options,
        color_next,
        numeric_options,
        size_next,
        field_options,
        facet_row_next,
        field_options,
        facet_col_next,
    )


@app.callback(
    Output("bi-chart", "figure"),
    Output("bi-table", "data"),
    Output("bi-table", "columns"),
    Output("bi-status", "children"),
    Input("bi-series-select", "value"),
    Input("bi-dtype-select", "value"),
    Input("bi-lookback", "value"),
    Input("bi-x", "value"),
    Input("bi-y", "value"),
    Input("bi-color", "value"),
    Input("bi-size", "value"),
    Input("bi-facet-row", "value"),
    Input("bi-facet-col", "value"),
    Input("bi-chart-type", "value"),
    Input("bi-agg", "value"),
)
def update_bi_chart(
    selected_keys,
    dtype_key,
    lookback,
    x_field,
    y_field,
    color_field,
    size_field,
    facet_row,
    facet_col,
    chart_type,
    agg_method,
):
    if not selected_keys:
        fig = _make_empty_figure("Select series to start.")
        fig.update_layout(height=520)
        return fig, [], [], "Select series to start."

    selected_infos = _collect_selected_infos(selected_keys, SERIES_REGISTRY)
    dtype_key = dtype_key or "raw_data"
    df_long = _build_long_dataframe(selected_infos, dtype_key)
    if df_long.empty:
        fig = _make_empty_figure("No data available.")
        fig.update_layout(height=520)
        return fig, [], [], "No data available."

    if lookback:
        latest_date = df_long["date"].max()
        if pd.notna(latest_date):
            cutoff = latest_date - pd.DateOffset(months=int(lookback))
            df_long = df_long[df_long["date"] >= cutoff]

    if df_long.empty:
        fig = _make_empty_figure("No data after filters.")
        fig.update_layout(height=520)
        return fig, [], [], "No data after filters."

    df_plot = df_long.copy()
    if agg_method and agg_method != "none":
        group_cols = [col for col in [x_field, color_field, facet_row, facet_col] if col]
        if group_cols:
            if agg_method == "last":
                df_plot = (
                    df_plot.sort_values(x_field)
                    .groupby(group_cols, dropna=False)[y_field]
                    .last()
                    .reset_index()
                )
            else:
                df_plot = (
                    df_plot.groupby(group_cols, dropna=False)[y_field]
                    .agg(agg_method)
                    .reset_index()
                )

    if x_field in df_plot.columns:
        df_plot = df_plot.sort_values(x_field)

    fig = None
    color_arg = color_field or None
    size_arg = size_field or None
    facet_row_arg = facet_row or None
    facet_col_arg = facet_col or None

    if chart_type == "line":
        fig = px.line(df_plot, x=x_field, y=y_field, color=color_arg, facet_row=facet_row_arg, facet_col=facet_col_arg)
    elif chart_type == "bar":
        fig = px.bar(df_plot, x=x_field, y=y_field, color=color_arg, facet_row=facet_row_arg, facet_col=facet_col_arg, barmode="group")
    elif chart_type == "scatter":
        fig = px.scatter(df_plot, x=x_field, y=y_field, color=color_arg, size=size_arg, facet_row=facet_row_arg, facet_col=facet_col_arg)
    elif chart_type == "area":
        fig = px.area(df_plot, x=x_field, y=y_field, color=color_arg, facet_row=facet_row_arg, facet_col=facet_col_arg)
    elif chart_type == "box":
        fig = px.box(df_plot, x=x_field, y=y_field, color=color_arg, facet_row=facet_row_arg, facet_col=facet_col_arg)
    else:
        fig = px.line(df_plot, x=x_field, y=y_field, color=color_arg)

    fig.update_layout(template="plotly_white", height=520)

    preview = df_plot.copy()
    if "date" in preview.columns:
        preview["date"] = preview["date"].dt.strftime("%Y-%m-%d")
    preview = preview.head(200)
    table_data = preview.to_dict("records")
    table_columns = [{"name": col, "id": col} for col in preview.columns]

    status = f"Rows: {len(df_plot):,}" if df_plot is not None else ""
    return fig, table_data, table_columns, status


app.clientside_callback(
    """
    async function(n_clicks) {
        if (!n_clicks) {
            return '';
        }
        const container = document.getElementById('chart');
        if (!container) {
            return 'Chart not found.';
        }
        const plotlyDiv = container.querySelector('.js-plotly-plot');
        if (!plotlyDiv) {
            return 'Chart not ready.';
        }
        try {
            if (!navigator.clipboard || !window.ClipboardItem) {
                return 'Clipboard API not available. Use the download button.';
            }
            const dataUrl = await Plotly.toImage(plotlyDiv, {format: 'png', scale: 2});
            const response = await fetch(dataUrl);
            const blob = await response.blob();
            await navigator.clipboard.write([new ClipboardItem({[blob.type]: blob})]);
            return 'Chart image copied to clipboard.';
        } catch (err) {
            return 'Copy failed: ' + err;
        }
    }
    """,
    Output("copy-status", "children"),
    Input("copy-chart-btn", "n_clicks"),
)


if __name__ == "__main__":
    app.run_server(debug=True)
