"""Streamlit 대시보드: us_eco 폴더 내 각 경제 지표 모듈을 통합 시각화."""

from __future__ import annotations

import json
import importlib
import importlib.util
import inspect
import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

try:
    from . import us_eco_utils as utils_module
except ImportError:
    sys.path.append(os.path.dirname(__file__))
    import us_eco_utils as utils_module

from us_eco_utils import (
    plot_economic_series,
    calculate_mom_percent,
    calculate_mom_change,
    calculate_yoy_percent,
    calculate_yoy_change,
)

try:
    from streamlit_tree_select import tree_select
    TREE_AVAILABLE = True
except ImportError:
    TREE_AVAILABLE = False

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
    "멀티 라인": "multi_line",
    "단일 라인": "single_line",
    "이중 축": "dual_axis",
    "가로 막대": "horizontal_bar",
    "세로 막대": "vertical_bar",
    "5년 비교": "five_year",
}

FREQUENCY_OPTIONS = ["원본 유지", "주간 → 월말 변환"]


STANDARD_DATA_KEYS = [
    ("raw_data", "수준"),
    ("mom_data", "전월 대비 %"),
    ("mom_change", "전월 대비 변화량"),
    ("yoy_data", "전년동월 대비 %"),
    ("yoy_change", "전년동월 대비 변화량"),
]


def _restore_module_loaders(module) -> None:
    """Ensure economic data loader functions point back to the real utilities."""
    real_loader = getattr(utils_module, "load_economic_data", None)
    if callable(real_loader):
        setattr(module, "load_economic_data", real_loader)
    real_group_loader = getattr(utils_module, "load_economic_data_grouped", None)
    if callable(real_group_loader):
        setattr(module, "load_economic_data_grouped", real_group_loader)


FRIENDLY_TITLES = {
    "CPI_analysis_refactor": "CPI",
    "PPI_analysis_refactor": "PPI",
    "CES_employ_refactor": "CES 고용",
    "CPS_employ_refactor": "CPS 고용",
    "ADP_employ_refactor": "ADP 고용",
    "industrial_production_refactor": "산업생산",
    "retail_sales_refactor": "소매판매",
    "construction_spending_refactor": "건설지출",
    "durable_goods_refactor": "내구재 주문",
    "new_residential_construction_refactor": "주택 착공",
    "atlanta_wage_growth_refactor": "애틀랜타 임금",
    "house_price_refactor": "주택 가격",
    "house_sales_stock_refactor": "주택 판매/재고",
    "personal_income_refactor": "개인소득",
    "pce_analysis_refactor": "PCE",
    "import_price_refactor_v2": "수입물가",
    "int_trade_refactor": "무역",
    "ism_pmi_refactor": "ISM PMI",
    "fed_pmi_refactor": "연준 PMI",
    "fed_balance_sheet_refactor": "연준 대차대조표",
    "realtor_housing_inventory_refactor": "리얼터 주택재고",
    "unemployment_claims_analysis": "실업보험청구",
    "phillips_curve_enhanced": "필립스 곡선",
    "beveridge_curve_enhanced": "베버리지 곡선",
    "gdp_analysis_refactor": "GDP",
    "JOLTS_employ_refactor": "JOLTS",
    "misc_fred_series_refactor": "기타 FRED 물가",
}


CATEGORY_MAP: dict[str, list[str]] = {
    "물가": [
        "CPI_analysis_refactor",
        "PPI_analysis_refactor",
        "pce_analysis_refactor",
        "import_price_refactor_v2",
        "misc_fred_series_refactor",
    ],
    "고용": [
        "CES_employ_refactor",
        "CPS_employ_refactor",
        "ADP_employ_refactor",
        "JOLTS_employ_refactor",
        "atlanta_wage_growth_refactor",
        "unemployment_claims_analysis",
        "phillips_curve_enhanced",
        "beveridge_curve_enhanced",
    ],
    "주택·건설": [
        "house_price_refactor",
        "house_sales_stock_refactor",
        "realtor_housing_inventory_refactor",
        "construction_spending_refactor",
        "new_residential_construction_refactor",
    ],
    "생산·소비": [
        "industrial_production_refactor",
        "retail_sales_refactor",
        "ism_pmi_refactor",
        "fed_pmi_refactor",
    ],
}

CATEGORY_ORDER = ["물가", "고용", "주택·건설", "생산·소비", "기타"]

DEFAULT_CATEGORY = "물가"
DEFAULT_MODULE_FOR_SELECTION = "CPI_analysis_refactor"


@st.cache_data(show_spinner=False)
def load_cached_dataframe(csv_path: str) -> pd.DataFrame | None:
    if not csv_path or not os.path.exists(csv_path):
        return None
    try:
        df = pd.read_csv(csv_path, index_col=0)
        df.index = pd.to_datetime(df.index)
        return df.sort_index()
    except Exception as exc:
        print(f"⚠️ CSV 로드 실패 ({csv_path}): {exc}")
        return None


def _ensure_presets_dir() -> None:
    try:
        PRESETS_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"⚠️ 프리셋 디렉터리 생성 실패: {exc}")


def load_dashboard_presets() -> dict[str, Any]:
    if not PRESETS_FILE_PATH.exists():
        return {}
    try:
        with PRESETS_FILE_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"⚠️ 프리셋 파일 로드 실패: {exc}")
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def save_dashboard_presets(presets: dict[str, Any]) -> None:
    _ensure_presets_dir()
    try:
        with PRESETS_FILE_PATH.open("w", encoding="utf-8") as handle:
            json.dump(presets, handle, ensure_ascii=False, indent=2)
    except OSError as exc:
        print(f"⚠️ 프리셋 저장 실패: {exc}")


def _chart_label_from_type(chart_type: str) -> str:
    for label, code in CHART_TYPE_LABELS.items():
        if code == chart_type:
            return label
    return next(iter(CHART_TYPE_LABELS.keys()))


def _trigger_rerun() -> None:
    rerun_candidate = getattr(st, "experimental_rerun", None)
    if callable(rerun_candidate):
        rerun_candidate()
        return
    rerun_candidate = getattr(st, "rerun", None)
    if callable(rerun_candidate):
        rerun_candidate()
        return
    try:
        from streamlit.runtime.scriptrunner import RerunException, RerunData  # type: ignore

        raise RerunException(RerunData(None))
    except Exception as exc:  # pragma: no cover - fallback path
        print(f"⚠️ rerun 호출 실패: {exc}")


def apply_dashboard_preset(preset: dict[str, Any], registry: dict[str, dict[str, Any]]) -> bool:
    if not preset:
        st.session_state["global_preset_error"] = "프리셋 데이터가 비어 있습니다."
        return False

    series_keys = [key for key in preset.get("series_keys", []) if isinstance(key, str)]
    if not series_keys:
        st.session_state["global_preset_error"] = "프리셋에 저장된 시리즈가 없습니다."
        return False

    available_keys = [key for key in series_keys if key in registry]
    missing_keys = [key for key in series_keys if key not in registry]
    if not available_keys:
        st.session_state["global_preset_error"] = "프리셋에 포함된 시리즈를 찾을 수 없습니다."
        return False

    if missing_keys:
        st.session_state["global_preset_warning"] = {
            "name": preset.get("name"),
            "missing": missing_keys,
        }

    st.session_state.pop("global_preset_error", None)

    st.session_state["global_selection"] = list(available_keys)
    st.session_state["global_series_selection"] = list(available_keys)
    st.session_state["global_selection_version"] = st.session_state.get("global_selection_version", 0) + 1
    st.session_state["global_selection_source"] = "preset"

    st.session_state.pop("global_series_tree", None)
    st.session_state["global_tree_version"] = st.session_state.get("global_tree_version", 0) + 1

    custom_labels = {
        key: value
        for key, value in (preset.get("custom_labels") or {}).items()
        if isinstance(key, str) and isinstance(value, str)
    }
    st.session_state["global_custom_labels"] = dict(custom_labels)

    for session_key in list(st.session_state.keys()):
        if session_key.startswith("custom_label_input_"):
            base_key = session_key.replace("custom_label_input_", "", 1)
            if base_key not in custom_labels:
                st.session_state.pop(session_key, None)
    for key, label in custom_labels.items():
        st.session_state[f"custom_label_input_{key}"] = label

    dtype_key = preset.get("global_dtype_key")
    if dtype_key:
        st.session_state["global_dtype_key"] = dtype_key

    per_series_override = bool(preset.get("per_series_override", False))
    st.session_state["global_dtype_override"] = per_series_override

    dtype_map = preset.get("dtype_map", {}) or {}
    for session_key in list(st.session_state.keys()):
        if session_key.startswith("dtype_override_"):
            base_key = session_key.replace("dtype_override_", "", 1)
            if base_key not in dtype_map:
                st.session_state.pop(session_key, None)
    for key, value in dtype_map.items():
        if isinstance(key, str) and isinstance(value, str):
            st.session_state[f"dtype_override_{key}"] = value

    freq_option = preset.get("frequency_option")
    if isinstance(freq_option, str) and freq_option in FREQUENCY_OPTIONS:
        st.session_state["global_frequency_option"] = freq_option

    chart_type_code = preset.get("chart_type")
    if chart_type_code:
        st.session_state["global_chart_type"] = _chart_label_from_type(chart_type_code)

    chart_width = preset.get("chart_width_cm")
    chart_height = preset.get("chart_height_cm")
    if chart_width is not None:
        st.session_state["global_chart_width"] = float(chart_width)
    if chart_height is not None:
        st.session_state["global_chart_height"] = float(chart_height)

    st.session_state["global_single_axis_title"] = preset.get("single_axis_title", "")
    st.session_state["global_left_axis_title"] = preset.get("left_axis_title", "")
    st.session_state["global_right_axis_title"] = preset.get("right_axis_title", "")

    left_offset = preset.get("left_title_offset")
    right_offset = preset.get("right_title_offset")
    st.session_state["global_left_title_offset"] = float(left_offset) if left_offset is not None else 0.0
    st.session_state["global_right_title_offset"] = float(right_offset) if right_offset is not None else 0.0

    dual_left = preset.get("dual_axis_left", []) or []
    dual_right = preset.get("dual_axis_right", []) or []
    st.session_state["global_dual_left"] = list(dual_left)
    st.session_state["global_dual_right"] = list(dual_right)

    five_year_recent = preset.get("five_year_recent_years")
    if five_year_recent is not None:
        st.session_state["global_five_year_recent"] = int(five_year_recent)

    single_manual_range = bool(preset.get("single_axis_manual_range"))
    st.session_state["global_single_axis_manual_range"] = single_manual_range
    if single_manual_range:
        rng_single = preset.get("single_axis_range")
        if isinstance(rng_single, (list, tuple)) and len(rng_single) == 2:
            try:
                st.session_state["global_single_axis_min"] = float(rng_single[0])
                st.session_state["global_single_axis_max"] = float(rng_single[1])
            except (TypeError, ValueError):
                pass

    left_manual_range = bool(preset.get("left_axis_manual_range"))
    st.session_state["global_left_axis_manual_range"] = left_manual_range
    if left_manual_range:
        rng_left = preset.get("left_axis_range")
        if isinstance(rng_left, (list, tuple)) and len(rng_left) == 2:
            try:
                st.session_state["global_left_axis_min"] = float(rng_left[0])
                st.session_state["global_left_axis_max"] = float(rng_left[1])
            except (TypeError, ValueError):
                pass

    right_manual_range = bool(preset.get("right_axis_manual_range"))
    st.session_state["global_right_axis_manual_range"] = right_manual_range
    if right_manual_range:
        rng_right = preset.get("right_axis_range")
        if isinstance(rng_right, (list, tuple)) and len(rng_right) == 2:
            try:
                st.session_state["global_right_axis_min"] = float(rng_right[0])
                st.session_state["global_right_axis_max"] = float(rng_right[1])
            except (TypeError, ValueError):
                pass

    if "phillips_settings" in preset:
        settings = preset["phillips_settings"] or {}
        if "inflation_type" in settings:
            st.session_state["phillips_global_inflation"] = settings["inflation_type"]
        if "color_by_period" in settings:
            st.session_state["phillips_global_color"] = bool(settings["color_by_period"])
        if "show_labels" in settings:
            st.session_state["phillips_global_labels"] = bool(settings["show_labels"])
        if "variant" in settings:
            st.session_state["phillips_global_variant"] = settings["variant"]

    if "beveridge_settings" in preset:
        settings = preset["beveridge_settings"] or {}
        if "color_by_period" in settings:
            st.session_state["beveridge_global_color"] = bool(settings["color_by_period"])
        if "show_labels" in settings:
            st.session_state["beveridge_global_labels"] = bool(settings["show_labels"])
        if "variant" in settings:
            st.session_state["beveridge_global_variant"] = settings["variant"]

    active_keys = preset.get("active_series_keys")
    if isinstance(active_keys, list) and active_keys:
        st.session_state["global_active_series_override"] = list(active_keys)
    else:
        st.session_state.pop("global_active_series_override", None)

    st.session_state["global_preset_loaded"] = preset.get("name")

    _trigger_rerun()
    return True


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
        module.PHILLIPS_DATA['processed_data'] = df_cached
        module.PHILLIPS_DATA['combined_data'] = df_cached
        module.PHILLIPS_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.now(),
            'start_date': df_cached.index.min().strftime('%Y-%m-%d') if not df_cached.empty else None,
            'series_count': df_cached.shape[1],
            'data_points': df_cached.shape[0],
            'source': 'CSV (cache)'
        }
        latest_values = {}
        for col in df_cached.columns:
            if col not in ['period', 'detailed_period'] and not df_cached[col].empty:
                latest_values[col] = {
                    'value': df_cached[col].iloc[-1],
                    'date': df_cached.index[-1].strftime('%Y-%m')
                }
        module.PHILLIPS_DATA['latest_values'] = latest_values

    if hasattr(module, "BEVERIDGE_DATA") and isinstance(cached_df, pd.DataFrame):
        df_cached = cached_df.copy()
        numeric_cols = df_cached.columns.difference(["period", "detailed_period"])
        df_cached[numeric_cols] = df_cached[numeric_cols].apply(pd.to_numeric, errors="coerce")
        module.BEVERIDGE_DATA['raw_data'] = df_cached
        module.BEVERIDGE_DATA['combined_data'] = df_cached.dropna()
        module.BEVERIDGE_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.now(),
            'start_date': df_cached.index.min().strftime('%Y-%m-%d') if not df_cached.empty else None,
            'series_count': df_cached.shape[1],
            'data_points': df_cached.shape[0],
            'source': 'CSV (cache)'
        }
        module.BEVERIDGE_DATA['latest_values'] = {
            col: {'value': df_cached[col].iloc[-1], 'date': df_cached.index[-1].strftime('%Y-%m')}
            for col in df_cached.columns if not df_cached[col].empty
        }

    return data_dict


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
    return "기타"


def build_series_registry(metas: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[str]]:
    category_nodes: dict[str, dict[str, Any]] = {}
    for category in CATEGORY_ORDER:
        category_nodes[category] = {
            "label": category,
            "value": f"category::{category}",
            "children": [],
        }

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
        module_node = {
            "label": module_label,
            "value": f"module::{stem}",
            "children": [],
        }

        series_defs = meta.get("series_defs", {}) or {}
        rename_map = meta.get("rename_map") or {}

        if korean_names and rename_map:
            for raw_key, alias_key in rename_map.items():
                if alias_key not in korean_names and raw_key in korean_names:
                    korean_names[alias_key] = korean_names[raw_key]

        for series in raw_df.columns:
            resolved_key = rename_map.get(series, series)
            available_types: list[str] = []
            for dtype_key, _ in STANDARD_DATA_KEYS:
                df_candidate = data_dict.get(dtype_key)
                if isinstance(df_candidate, pd.DataFrame) and series in df_candidate.columns:
                    available_types.append(dtype_key)
            if not available_types:
                continue

            base_label = (
                korean_names.get(resolved_key)
                or korean_names.get(series)
                or resolved_key
            )
            display_label = f"{module_label} · {base_label}"
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

            module_node["children"].append({"label": display_label, "value": series_key})

            if not default_checked and stem == DEFAULT_MODULE_FOR_SELECTION:
                default_checked.append(series_key)

        if module_node["children"]:
            category_nodes[category]["children"].append(module_node)

    tree_nodes = [category_nodes[cat] for cat in CATEGORY_ORDER if category_nodes[cat]["children"]]

    return registry, tree_nodes, default_checked


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
    combined = pd.concat(frames, axis=1, join='outer').sort_index()
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
    stats["평균"] = numeric.mean(axis=1, skipna=True)
    stats["Min"] = numeric.min(axis=1, skipna=True)
    stats["Min~Max"] = numeric.max(axis=1, skipna=True)
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
        text_val = (ann_dict.get("text") or "").strip()
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


def _render_phillips_curves(
    meta: dict[str, Any],
    chart_width: int,
    chart_height: int,
) -> None:
    module = meta["module"]
    data_dict = ensure_module_data(meta)
    if not _data_dict_ready(data_dict):
        st.info("필립스 커브 데이터를 먼저 로드하세요.")
        return

    raw_df = data_dict.get("raw_data")
    if raw_df is None or raw_df.empty:
        st.info("표시할 필립스 커브 데이터가 없습니다.")
        return

    korean_names = getattr(module, meta.get("korean_name_attr", ""), {})

    inflation_candidates = [
        col
        for col in raw_df.columns
        if col not in {"unemployment_rate", "period", "detailed_period"}
    ]

    st.markdown("#### 필립스 곡선")

    if not inflation_candidates:
        st.warning("사용 가능한 인플레이션 시리즈가 없습니다.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        inflation_choice = st.selectbox(
            "인플레이션 지표",
            inflation_candidates,
            format_func=lambda x: korean_names.get(x, x),
            key="phillips_global_inflation",
        )
    with col2:
        color_by_period = st.checkbox(
            "시기별 색상 구분",
            value=True,
            key="phillips_global_color",
        )
    with col3:
        show_labels = st.checkbox(
            "라벨 표시",
            value=True,
            key="phillips_global_labels",
        )

    variant = st.radio(
        "차트 유형",
        ("기본", "세부"),
        index=0,
        key="phillips_global_variant",
        horizontal=True,
    )

    fig = None
    if variant == "기본":
        func_basic = getattr(module, "create_phillips_curve", None)
        if callable(func_basic):
            fig = func_basic(
                inflation_type=inflation_choice,
                color_by_period=color_by_period,
                show_labels=show_labels,
            )
    else:
        func_detailed = getattr(module, "create_phillips_curve_detailed", None)
        if callable(func_detailed):
            fig = func_detailed(
                inflation_type=inflation_choice,
                show_labels=show_labels,
            )

    if fig is not None:
        fig.update_layout(width=chart_width, height=chart_height)
        st.plotly_chart(_sanitize_plotly_figure(fig), use_container_width=False)
    else:
        st.info("필립스 커브 차트를 생성할 수 없습니다.")

    combined = getattr(module, "PHILLIPS_DATA", {}).get("combined_data")
    if isinstance(combined, pd.DataFrame) and not combined.empty:
        display_df = combined.rename(columns=lambda c: korean_names.get(c, c))
        st.markdown("##### 데이터 테이블")
        st.dataframe(display_df)


def _render_beveridge_curves(
    meta: dict[str, Any],
    chart_width: int,
    chart_height: int,
) -> None:
    module = meta["module"]
    data_dict = ensure_module_data(meta)
    if not _data_dict_ready(data_dict):
        st.info("베버리지 커브 데이터를 먼저 로드하세요.")
        return

    st.markdown("#### 베버리지 곡선")

    color_by_period = st.checkbox(
        "시기별 색상 구분",
        value=True,
        key="beveridge_global_color",
    )
    show_labels = st.checkbox(
        "라벨 표시",
        value=True,
        key="beveridge_global_labels",
    )

    variant = st.radio(
        "차트 유형",
        ("기본", "세부"),
        index=0,
        key="beveridge_global_variant",
        horizontal=True,
    )

    fig = None
    if variant == "기본":
        func_basic = getattr(module, "create_beveridge_curve", None)
        if callable(func_basic):
            fig = func_basic(color_by_period=color_by_period, show_labels=show_labels)
    else:
        func_detailed = getattr(module, "create_beveridge_curve_detailed", None)
        if callable(func_detailed):
            fig = func_detailed(show_labels=show_labels)

    if fig is not None:
        fig.update_layout(width=chart_width, height=chart_height)
        st.plotly_chart(_sanitize_plotly_figure(fig), use_container_width=False)
    else:
        st.info("베버리지 커브 차트를 생성할 수 없습니다.")

    combined = getattr(module, "BEVERIDGE_DATA", {}).get("combined_data")
    if isinstance(combined, pd.DataFrame) and not combined.empty:
        st.markdown("##### 데이터 테이블")
        st.dataframe(combined)


def discover_modules() -> list[dict[str, Any]]:
    module_dir = Path(__file__).parent
    exclude = {"us_eco_utils", "us_eco_dashboard", "cpi_complete_all_series"}
    modules: list[dict[str, Any]] = []
    for path in sorted(module_dir.glob("*.py")):
        stem = path.stem
        if stem.startswith("__") or stem in exclude:
            continue
        modules.append({"stem": stem, "file_path": path})
    return modules


@st.cache_resource(show_spinner=False)
def get_cached_module_metadata(stem: str, file_path_str: str, file_mtime: float) -> dict[str, Any] | None:
    del file_mtime  # used to bust cache when source file updates
    info = {"stem": stem, "file_path": Path(file_path_str)}
    return load_module_metadata(info)


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

    def stub_load(series_dict, data_source='BLS', csv_file_path=None, **kwargs):
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

    def stub_group(series_groups, data_source='FRED', csv_file_path=None, **kwargs):
        return {}

    if use_stub and original_load is not None:
        utils_module.load_economic_data = stub_load  # type: ignore[assignment]
    if use_stub and original_group is not None:
        utils_module.load_economic_data_grouped = stub_group  # type: ignore[assignment]

    try:
        spec.loader.exec_module(module)  # type: ignore[arg-type]
    except Exception as exc:
        print(f"⚠️ {file_path} 로드 중 예외 발생: {exc}")
    finally:
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
        # fallback: accept imported functions if no local load function found
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


def render_global_dashboard(metas: list[dict[str, Any]]) -> None:
    registry, tree_nodes, default_checked = build_series_registry(metas)
    if not registry:
        st.warning("표시할 수 있는 시리즈가 없습니다.")
        return

    presets = load_dashboard_presets()
    preset_names = sorted(presets.keys())

    preset_error = st.session_state.pop("global_preset_error", None)
    if preset_error:
        st.error(preset_error)

    preset_warning_payload = st.session_state.pop("global_preset_warning", None)
    if isinstance(preset_warning_payload, dict):
        missing_list = [
            str(item).replace("::", " · ")
            for item in preset_warning_payload.get("missing", [])
        ]
        if missing_list:
            name = preset_warning_payload.get("name") or "프리셋"
            st.warning(
                f"'{name}' 프리셋에서 일부 시리즈를 찾지 못했습니다: "
                + ", ".join(missing_list)
            )

    tab_labels = ["현재 설정"]
    tab_labels.extend([f"프리셋 · {name}" for name in preset_names])
    tab_labels.append("프리셋 관리")

    tab_refs = st.tabs(tab_labels)
    current_state_tab = tab_refs[0]
    preset_management_tab = tab_refs[-1]
    preset_tab_map = {
        name: tab_refs[idx + 1]
        for idx, name in enumerate(preset_names)
    }

    for preset_name, tab in preset_tab_map.items():
        payload = presets.get(preset_name) or {}
        with tab:
            st.subheader(preset_name)
            saved_at = payload.get("saved_at")
            if saved_at:
                st.caption(f"저장 시각: {saved_at}")
            chart_type_code = payload.get("chart_type")
            if chart_type_code:
                st.write(f"- 차트 유형: {_chart_label_from_type(chart_type_code)}")
            series_keys_snapshot = payload.get("series_keys") or []
            st.write(f"- 저장된 시리즈: {len(series_keys_snapshot)}개")
            series_info_snapshot = payload.get("series_info")
            if isinstance(series_info_snapshot, list) and series_info_snapshot:
                preview_df = pd.DataFrame(series_info_snapshot)
                subset_cols = [
                    col for col in ["표시 이름", "모듈", "데이터 타입"] if col in preview_df.columns
                ]
                if subset_cols:
                    st.dataframe(preview_df[subset_cols], use_container_width=True)
                else:
                    st.dataframe(preview_df, use_container_width=True)
            if st.button("이 프리셋 불러오기", key=f"apply_preset_{preset_name}"):
                payload_copy = dict(payload)
                payload_copy["name"] = preset_name
                apply_dashboard_preset(payload_copy, registry)

    st.sidebar.markdown("### 전역 시리즈 선택")

    stem_to_meta = {meta.get("stem"): meta for meta in metas if meta.get("stem")}

    with st.sidebar.expander("데이터 업데이트", expanded=False):
        module_choices = sorted(
            (
                meta.get("friendly_title", meta.get("stem", "")),
                meta.get("stem") or "",
            )
            for meta in metas
            if meta.get("stem") and meta.get("load_fn_name")
        )

        if module_choices:
            labels = [label for label, _ in module_choices]
            module_map = {label: stem for label, stem in module_choices}

            current_label = st.selectbox(
                "모듈 선택",
                options=labels,
                key="module_update_selection",
            )
            selected_stem = module_map.get(current_label)
            meta_target = stem_to_meta.get(selected_stem) if selected_stem else None

            if meta_target:
                start_key = f"module_refresh_start_{selected_stem}"
                smart_key = f"module_refresh_smart_{selected_stem}"
                force_key = f"module_refresh_force_{selected_stem}"

                default_start_value = st.session_state.get(start_key)
                if isinstance(default_start_value, datetime):
                    default_start_date = default_start_value.date()
                elif isinstance(default_start_value, date):
                    default_start_date = default_start_value
                else:
                    default_start_date = date(2000, 1, 1)

                start_date_input = st.date_input(
                    "시작일",
                    value=default_start_date,
                    key=start_key,
                )

                smart_update_flag = st.checkbox(
                    "스마트 업데이트",
                    value=st.session_state.get(smart_key, True),
                    key=smart_key,
                )
                force_reload_flag = st.checkbox(
                    "강제 재로드",
                    value=st.session_state.get(force_key, False),
                    key=force_key,
                )

                if st.button("선택 모듈 데이터 업데이트", key="module_refresh_trigger"):
                    module_obj = meta_target.get("module")
                    if module_obj is None:
                        st.warning("모듈을 찾을 수 없습니다.")
                    else:
                        _restore_module_loaders(module_obj)
                        ok = call_load_function(
                            module_obj,
                            meta_target.get("load_fn_name"),
                            start_date=start_date_input.strftime("%Y-%m-%d"),
                            smart_update=bool(smart_update_flag),
                            force_reload=bool(force_reload_flag),
                        )
                        if ok:
                            csv_path = meta_target.get("csv_path")
                            if csv_path:
                                load_cached_dataframe.clear()
                                cached = load_cached_dataframe(csv_path)
                                if cached is not None:
                                    rename_map = meta_target.get("rename_map")
                                    data_attr_name = meta_target.get("data_attr_name")
                                    data_dict_from_csv = build_data_dict_from_raw(cached, rename_map=rename_map)
                                    if data_attr_name:
                                        setattr(module_obj, data_attr_name, data_dict_from_csv)
                                    cached_renamed = cached.rename(
                                        columns=lambda c: rename_map.get(c, c) if rename_map else c
                                    )
                                    numeric_cols = cached_renamed.columns.difference(["period", "detailed_period"])
                                    cached_renamed[numeric_cols] = cached_renamed[numeric_cols].apply(
                                        pd.to_numeric, errors="coerce"
                                    )
                                    meta_target["cached_df"] = cached_renamed
                            st.success("데이터가 업데이트되었습니다.")
                            _trigger_rerun()
                        else:
                            st.warning("데이터 업데이트에 실패했습니다. 로그를 확인하세요.")
        else:
            st.info("업데이트할 모듈이 없습니다.")

    session_key = "global_series_selection"
    if session_key not in st.session_state:
        st.session_state[session_key] = []

    if "global_selection" not in st.session_state or not st.session_state["global_selection"]:
        st.session_state["global_selection"] = list(st.session_state[session_key])
    if "global_selection_version" not in st.session_state:
        st.session_state["global_selection_version"] = 0
    if "global_selection_source" not in st.session_state:
        st.session_state["global_selection_source"] = "init"

    selected_series_keys = list(st.session_state["global_selection"]) or []

    tree_widget_key = "global_series_tree"
    tree_version_key = "global_tree_version"

    if tree_version_key not in st.session_state:
        st.session_state[tree_version_key] = 0

    if st.button("선택 초기화", key="global_selection_reset"):
        st.session_state["global_selection"] = []
        st.session_state[session_key] = []
        st.session_state["global_selection_version"] += 1
        st.session_state["global_selection_source"] = "reset"
        st.session_state.pop(tree_widget_key, None)
        st.session_state[tree_version_key] += 1
        selected_series_keys = []

    if TREE_AVAILABLE:
        if st.session_state.get("global_selection_source") != "tree":
            st.session_state.pop(tree_widget_key, None)
        tree_result = tree_select(
            nodes=tree_nodes,
            check_model="leaf",
            only_leaf_checkboxes=True,
            show_expand_all=True,
            checked=selected_series_keys,
            key=f"{tree_widget_key}_{st.session_state[tree_version_key]}",
        )
        selected_from_tree = tree_result.get("checked", [])
        if selected_from_tree is None:
            selected_from_tree = []
        if set(selected_from_tree) != set(st.session_state["global_selection"]):
            st.session_state["global_selection"] = selected_from_tree
            st.session_state["global_selection_version"] += 1
        st.session_state["global_selection_source"] = "tree"
        selected_series_keys = list(st.session_state["global_selection"])
    else:
        st.sidebar.info("streamlit-tree-select 미설치: 기본 멀티 선택 UI 사용")
        label_to_key = {info["display_label"]: info["key"] for info in registry.values()}
        default_labels = [
            registry[key]["display_label"]
            for key in selected_series_keys
            if key in registry
        ]
        selected_labels = st.sidebar.multiselect(
            "시리즈",
            options=sorted(label_to_key.keys()),
            default=default_labels
            or [registry[key]["display_label"] for key in default_checked if key in registry],
            key="global_series_fallback",
        )
        selected_manual = [label_to_key[label] for label in selected_labels]
        if not selected_manual:
            selected_manual = list(default_checked)
        if set(selected_manual) != set(st.session_state["global_selection"]):
            st.session_state["global_selection"] = selected_manual
            st.session_state["global_selection_version"] += 1
        st.session_state["global_selection_source"] = "sidebar"
        selected_series_keys = list(st.session_state["global_selection"])

    st.session_state[session_key] = list(selected_series_keys)

    selected_infos = [registry[key] for key in selected_series_keys if key in registry]
    if not selected_infos:
        st.info("시리즈를 선택하세요.")
        return

    label_order = [info["display_label"] for info in selected_infos]
    label_to_info = {info["display_label"]: info for info in selected_infos}

    custom_label_state = st.session_state.setdefault("global_custom_labels", {})
    active_keys_set = {info["key"] for info in selected_infos}
    for stale_key in list(custom_label_state.keys()):
        if stale_key not in active_keys_set:
            custom_label_state.pop(stale_key, None)
            st.session_state.pop(f"custom_label_input_{stale_key}", None)

    with st.sidebar.expander("시리즈 레이블 편집", expanded=False):
        for info in selected_infos:
            storage_key = info["key"]
            input_key = f"custom_label_input_{storage_key}"
            default_label = custom_label_state.get(storage_key, info["display_label"])
            if input_key not in st.session_state:
                st.session_state[input_key] = default_label
            current_value = st.text_input(
                info["display_label"],
                key=input_key,
            )
            clean_value = current_value.strip()
            if clean_value and clean_value != info["display_label"]:
                custom_label_state[storage_key] = clean_value
            else:
                custom_label_state.pop(storage_key, None)

    effective_label_map = {}
    for info in selected_infos:
        chosen = custom_label_state.get(info["key"], "").strip()
        final_label = chosen if chosen else info["display_label"]
        effective_label_map[info["display_label"]] = final_label
        info["effective_label"] = final_label

    sidebar_key = f"global_active_labels_{st.session_state['global_selection_version']}"
    override_keys = st.session_state.pop("global_active_series_override", None)
    if override_keys:
        override_labels: list[str] = []
        key_to_info = {info["key"]: info for info in selected_infos}
        for series_key in override_keys:
            info = key_to_info.get(series_key)
            if info:
                override_labels.append(info["display_label"])
        if override_labels:
            st.session_state[sidebar_key] = override_labels
    active_labels = st.sidebar.multiselect(
        "현재 선택된 시리즈",
        options=label_order,
        default=label_order,
        key=sidebar_key,
        help="체크 해제하면 해당 시리즈가 제외됩니다.",
    )

    if not active_labels:
        st.sidebar.warning("최소 한 개의 시리즈를 선택하세요.")
        active_labels = label_order

    active_keys = [label_to_info[label]["key"] for label in active_labels if label in label_to_info]
    if not active_keys:
        active_keys = list(default_checked)

    if set(active_keys) != set(selected_series_keys):
        st.session_state["global_selection"] = active_keys
        st.session_state["global_selection_version"] += 1
        st.session_state["global_selection_source"] = "sidebar"
        st.session_state.pop(tree_widget_key, None)
        st.session_state[session_key] = list(active_keys)
        selected_series_keys = list(active_keys)
        selected_infos = [registry[key] for key in selected_series_keys if key in registry]
        if not selected_infos:
            st.info("선택된 시리즈가 없습니다.")
            return
        label_order = [info["display_label"] for info in selected_infos]
        label_to_info = {info["display_label"]: info for info in selected_infos}

    label_order = [info["display_label"] for info in selected_infos]
    label_to_info = {info["display_label"]: info for info in selected_infos}


    data_type_labels = {key: label for key, label in STANDARD_DATA_KEYS}
    available_union = [
        key
        for key, _ in STANDARD_DATA_KEYS
        if any(key in info["available_types"] for info in selected_infos)
    ]
    if not available_union:
        st.warning("선택한 시리즈에서 사용할 수 있는 데이터 타입이 없습니다.")
        return

    default_dtype_key = "raw_data" if "raw_data" in available_union else available_union[0]
    global_dtype_key = st.sidebar.selectbox(
        "기본 데이터 타입",
        options=available_union,
        index=available_union.index(default_dtype_key),
        format_func=lambda key: data_type_labels.get(key, key),
        key="global_dtype_key",
    )

    per_series_override = False
    if len(available_union) > 1 and len(selected_infos) > 1:
        per_series_override = st.sidebar.checkbox(
            "시리즈별 데이터 타입 지정",
            value=False,
            key="global_dtype_override",
        )

    if "global_frequency_option" not in st.session_state:
        st.session_state["global_frequency_option"] = FREQUENCY_OPTIONS[0]
    frequency_option = st.sidebar.selectbox(
        "주기 정규화",
        options=FREQUENCY_OPTIONS,
        index=FREQUENCY_OPTIONS.index(st.session_state.get("global_frequency_option", FREQUENCY_OPTIONS[0])),
        key="global_frequency_option",
        help="주간 데이터를 월말 값으로 변환해 월간 지표와 함께 비교할 수 있습니다.",
    )

    dtype_map: dict[str, str] = {}
    fallback_messages: list[str] = []
    if per_series_override:
        st.sidebar.markdown("#### 시리즈 데이터 타입")
        for info in selected_infos:
            allowed_options = [
                key for key, _ in STANDARD_DATA_KEYS if key in info["available_types"]
            ]
            if not allowed_options:
                continue
            default_index = (
                allowed_options.index(global_dtype_key)
                if global_dtype_key in allowed_options
                else 0
            )
            dtype_choice = st.sidebar.selectbox(
                info["display_label"],
                options=allowed_options,
                index=default_index,
                format_func=lambda key, labels=data_type_labels: labels.get(key, key),
                key=f"dtype_override_{info['key']}",
            )
            dtype_map[info["key"]] = dtype_choice
    else:
        for info in selected_infos:
            dtype_choice = (
                global_dtype_key
                if global_dtype_key in info["available_types"]
                else _default_dtype_for_series(info["available_types"])
            )
            if not dtype_choice:
                continue
            if dtype_choice != global_dtype_key:
                fallback_messages.append(
                    f"{info['display_label']} → {data_type_labels.get(dtype_choice, dtype_choice)}"
                )
            dtype_map[info["key"]] = dtype_choice

    if not dtype_map:
        st.warning("선택한 시리즈에 대한 데이터 타입을 결정할 수 없습니다.")
        return

    combined_df, _ = build_combined_dataframe(selected_infos, dtype_map)
    if effective_label_map:
        combined_df = combined_df.rename(columns=effective_label_map)
    if combined_df.empty:
        st.warning("선택한 시리즈 조합에 대한 데이터가 없습니다.")
        return

    frequency_option = st.session_state.get("global_frequency_option")

    if isinstance(combined_df.index, pd.DatetimeIndex) and frequency_option == FREQUENCY_OPTIONS[1]:
        raw_map = {info["key"]: "raw_data" for info in selected_infos}
        raw_df, _ = build_combined_dataframe(selected_infos, raw_map)
        if effective_label_map:
            raw_df = raw_df.rename(columns=effective_label_map)
        raw_df = raw_df.resample("M").last()

        raw_df = raw_df.dropna(how="all")
        raw_df = raw_df.dropna(axis=1, how="all")
        if raw_df.empty:
            st.warning("주간 데이터를 월 기준으로 변환한 결과가 비어 있습니다.")
            return

        mom_df = calculate_mom_percent(raw_df)
        mom_change_df = calculate_mom_change(raw_df)
        yoy_df = calculate_yoy_percent(raw_df)
        yoy_change_df = calculate_yoy_change(raw_df)

        converted = pd.DataFrame(index=raw_df.index)
        for info in selected_infos:
            label = info.get("effective_label", info["display_label"])
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
        combined_df = converted

    combined_df = combined_df.dropna(how="all")
    combined_df = combined_df.dropna(axis=1, how="all")
    if combined_df.empty:
        st.warning("선택한 시리즈의 유효한 데이터가 없습니다.")
        return

    label_replacements: dict[str, str] = {}
    missing_series_labels: list[str] = []
    for info in selected_infos:
        base_label = info.get("display_label")
        effective_label = info.get("effective_label", base_label)
        if effective_label in combined_df.columns:
            continue
        if base_label and base_label in combined_df.columns:
            if effective_label and effective_label != base_label:
                label_replacements[base_label] = effective_label
            continue
        missing_series_labels.append(effective_label or base_label or info.get("series_label", ""))

    if label_replacements:
        combined_df = combined_df.rename(columns=label_replacements)

    valid_labels = set(combined_df.columns)
    selected_infos = [info for info in selected_infos if info.get("effective_label") in valid_labels]
    if not selected_infos:
        if missing_series_labels:
            pretty_missing = ", ".join(sorted({label for label in missing_series_labels if label}))
            st.warning(
                "다음 시리즈는 데이터가 비어 있거나 매칭되지 않습니다: "
                + (pretty_missing if pretty_missing else "(알 수 없음)")
            )
        else:
            st.warning("선택한 시리즈의 데이터가 모두 비어 있습니다.")
        return

    if fallback_messages:
        st.sidebar.caption(
            "기본 타입을 지원하지 않는 시리즈는 다음 타입으로 대체되었습니다:\n- "
            + "\n- ".join(fallback_messages)
        )

    frequency_map: dict[str, str] = {}
    for info in selected_infos:
        label = info.get("effective_label", info["display_label"])
        series = combined_df.get(label)
        if series is None:
            continue
        inferred = _infer_series_period(series.dropna())
        frequency_map[label] = inferred

    display_labels = list(combined_df.columns)
    series_type_map = {info.get("effective_label", info["display_label"]): dtype_map.get(info["key"], "") for info in selected_infos}
    dtype_set = {series_type_map[label] for label in display_labels if series_type_map.get(label)}

    axis_label_default = compute_axis_label(selected_infos, dtype_map)
    if not axis_label_default and len(dtype_set) == 1:
        axis_label_default = data_type_labels.get(next(iter(dtype_set)), "")

    selected_stems = {info["stem"] for info in selected_infos}
    has_phillips = "phillips_curve_enhanced" in selected_stems
    has_beveridge = "beveridge_curve_enhanced" in selected_stems
    special_present = has_phillips or has_beveridge

    if special_present:
        general_tab, special_tab = st.tabs(["일반 차트", "특수 차트"])
        general_container = general_tab
    else:
        general_container = st.container()
        special_tab = None

    with general_container:
        st.subheader("선택된 시리즈")
        summary_rows = []
        for info in selected_infos:
            dtype_key = dtype_map.get(info["key"], "")
            summary_rows.append(
                {
                    "대분류": info.get("category", ""),
                    "모듈": info["module_label"],
                    "시리즈": info["series_label"],
                    "표시 이름": info.get("effective_label", info["display_label"]),
                    "데이터 타입": data_type_labels.get(dtype_key, dtype_key),
                    "단위": info.get("unit", ""),
                    "원본 ID": info["series_name"],
                }
            )
        summary_df = pd.DataFrame(summary_rows)
        st.dataframe(summary_df)

        st.sidebar.markdown("### 시각화 설정")
        chart_type_name = st.sidebar.selectbox(
            "차트 유형",
            list(CHART_TYPE_LABELS.keys()),
            index=0,
            key="global_chart_type",
        )
        chart_type = CHART_TYPE_LABELS.get(chart_type_name, "multi_line")

        auto_frequency_split = False
        auto_axis_allocation: dict[str, list[str]] | None = None
        if (
            frequency_option == FREQUENCY_OPTIONS[0]
            and chart_type == "multi_line"
            and len(display_labels) > 1
        ):
            weekly_labels = [label for label in display_labels if frequency_map.get(label) == "week"]
            non_weekly_labels = [label for label in display_labels if label not in weekly_labels]
            if weekly_labels and non_weekly_labels:
                auto_frequency_split = True
                auto_axis_allocation = {
                    "left": non_weekly_labels,
                    "right": weekly_labels,
                }
                chart_type = "dual_axis"

        chart_width_cm = st.sidebar.slider(
            "차트 너비 (cm)",
            min_value=15.0,
            max_value=45.0,
            value=24.0,
            step=0.5,
            key="global_chart_width",
        )
        chart_height_cm = st.sidebar.slider(
            "차트 높이 (cm)",
            min_value=10.0,
            max_value=25.0,
            value=12.0,
            step=0.5,
            key="global_chart_height",
        )
        chart_width = int(chart_width_cm * PX_PER_CM)
        chart_height = int(chart_height_cm * PX_PER_CM)

        custom_single_axis_title = ""
        custom_left_axis_title = ""
        custom_right_axis_title = ""
        left_title_offset = 0.0
        right_title_offset = 0.0

        if chart_type in {"multi_line", "single_line", "horizontal_bar", "vertical_bar", "five_year"}:
            custom_single_axis_title = st.sidebar.text_input(
                "축 제목 (단위 Annotation)",
                value="",
                key="global_single_axis_title",
            )

        preset_left = auto_axis_allocation["left"] if auto_axis_allocation else None
        preset_right = auto_axis_allocation["right"] if auto_axis_allocation else None

        axis_allocation = {"left": [], "right": []}
        if chart_type == "dual_axis":
            custom_left_axis_title = st.sidebar.text_input(
                "왼쪽 축 제목",
                value="",
                key="global_left_axis_title",
            )
            custom_right_axis_title = st.sidebar.text_input(
                "오른쪽 축 제목",
                value="",
                key="global_right_axis_title",
            )
            left_title_offset = st.sidebar.slider(
                "왼쪽 축 제목 위치 보정",
                min_value=-0.2,
                max_value=0.2,
                value=0.0,
                step=0.01,
                key="global_left_title_offset",
            )
            right_title_offset = st.sidebar.slider(
                "오른쪽 축 제목 위치 보정",
                min_value=-0.2,
                max_value=0.2,
                value=0.0,
                step=0.01,
                key="global_right_title_offset",
            )
            if len(display_labels) < 2:
                st.warning("이중 축 차트는 2개 이상의 시리즈가 필요합니다. 다른 차트 유형을 선택하세요.")
            if preset_left is not None and preset_right is not None:
                default_left = [label for label in preset_left if label in display_labels]
                default_right = [label for label in preset_right if label in display_labels]
            else:
                default_left = display_labels[: max(1, len(display_labels) // 2)]
                default_right = [label for label in display_labels if label not in default_left]
            if not default_right and len(display_labels) > 1:
                default_right = [display_labels[-1]]
                default_left = [label for label in display_labels if label != default_right[0]]
            
            left_default_clean = [label for label in default_left if label in display_labels]
            if not left_default_clean and display_labels:
                left_default_clean = [display_labels[0]]

            left_selected = st.sidebar.multiselect(
                "왼쪽 축 시리즈",
                display_labels,
                default=left_default_clean,
                key="global_dual_left",
            )
            right_candidates = [label for label in display_labels if label not in left_selected]
            right_options = right_candidates if right_candidates else display_labels
            right_default_clean = [label for label in default_right if label in right_options]
            if not right_default_clean and right_options:
                fallback_label = next((label for label in right_options if label not in left_selected), None)
                right_default_clean = [fallback_label] if fallback_label else right_options[:1]

            right_selected = st.sidebar.multiselect(
                "오른쪽 축 시리즈",
                right_options,
                default=right_default_clean,
                key="global_dual_right",
            )
            left_clean = [label for label in left_selected if label in display_labels]
            right_clean = [label for label in right_selected if label in display_labels and label not in left_clean]
            if not left_clean and display_labels:
                left_clean = [display_labels[0]]
            if not right_clean and len(display_labels) > 1:
                right_clean = [label for label in display_labels if label not in left_clean][:1]
            axis_allocation = {"left": left_clean, "right": right_clean}

        single_axis_range: list[float] | None = None
        left_axis_range_override: list[float] | None = None
        right_axis_range_override: list[float] | None = None

        if chart_type == "dual_axis":
            left_auto_range = _compute_axis_range(
                [combined_df[col] for col in axis_allocation.get("left", []) if col in combined_df.columns]
            ) or [0.0, 1.0]
            right_auto_range = _compute_axis_range(
                [combined_df[col] for col in axis_allocation.get("right", []) if col in combined_df.columns]
            ) or [0.0, 1.0]

            left_manual = st.sidebar.checkbox(
                "왼쪽 축 범위 직접 설정",
                value=False,
                key="global_left_axis_manual_range",
            )
            if left_manual:
                default_left_min = st.session_state.get("global_left_axis_min", left_auto_range[0])
                default_left_max = st.session_state.get("global_left_axis_max", left_auto_range[1])
                left_min = st.sidebar.number_input(
                    "왼쪽 축 최소값",
                    value=float(default_left_min),
                    step=0.1,
                    key="global_left_axis_min",
                )
                left_max = st.sidebar.number_input(
                    "왼쪽 축 최대값",
                    value=float(default_left_max if default_left_max is not None else left_min + 1),
                    step=0.1,
                    key="global_left_axis_max",
                )
                if left_min < left_max:
                    left_axis_range_override = [float(left_min), float(left_max)]
                else:
                    st.sidebar.warning("왼쪽 축 최소/최대값을 확인하세요.")

            right_manual = st.sidebar.checkbox(
                "오른쪽 축 범위 직접 설정",
                value=False,
                key="global_right_axis_manual_range",
            )
            if right_manual:
                default_right_min = st.session_state.get("global_right_axis_min", right_auto_range[0])
                default_right_max = st.session_state.get("global_right_axis_max", right_auto_range[1])
                right_min = st.sidebar.number_input(
                    "오른쪽 축 최소값",
                    value=float(default_right_min),
                    step=0.1,
                    key="global_right_axis_min",
                )
                right_max = st.sidebar.number_input(
                    "오른쪽 축 최대값",
                    value=float(default_right_max if default_right_max is not None else right_min + 1),
                    step=0.1,
                    key="global_right_axis_max",
                )
                if right_min < right_max:
                    right_axis_range_override = [float(right_min), float(right_max)]
                else:
                    st.sidebar.warning("오른쪽 축 최소/최대값을 확인하세요.")
        else:
            auto_range = _compute_axis_range([combined_df[col] for col in combined_df.columns]) or [0.0, 1.0]
            manual_enabled = st.sidebar.checkbox(
                "Y축 범위 직접 설정",
                value=False,
                key="global_single_axis_manual_range",
            )
            if manual_enabled:
                default_min = st.session_state.get("global_single_axis_min", auto_range[0])
                default_max = st.session_state.get("global_single_axis_max", auto_range[1])
                axis_min_val = st.sidebar.number_input(
                    "Y축 최소값",
                    value=float(default_min),
                    step=0.1,
                    key="global_single_axis_min",
                )
                axis_max_val = st.sidebar.number_input(
                    "Y축 최대값",
                    value=float(default_max if default_max is not None else axis_min_val + 1),
                    step=0.1,
                    key="global_single_axis_max",
                )
                if axis_min_val < axis_max_val:
                    single_axis_range = [float(axis_min_val), float(axis_max_val)]
                else:
                    st.sidebar.warning("Y축 최소/최대값을 확인하세요.")

        five_year_recent_years: int | None = None
        if chart_type == "five_year":
            five_year_recent_years = st.sidebar.slider(
                "최근 비교 연도 수",
                min_value=3,
                max_value=6,
                value=5,
                step=1,
                key="global_five_year_recent",
            )

        single_axis_title = (custom_single_axis_title or axis_label_default or "").strip()
        zero_line_types = {"mom_data", "yoy_data", "mom_change", "yoy_change"}
        zero_line = bool(display_labels) and all(
            series_type_map.get(label) in zero_line_types for label in display_labels
        )

        series_info_snapshot: list[dict[str, Any]] = []
        series_labels_map: dict[str, str] = {}
        for info in selected_infos:
            dtype_key = dtype_map.get(info["key"], "")
            entry = {
                "key": info["key"],
                "표시 이름": info.get("effective_label", info["display_label"]),
                "모듈": info["module_label"],
                "대분류": info.get("category", ""),
                "데이터 타입": data_type_labels.get(dtype_key, dtype_key),
                "원본 ID": info["series_name"],
            }
            unit_value = info.get("unit")
            if unit_value:
                entry["단위"] = unit_value
            series_info_snapshot.append(entry)
            series_labels_map[info["key"]] = entry["표시 이름"]

        snapshot = {
            "series_keys": list(selected_series_keys),
            "series_labels": series_labels_map,
            "series_info": series_info_snapshot,
            "custom_labels": dict(custom_label_state),
            "global_dtype_key": global_dtype_key,
            "per_series_override": per_series_override,
            "dtype_map": {key: dtype_map.get(key, "") for key in series_labels_map},
            "series_type_map": series_type_map,
            "chart_type": chart_type,
            "chart_type_label": chart_type_name,
            "chart_width_cm": float(chart_width_cm),
            "chart_height_cm": float(chart_height_cm),
            "single_axis_title": custom_single_axis_title,
            "left_axis_title": custom_left_axis_title,
            "right_axis_title": custom_right_axis_title,
            "left_title_offset": left_title_offset,
            "right_title_offset": right_title_offset,
            "dual_axis_left": list(axis_allocation.get("left", [])),
            "dual_axis_right": list(axis_allocation.get("right", [])),
            "five_year_recent_years": five_year_recent_years,
            "active_series_keys": list(selected_series_keys),
            "display_labels": list(display_labels),
            "axis_label_default": axis_label_default,
            "single_axis_manual_range": False,
            "single_axis_range": None,
            "left_axis_manual_range": False,
            "left_axis_range": None,
            "right_axis_manual_range": False,
            "right_axis_range": None,
            "frequency_option": frequency_option,
        }

        if chart_type != "dual_axis":
            single_manual_flag = bool(st.session_state.get("global_single_axis_manual_range"))
            snapshot["single_axis_manual_range"] = single_manual_flag
            if single_manual_flag:
                min_val = st.session_state.get("global_single_axis_min")
                max_val = st.session_state.get("global_single_axis_max")
                if min_val is not None and max_val is not None:
                    snapshot["single_axis_range"] = [float(min_val), float(max_val)]
        else:
            left_manual_flag = bool(st.session_state.get("global_left_axis_manual_range"))
            right_manual_flag = bool(st.session_state.get("global_right_axis_manual_range"))
            snapshot["left_axis_manual_range"] = left_manual_flag
            snapshot["right_axis_manual_range"] = right_manual_flag
            if left_manual_flag:
                left_min_val = st.session_state.get("global_left_axis_min")
                left_max_val = st.session_state.get("global_left_axis_max")
                if left_min_val is not None and left_max_val is not None:
                    snapshot["left_axis_range"] = [float(left_min_val), float(left_max_val)]
            if right_manual_flag:
                right_min_val = st.session_state.get("global_right_axis_min")
                right_max_val = st.session_state.get("global_right_axis_max")
                if right_min_val is not None and right_max_val is not None:
                    snapshot["right_axis_range"] = [float(right_min_val), float(right_max_val)]

        if has_phillips:
            snapshot["phillips_settings"] = {
                "inflation_type": st.session_state.get("phillips_global_inflation"),
                "color_by_period": st.session_state.get("phillips_global_color"),
                "show_labels": st.session_state.get("phillips_global_labels"),
                "variant": st.session_state.get("phillips_global_variant"),
            }
        if has_beveridge:
            snapshot["beveridge_settings"] = {
                "color_by_period": st.session_state.get("beveridge_global_color"),
                "show_labels": st.session_state.get("beveridge_global_labels"),
                "variant": st.session_state.get("beveridge_global_variant"),
            }

        snapshot["generated_at"] = datetime.now().isoformat(timespec="seconds")
        st.session_state["global_last_snapshot"] = snapshot

        fig: go.Figure | None = None
        table_df: pd.DataFrame | None = combined_df.copy()

        if chart_type == "multi_line":
            fig = _create_single_axis_line_chart(
                combined_df, single_axis_title, chart_width, chart_height, zero_line, frequency_map
            )
        elif chart_type == "single_line":
            if len(display_labels) > 1:
                st.warning("단일 라인 차트는 1개 시리즈만 표시합니다. 첫 번째 시리즈만 사용합니다.")
            single_df = combined_df[[display_labels[0]]] if display_labels else combined_df
            fig = _create_single_axis_line_chart(
                single_df, single_axis_title, chart_width, chart_height, zero_line, frequency_map
            )
        elif chart_type == "horizontal_bar":
            latest_values = pd.Series(
                {
                    label: combined_df[label].dropna().iloc[-1]
                    for label in display_labels
                    if not combined_df[label].dropna().empty
                }
            )
            fig = _create_horizontal_bar_chart_simple(
                latest_values, single_axis_title, chart_width, chart_height
            )
        elif chart_type == "vertical_bar":
            fig = _create_vertical_bar_chart_simple(
                combined_df, single_axis_title, chart_width, chart_height
            )
        elif chart_type == "five_year":
            if len(display_labels) != 1:
                st.warning("5년 비교 차트는 한 개의 시리즈만 지원합니다.")
                fig = None
            else:
                series_label = display_labels[0]
                fig, formatted, period_type = _create_five_year_chart(
                    series=combined_df[series_label],
                    series_name=series_label,
                    unit_label=single_axis_title,
                    recent_years=five_year_recent_years or 5,
                    chart_width=chart_width,
                    chart_height=chart_height,
                )
                if formatted is not None:
                    formatted = formatted.copy()
                    if period_type == "month":
                        month_labels = [
                            "Jan",
                            "Feb",
                            "Mar",
                            "Apr",
                            "May",
                            "Jun",
                            "Jul",
                            "Aug",
                            "Sep",
                            "Oct",
                            "Nov",
                            "Dec",
                        ]
                        formatted.index = [
                            month_labels[i - 1] if 1 <= i <= 12 else str(i)
                            for i in formatted.index
                        ]
                        formatted.index.name = "월"
                    else:
                        formatted.index = [f"W{i:02d}" for i in formatted.index]
                        formatted.index.name = "주"
                table_df = formatted
        elif chart_type == "dual_axis":
            if len(display_labels) < 2:
                st.warning("이중 축 차트는 2개 이상의 시리즈가 필요합니다.")
            elif not axis_allocation["left"] or not axis_allocation["right"]:
                st.warning("왼쪽/오른쪽 축에 최소 한 개 이상의 시리즈를 배치하세요.")
            else:
                left_infos = [
                    info
                    for info in selected_infos
                    if info.get("effective_label", info["display_label"]) in axis_allocation["left"]
                ]
                right_infos = [
                    info
                    for info in selected_infos
                    if info.get("effective_label", info["display_label"]) in axis_allocation["right"]
                ]
                left_label_default = compute_axis_label(left_infos, dtype_map)
                right_label_default = compute_axis_label(right_infos, dtype_map)
                left_dtype_keys = {
                    series_type_map.get(label)
                    for label in axis_allocation["left"]
                    if series_type_map.get(label)
                }
                right_dtype_keys = {
                    series_type_map.get(label)
                    for label in axis_allocation["right"]
                    if series_type_map.get(label)
                }
                if not left_label_default and len(left_dtype_keys) == 1:
                    left_label_default = data_type_labels.get(next(iter(left_dtype_keys)), "")
                if not right_label_default and len(right_dtype_keys) == 1:
                    right_label_default = data_type_labels.get(next(iter(right_dtype_keys)), "")
                selected_dtype_keys = {
                    series_type_map.get(label)
                    for label in display_labels
                    if series_type_map.get(label)
                }
                base_data_type = (
                    _dtype_key_to_axis(next(iter(selected_dtype_keys)))
                    if len(selected_dtype_keys) == 1
                    else "raw"
                )
                series_defs = {
                    info.get("effective_label", info["display_label"]): {"unit": info.get("unit", "")}
                    for info in selected_infos
                }
                fig = _create_dual_axis_chart(
                    df=combined_df,
                    axis_allocation=axis_allocation,
                    label_map={label: label for label in display_labels},
                    data_type=base_data_type,
                    series_defs=series_defs,
                    chart_width=chart_width,
                    chart_height=chart_height,
                    left_title_offset=left_title_offset,
                    right_title_offset=right_title_offset,
                    left_axis_data_type=_dtype_key_to_axis(next(iter(left_dtype_keys)))
                    if len(left_dtype_keys) == 1
                    else None,
                    right_axis_data_type=_dtype_key_to_axis(next(iter(right_dtype_keys)))
                    if len(right_dtype_keys) == 1
                    else None,
                    left_title_override=(custom_left_axis_title or left_label_default or None),
                    right_title_override=(custom_right_axis_title or right_label_default or None),
                    left_axis_range_override=left_axis_range_override,
                    right_axis_range_override=right_axis_range_override,
                    connect_map=frequency_map,
                )

        if fig is not None:
            if chart_type != "dual_axis" and single_axis_range:
                if chart_type == "horizontal_bar":
                    fig.update_xaxes(range=single_axis_range)
                else:
                    fig.update_yaxes(range=single_axis_range)
            st.plotly_chart(fig, use_container_width=False)
        else:
            st.info("차트를 생성할 수 없습니다. 선택한 설정을 확인하세요.")

        if table_df is None or (isinstance(table_df, pd.DataFrame) and table_df.empty):
            st.warning("표시할 데이터가 없습니다.")
            return

        st.markdown("## 데이터 테이블")
        display_table = table_df.copy() if isinstance(table_df, pd.DataFrame) else pd.DataFrame(table_df)
        if isinstance(display_table.index, pd.DatetimeIndex):
            display_table.index.name = "날짜"
        st.dataframe(display_table)

        csv_bytes = display_table.to_csv(index=True).encode("utf-8-sig")
        st.download_button(
            label="CSV 다운로드",
            data=csv_bytes,
            file_name=f"us_eco_global_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )

    if special_tab is not None:
        with special_tab:
            st.subheader("특수 차트")
            rendered_any = False
            if has_phillips:
                meta_ph = stem_to_meta.get("phillips_curve_enhanced")
                if meta_ph:
                    _render_phillips_curves(meta_ph, chart_width, chart_height)
                    rendered_any = True
            if has_beveridge:
                meta_bev = stem_to_meta.get("beveridge_curve_enhanced")
                if meta_bev:
                    _render_beveridge_curves(meta_bev, chart_width, chart_height)
                    rendered_any = True
            if not rendered_any:
                st.info("필립스/베버리지 모듈을 선택하면 특수 차트를 볼 수 있습니다.")

    snapshot = st.session_state.get("global_last_snapshot")

    with current_state_tab:
        st.markdown("#### 현재 구성 요약")
        if not snapshot or not snapshot.get("series_keys"):
            st.info("시리즈를 선택하면 요약이 표시됩니다.")
        else:
            chart_label = snapshot.get("chart_type_label") or _chart_label_from_type(snapshot.get("chart_type", ""))
            st.caption(
                f"시리즈 {len(snapshot['series_keys'])}개 · 차트 유형: {chart_label}"
            )
            info_rows = snapshot.get("series_info") or []
            if info_rows:
                info_df = pd.DataFrame(info_rows)
                display_cols = [
                    col for col in ["표시 이름", "모듈", "데이터 타입", "단위"] if col in info_df.columns
                ]
                st.dataframe(
                    info_df[display_cols] if display_cols else info_df,
                    use_container_width=True,
                )
            dtype_label = snapshot.get("global_dtype_key") or ""
            per_series_flag = "사용" if snapshot.get("per_series_override") else "미사용"
            st.caption(f"기본 데이터 타입: {dtype_label} · 시리즈별 지정: {per_series_flag}")
            if snapshot.get("custom_labels"):
                st.caption("커스텀 레이블 적용됨")
            last_loaded = st.session_state.get("global_preset_loaded")
            if last_loaded:
                st.caption(f"최근 불러온 프리셋: {last_loaded}")

    with preset_management_tab:
        st.markdown("#### 프리셋 저장")
        presets_current = load_dashboard_presets()
        if not snapshot or not snapshot.get("series_keys"):
            st.info("저장할 설정이 없습니다. 시리즈와 차트 옵션을 먼저 구성하세요.")
        else:
            default_name = st.session_state.get("preset_save_name", "")
            preset_name_input = st.text_input("프리셋 이름", value=default_name, key="preset_save_name")
            overwrite_allowed = st.checkbox(
                "동일 이름 덮어쓰기 허용",
                value=True,
                key="preset_overwrite_allowed",
            )
            if st.button("현재 설정 저장", key="preset_save_button"):
                preset_name = preset_name_input.strip()
                if not preset_name:
                    st.warning("프리셋 이름을 입력하세요.")
                elif not overwrite_allowed and preset_name in presets_current:
                    st.warning("이미 존재하는 이름입니다. 다른 이름을 사용하거나 덮어쓰기를 허용하세요.")
                else:
                    payload_to_save = dict(snapshot)
                    payload_to_save["saved_at"] = datetime.now().isoformat(timespec="seconds")
                    payload_to_save["name"] = preset_name
                    presets_current[preset_name] = payload_to_save
                    save_dashboard_presets(presets_current)
                    st.success(f"'{preset_name}' 프리셋을 저장했습니다.")
                    _trigger_rerun()

        st.markdown("#### 프리셋 삭제")
        current_names = sorted(presets_current.keys())
        if current_names:
            delete_choice = st.selectbox(
                "삭제할 프리셋",
                current_names,
                key="preset_delete_choice",
            )
            if st.button("선택한 프리셋 삭제", key="preset_delete_button"):
                presets_current.pop(delete_choice, None)
                save_dashboard_presets(presets_current)
                st.success(f"'{delete_choice}' 프리셋을 삭제했습니다.")
                _trigger_rerun()
        else:
            st.info("저장된 프리셋이 없습니다.")


def call_load_function(module, fn_name: str, start_date: str, smart_update: bool, force_reload: bool) -> bool:
    load_fn = getattr(module, fn_name)
    kwargs = {}
    sig = inspect.signature(load_fn)
    if "start_date" in sig.parameters:
        kwargs["start_date"] = start_date
    if "smart_update" in sig.parameters:
        kwargs["smart_update"] = smart_update
    if "force_reload" in sig.parameters:
        kwargs["force_reload"] = force_reload
    try:
        result = load_fn(**kwargs)
    except Exception as exc:
        st.error(f"데이터 로드 중 오류 발생: {exc}")
        return False
    return bool(result)


def module_page(meta: dict[str, Any]) -> None:
    module = meta["module"]
    _restore_module_loaders(module)
    if "file_path" not in meta or not meta["file_path"]:
        file_path_obj = Path(getattr(module, "__file__", ""))
        if file_path_obj.exists():
            meta["file_path"] = str(file_path_obj)
    load_fn_name = meta["load_fn_name"]
    data_attr_name = meta["data_attr_name"]
    korean_name_attr = meta["korean_name_attr"]
    csv_path = meta.get("csv_path")

    st.subheader(meta["friendly_title"])

    if module.__doc__:
        doc_lines = [line.strip() for line in module.__doc__.splitlines() if line.strip()]
        if doc_lines:
            st.caption(doc_lines[0])

    with st.sidebar:
        st.markdown(f"### {meta['friendly_title']} 설정")
        default_start = st.session_state.get("start_date", datetime(2015, 1, 1))
        start_date = st.date_input(
            "시작일",
            value=default_start,
            key=f"start_{meta['module_path']}",
        )
        smart_update = st.checkbox(
            "스마트 업데이트 사용",
            value=True,
            key=f"smart_{meta['module_path']}",
        )
        force_reload = st.checkbox(
            "강제 재로드",
            value=False,
            key=f"force_{meta['module_path']}",
        )

        refresh_clicked = st.button("데이터 새로고침", key=f"refresh_{meta['module_path']}")

    if refresh_clicked:
        file_path_str = meta.get("file_path")
        if file_path_str:
            module_name = meta.get("module_path") or f"us_eco_dynamic_{Path(file_path_str).stem}"
            if module_name:
                sys.modules.pop(module_name, None)
            module = load_module_from_path(
                module_name,
                Path(file_path_str),
                use_stub=False,
            )
            meta["module"] = module
            _restore_module_loaders(module)
        ok = call_load_function(
            module,
            load_fn_name,
            start_date=start_date.strftime("%Y-%m-%d"),
            smart_update=smart_update,
            force_reload=force_reload,
        )
        if ok:
            if csv_path:
                load_cached_dataframe.clear()
                cached = load_cached_dataframe(csv_path)
                if cached is not None:
                    rename_map = meta.get("rename_map")
                    data_dict_from_csv = build_data_dict_from_raw(cached, rename_map=rename_map)
                    setattr(module, data_attr_name, data_dict_from_csv)
                    cached_renamed = cached.rename(columns=lambda c: rename_map.get(c, c) if rename_map else c)
                    numeric_cols = cached_renamed.columns.difference(["period", "detailed_period"])
                    cached_renamed[numeric_cols] = cached_renamed[numeric_cols].apply(pd.to_numeric, errors="coerce")
                    meta["cached_df"] = cached_renamed
            st.success("데이터 로드 완료")
        else:
            st.warning("데이터 로드에 실패했거나 변경 사항이 없습니다.")

    data_dict = ensure_module_data(meta)
    if not _data_dict_ready(data_dict):
        st.info("데이터가 로드되지 않았습니다. 상단 버튼으로 먼저 데이터를 불러오세요.")
        return

    series_defs = {}
    if hasattr(module, "ALL_SERIES"):
        series_defs = getattr(module, "ALL_SERIES")
    elif hasattr(module, "ALL_ADP_SERIES"):
        series_defs = getattr(module, "ALL_ADP_SERIES")

    raw_df = data_dict["raw_data"]

    series_options = raw_df.columns.tolist()
    if not series_options:
        st.warning("표시할 시리즈가 없습니다.")
        return

    korean_names = getattr(module, korean_name_attr, {}) if korean_name_attr else {}

    phillips_inflation = None
    phillips_show_labels = True
    phillips_color_by = True
    bev_show_labels = True
    bev_color_by = True

    per_series_override = False
    series_type_map: dict[str, str] = {}
    custom_left_axis_title = ""
    custom_right_axis_title = ""
    custom_single_axis_title = ""

    with st.sidebar:
        st.markdown("### 시각화 설정")

        if TREE_AVAILABLE:
            nodes = [
                {
                    "label": meta["friendly_title"],
                    "value": meta["module_path"],
                    "children": [
                        {
                            "label": korean_names.get(series, series),
                            "value": series,
                        }
                        for series in series_options
                    ],
                }
            ]
            default_selection = series_options[: min(2, len(series_options))]
            tree_result = tree_select(
                nodes=nodes,
                check_model="leaf",
                only_leaf_checkboxes=True,
                show_expand_all=True,
                checked=default_selection,
                key=f"tree_{meta['module_path']}",
            )
            selected_series = tree_result.get("checked", [])
        else:
            default_selection = series_options[: min(2, len(series_options))]
            selected_series = st.multiselect(
                "시리즈",
                options=series_options,
                default=default_selection,
                format_func=lambda x: korean_names.get(x, x),
                key=f"series_{meta['module_path']}",
            )

        if not selected_series:
            st.info("시리즈를 선택하세요.")
            return

        data_type_labels = {
            key: label for key, label in STANDARD_DATA_KEYS if key in data_dict
        }
        data_type_key = st.selectbox(
            "데이터 타입",
            options=list(data_type_labels.keys()),
            format_func=lambda k: data_type_labels[k],
            key=f"dtype_{meta['module_path']}",
        )

        chart_options = [
            ("multi_line", "멀티 라인"),
            ("single_line", "단일 라인"),
            ("dual_axis", "이중 축"),
            ("horizontal_bar", "가로 바"),
            ("vertical_bar", "세로 바"),
            ("five_year", "5년 비교 (월간)"),
        ]
        if hasattr(module, "create_phillips_curve"):
            chart_options.append(("phillips_curve", "필립스 커브"))
        if hasattr(module, "create_phillips_curve_detailed"):
            chart_options.append(("phillips_curve_detailed", "필립스 커브 (세부)"))
        if hasattr(module, "create_beveridge_curve"):
            chart_options.append(("beveridge_curve", "베버리지 커브"))
        if hasattr(module, "create_beveridge_curve_detailed"):
            chart_options.append(("beveridge_curve_detailed", "베버리지 커브 (세부)"))

        chart_type = st.selectbox(
            "차트 유형",
            chart_options,
            format_func=lambda x: x[1],
            key=f"ctype_{meta['module_path']}",
        )[0]

        allow_series_override = chart_type in {"multi_line", "single_line", "dual_axis", "horizontal_bar", "vertical_bar"}
        if allow_series_override and len(data_type_labels) > 1:
            per_series_override = st.checkbox(
                "시리즈별 데이터 타입 직접 설정",
                value=False,
                key=f"per_series_dtype_{meta['module_path']}",
            )
            if per_series_override:
                st.markdown("#### 시리즈 데이터 타입")
                for series in selected_series:
                    series_type_map[series] = st.selectbox(
                        korean_names.get(series, series),
                        options=list(data_type_labels.keys()),
                        index=list(data_type_labels.keys()).index(data_type_key) if data_type_key in data_type_labels else 0,
                        format_func=lambda k, d=data_type_labels: d.get(k, k),
                        key=f"dtype_select_{meta['module_path']}_{series}",
                    )

        single_axis_chart = chart_type in {"multi_line", "single_line", "horizontal_bar", "vertical_bar", "five_year"}
        if single_axis_chart and chart_type != "dual_axis":
            custom_single_axis_title = st.text_input(
                "Y축 제목 (직접 입력)",
                value="",
                key=f"single_axis_title_{meta['module_path']}",
            )

        chart_width_cm = st.slider(
            "차트 너비 (cm)", min_value=15.0, max_value=45.0, value=24.0, step=0.5, key=f"width_{meta['module_path']}"
        )
        chart_height_cm = st.slider(
            "차트 높이 (cm)", min_value=10.0, max_value=25.0, value=12.0, step=0.5, key=f"height_{meta['module_path']}"
        )
        chart_width = int(chart_width_cm * PX_PER_CM)
        chart_height = int(chart_height_cm * PX_PER_CM)

        left_title_offset = st.slider(
            "왼쪽 축 제목 위치 보정",
            min_value=-0.2,
            max_value=0.2,
            value=0.0,
            step=0.01,
            key=f"left_offset_{meta['module_path']}"
        )
        right_title_offset = st.slider(
            "오른쪽 축 제목 위치 보정",
            min_value=-0.2,
            max_value=0.2,
            value=0.0,
            step=0.01,
            key=f"right_offset_{meta['module_path']}"
        )

        five_year_recent_years = None
        if chart_type == "five_year":
            five_year_recent_years = st.slider(
                "최근 비교 연도 수",
                min_value=3,
                max_value=6,
                value=5,
                step=1,
                key=f"five_year_recent_{meta['module_path']}"
            )

        if chart_type in {"phillips_curve", "phillips_curve_detailed"}:
            inflation_candidates = [col for col in raw_df.columns if col not in ["unemployment_rate", "period", "detailed_period"]]
            if inflation_candidates:
                phillips_inflation = st.selectbox(
                    "인플레이션 지표",
                    inflation_candidates,
                    format_func=lambda x: korean_names.get(x, x),
                    key=f"phillips_inflation_{meta['module_path']}"
                )
            phillips_color_by = st.checkbox("시기별 색상 구분", value=True, key=f"phillips_color_{meta['module_path']}")
            phillips_show_labels = st.checkbox("라벨 표시", value=True, key=f"phillips_labels_{meta['module_path']}")

        if chart_type in {"beveridge_curve", "beveridge_curve_detailed"}:
            bev_color_by = st.checkbox("시기별 색상 구분", value=True, key=f"bev_color_{meta['module_path']}")
            bev_show_labels = st.checkbox("라벨 표시", value=True, key=f"bev_labels_{meta['module_path']}")

        axis_allocation = None
        if chart_type == "dual_axis":
            if len(selected_series) < 2:
                st.warning("이중 축은 두 개 이상의 시리즈가 필요합니다.")
                return
            st.markdown("#### 이중축 구성")
            default_left = selected_series[: max(1, len(selected_series) // 2)]
            left_selection = st.multiselect(
                "왼쪽 축",
                selected_series,
                default=default_left,
                format_func=lambda x: korean_names.get(x, x),
                key=f"dual_left_{meta['module_path']}"
            )
            default_right = [s for s in selected_series if s not in left_selection] or selected_series[-1:]
            right_selection = st.multiselect(
                "오른쪽 축",
                selected_series,
                default=default_right,
                format_func=lambda x: korean_names.get(x, x),
                key=f"dual_right_{meta['module_path']}"
            )
            left_final = [s for s in left_selection if s in selected_series] or default_left or selected_series[:1]
            right_final = [s for s in right_selection if s in selected_series and s not in left_final]
            leftovers = [s for s in selected_series if s not in left_final and s not in right_final]
            if not right_final:
                right_final = leftovers or (left_final[-1:] if len(left_final) > 1 else [])
            if not right_final:
                st.warning("오른쪽 축에 배치할 시리즈가 필요합니다.")
                return
            axis_allocation = {"left": left_final, "right": right_final}
            custom_left_axis_title = st.text_input(
                "왼쪽 축 제목 (직접 입력)",
                value="",
                key=f"dual_left_title_{meta['module_path']}",
            )
            custom_right_axis_title = st.text_input(
                "오른쪽 축 제목 (직접 입력)",
                value="",
                key=f"dual_right_title_{meta['module_path']}",
            )

    data_selected = {}
    for key, _ in STANDARD_DATA_KEYS:
        if key in data_dict:
            data_selected[key] = data_dict[key]

    if not per_series_override:
        series_type_map = {series: data_type_key for series in selected_series}
    else:
        for series in selected_series:
            series_type_map.setdefault(series, data_type_key)

    def build_series_dataframe(series_list: list[str], dtype_map: dict[str, str]) -> tuple[pd.DataFrame, list[str]]:
        frames: list[pd.Series] = []
        missing: list[str] = []
        for series_name in series_list:
            dtype_key = dtype_map.get(series_name)
            source_df = data_selected.get(dtype_key)
            if source_df is None or series_name not in source_df.columns:
                missing.append(series_name)
                continue
            frames.append(source_df[series_name].rename(series_name))
        if not frames:
            return pd.DataFrame(), missing
        combined = pd.concat(frames, axis=1, join="outer").sort_index()
        return combined, missing

    standard_chart_types = {"multi_line", "single_line", "dual_axis", "horizontal_bar", "vertical_bar"}
    use_standard_flow = chart_type in standard_chart_types

    filtered_standard: pd.DataFrame | None = None
    filtered_default: pd.DataFrame | None = None
    missing_series: list[str] = []

    if use_standard_flow:
        combined_df, missing_series = build_series_dataframe(selected_series, series_type_map)
        if missing_series:
            missing_labels = ", ".join(korean_names.get(s, s) for s in missing_series)
            st.warning(f"선택한 데이터 타입에서 찾을 수 없는 시리즈: {missing_labels}")
        if combined_df.empty:
            st.warning("선택한 시리즈 데이터가 없습니다.")
            return
        filtered_standard = combined_df.reindex(columns=selected_series)
    else:
        if chart_type == "five_year" and data_type_key != "raw_data":
            st.warning("5년 비교 차트는 수준(raw) 데이터만 지원합니다. 데이터 타입을 '수준'으로 변경하세요.")
            return
        source_df = data_selected.get(data_type_key)
        if source_df is None:
            st.warning("선택한 데이터 타입을 사용할 수 없습니다.")
            return
        filtered_default = source_df[selected_series]

    filtered_df = filtered_standard if filtered_standard is not None else filtered_default
    if filtered_df is None or filtered_df.empty:
        st.warning("표시할 데이터가 없습니다.")
        return

    fig = None
    table_df = None

    if chart_type == "five_year":
        if len(selected_series) != 1:
            st.warning("5년 비교는 하나의 시리즈만 선택하세요.")
            return
        series_name = selected_series[0]
        unit_label = _axis_title_for("raw", [series_name], {})
        fig, fmt, period_type = _create_five_year_chart(
            series=raw_df[series_name],
            series_name=korean_names.get(series_name, series_name),
            unit_label=unit_label,
            recent_years=five_year_recent_years or 5,
            chart_width=chart_width,
            chart_height=chart_height,
        )
        if fmt is None or fig is None:
            st.warning("5년 비교 차트를 생성할 수 없습니다.")
            return
        _apply_custom_axis_title(fig, "y", custom_single_axis_title)
        fmt = fmt.copy()
        if period_type == "month":
            month_labels = [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ]
            fmt.index = [month_labels[i - 1] if 1 <= i <= 12 else str(i) for i in fmt.index]
            fmt.index.name = "월"
        else:
            fmt.index = [f"W{i:02d}" for i in fmt.index]
            fmt.index.name = "주"
        table_df = fmt
    elif chart_type == "dual_axis" and axis_allocation and filtered_standard is not None:
        def _dtype_key_to_axis_arg(dtype_key: str | None) -> str:
            if not dtype_key:
                return ""
            return dtype_key[:-5] if dtype_key.endswith("_data") else dtype_key

        left_keys = {series_type_map.get(s) for s in axis_allocation.get("left", []) if series_type_map.get(s)}
        right_keys = {series_type_map.get(s) for s in axis_allocation.get("right", []) if series_type_map.get(s)}
        left_axis_dtype = _dtype_key_to_axis_arg(left_keys.pop()) if len(left_keys) == 1 else ""
        right_axis_dtype = _dtype_key_to_axis_arg(right_keys.pop()) if len(right_keys) == 1 else ""
        base_data_type = data_type_key.replace("_data", "") if len({series_type_map.get(s) for s in selected_series}) == 1 else "raw"
        left_override = custom_left_axis_title.strip()
        right_override = custom_right_axis_title.strip()
        connect_map_local = {}
        for label in filtered_standard.columns:
            series = filtered_standard[label].dropna()
            if isinstance(series.index, pd.DatetimeIndex):
                connect_map_local[korean_names.get(label, label)] = _infer_series_period(series)
        fig = _create_dual_axis_chart(
            df=filtered_standard,
            axis_allocation=axis_allocation,
            label_map={s: korean_names.get(s, s) for s in selected_series},
            data_type=base_data_type,
            series_defs={s: {"unit": s} for s in selected_series},
            chart_width=chart_width,
            chart_height=chart_height,
            left_title_offset=left_title_offset,
            right_title_offset=right_title_offset,
            left_axis_data_type=left_axis_dtype,
            right_axis_data_type=right_axis_dtype,
            left_title_override=left_override or None,
            right_title_override=right_override or None,
            connect_map=connect_map_local,
        )
        table_df = filtered_standard.rename(columns=lambda x: korean_names.get(x, x))
        table_df.index.name = "날짜"
    elif chart_type in {"phillips_curve", "phillips_curve_detailed"}:
        func_basic = getattr(module, "create_phillips_curve", None)
        func_detailed = getattr(module, "create_phillips_curve_detailed", None)
        if chart_type == "phillips_curve" and callable(func_basic):
            fig = func_basic(
                inflation_type=phillips_inflation or selected_series[0],
                color_by_period=phillips_color_by,
                show_labels=phillips_show_labels,
            )
        elif chart_type == "phillips_curve_detailed" and callable(func_detailed):
            fig = func_detailed(
                inflation_type=phillips_inflation or selected_series[0],
                show_labels=phillips_show_labels,
            )
        else:
            st.warning("필립스 커브 함수를 찾을 수 없습니다.")
            return
        if fig is not None:
            fig.update_layout(width=chart_width, height=chart_height)
        combined = getattr(module, "PHILLIPS_DATA", {}).get("combined_data")
        if isinstance(combined, pd.DataFrame) and not combined.empty:
            display_cols = combined.columns
            table_df = combined[display_cols].rename(columns=lambda x: korean_names.get(x, x))
            table_df.index.name = "날짜"
        else:
            table_df = filtered_df.rename(columns=lambda x: korean_names.get(x, x))
            table_df.index.name = "날짜"
    elif chart_type in {"beveridge_curve", "beveridge_curve_detailed"}:
        func_basic = getattr(module, "create_beveridge_curve", None)
        func_detailed = getattr(module, "create_beveridge_curve_detailed", None)
        if chart_type == "beveridge_curve" and callable(func_basic):
            fig = func_basic(color_by_period=bev_color_by, show_labels=bev_show_labels)
        elif chart_type == "beveridge_curve_detailed" and callable(func_detailed):
            fig = func_detailed(show_labels=bev_show_labels)
        else:
            st.warning("베버리지 커브 함수를 찾을 수 없습니다.")
            return
        if fig is not None:
            fig.update_layout(width=chart_width, height=chart_height)
        combined = getattr(module, "BEVERIDGE_DATA", {}).get("combined_data")
        if isinstance(combined, pd.DataFrame) and not combined.empty:
            table_df = combined.rename(columns=lambda x: korean_names.get(x, x))
            table_df.index.name = "날짜"
        else:
            table_df = filtered_df.rename(columns=lambda x: korean_names.get(x, x))
            table_df.index.name = "날짜"
    else:
        if use_standard_flow and filtered_standard is not None:
            working_dict = {"raw_data": filtered_standard}
            fig = plot_economic_series(
                data_dict=working_dict,
                series_list=selected_series,
                chart_type=chart_type,
                data_type="raw",
                labels={s: korean_names.get(s, s) for s in selected_series},
                korean_names={s: korean_names.get(s, s) for s in selected_series},
            )
            if fig is not None:
                fig.update_layout(width=chart_width, height=chart_height)
                if chart_type == "horizontal_bar":
                    _apply_custom_axis_title(fig, "x", custom_single_axis_title)
                else:
                    _apply_custom_axis_title(fig, "y", custom_single_axis_title)
            table_df = filtered_standard.rename(columns=lambda x: korean_names.get(x, x))
            table_df.index.name = "날짜"
        else:
            fig = plot_economic_series(
                data_dict=data_dict,
                series_list=selected_series,
                chart_type=chart_type,
                data_type=data_type_key.replace("_data", ""),
                labels={s: korean_names.get(s, s) for s in selected_series},
                korean_names={s: korean_names.get(s, s) for s in selected_series},
            )
            if fig is not None:
                fig.update_layout(width=chart_width, height=chart_height)
                if chart_type == "horizontal_bar":
                    _apply_custom_axis_title(fig, "x", custom_single_axis_title)
                else:
                    _apply_custom_axis_title(fig, "y", custom_single_axis_title)
            table_df = filtered_df.rename(columns=lambda x: korean_names.get(x, x))
            table_df.index.name = "날짜"

    if fig is not None:
        fig = _sanitize_plotly_figure(fig)
        st.plotly_chart(fig, use_container_width=False)

    st.markdown("## 데이터 테이블")
    if table_df is None:
        st.warning("표시할 데이터가 없습니다.")
        return

    extras = data_dict.get("extra_data")
    if isinstance(extras, pd.DataFrame) and not extras.empty:
        extras_to_join = extras.loc[table_df.index.intersection(extras.index)]
        extras_to_join = extras_to_join[[c for c in extras_to_join.columns if c not in table_df.columns]]
        if not extras_to_join.empty:
            table_df = table_df.join(extras_to_join, how="left")

    st.dataframe(table_df)

    csv_bytes = table_df.to_csv(index=True).encode("utf-8-sig")
    st.download_button(
        label="CSV 다운로드",
        data=csv_bytes,
        file_name=f"{meta['module_path'].split('.')[-1]}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
    )


def main():
    st.set_page_config(page_title="미국 경제 지표 대시보드", layout="wide")
    st.title("미국 경제 지표 통합 대시보드")

    module_infos = discover_modules()
    if not module_infos:
        st.error("불러올 수 있는 경제 지표 모듈을 찾지 못했습니다.")
        return

    metas: list[dict[str, Any]] = []
    load_errors: list[str] = []

    for info in module_infos:
        friendly = FRIENDLY_TITLES.get(info["stem"], info["stem"].replace("_", " ").title())
        try:
            file_mtime = info["file_path"].stat().st_mtime
        except OSError:
            load_errors.append(f"파일에 접근할 수 없습니다: {info['file_path']}")
            continue

        meta = get_cached_module_metadata(
            stem=info["stem"],
            file_path_str=str(info["file_path"]),
            file_mtime=file_mtime,
        )
        if not meta:
            load_errors.append(f"메타데이터 로드 실패: {info['stem']}")
            continue

        meta["friendly_title"] = friendly
        meta["stem"] = info["stem"]
        metas.append(meta)

    if load_errors:
        with st.expander("메타데이터 로드 경고", expanded=False):
            for msg in load_errors:
                st.write(f"- {msg}")

    if not metas:
        st.error("표시할 메타데이터를 불러오지 못했습니다.")
        return

    render_global_dashboard(metas)


if __name__ == "__main__":
    main()
