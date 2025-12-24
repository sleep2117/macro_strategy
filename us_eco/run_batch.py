"""
배치 실행 스크립트: 지정된 모듈만 순차 실행하며 모듈 간 대기 시간을 둬서 API rate-limit을 피한다.
"""

from __future__ import annotations

import argparse
import importlib.util
import inspect
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Callable

# us_eco 루트 추가
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO_ROOT))

import us_eco_utils as utils_module  # type: ignore
from us_eco_utils import api_config, switch_bls_api_key  # type: ignore


# 배치 정의
BATCHES: dict[str, list[str]] = {
    "batch1": ["CPI_analysis_refactor"],  # BLS
    "batch2": ["PPI_analysis_refactor"],  # BLS
    "batch3": ["CES_employ_refactor", "CPS_employ_refactor"],  # BLS
    "batch4": ["JOLTS_employ_refactor"],  # BLS
    "batch5": ["retail_sales_refactor", "durable_goods_refactor", "int_trade_refactor", "import_price_refactor_v2"],
    "batch6": ["industrial_production_refactor", "construction_spending_refactor", "new_residential_construction_refactor"],
    "batch7": ["house_price_refactor", "house_sales_stock_refactor", "realtor_housing_inventory_refactor"],
    "batch8": ["ADP_employ_refactor", "atlanta_wage_growth_refactor", "unemployment_claims_analysis", "indeed_jobs"],
    "batch9": ["gdp_analysis_refactor", "personal_income_refactor", "pce_analysis_refactor", "misc_fred_series_refactor"],
    "batch10": ["fed_balance_sheet_refactor", "fed_pmi_refactor", "ism_pmi_refactor"],
    "batch11": ["beveridge_curve_enhanced", "phillips_curve_enhanced"],
}

# BLS 모듈별 키 매핑 (배치 내에서도 다른 키 사용 가능)
BLS_MODULE_KEYS: dict[str, str] = {
    "CPI_analysis_refactor": "BLS_API_KEY_1",
    "PPI_analysis_refactor": "BLS_API_KEY_2",
    "CES_employ_refactor": "BLS_API_KEY_3",
    "CPS_employ_refactor": "BLS_API_KEY_3",
    "JOLTS_employ_refactor": "BLS_API_KEY_1",
}

# env에 설정된 값 우선
KEY_POOL = {
    "BLS_API_KEY_1": os.getenv("BLS_API_KEY_1", api_config.BLS_API_KEY),
    "BLS_API_KEY_2": os.getenv("BLS_API_KEY_2", api_config.BLS_API_KEY2),
    "BLS_API_KEY_3": os.getenv("BLS_API_KEY_3", api_config.BLS_API_KEY3),
}


def _restore_module_loaders(module) -> None:
    real_loader = getattr(utils_module, "load_economic_data", None)
    if callable(real_loader):
        setattr(module, "load_economic_data", real_loader)
    real_group_loader = getattr(utils_module, "load_economic_data_grouped", None)
    if callable(real_group_loader):
        setattr(module, "load_economic_data_grouped", real_group_loader)


def _load_module_from_path(stem: str):
    path = HERE / f"{stem}.py"
    if not path.exists():
        raise FileNotFoundError(f"module file not found: {path}")
    module_name = f"us_eco_dynamic_{stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"spec not found for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module

    original_load = getattr(utils_module, "load_economic_data", None)
    original_group = getattr(utils_module, "load_economic_data_grouped", None)

    def stub_load(*_args, **_kwargs):
        return None

    def stub_group(*_args, **_kwargs):
        return None

    if original_load is not None:
        utils_module.load_economic_data = stub_load  # type: ignore[assignment]
    if original_group is not None:
        utils_module.load_economic_data_grouped = stub_group  # type: ignore[assignment]

    try:
        spec.loader.exec_module(module)  # type: ignore[arg-type]
    finally:
        if original_load is not None:
            utils_module.load_economic_data = original_load  # type: ignore[assignment]
        if original_group is not None:
            utils_module.load_economic_data_grouped = original_group  # type: ignore[assignment]
        _restore_module_loaders(module)
    return module


def _find_load_function(module) -> Callable | None:
    for name, obj in inspect.getmembers(module, inspect.isfunction):
        if obj.__module__ != module.__name__:
            continue
        if name.startswith("load_") and name.endswith("_data"):
            return obj
    return None


def _set_bls_key_for_module(stem: str) -> str | None:
    key_name = BLS_MODULE_KEYS.get(stem)
    if not key_name:
        return None
    key_value = KEY_POOL.get(key_name)
    if key_value:
        if api_config.CURRENT_BLS_KEY != key_value and hasattr(api_config, "BLS_RATE_LIMITED"):
            api_config.BLS_RATE_LIMITED = False
        api_config.CURRENT_BLS_KEY = key_value
        api_config.BLS_API_KEY = key_value
    return key_value


def run_module(stem: str, start_date: str | None, smart_update: bool, force_reload: bool) -> tuple[bool, str]:
    """단일 모듈을 로드/실행하고 결과를 반환한다."""
    try:
        module = _load_module_from_path(stem)
    except Exception as exc:
        return False, f"모듈 로드 실패 ({stem}): {exc}"

    load_fn = _find_load_function(module)
    if load_fn is None:
        return False, f"load_*_data 함수가 없습니다 ({stem})"

    # BLS 키 설정 (필요 시)
    active_key = _set_bls_key_for_module(stem)

    kwargs = {}
    sig = inspect.signature(load_fn)
    if "start_date" in sig.parameters and start_date:
        kwargs["start_date"] = start_date
    if "smart_update" in sig.parameters:
        kwargs["smart_update"] = smart_update
    if "force_reload" in sig.parameters:
        kwargs["force_reload"] = force_reload

    retries = 2 if stem in BLS_MODULE_KEYS else 1
    last_error = None
    for attempt in range(retries):
        try:
            result = load_fn(**kwargs)
            return bool(result), f"성공 (키:{active_key})" if active_key else "성공"
        except Exception as exc:
            last_error = exc
            traceback.print_exc()
            if stem in BLS_MODULE_KEYS and attempt == 0:
                switch_bls_api_key()
                continue
            break

    return False, f"실패: {last_error}"


def parse_args():
    parser = argparse.ArgumentParser(description="us_eco 배치 실행기")
    parser.add_argument("--batch", "-b", action="append", help="실행할 배치 이름(여러 개 지정 가능)", default=[])
    parser.add_argument("--batch-list", help="콤마로 구분된 배치 목록", default="")
    parser.add_argument("--sleep-seconds", type=int, default=60, help="모듈 간 대기(초)")
    parser.add_argument("--start-date", help="모듈 start_date 인자가 있을 때 덮어쓸 값 (예: 2020-01-01)", default=None)
    parser.add_argument("--no-smart-update", action="store_true", help="smart_update를 False로 실행")
    parser.add_argument("--force-reload", action="store_true", help="force_reload를 True로 실행")
    parser.add_argument("--summary-file", help="JSON 요약을 기록할 파일 경로", default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    batch_names = set(args.batch or [])
    if args.batch_list:
        batch_names.update([b.strip() for b in args.batch_list.split(",") if b.strip()])
    if not batch_names:
        print("실행할 배치를 지정하세요. --batch 혹은 --batch-list")
        return 1

    resolved: list[str] = []
    for name in batch_names:
        if name not in BATCHES:
            print(f"알 수 없는 배치: {name}")
            continue
        resolved.append(name)

    if not resolved:
        print("유효한 배치가 없습니다.")
        return 1

    results = []
    for idx, batch_name in enumerate(sorted(resolved)):
        modules = BATCHES[batch_name]
        print(f"\n=== 배치 실행: {batch_name} ({len(modules)}개 모듈) ===")
        for jdx, stem in enumerate(modules):
            print(f"\n[{batch_name}] 모듈 실행: {stem}")
            ok, msg = run_module(
                stem,
                start_date=args.start_date,
                smart_update=not args.no_smart_update,
                force_reload=args.force_reload,
            )
            results.append({"batch": batch_name, "module": stem, "ok": ok, "message": msg})
            print(f"[{batch_name}] {stem}: {msg}")
            if jdx < len(modules) - 1 and args.sleep_seconds > 0:
                print(f"모듈 간 대기 {args.sleep_seconds}초...")
                time.sleep(args.sleep_seconds)
        if idx < len(resolved) - 1 and args.sleep_seconds > 0:
            print(f"\n배치 간 대기 {args.sleep_seconds}초...")
            time.sleep(args.sleep_seconds)

    # 요약
    ok_count = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count
    print("\n=== 결과 요약 ===")
    for r in results:
        status = "✅" if r["ok"] else "❌"
        print(f"{status} {r['batch']} :: {r['module']} :: {r['message']}")

    if args.summary_file:
        try:
            import json

            Path(args.summary_file).write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            print(f"요약 파일 저장 실패: {exc}")

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
