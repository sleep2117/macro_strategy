# %%
"""
Import Prices 데이터 분석 (리팩토링 버전)
- us_eco_utils를 사용한 통합 구조
- 시리즈 정의와 분석 로직만 포함
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import warnings
warnings.filterwarnings('ignore')

# 통합 유틸리티 함수 불러오기
from us_eco_utils import *

# %%
# === BLS API 키 설정 ===
api_config.BLS_API_KEY = '56b193612b614cdc9416359fd1c73a74'
api_config.BLS_API_KEY2 = '0450ef37363c48b5bedd2ae6fc92dd6e'
api_config.BLS_API_KEY3 = 'daf1ca7970b74e81b6a5c7a80a8b8a7f'

# %%
# === Import Prices 시리즈 정의 ===

# Import Prices 시리즈 매핑 (시리즈 이름: API ID)
IMPORT_SERIES = {
    'all_commodities': 'EIUIR',
    'fuels_lubricants': 'EIUIR10',
    'all_excluding_fuels': 'EIUIREXFUELS',
    'foods_feeds_beverages': 'EIUIR0',
    'industrial_supplies': 'EIUIR1',
    'capital_goods': 'EIUIR2',
    'automotive_vehicles': 'EIUIR3',
    'consumer_goods': 'EIUIR4',
    'air_freight': 'EIUIV131'
}

# 한국어 이름 매핑
IMPORT_KOREAN_NAMES = {
    'all_commodities': '수입품 - 전체',
    'fuels_lubricants': '수입품 - 연료 및 윤활유',
    'all_excluding_fuels': '수입품 - 연료 제외 전체',
    'foods_feeds_beverages': '수입품 - 식품, 사료 및 음료',
    'industrial_supplies': '수입품 - 산업용 원자재',
    'capital_goods': '수입품 - 자본재',
    'automotive_vehicles': '수입품 - 자동차',
    'consumer_goods': '수입품 - 소비재',
    'air_freight': '수입 항공 화물'
}

# Import Prices 계층 구조 (카테고리)
IMPORT_CATEGORIES = {
    '주요 지표': {
        'Total': ['all_commodities', 'all_excluding_fuels'],
        'Energy': ['fuels_lubricants']
    },
    '세부 품목': {
        'Consumer': ['foods_feeds_beverages', 'consumer_goods'],
        'Industrial': ['industrial_supplies', 'capital_goods'],
        'Transportation': ['automotive_vehicles', 'air_freight']
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/import_prices_data.csv'
IMPORT_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_import_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 Import Prices 데이터 로드"""
    global IMPORT_DATA

    result = load_economic_data(
        series_dict=IMPORT_SERIES,
        data_source='BLS',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=10.0  # Import Price 데이터 허용 오차
    )

    if result:
        IMPORT_DATA = result
        print_load_info()
        return True
    else:
        print("❌ Import Prices 데이터 로드 실패")
        return False

def print_load_info():
    """Import Prices 데이터 로드 정보 출력"""
    if not IMPORT_DATA or 'load_info' not in IMPORT_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = IMPORT_DATA['load_info']
    print(f"\n📊 Import Prices 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in IMPORT_DATA and not IMPORT_DATA['raw_data'].empty:
        latest_date = IMPORT_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_import_series_advanced(series_list, chart_type='multi_line', 
                                data_type='mom', periods=None, target_date=None):
    """범용 Import Prices 시각화 함수 - plot_economic_series 활용"""
    if not IMPORT_DATA:
        print("⚠️ 먼저 load_import_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=IMPORT_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=IMPORT_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_import_data(series_list, data_type='mom', periods=None, 
                       target_date=None, export_path=None, file_format='excel'):
    """Import Prices 데이터 export 함수 - export_economic_data 활용"""
    if not IMPORT_DATA:
        print("⚠️ 먼저 load_import_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=IMPORT_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=IMPORT_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_import_data():
    """Import Prices 데이터 초기화"""
    global IMPORT_DATA
    IMPORT_DATA = {}
    print("🗑️ Import Prices 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not IMPORT_DATA or 'raw_data' not in IMPORT_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_import_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return IMPORT_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in IMPORT_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return IMPORT_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not IMPORT_DATA or 'mom_data' not in IMPORT_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_import_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return IMPORT_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in IMPORT_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return IMPORT_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not IMPORT_DATA or 'yoy_data' not in IMPORT_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_import_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return IMPORT_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in IMPORT_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return IMPORT_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not IMPORT_DATA or 'raw_data' not in IMPORT_DATA:
        return []
    return list(IMPORT_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 Import Prices 시리즈 표시"""
    print("=== 사용 가능한 Import Prices 시리즈 ===")
    
    for series_name, series_id in IMPORT_SERIES.items():
        korean_name = IMPORT_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in IMPORT_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_name in series_list:
                korean_name = IMPORT_KOREAN_NAMES.get(series_name, series_name)
                api_id = IMPORT_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not IMPORT_DATA or 'load_info' not in IMPORT_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': IMPORT_DATA['load_info']['loaded'],
        'series_count': IMPORT_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': IMPORT_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 Import Prices 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_import_data()  # 스마트 업데이트")
print("   load_import_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_import_series_advanced(['all_commodities', 'fuels_lubricants'], 'multi_line', 'mom')")
print("   plot_import_series_advanced(['all_commodities'], 'horizontal_bar', 'yoy')")
print("   plot_import_series_advanced(['all_commodities'], 'single_line', 'mom', periods=24)")
print()
print("3. 🔥 데이터 Export:")
print("   export_import_data(['all_commodities', 'fuels_lubricants'], 'mom')")
print("   export_import_data(['all_commodities'], 'raw', periods=24, file_format='csv')")
print("   export_import_data(['all_commodities'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_import_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_import_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_import_data()
plot_import_series_advanced(['all_commodities', 'fuels_lubricants'], 'multi_line', 'mom')

# %%
plot_import_series_advanced(['all_commodities'], 'horizontal_bar', 'yoy')