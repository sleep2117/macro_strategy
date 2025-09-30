# %%
"""
US Retail Sales 데이터 분석 (리팩토링 버전)
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
# === FRED API 키 설정 ===
api_config.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'

# %%
# === US Retail Sales 시리즈 정의 ===

# Advance Retail Sales 시리즈 맵 (FRED Advance 시리즈 사용 - seasonally adjusted)
RETAIL_SALES_SERIES = {
    # 핵심 총계 지표 (Advance series)
    'total': 'RSAFS',                             # Retail & Food Services - Total (Advance)
    'ex_auto': 'RSFSXMV',                         # Ex-Motor-Vehicle & Parts (Advance)
    'ex_gas': 'MARTSSM44Z72USS',                  # Ex-Gasoline Stations (Advance)
    'ex_auto_gas': 'MARTSSM44W72USS',             # Ex-Auto & Gas (Advance)
    'retail_only': 'RSXFS',                       # Retail Trade - Total (no restaurants, Advance)
    
    # 주요 업종별 세부 데이터 (Advance series)
    'motor_vehicles': 'RSMVPD',                   # Motor Vehicle & Parts Dealers (Advance)
    'furniture': 'RSFHFS',                        # Furniture & Home Furnishings Stores (Advance)
    'electronics': 'RSEAS',                       # Electronics & Appliance Stores (Advance)
    'building_materials': 'RSBMGESD',             # Building Materials & Garden Equipment Dealers (Advance)
    'food_beverage': 'RSDBS',                     # Food & Beverage Stores (Advance)
    'health_care': 'RSHPCS',                      # Health & Personal Care Stores (Advance)
    'gasoline': 'RSGASSN',                        # Gasoline Stations (Advance)
    'clothing': 'RSCCAS',                         # Clothing & Accessory Stores (Advance)
    'sporting_goods': 'RSSGHBMS',                 # Sporting Goods, Hobby, Musical Instr. & Books (Advance)
    'general_merchandise': 'RSGMS',               # General Merchandise Stores (Advance)
    'misc_retailers': 'RSMSR',                    # Miscellaneous Store Retailers (Advance)
    'nonstore_ecommerce': 'RSNSR',                # Non-store Retailers (incl. e-commerce, Advance)
    'food_services': 'RSFSDP'                     # Food Services & Drinking Places (Advance)
}

# 한국어 매핑 딕셔너리
KOREAN_NAMES = {
    'total': '소매판매 총계',
    'ex_auto': '소매판매 (자동차 제외)',
    'ex_gas': '소매판매 (주유소 제외)',
    'ex_auto_gas': '소매판매 (자동차·주유소 제외)',
    'retail_only': '소매업 (음식서비스 제외)',
    'motor_vehicles': '자동차·부품 판매업',
    'furniture': '가구·가정용품 판매업',
    'electronics': '전자제품·가전 판매업',
    'building_materials': '건축자재·정원용품 판매업',
    'food_beverage': '식품·음료 판매업',
    'health_care': '건강·개인관리 용품 판매업',
    'gasoline': '주유소',
    'clothing': '의류·액세서리 판매업',
    'sporting_goods': '스포츠·취미·도서 판매업',
    'general_merchandise': '종합소매업',
    'misc_retailers': '기타 소매업',
    'nonstore_ecommerce': '무점포 소매업 (전자상거래 포함)',
    'food_services': '음식서비스업'
}

# 카테고리 분류
CATEGORIES = {
    'core_measures': {
        'name': 'Core Retail Sales Measures',
        'series': ['total', 'ex_auto', 'ex_gas', 'ex_auto_gas', 'retail_only']
    },
    'discretionary_spending': {
        'name': 'Discretionary Consumer Spending',
        'series': ['motor_vehicles', 'furniture', 'electronics', 'clothing', 'sporting_goods']
    },
    'staples_spending': {
        'name': 'Consumer Staples Spending',
        'series': ['food_beverage', 'health_care', 'gasoline']
    },
    'housing_related': {
        'name': 'Housing-Related Retail',
        'series': ['furniture', 'building_materials', 'electronics']
    },
    'modern_retail': {
        'name': 'Modern Retail Channels',
        'series': ['general_merchandise', 'nonstore_ecommerce', 'misc_retailers']
    },
    'services': {
        'name': 'Food Services',
        'series': ['food_services']
    }
}

print("✓ Retail Sales 데이터 구조 정의 완료")

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/retail_sales_data_complete.csv'
RETAIL_SALES_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_retail_sales_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 Retail Sales 데이터 로드"""
    global RETAIL_SALES_DATA

    # 시리즈 딕셔너리를 올바른 {name: fred_id} 형태로 전달
    series_dict = RETAIL_SALES_SERIES

    result = load_economic_data(
        series_dict=series_dict,
        data_source='FRED',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=10.0  # 일반 지표용 허용 오차
    )

    if result:
        RETAIL_SALES_DATA = result
        print_load_info()
        return True
    else:
        print("❌ Retail Sales 데이터 로드 실패")
        return False

def print_load_info():
    """Retail Sales 데이터 로드 정보 출력"""
    if not RETAIL_SALES_DATA or 'load_info' not in RETAIL_SALES_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = RETAIL_SALES_DATA['load_info']
    print(f"\n📊 Retail Sales 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in RETAIL_SALES_DATA and not RETAIL_SALES_DATA['raw_data'].empty:
        latest_date = RETAIL_SALES_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_retail_sales_series_advanced(series_list, chart_type='multi_line', 
                                      data_type='mom', periods=None, target_date=None,
                                      left_ytitle=None, right_ytitle=None):
    """범용 Retail Sales 시각화 함수 - plot_economic_series 활용"""
    if not RETAIL_SALES_DATA:
        print("⚠️ 먼저 load_retail_sales_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=RETAIL_SALES_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle,
        right_ytitle=right_ytitle,
        korean_names=KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_retail_sales_data(series_list, data_type='mom', periods=None, 
                             target_date=None, export_path=None, file_format='excel'):
    """Retail Sales 데이터 export 함수 - export_economic_data 활용"""
    if not RETAIL_SALES_DATA:
        print("⚠️ 먼저 load_retail_sales_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=RETAIL_SALES_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_retail_sales_data():
    """Retail Sales 데이터 초기화"""
    global RETAIL_SALES_DATA
    RETAIL_SALES_DATA = {}
    print("🗑️ Retail Sales 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not RETAIL_SALES_DATA or 'raw_data' not in RETAIL_SALES_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_retail_sales_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return RETAIL_SALES_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in RETAIL_SALES_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return RETAIL_SALES_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not RETAIL_SALES_DATA or 'mom_data' not in RETAIL_SALES_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_retail_sales_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return RETAIL_SALES_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in RETAIL_SALES_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return RETAIL_SALES_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not RETAIL_SALES_DATA or 'yoy_data' not in RETAIL_SALES_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_retail_sales_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return RETAIL_SALES_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in RETAIL_SALES_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return RETAIL_SALES_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not RETAIL_SALES_DATA or 'raw_data' not in RETAIL_SALES_DATA:
        return []
    return list(RETAIL_SALES_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 Retail Sales 시리즈 표시"""
    print("=== 사용 가능한 Retail Sales 시리즈 ===")
    
    for series_id, description in RETAIL_SALES_SERIES.items():
        korean_name = KOREAN_NAMES.get(series_id, description)
        print(f"  '{series_id}': {korean_name} ({description})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, info in CATEGORIES.items():
        print(f"\n{category} ({info['name']}):")
        for series_id in info['series']:
            korean_name = KOREAN_NAMES.get(series_id, series_id)
            print(f"  - {series_id}: {korean_name}")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not RETAIL_SALES_DATA or 'load_info' not in RETAIL_SALES_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': RETAIL_SALES_DATA['load_info']['loaded'],
        'series_count': RETAIL_SALES_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': RETAIL_SALES_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 Retail Sales 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_retail_sales_data()  # 스마트 업데이트")
print("   load_retail_sales_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_retail_sales_series_advanced(['total', 'ex_auto'], 'multi_line', 'mom')")
print("   plot_retail_sales_series_advanced(['total'], 'horizontal_bar', 'yoy', left_ytitle='%')")
print("   plot_retail_sales_series_advanced(['ex_auto_gas'], 'single_line', 'mom', periods=24, left_ytitle='%')")
print("   plot_retail_sales_series_advanced(['total', 'ex_auto'], 'dual_axis', 'raw', left_ytitle='십억$', right_ytitle='십억$')")
print()
print("3. 🔥 데이터 Export:")
print("   export_retail_sales_data(['total', 'ex_auto'], 'mom')")
print("   export_retail_sales_data(['ex_auto_gas'], 'raw', periods=24, file_format='csv')")
print("   export_retail_sales_data(['total'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_retail_sales_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_retail_sales_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_retail_sales_data()
plot_retail_sales_series_advanced(['total', 'ex_auto'], 'multi_line', 'mom')
# %%
