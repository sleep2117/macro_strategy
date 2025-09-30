# %%
"""
US Personal Income 데이터 분석 (리팩토링 버전)
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
# === US Personal Income 시리즈 정의 ===

# PI 주요 구성 요소 시리즈 맵 (월별 레벨 데이터)
PI_MAIN_SERIES = {
    # 핵심 소득 지표
    'personal_income': 'PI',  # Personal income (총 개인소득)
    'disposable_income': 'DSPI',  # Disposable personal income (가처분소득)
    'real_disposable_income': 'DSPIC96',  # Real disposable personal income (실질 가처분소득)
    'personal_consumption': 'PCE',  # Personal consumption expenditures (개인소비지출)
    'personal_saving': 'PMSAVE',  # Personal saving (개인저축)
    'saving_rate': 'PSAVERT',  # Personal saving rate (저축률)
    
    # 소득 구성 요소
    'compensation': 'W209RC1',  # Compensation of employees (임금 및 급여)
    'wages_salaries': 'A576RC1',  # Wages and salaries (임금)
    'private_wages': 'A132RC1',  # Private industry wages (민간부문 임금)
    'govt_wages': 'B202RC1',  # Government wages (정부부문 임금)
    'supplements': 'A038RC1',  # Supplements to wages & salaries (부가급여)
    
    # 사업소득 및 자본소득
    'proprietors_income': 'A041RC1',  # Proprietors' income (사업소득)
    'farm_income': 'B042RC1',  # Farm income (농업소득)
    'nonfarm_income': 'A045RC1',  # Nonfarm income (비농업소득)
    'rental_income': 'A048RC1',  # Rental income (임대소득)
    'interest_income': 'PII',  # Personal interest income (이자소득)
    'dividend_income': 'PDI',  # Personal dividend income (배당소득)
    
    # 정부 이전소득
    'transfer_receipts': 'PCTR',  # Personal current transfer receipts (이전수입)
    'social_security': 'A063RC1',  # Social Security (사회보장급여)
    'medicare': 'W824RC1',  # Medicare (메디케어)
    'medicaid': 'W729RC1',  # Medicaid (메디케이드)
    'unemployment': 'W825RC1',  # Unemployment insurance (실업급여)
    'veterans': 'W826RC1',  # Veterans' benefits (재향군인급여)
    
    # 공제 항목
    'social_contributions': 'A061RC1',  # Contributions for govt social insurance (사회보장기여금)
    'personal_taxes': 'W055RC1',  # Personal current taxes (개인소득세)
    
    # 인구 및 1인당 지표
    'population': 'POPTHM',  # Population (인구)
    'dpi_per_capita': 'A229RC0',  # DPI per capita, current $ (1인당 가처분소득)
}

# 한국어 매핑 딕셔너리
KOREAN_NAMES = {
    # 핵심 소득 지표
    'personal_income': '개인소득',
    'disposable_income': '가처분소득',
    'real_disposable_income': '실질 가처분소득',
    'personal_consumption': '개인소비지출',
    'personal_saving': '개인저축',
    'saving_rate': '저축률',
    
    # 소득 구성 요소
    'compensation': '근로자 보상',
    'wages_salaries': '임금·급여',
    'private_wages': '민간부문 임금',
    'govt_wages': '정부부문 임금',
    'supplements': '부가급여',
    
    # 사업소득 및 자본소득
    'proprietors_income': '사업소득',
    'farm_income': '농업소득',
    'nonfarm_income': '비농업소득',
    'rental_income': '임대소득',
    'interest_income': '이자소득',
    'dividend_income': '배당소득',
    
    # 정부 이전소득
    'transfer_receipts': '정부 이전수입',
    'social_security': '사회보장급여',
    'medicare': '메디케어',
    'medicaid': '메디케이드',
    'unemployment': '실업급여',
    'veterans': '재향군인급여',
    
    # 공제 항목
    'social_contributions': '사회보장기여금',
    'personal_taxes': '개인소득세',
    
    # 인구 및 1인당 지표
    'population': '인구',
    'dpi_per_capita': '1인당 가처분소득'
}

# 카테고리 분류
CATEGORIES = {
    'core_measures': {
        'name': 'Core Personal Income Measures',
        'series': ['personal_income', 'disposable_income', 'real_disposable_income', 'personal_consumption', 'saving_rate']
    },
    'labor_income': {
        'name': 'Labor Income Components',
        'series': ['compensation', 'wages_salaries', 'private_wages', 'govt_wages', 'supplements']
    },
    'capital_income': {
        'name': 'Capital Income Components',
        'series': ['proprietors_income', 'rental_income', 'interest_income', 'dividend_income']
    },
    'government_transfers': {
        'name': 'Government Transfer Payments',
        'series': ['transfer_receipts', 'social_security', 'medicare', 'medicaid', 'unemployment', 'veterans']
    },
    'deductions': {
        'name': 'Income Deductions',
        'series': ['social_contributions', 'personal_taxes']
    },
    'per_capita': {
        'name': 'Per Capita Indicators',
        'series': ['dpi_per_capita', 'population']
    }
}

print("✓ Personal Income 데이터 구조 정의 완료")

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/pi_data_complete.csv'
PERSONAL_INCOME_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_personal_income_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 Personal Income 데이터 로드"""
    global PERSONAL_INCOME_DATA

    # 시리즈 딕셔너리를 올바른 {name: fred_id} 형태로 전달
    series_dict = PI_MAIN_SERIES

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
        PERSONAL_INCOME_DATA = result
        print_load_info()
        return True
    else:
        print("❌ Personal Income 데이터 로드 실패")
        return False

def print_load_info():
    """Personal Income 데이터 로드 정보 출력"""
    if not PERSONAL_INCOME_DATA or 'load_info' not in PERSONAL_INCOME_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = PERSONAL_INCOME_DATA['load_info']
    print(f"\n📊 Personal Income 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in PERSONAL_INCOME_DATA and not PERSONAL_INCOME_DATA['raw_data'].empty:
        latest_date = PERSONAL_INCOME_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_personal_income_series_advanced(series_list, chart_type='multi_line', 
                                         data_type='mom', periods=None, target_date=None,
                                         left_ytitle=None, right_ytitle=None):
    """범용 Personal Income 시각화 함수 - plot_economic_series 활용"""
    if not PERSONAL_INCOME_DATA:
        print("⚠️ 먼저 load_personal_income_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=PERSONAL_INCOME_DATA,
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
def export_personal_income_data(series_list, data_type='mom', periods=None, 
                                target_date=None, export_path=None, file_format='excel'):
    """Personal Income 데이터 export 함수 - export_economic_data 활용"""
    if not PERSONAL_INCOME_DATA:
        print("⚠️ 먼저 load_personal_income_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=PERSONAL_INCOME_DATA,
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

def clear_personal_income_data():
    """Personal Income 데이터 초기화"""
    global PERSONAL_INCOME_DATA
    PERSONAL_INCOME_DATA = {}
    print("🗑️ Personal Income 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not PERSONAL_INCOME_DATA or 'raw_data' not in PERSONAL_INCOME_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_personal_income_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PERSONAL_INCOME_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in PERSONAL_INCOME_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PERSONAL_INCOME_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not PERSONAL_INCOME_DATA or 'mom_data' not in PERSONAL_INCOME_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_personal_income_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PERSONAL_INCOME_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in PERSONAL_INCOME_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PERSONAL_INCOME_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not PERSONAL_INCOME_DATA or 'yoy_data' not in PERSONAL_INCOME_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_personal_income_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PERSONAL_INCOME_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in PERSONAL_INCOME_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PERSONAL_INCOME_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not PERSONAL_INCOME_DATA or 'raw_data' not in PERSONAL_INCOME_DATA:
        return []
    return list(PERSONAL_INCOME_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 Personal Income 시리즈 표시"""
    print("=== 사용 가능한 Personal Income 시리즈 ===")
    
    for series_id, description in PI_MAIN_SERIES.items():
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
    if not PERSONAL_INCOME_DATA or 'load_info' not in PERSONAL_INCOME_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': PERSONAL_INCOME_DATA['load_info']['loaded'],
        'series_count': PERSONAL_INCOME_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': PERSONAL_INCOME_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 Personal Income 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_personal_income_data()  # 스마트 업데이트")
print("   load_personal_income_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_personal_income_series_advanced(['personal_income', 'disposable_income'], 'multi_line', 'mom')")
print("   plot_personal_income_series_advanced(['saving_rate'], 'horizontal_bar', 'yoy', left_ytitle='%')")
print("   plot_personal_income_series_advanced(['real_disposable_income'], 'single_line', 'mom', periods=24, left_ytitle='%')")
print("   plot_personal_income_series_advanced(['personal_income', 'personal_consumption'], 'dual_axis', 'raw', left_ytitle='십억$', right_ytitle='십억$')")
print()
print("3. 🔥 데이터 Export:")
print("   export_personal_income_data(['personal_income', 'disposable_income'], 'mom')")
print("   export_personal_income_data(['saving_rate'], 'raw', periods=24, file_format='csv')")
print("   export_personal_income_data(['compensation'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_personal_income_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_personal_income_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_personal_income_data()
plot_personal_income_series_advanced(['personal_income', 'disposable_income'], 'multi_line', 'mom')

# %%
plot_personal_income_series_advanced(['personal_income', 'disposable_income'], 'multi_line', 'yoy')
# %%
