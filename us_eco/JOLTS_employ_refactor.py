# %%
"""
US JOLTS 데이터 분석 (리팩토링 버전)
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
# === US JOLTS 시리즈 정의 ===

# JOLTS 시리즈 매핑 (시리즈 이름: API ID)
JOLTS_SERIES = {
    # Total nonfarm
    'total_nonfarm_hires': 'JTS000000000000000HIL',
    'total_nonfarm_openings': 'JTS000000000000000JOL',
    'total_nonfarm_layoffs': 'JTS000000000000000LDL',
    
    # Total private
    'total_private_hires': 'JTS100000000000000HIL',
    'total_private_openings': 'JTS100000000000000JOL',
    'total_private_layoffs': 'JTS100000000000000LDL',
    
    # Mining and logging
    'mining_logging_hires': 'JTS110099000000000HIL',
    'mining_logging_openings': 'JTS110099000000000JOL',
    'mining_logging_layoffs': 'JTS110099000000000LDL',
    
    # Construction
    'construction_hires': 'JTS230000000000000HIL',
    'construction_openings': 'JTS230000000000000JOL',
    'construction_layoffs': 'JTS230000000000000LDL',
    
    # Manufacturing
    'manufacturing_hires': 'JTS300000000000000HIL',
    'manufacturing_openings': 'JTS300000000000000JOL',
    'manufacturing_layoffs': 'JTS300000000000000LDL',
    
    # Trade, transportation, and utilities
    'trade_transport_hires': 'JTS400000000000000HIL',
    'trade_transport_openings': 'JTS400000000000000JOL',
    'trade_transport_layoffs': 'JTS400000000000000LDL',
    
    # Information
    'information_hires': 'JTS510000000000000HIL',
    'information_openings': 'JTS510000000000000JOL',
    'information_layoffs': 'JTS510000000000000LDL',
    
    # Financial activities
    'financial_hires': 'JTS510099000000000HIL',
    'financial_openings': 'JTS510099000000000JOL',
    'financial_layoffs': 'JTS510099000000000LDL',
    
    # Professional and business services
    'professional_business_hires': 'JTS540099000000000HIL',
    'professional_business_openings': 'JTS540099000000000JOL',
    'professional_business_layoffs': 'JTS540099000000000LDL',
    
    # Private education and health services
    'education_health_hires': 'JTS600000000000000HIL',
    'education_health_openings': 'JTS600000000000000JOL',
    'education_health_layoffs': 'JTS600000000000000LDL',
    
    # Leisure and hospitality
    'leisure_hospitality_hires': 'JTS700000000000000HIL',
    'leisure_hospitality_openings': 'JTS700000000000000JOL',
    'leisure_hospitality_layoffs': 'JTS700000000000000LDL',
    
    # Other services
    'other_services_hires': 'JTS810000000000000HIL',
    'other_services_openings': 'JTS810000000000000JOL',
    'other_services_layoffs': 'JTS810000000000000LDL',
    
    # Government
    'government_hires': 'JTS900000000000000HIL',
    'government_openings': 'JTS900000000000000JOL',
    'government_layoffs': 'JTS900000000000000LDL'
}

# 한국어 매핑 딕셔너리
KOREAN_NAMES = {
    # Total nonfarm
    'total_nonfarm_hires': '전체 비농업 - 채용',
    'total_nonfarm_openings': '전체 비농업 - 구인',
    'total_nonfarm_layoffs': '전체 비농업 - 해고 및 퇴직',
    
    # Total private
    'total_private_hires': '전체 민간 - 채용',
    'total_private_openings': '전체 민간 - 구인',
    'total_private_layoffs': '전체 민간 - 해고 및 퇴직',
    
    # Sectors
    'mining_logging_hires': '광업 및 벌목업 - 채용',
    'mining_logging_openings': '광업 및 벌목업 - 구인',
    'mining_logging_layoffs': '광업 및 벌목업 - 해고 및 퇴직',
    
    'construction_hires': '건설업 - 채용',
    'construction_openings': '건설업 - 구인',
    'construction_layoffs': '건설업 - 해고 및 퇴직',
    
    'manufacturing_hires': '제조업 - 채용',
    'manufacturing_openings': '제조업 - 구인',
    'manufacturing_layoffs': '제조업 - 해고 및 퇴직',
    
    'trade_transport_hires': '무역/운송/유틸리티 - 채용',
    'trade_transport_openings': '무역/운송/유틸리티 - 구인',
    'trade_transport_layoffs': '무역/운송/유틸리티 - 해고 및 퇴직',
    
    'information_hires': '정보산업 - 채용',
    'information_openings': '정보산업 - 구인',
    'information_layoffs': '정보산업 - 해고 및 퇴직',
    
    'financial_hires': '금융업 - 채용',
    'financial_openings': '금융업 - 구인',
    'financial_layoffs': '금융업 - 해고 및 퇴직',
    
    'professional_business_hires': '전문/비즈니스 서비스 - 채용',
    'professional_business_openings': '전문/비즈니스 서비스 - 구인',
    'professional_business_layoffs': '전문/비즈니스 서비스 - 해고 및 퇴직',
    
    'education_health_hires': '교육/의료 서비스(민간) - 채용',
    'education_health_openings': '교육/의료 서비스(민간) - 구인',
    'education_health_layoffs': '교육/의료 서비스(민간) - 해고 및 퇴직',
    
    'leisure_hospitality_hires': '레저/숙박업 - 채용',
    'leisure_hospitality_openings': '레저/숙박업 - 구인',
    'leisure_hospitality_layoffs': '레저/숙박업 - 해고 및 퇴직',
    
    'other_services_hires': '기타 서비스 - 채용',
    'other_services_openings': '기타 서비스 - 구인',
    'other_services_layoffs': '기타 서비스 - 해고 및 퇴직',
    
    'government_hires': '정부 - 채용',
    'government_openings': '정부 - 구인',
    'government_layoffs': '정부 - 해고 및 퇴직'
}

# 카테고리 분류
CATEGORIES = {
    'aggregates': {
        'name': 'Aggregate Measures',
        'Total': ['total_nonfarm_hires', 'total_nonfarm_openings', 'total_nonfarm_layoffs'],
        'Private': ['total_private_hires', 'total_private_openings', 'total_private_layoffs'],
        'Government': ['government_hires', 'government_openings', 'government_layoffs']
    },
    'by_industry': {
        'name': 'Industry Analysis',
        'Goods': ['mining_logging_openings', 'construction_openings', 'manufacturing_openings'],
        'Services': ['trade_transport_openings', 'information_openings', 'financial_openings', 
                    'professional_business_openings', 'education_health_openings', 'leisure_hospitality_openings', 
                    'other_services_openings'],
        'Tech_Finance': ['information_openings', 'financial_openings', 'professional_business_openings']
    },
    'by_indicator': {
        'name': 'Indicator Analysis',
        'Job_Openings': ['total_nonfarm_openings', 'total_private_openings', 'government_openings'],
        'Hires': ['total_nonfarm_hires', 'total_private_hires', 'government_hires'],
        'Layoffs': ['total_nonfarm_layoffs', 'total_private_layoffs', 'government_layoffs']
    }
}

print("✓ JOLTS 데이터 구조 정의 완료")

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/jolts_data.csv'
JOLTS_DATA = {
    'raw_data': pd.DataFrame(),
    'mom_data': pd.DataFrame(),
    'mom_change': pd.DataFrame(),
    'yoy_data': pd.DataFrame(),
    'yoy_change': pd.DataFrame(),
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0,
        'source': None,
    },
}

# %%
# === 데이터 로드 함수 ===
def load_jolts_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 JOLTS 데이터 로드"""
    global JOLTS_DATA

    # 시리즈 딕셔너리를 올바른 {name: bls_id} 형태로 전달
    series_dict = JOLTS_SERIES

    result = load_economic_data(
        series_dict=series_dict,
        data_source='BLS',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=1000.0  # 고용 데이터용 허용 오차
    )

    if result:
        JOLTS_DATA = result
        print_load_info()
        return True
    else:
        print("❌ JOLTS 데이터 로드 실패")
        return False

def print_load_info():
    """JOLTS 데이터 로드 정보 출력"""
    if not JOLTS_DATA or 'load_info' not in JOLTS_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = JOLTS_DATA['load_info']
    print(f"\n📊 JOLTS 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in JOLTS_DATA and not JOLTS_DATA['raw_data'].empty:
        latest_date = JOLTS_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_jolts_series_advanced(series_list, chart_type='multi_line', 
                               data_type='mom', periods=None, target_date=None,
                               left_ytitle=None, right_ytitle=None):
    """범용 JOLTS 시각화 함수 - plot_economic_series 활용"""
    if not JOLTS_DATA:
        print("⚠️ 먼저 load_jolts_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=JOLTS_DATA,
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
def export_jolts_data(series_list, data_type='mom', periods=None, 
                      target_date=None, export_path=None, file_format='excel'):
    """JOLTS 데이터 export 함수 - export_economic_data 활용"""
    if not JOLTS_DATA:
        print("⚠️ 먼저 load_jolts_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=JOLTS_DATA,
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

def clear_jolts_data():
    """JOLTS 데이터 초기화"""
    global JOLTS_DATA
    JOLTS_DATA = {}
    print("🗑️ JOLTS 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not JOLTS_DATA or 'raw_data' not in JOLTS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_jolts_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return JOLTS_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in JOLTS_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return JOLTS_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not JOLTS_DATA or 'mom_data' not in JOLTS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_jolts_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return JOLTS_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in JOLTS_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return JOLTS_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not JOLTS_DATA or 'yoy_data' not in JOLTS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_jolts_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return JOLTS_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in JOLTS_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return JOLTS_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not JOLTS_DATA or 'raw_data' not in JOLTS_DATA:
        return []
    return list(JOLTS_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 JOLTS 시리즈 표시"""
    print("=== 사용 가능한 JOLTS 시리즈 ===")
    
    for series_name, series_id in JOLTS_SERIES.items():
        korean_name = KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, info in CATEGORIES.items():
        print(f"\n{category} ({info['name']}):")
        for group_name, series_list in info.items():
            if group_name != 'name':
                print(f"  {group_name}: {len(series_list)}개 시리즈")
                for series_name in series_list:
                    korean_name = KOREAN_NAMES.get(series_name, series_name)
                    api_id = JOLTS_SERIES.get(series_name, series_name)
                    print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not JOLTS_DATA or 'load_info' not in JOLTS_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': JOLTS_DATA['load_info']['loaded'],
        'series_count': JOLTS_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': JOLTS_DATA['load_info']
    }

if __name__ == "__main__":
    print("=== 리팩토링된 JOLTS 분석 도구 사용법 ===")
    print("1. 데이터 로드:")
    print("   load_jolts_data()  # 스마트 업데이트")
    print("   load_jolts_data(force_reload=True)  # 강제 재로드")
    print()
    print("2. 🔥 범용 시각화 (가장 강력!):")
    print("   plot_jolts_series_advanced(['total_nonfarm_openings', 'total_nonfarm_hires'], 'multi_line', 'mom')")
    print("   plot_jolts_series_advanced(['total_nonfarm_openings'], 'horizontal_bar', 'yoy', left_ytitle='천 명')")
    print("   plot_jolts_series_advanced(['total_private_openings'], 'single_line', 'mom', periods=24, left_ytitle='천 명')")
    print("   plot_jolts_series_advanced(['total_nonfarm_openings', 'total_nonfarm_hires'], 'dual_axis', 'raw', left_ytitle='천 명', right_ytitle='천 명')")
    print()
    print("3. 🔥 데이터 Export:")
    print("   export_jolts_data(['total_nonfarm_openings', 'total_nonfarm_hires'], 'mom')")
    print("   export_jolts_data(['total_nonfarm_openings'], 'raw', periods=24, file_format='csv')")
    print("   export_jolts_data(['total_private_openings'], 'yoy', target_date='2024-06-01')")
    print()
    print("✅ plot_jolts_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
    print("✅ export_jolts_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
    print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

    load_jolts_data()
    plot_jolts_series_advanced(['total_nonfarm_openings', 'total_nonfarm_hires'], 'multi_line', 'mom')
