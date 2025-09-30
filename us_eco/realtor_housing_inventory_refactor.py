# %%
"""
Realtor.com Housing Inventory 데이터 분석 (리팩토링 버전)
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

# FRED 데이터인 경우:
api_config.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'

# %%
# === Realtor.com Housing Inventory 시리즈 정의 ===

# 메인 시리즈 딕셔너리 (올바른 구조: 시리즈_이름: API_ID)
REALTOR_HOUSING_INVENTORY_SERIES = {
    # === National Level 전체 지표 (5개) - FRED에서 실제 제공되는 시리즈만 ===
    'active_listings_us': 'ACTLISCOUUS',                    # 현재 판매중인 주택 수
    'median_days_market_us': 'MEDDAYONMARUS',               # 매물의 시장 체류 기간 중간값
    'median_price_us': 'MEDLISPRIUS',                       # 중간 매물 가격
    'median_price_sqft_us': 'MEDLISPRIPERSQUFEEUS',         # 평방피트당 중간 가격
    'median_sqft_us': 'MEDSQUFEEUS',                        # 중간 주택 크기
    'new_listings_us': 'NEWLISCOUUS',                       # 신규 매물 수 (전국)
    'price_increased_us': 'PRIINCRCOUUS',                   # 가격 인상 매물 수 (전국)
    'price_reduced_us': 'PRIREDCOUUS',                      # 가격 인하 매물 수 (전국)
    
    # === 주요 주(State)별 핵심 지표 ===
    
    # California (CA) - 최대 주택시장
    'active_listings_ca': 'ACTLISCOUCA',
    'median_price_ca': 'MEDLISPRICA',
    'median_days_market_ca': 'MEDDAYONMARCA',
    'new_listings_ca': 'NEWLISCOUCA',
    'median_price_sqft_ca': 'MEDLISPRIPERSQUFEECA',
    
    # Texas (TX) - 두 번째 최대 시장
    'active_listings_tx': 'ACTLISCOUTX',
    'median_price_tx': 'MEDLISPRITX',
    'median_days_market_tx': 'MEDDAYONMARTX',
    'new_listings_tx': 'NEWLISCOUTX',
    'median_price_sqft_tx': 'MEDLISPRIPERSQUFEETX',
    
    # Florida (FL) - 주요 주택시장
    'active_listings_fl': 'ACTLISCOUFL',
    'median_price_fl': 'MEDLISPRIFL',
    'median_days_market_fl': 'MEDDAYONMARFL',
    'new_listings_fl': 'NEWLISCOUFL',
    'median_price_sqft_fl': 'MEDLISPRIPERSQUFEEFL',
    
    # New York (NY)
    'active_listings_ny': 'ACTLISCOUNY',
    'median_price_ny': 'MEDLISPRINY',
    'median_days_market_ny': 'MEDDAYONMARNY',
    'new_listings_ny': 'NEWLISCOUNY',
    'median_price_sqft_ny': 'MEDLISPRIPERSQUFEENY',
    
    # Illinois (IL)
    'active_listings_il': 'ACTLISCOUIL',
    'median_price_il': 'MEDLISPRIIL',
    'median_days_market_il': 'MEDDAYONMARIL',
    'new_listings_il': 'NEWLISCOUIL',
    
    # Pennsylvania (PA)
    'active_listings_pa': 'ACTLISCOUPA',
    'median_price_pa': 'MEDLISPRIPA',
    'median_days_market_pa': 'MEDDAYONMARPA',
    'new_listings_pa': 'NEWLISCOUPA',
    
    # Ohio (OH)
    'active_listings_oh': 'ACTLISCOUOH',
    'median_price_oh': 'MEDLISPRIOH',
    'median_days_market_oh': 'MEDDAYONMAROH',
    'new_listings_oh': 'NEWLISCOUOH',
    
    # Georgia (GA)
    'active_listings_ga': 'ACTLISCOUGA',
    'median_price_ga': 'MEDLISPRIGA',
    'median_days_market_ga': 'MEDDAYONMARGA',
    'new_listings_ga': 'NEWLISCOUGA',
    
    # North Carolina (NC)
    'active_listings_nc': 'ACTLISCOUNC',
    'median_price_nc': 'MEDLISPRINC',
    'median_days_market_nc': 'MEDDAYONMARNC',
    'new_listings_nc': 'NEWLISCOUNC',
    
    # Michigan (MI)
    'active_listings_mi': 'ACTLISCOUMI',
    'median_price_mi': 'MEDLISPRIMI',
    'median_days_market_mi': 'MEDDAYONMARMI',
    'new_listings_mi': 'NEWLISCOUMI',
    
    # Washington (WA)
    'active_listings_wa': 'ACTLISCOUWA',
    'median_price_wa': 'MEDLISPRIWA',
    'median_days_market_wa': 'MEDDAYONMARWA',
    'new_listings_wa': 'NEWLISCOUWA',
    
    # Virginia (VA)
    'active_listings_va': 'ACTLISCOUVA',
    'median_price_va': 'MEDLISPRIVA',
    'median_days_market_va': 'MEDDAYONMARVA',
    'new_listings_va': 'NEWLISCOUVA',
    
    # Arizona (AZ)
    'active_listings_az': 'ACTLISCOUAZ',
    'median_price_az': 'MEDLISPRIAZ',
    'median_days_market_az': 'MEDDAYONMARAZ',
    'new_listings_az': 'NEWLISCOUAZ',
    
    # Tennessee (TN)
    'active_listings_tn': 'ACTLISCOUTN',
    'median_price_tn': 'MEDLISPRITN',
    'median_days_market_tn': 'MEDDAYONMARTN',
    'new_listings_tn': 'NEWLISCOUTN',
    
    # Massachusetts (MA)
    'active_listings_ma': 'ACTLISCOUMA',
    'median_price_ma': 'MEDLISPRIMA',
    'median_days_market_ma': 'MEDDAYONMARMA',
    'new_listings_ma': 'NEWLISCOUMA',
    
    # Colorado (CO)
    'active_listings_co': 'ACTLISCOUCO',
    'median_price_co': 'MEDLISPRICO',
    'median_days_market_co': 'MEDDAYONMARCO',
    'new_listings_co': 'NEWLISCOUCO',
}

# 한국어 이름 매핑
REALTOR_HOUSING_INVENTORY_KOREAN_NAMES = {
    # National Level - FRED에서 실제 제공되는 시리즈만
    'active_listings_us': '전국 활성 매물 수',
    'median_days_market_us': '전국 매물 시장체류기간 중간값',
    'median_price_us': '전국 중간 매물 가격',
    'median_price_sqft_us': '전국 평방피트당 중간 가격',
    'median_sqft_us': '전국 중간 주택 크기',
    'new_listings_us': '전국 신규 매물 수',
    'price_increased_us': '전국 가격 인상 매물 수',
    'price_reduced_us': '전국 가격 인하 매물 수',
    
    # California
    'active_listings_ca': '캘리포니아 활성 매물 수',
    'median_price_ca': '캘리포니아 중간 매물 가격',
    'median_days_market_ca': '캘리포니아 매물 시장체류기간',
    'new_listings_ca': '캘리포니아 신규 매물 수',
    'median_price_sqft_ca': '캘리포니아 평방피트당 중간 가격',
    
    # Texas
    'active_listings_tx': '텍사스 활성 매물 수',
    'median_price_tx': '텍사스 중간 매물 가격',
    'median_days_market_tx': '텍사스 매물 시장체류기간',
    'new_listings_tx': '텍사스 신규 매물 수',
    'median_price_sqft_tx': '텍사스 평방피트당 중간 가격',
    
    # Florida
    'active_listings_fl': '플로리다 활성 매물 수',
    'median_price_fl': '플로리다 중간 매물 가격',
    'median_days_market_fl': '플로리다 매물 시장체류기간',
    'new_listings_fl': '플로리다 신규 매물 수',
    'median_price_sqft_fl': '플로리다 평방피트당 중간 가격',
    
    # New York
    'active_listings_ny': '뉴욕 활성 매물 수',
    'median_price_ny': '뉴욕 중간 매물 가격',
    'median_days_market_ny': '뉴욕 매물 시장체류기간',
    'new_listings_ny': '뉴욕 신규 매물 수',
    'median_price_sqft_ny': '뉴욕 평방피트당 중간 가격',
    
    # Illinois
    'active_listings_il': '일리노이 활성 매물 수',
    'median_price_il': '일리노이 중간 매물 가격',
    'median_days_market_il': '일리노이 매물 시장체류기간',
    'new_listings_il': '일리노이 신규 매물 수',
    
    # Pennsylvania
    'active_listings_pa': '펜실베니아 활성 매물 수',
    'median_price_pa': '펜실베니아 중간 매물 가격',
    'median_days_market_pa': '펜실베니아 매물 시장체류기간',
    'new_listings_pa': '펜실베니아 신규 매물 수',
    
    # Ohio
    'active_listings_oh': '오하이오 활성 매물 수',
    'median_price_oh': '오하이오 중간 매물 가격',
    'median_days_market_oh': '오하이오 매물 시장체류기간',
    'new_listings_oh': '오하이오 신규 매물 수',
    
    # Georgia
    'active_listings_ga': '조지아 활성 매물 수',
    'median_price_ga': '조지아 중간 매물 가격',
    'median_days_market_ga': '조지아 매물 시장체류기간',
    'new_listings_ga': '조지아 신규 매물 수',
    
    # North Carolina
    'active_listings_nc': '노스캐롤라이나 활성 매물 수',
    'median_price_nc': '노스캐롤라이나 중간 매물 가격',
    'median_days_market_nc': '노스캐롤라이나 매물 시장체류기간',
    'new_listings_nc': '노스캐롤라이나 신규 매물 수',
    
    # Michigan
    'active_listings_mi': '미시간 활성 매물 수',
    'median_price_mi': '미시간 중간 매물 가격',
    'median_days_market_mi': '미시간 매물 시장체류기간',
    'new_listings_mi': '미시간 신규 매물 수',
    
    # Washington
    'active_listings_wa': '워싱턴 활성 매물 수',
    'median_price_wa': '워싱턴 중간 매물 가격',
    'median_days_market_wa': '워싱턴 매물 시장체류기간',
    'new_listings_wa': '워싱턴 신규 매물 수',
    
    # Virginia
    'active_listings_va': '버지니아 활성 매물 수',
    'median_price_va': '버지니아 중간 매물 가격',
    'median_days_market_va': '버지니아 매물 시장체류기간',
    'new_listings_va': '버지니아 신규 매물 수',
    
    # Arizona
    'active_listings_az': '애리조나 활성 매물 수',
    'median_price_az': '애리조나 중간 매물 가격',
    'median_days_market_az': '애리조나 매물 시장체류기간',
    'new_listings_az': '애리조나 신규 매물 수',
    
    # Tennessee
    'active_listings_tn': '테네시 활성 매물 수',
    'median_price_tn': '테네시 중간 매물 가격',
    'median_days_market_tn': '테네시 매물 시장체류기간',
    'new_listings_tn': '테네시 신규 매물 수',
    
    # Massachusetts
    'active_listings_ma': '매사추세츠 활성 매물 수',
    'median_price_ma': '매사추세츠 중간 매물 가격',
    'median_days_market_ma': '매사추세츠 매물 시장체류기간',
    'new_listings_ma': '매사추세츠 신규 매물 수',
    
    # Colorado
    'active_listings_co': '콜로라도 활성 매물 수',
    'median_price_co': '콜로라도 중간 매물 가격',
    'median_days_market_co': '콜로라도 매물 시장체류기간',
    'new_listings_co': '콜로라도 신규 매물 수',
}

# 카테고리 분류
REALTOR_HOUSING_INVENTORY_CATEGORIES = {
    '전국 핵심 지표': {
        'Supply Metrics': ['active_listings_us', 'new_listings_us'],
        'Price Metrics': ['median_price_us', 'median_price_sqft_us'],
        'Market Activity': ['median_days_market_us', 'price_increased_us', 'price_reduced_us'],
        'Housing Characteristics': ['median_sqft_us']
    },
    '주요 주별 활성 매물': {
        'Large States': ['active_listings_ca', 'active_listings_tx', 'active_listings_fl', 'active_listings_ny'],
        'Midwest': ['active_listings_il', 'active_listings_oh', 'active_listings_mi'],
        'Southeast': ['active_listings_ga', 'active_listings_nc', 'active_listings_va', 'active_listings_tn'],
        'West': ['active_listings_wa', 'active_listings_az', 'active_listings_co', 'active_listings_ma']
    },
    '주요 주별 중간 가격': {
        'High Price States': ['median_price_ca', 'median_price_ny', 'median_price_ma', 'median_price_wa'],
        'Medium Price States': ['median_price_fl', 'median_price_va', 'median_price_co', 'median_price_az'],
        'Lower Price States': ['median_price_tx', 'median_price_il', 'median_price_oh', 'median_price_ga']
    },
    '주요 주별 시장 속도': {
        'Fast Markets': ['median_days_market_ca', 'median_days_market_tx', 'median_days_market_fl'],
        'Medium Markets': ['median_days_market_ny', 'median_days_market_il', 'median_days_market_ga'],
        'Regional Comparison': ['median_days_market_wa', 'median_days_market_nc', 'median_days_market_co']
    },
    '주요 주별 신규 공급': {
        'High Volume': ['new_listings_ca', 'new_listings_tx', 'new_listings_fl', 'new_listings_ny'],
        'Medium Volume': ['new_listings_il', 'new_listings_pa', 'new_listings_oh', 'new_listings_ga'],
        'Growing Markets': ['new_listings_nc', 'new_listings_tn', 'new_listings_az', 'new_listings_co']
    },
    '가격 효율성 분석': {
        'Price per SqFt': ['median_price_sqft_us', 'median_price_sqft_ca', 'median_price_sqft_ny', 'median_price_sqft_tx'],
        'Market Activity': ['median_days_market_us', 'active_listings_us']
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/realtor_housing_inventory_data_refactored.csv'
REALTOR_HOUSING_INVENTORY_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_realtor_housing_inventory_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 Realtor.com Housing Inventory 데이터 로드"""
    global REALTOR_HOUSING_INVENTORY_DATA

    # 데이터 소스별 분기
    data_source = 'FRED'  # Realtor.com 데이터는 FRED에서 제공
    tolerance = 100.0  # 주택 매물 수는 큰 수치이므로 큰 허용 오차 사용
    
    result = load_economic_data(
        series_dict=REALTOR_HOUSING_INVENTORY_SERIES,
        data_source=data_source,
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=tolerance
    )

    if result:
        REALTOR_HOUSING_INVENTORY_DATA = result
        print_load_info()
        return True
    else:
        print("❌ Realtor.com Housing Inventory 데이터 로드 실패")
        return False

def print_load_info():
    """Realtor.com Housing Inventory 데이터 로드 정보 출력"""
    if not REALTOR_HOUSING_INVENTORY_DATA or 'load_info' not in REALTOR_HOUSING_INVENTORY_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = REALTOR_HOUSING_INVENTORY_DATA['load_info']
    print(f"\n📊 Realtor.com Housing Inventory 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in REALTOR_HOUSING_INVENTORY_DATA and not REALTOR_HOUSING_INVENTORY_DATA['raw_data'].empty:
        latest_date = REALTOR_HOUSING_INVENTORY_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_realtor_housing_inventory_series_advanced(series_list, chart_type='multi_line', 
                                                 data_type='raw', periods=None, target_date=None,
                                                 left_ytitle=None, right_ytitle=None):
    """범용 Realtor.com Housing Inventory 시각화 함수 - plot_economic_series 활용"""
    if not REALTOR_HOUSING_INVENTORY_DATA:
        print("⚠️ 먼저 load_realtor_housing_inventory_data()를 실행하세요.")
        return None

    # 단위별 기본 Y축 제목 설정 (시리즈에 따라 다름)
    if data_type == 'mom':
        default_ytitle = "%"
    elif data_type == 'yoy':
        default_ytitle = "%"  
    else:  # raw
        # 첫 번째 시리즈를 기준으로 단위 결정
        if series_list:
            first_series = series_list[0]
            if 'price' in first_series and 'sqft' in first_series:
                default_ytitle = "달러/평방피트"
            elif 'price' in first_series:
                default_ytitle = "달러"
            elif 'days' in first_series:
                default_ytitle = "일"
            elif 'sqft' in first_series:
                default_ytitle = "평방피트"
            else:
                default_ytitle = "개수"
        else:
            default_ytitle = "단위"

    return plot_economic_series(
        data_dict=REALTOR_HOUSING_INVENTORY_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle or default_ytitle,
        right_ytitle=right_ytitle or default_ytitle,
        korean_names=REALTOR_HOUSING_INVENTORY_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_realtor_housing_inventory_data(series_list, data_type='raw', periods=None, 
                                        target_date=None, export_path=None, file_format='excel'):
    """Realtor.com Housing Inventory 데이터 export 함수 - export_economic_data 활용"""
    if not REALTOR_HOUSING_INVENTORY_DATA:
        print("⚠️ 먼저 load_realtor_housing_inventory_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=REALTOR_HOUSING_INVENTORY_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=REALTOR_HOUSING_INVENTORY_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_realtor_housing_inventory_data():
    """Realtor.com Housing Inventory 데이터 초기화"""
    global REALTOR_HOUSING_INVENTORY_DATA
    REALTOR_HOUSING_INVENTORY_DATA = {}
    print("🗑️ Realtor.com Housing Inventory 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not REALTOR_HOUSING_INVENTORY_DATA or 'raw_data' not in REALTOR_HOUSING_INVENTORY_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_realtor_housing_inventory_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return REALTOR_HOUSING_INVENTORY_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in REALTOR_HOUSING_INVENTORY_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return REALTOR_HOUSING_INVENTORY_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not REALTOR_HOUSING_INVENTORY_DATA or 'mom_data' not in REALTOR_HOUSING_INVENTORY_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_realtor_housing_inventory_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return REALTOR_HOUSING_INVENTORY_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in REALTOR_HOUSING_INVENTORY_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return REALTOR_HOUSING_INVENTORY_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not REALTOR_HOUSING_INVENTORY_DATA or 'yoy_data' not in REALTOR_HOUSING_INVENTORY_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_realtor_housing_inventory_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return REALTOR_HOUSING_INVENTORY_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in REALTOR_HOUSING_INVENTORY_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return REALTOR_HOUSING_INVENTORY_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not REALTOR_HOUSING_INVENTORY_DATA or 'raw_data' not in REALTOR_HOUSING_INVENTORY_DATA:
        return []
    return list(REALTOR_HOUSING_INVENTORY_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 Realtor.com Housing Inventory 시리즈 표시"""
    print("=== 사용 가능한 Realtor.com Housing Inventory 시리즈 ===")
    
    for series_name, series_id in REALTOR_HOUSING_INVENTORY_SERIES.items():
        korean_name = REALTOR_HOUSING_INVENTORY_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in REALTOR_HOUSING_INVENTORY_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_name in series_list:
                korean_name = REALTOR_HOUSING_INVENTORY_KOREAN_NAMES.get(series_name, series_name)
                api_id = REALTOR_HOUSING_INVENTORY_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not REALTOR_HOUSING_INVENTORY_DATA or 'load_info' not in REALTOR_HOUSING_INVENTORY_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': REALTOR_HOUSING_INVENTORY_DATA['load_info']['loaded'],
        'series_count': REALTOR_HOUSING_INVENTORY_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': REALTOR_HOUSING_INVENTORY_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 Realtor.com Housing Inventory 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_realtor_housing_inventory_data()  # 스마트 업데이트")
print("   load_realtor_housing_inventory_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_realtor_housing_inventory_series_advanced(['active_listings_us', 'median_price_us'], 'multi_line', 'raw')")
print("   plot_realtor_housing_inventory_series_advanced(['median_price_ca', 'median_price_tx'], 'multi_line', 'raw', left_ytitle='달러')")
print("   plot_realtor_housing_inventory_series_advanced(['median_days_market_us'], 'single_line', 'yoy', periods=24, left_ytitle='%')")
print("   plot_realtor_housing_inventory_series_advanced(['active_listings_us', 'median_price_us'], 'dual_axis', 'raw', left_ytitle='개수', right_ytitle='달러')")
print()
print("3. 🔥 데이터 Export:")
print("   export_realtor_housing_inventory_data(['median_price_us', 'active_listings_us'], 'raw')")
print("   export_realtor_housing_inventory_data(['median_price_ca'], 'mom', periods=24, file_format='csv')")
print("   export_realtor_housing_inventory_data(['median_days_market_us'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_realtor_housing_inventory_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_realtor_housing_inventory_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_realtor_housing_inventory_data(force_reload=True)
plot_realtor_housing_inventory_series_advanced(['active_listings_us', 'median_price_us'], 'multi_line', 'raw')
