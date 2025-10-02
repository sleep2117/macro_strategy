# %%
"""
GDP 데이터 분석 (리팩토링 버전)
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
# === GDP 시리즈 정의 ===

# GDP 주요 구성 요소 시리즈 맵 - QoQ 변화율 (RL1Q225SBEA: 전분기대비 연율화 변화율)
GDP_MAIN_QOQ_SERIES = {
    'gdp_qoq': 'A191RL1Q225SBEA',  # Gross domestic product (quarterly change rate)
    'consumption_qoq': 'DPCERL1Q225SBEA',  # Personal consumption expenditures (change rate)
    'investment_qoq': 'A006RL1Q225SBEA',  # Gross private domestic investment (change rate)
    'government_qoq': 'A822RL1Q225SBEA',  # Government consumption & investment (change rate)
    'exports_qoq': 'A020RL1Q158SBEA',  # Exports (change rate)
    'imports_qoq': 'A021RL1Q158SBEA'   # Imports (change rate)
}

# GDP 주요 구성 요소 시리즈 맵 - YoY 변화율 (직접 FRED에서 제공하는 YoY 시리즈)
GDP_MAIN_YOY_SERIES = {
    'gdp_yoy': 'A191RL1A225NBEA',  # Gross domestic product (YoY)
    'consumption_yoy': 'DPCERL1A225NBEA',  # Personal consumption expenditures (YoY)
    'investment_yoy': 'A006RL1A225NBEA',  # Gross private domestic investment (YoY)
    'government_yoy': 'A822RL1A225NBEA',  # Government consumption & investment (YoY)
    'exports_yoy': 'A020RL1A225NBEA',  # Exports (YoY)
    'imports_yoy': 'A021RL1A225NBEA',   # Imports (YoY)
    'equipment_yoy': 'Y033RL1A225NBEA',  # Equipment (YoY)
    'info_processing_software_yoy': 'A679RL1A225NBEA',  # Information Processing Equipment and Software (YoY)
}

# GDP 주요 구성 요소 시리즈 맵 - 기여도 (RY2Q224SBEA: GDP 성장 기여도 포인트)
# 주의: 수입은 GDP에서 차감되므로 기여도가 음수로 나오는 경우가 많음
GDP_MAIN_CONTRIB_SERIES = {
    'consumption_contrib': 'DPCERY2Q224SBEA',  # Personal consumption expenditures (contribution)
    'investment_contrib': 'A006RY2Q224SBEA',  # Gross private domestic investment (contribution) 
    'government_contrib': 'A822RY2Q224SBEA',  # Government consumption & investment (contribution)
    'net_exports_contrib': 'A019RY2Q224SBEA',  # Net exports (contribution) - 수출-수입 순기여도
    'exports_contrib': 'A020RY2Q224SBEA',  # Exports (contribution)
    'imports_contrib': 'A021RY2Q224SBEA'   # Imports (contribution) - 보통 음수
}

# GDP 소비 세부 구성 요소 - 변화율
GDP_CONSUMPTION_CHANGE_SERIES = {
    'goods_qoq': 'DGDSRL1Q225SBEA',  # Goods (change rate)
    'durable_goods_qoq': 'DDURRL1Q225SBEA',  # Durable goods (change rate)
    'nondurable_goods_qoq': 'DNDGRL1Q225SBEA',  # Nondurable goods (change rate)
    'services_qoq': 'DSERRL1Q225SBEA',  # Services (change rate)
    'motor_vehicles_qoq': 'DMOTRL1Q225SBEA',  # Motor vehicles & parts (change rate)
    'housing_utilities_qoq': 'DHUTRL1Q225SBEA',  # Housing & utilities (change rate)
    'health_care_qoq': 'DHLCRL1Q225SBEA',  # Health care (change rate)
    'food_services_qoq': 'DFSARL1Q225SBEA'  # Food services & accommodation (change rate)
}

# GDP 소비 세부 구성 요소 - 기여도
GDP_CONSUMPTION_CONTRIB_SERIES = {
    'goods_contrib': 'DGDSRY2Q224SBEA',  # Goods (contribution)
    'durable_goods_contrib': 'DDURRY2Q224SBEA',  # Durable goods (contribution)
    'nondurable_goods_contrib': 'DNDGRY2Q224SBEA',  # Nondurable goods (contribution)
    'services_contrib': 'DSERRY2Q224SBEA',  # Services (contribution)
    'motor_vehicles_contrib': 'DMOTRY2Q224SBEA',  # Motor vehicles & parts (contribution)
    'housing_utilities_contrib': 'DHUTRY2Q224SBEA',  # Housing & utilities (contribution)
    'health_care_contrib': 'DHLCRY2Q224SBEA',  # Health care (contribution)
    'food_services_contrib': 'DFSARY2Q224SBEA',  # Food services & accommodation (contribution)
    'recreation_services_contrib': 'DRCARY2Q224SBEA',  # Recreation services (contribution)
    'recreational_goods_contrib': 'DREQRY2Q224SBEA',  # Recreational goods and vehicles (contribution)
    'transportation_services_contrib': 'DTRSRY2Q224SBEA',  # Transportation services (contribution)
    'financial_services_contrib': 'DIFSRY2Q224SBEA',  # Financial services and insurance (contribution)
    'nondurable_goods_alt_contrib': 'A356RY2Q224SBEA',  # Nondurable goods - alternative series (contribution)
}

# GDP 투자 세부 구성 요소 - 변화율
GDP_INVESTMENT_CHANGE_SERIES = {
    'fixed_investment_qoq': 'A007RL1Q225SBEA',  # Fixed investment (change rate)
    'nonresidential_qoq': 'A008RL1Q225SBEA',  # Non-residential (change rate)
    'residential_qoq': 'A011RL1Q225SBEA',  # Residential (change rate)
    'structures_qoq': 'A009RL1Q225SBEA',  # Structures (change rate)
    'equipment_qoq': 'Y033RL1Q225SBEA',  # Equipment (change rate)
    'intellectual_property_qoq': 'Y001RL1Q225SBEA',  # Intellectual property products (change rate)
    'software_qoq': 'B985RL1Q225SBEA',  # Software (change rate)
    'info_processing_software_qoq': 'A679RL1Q225SBEA',  # Information Processing Equipment and Software (change rate)
}

# GDP 투자 세부 구성 요소 - 기여도
GDP_INVESTMENT_CONTRIB_SERIES = {
    'fixed_investment_contrib': 'A007RY2Q224SBEA',  # Fixed investment (contribution)
    'nonresidential_contrib': 'A008RY2Q224SBEA',  # Non-residential (contribution)
    'residential_contrib': 'A011RY2Q224SBEA',  # Residential (contribution)
    'structures_contrib': 'A009RY2Q224SBEA',  # Structures (contribution)
    'equipment_contrib': 'Y033RY2Q224SBEA',  # Equipment (contribution)
    'intellectual_property_contrib': 'Y001RY2Q224SBEA',  # Intellectual property products (contribution)
    'software_contrib': 'B985RY2Q224SBEA',  # Software (contribution)
    'information_processing_equipment_contrib': 'Y034RY2Q224SBEA',  # Information Processing Equipment (contribution)
    'transportation_equipment_contrib': 'A681RY2Q224SBEA',  # Transportation equipment (contribution)
    'research_development_contrib': 'Y694RY2Q224SBEA',  # Research and Development (contribution)
    'inventory_change_contrib': 'A014RY2Q224SBEA',  # Change in private inventories (contribution)
}

# GDP 정부 지출 세부 구성 요소 - 변화율
GDP_GOVERNMENT_CHANGE_SERIES = {
    'federal_qoq': 'A823RL1Q225SBEA',  # Federal (change rate)
    'state_local_qoq': 'A829RL1Q225SBEA',  # State & local (change rate)
    'defense_qoq': 'A824RL1Q225SBEA',  # National defense (change rate)
    'nondefense_qoq': 'A825RL1Q225SBEA',  # Non-defense (change rate)
    'defense_consumption_qoq': 'A997RL1Q225SBEA',  # Defense consumption (change rate)
    'defense_investment_qoq': 'A788RL1Q225SBEA'  # Defense investment (change rate)
}

# GDP 정부 지출 세부 구성 요소 - 기여도
GDP_GOVERNMENT_CONTRIB_SERIES = {
    'federal_contrib': 'A823RY2Q224SBEA',  # Federal (contribution)
    'state_local_contrib': 'A829RY2Q224SBEA',  # State & local (contribution)
    'defense_contrib': 'A824RY2Q224SBEA',  # National defense (contribution)
    'nondefense_contrib': 'A825RY2Q224SBEA',  # Non-defense (contribution)
    'defense_consumption_contrib': 'A997RY2Q224SBEA',  # Defense consumption (contribution)
    'defense_investment_contrib': 'A788RY2Q224SBEA'  # Defense investment (contribution)
}

# GDP 무역 세부 구성 요소 - 변화율
GDP_TRADE_CHANGE_SERIES = {
    'exports_goods_qoq': 'A253RL1Q225SBEA',  # Exports goods (change rate)
    'exports_services_qoq': 'A646RL1Q225SBEA',  # Exports services (change rate)
    'imports_goods_qoq': 'A255RL1Q225SBEA',  # Imports goods (change rate)
    'imports_services_qoq': 'A656RL1Q225SBEA'  # Imports services (change rate)
}

# GDP 무역 세부 구성 요소 - 기여도
GDP_TRADE_CONTRIB_SERIES = {
    'exports_goods_contrib': 'A253RY2Q224SBEA',  # Exports goods (contribution)
    'exports_services_contrib': 'A646RY2Q224SBEA',  # Exports services (contribution)
    'imports_goods_contrib': 'A255RY2Q224SBEA',  # Imports goods (contribution)
    'imports_services_contrib': 'A656RY2Q224SBEA'  # Imports services (contribution)
}

# 모든 GDP 시리즈 통합 - 고유 키로 업데이트
ALL_GDP_SERIES = {
    **GDP_MAIN_QOQ_SERIES,    # QoQ 주요 시리즈
    **GDP_MAIN_YOY_SERIES,    # YoY 주요 시리즈 
    **GDP_MAIN_CONTRIB_SERIES,    # 기여도 주요 시리즈
    **GDP_CONSUMPTION_CHANGE_SERIES,    # 소비 QoQ 시리즈
    **GDP_CONSUMPTION_CONTRIB_SERIES,   # 소비 기여도 시리즈
    **GDP_INVESTMENT_CHANGE_SERIES,     # 투자 QoQ 시리즈
    **GDP_INVESTMENT_CONTRIB_SERIES,    # 투자 기여도 시리즈
    **GDP_GOVERNMENT_CHANGE_SERIES,     # 정부 QoQ 시리즈
    **GDP_GOVERNMENT_CONTRIB_SERIES,    # 정부 기여도 시리즈
    **GDP_TRADE_CHANGE_SERIES,          # 무역 QoQ 시리즈
    **GDP_TRADE_CONTRIB_SERIES          # 무역 기여도 시리즈
}

# 호환성을 위한 기존 변수들 (더 이상 필요없지만 기존 코드와의 호환성)
ALL_GDP_QOQ_SERIES = ALL_GDP_SERIES  # 이제 모든 시리즈 포함
ALL_GDP_YOY_SERIES = GDP_MAIN_YOY_SERIES  # YoY는 주요 구성요소만
ALL_GDP_CONTRIB_SERIES = {**GDP_MAIN_CONTRIB_SERIES, **GDP_CONSUMPTION_CONTRIB_SERIES, **GDP_INVESTMENT_CONTRIB_SERIES, **GDP_GOVERNMENT_CONTRIB_SERIES, **GDP_TRADE_CONTRIB_SERIES}

# 한국어 이름 매핑
GDP_KOREAN_NAMES = {
    # 주요 구성 요소 - QoQ
    'gdp_qoq': 'GDP (전분기비, %)',
    'consumption_qoq': '개인소비 (전분기비, %)',
    'investment_qoq': '민간투자 (전분기비, %)',
    'government_qoq': '정부지출 (전분기비, %)',
    'exports_qoq': '수출 (전분기비, %)',
    'imports_qoq': '수입 (전분기비, %)',
    
    # 주요 구성 요소 - YoY
    'gdp_yoy': 'GDP (전년비, %)',
    'consumption_yoy': '개인소비 (전년비, %)',
    'investment_yoy': '민간투자 (전년비, %)',
    'government_yoy': '정부지출 (전년비, %)',
    'exports_yoy': '수출 (전년비, %)',
    'imports_yoy': '수입 (전년비, %)',
    'equipment_yoy': '장비 (전년비, %)',
    'info_processing_software_yoy': '정보처리장비및소프트웨어 (전년비, %)',
    
    # 주요 구성 요소 - 기여도
    'consumption_contrib': '개인소비 (기여도)',
    'investment_contrib': '민간투자 (기여도)',
    'government_contrib': '정부지출 (기여도)',
    'net_exports_contrib': '순수출 (기여도)',
    'exports_contrib': '수출 (기여도)',
    'imports_contrib': '수입 (기여도)',
    
    # 소비 세부 항목 - QoQ
    'goods_qoq': '재화 (전분기비, %)',
    'durable_goods_qoq': '내구재 (전분기비, %)',
    'nondurable_goods_qoq': '비내구재 (전분기비, %)',
    'services_qoq': '서비스 (전분기비, %)',
    'motor_vehicles_qoq': '자동차 및 부품 (전분기비, %)',
    'housing_utilities_qoq': '주택 및 유틸리티 (전분기비, %)',
    'health_care_qoq': '의료 (전분기비, %)',
    'food_services_qoq': '외식 및 숙박 (전분기비, %)',
    
    # 소비 세부 항목 - 기여도
    'goods_contrib': '재화 (기여도)',
    'durable_goods_contrib': '내구재 (기여도)',
    'nondurable_goods_contrib': '비내구재 (기여도)',
    'services_contrib': '서비스 (기여도)',
    'motor_vehicles_contrib': '자동차 및 부품 (기여도)',
    'housing_utilities_contrib': '주택 및 유틸리티 (기여도)',
    'health_care_contrib': '의료 (기여도)',
    'food_services_contrib': '외식 및 숙박 (기여도)',
    'recreation_services_contrib': '여가서비스 (기여도)',
    'recreational_goods_contrib': '여가용품 및 차량 (기여도)',
    'transportation_services_contrib': '운송서비스 (기여도)',
    'financial_services_contrib': '금융 및 보험서비스 (기여도)',
    'nondurable_goods_alt_contrib': '비내구재_대안시리즈 (기여도)',
    
    # 투자 세부 항목 - QoQ
    'fixed_investment_qoq': '고정투자 (전분기비, %)',
    'nonresidential_qoq': '비주거용 (전분기비, %)',
    'residential_qoq': '주거용 (전분기비, %)',
    'structures_qoq': '구조물 (전분기비, %)',
    'equipment_qoq': '장비 (전분기비, %)',
    'intellectual_property_qoq': '지적재산 (전분기비, %)',
    'software_qoq': '소프트웨어 (전분기비, %)',
    'info_processing_software_qoq': '정보처리장비및소프트웨어 (전분기비, %)',
    
    # 투자 세부 항목 - 기여도
    'fixed_investment_contrib': '고정투자 (기여도)',
    'nonresidential_contrib': '비주거용 (기여도)',
    'residential_contrib': '주거용 (기여도)',
    'structures_contrib': '구조물 (기여도)',
    'equipment_contrib': '장비 (기여도)',
    'intellectual_property_contrib': '지적재산 (기여도)',
    'software_contrib': '소프트웨어 (기여도)',
    'information_processing_equipment_contrib': '정보처리장비 (기여도)',
    'transportation_equipment_contrib': '운송장비 (기여도)',
    'research_development_contrib': '연구개발 (기여도)',
    'inventory_change_contrib': '재고변동 (기여도)',
    
    # 정부 지출 세부 항목 - QoQ
    'federal_qoq': '연방정부 (전분기비, %)',
    'state_local_qoq': '주·지방정부 (전분기비, %)',
    'defense_qoq': '국방 (전분기비, %)',
    'nondefense_qoq': '비국방 (전분기비, %)',
    'defense_consumption_qoq': '국방소비 (전분기비, %)',
    'defense_investment_qoq': '국방투자 (전분기비, %)',
    
    # 정부 지출 세부 항목 - 기여도
    'federal_contrib': '연방정부 (기여도)',
    'state_local_contrib': '주·지방정부 (기여도)',
    'defense_contrib': '국방 (기여도)',
    'nondefense_contrib': '비국방 (기여도)',
    'defense_consumption_contrib': '국방소비 (기여도)',
    'defense_investment_contrib': '국방투자 (기여도)',
    
    # 무역 세부 항목 - QoQ
    'exports_goods_qoq': '수출 재화 (전분기비, %)',
    'exports_services_qoq': '수출 서비스 (전분기비, %)',
    'imports_goods_qoq': '수입 재화 (전분기비, %)',
    'imports_services_qoq': '수입 서비스 (전분기비, %)',
    
    # 무역 세부 항목 - 기여도
    'exports_goods_contrib': '수출 재화 (기여도)',
    'exports_services_contrib': '수출 서비스 (기여도)',
    'imports_goods_contrib': '수입 재화 (기여도)',
    'imports_services_contrib': '수입 서비스 (기여도)'
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = data_path('gdp_data.csv')
GDP_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_gdp_data(start_date='2020-01-01', smart_update=True, force_reload=False, include_contrib=True):
    """통합 함수 사용한 GDP 데이터 로드 (기여도 시리즈 포함)"""
    global GDP_DATA

    # 통합 시리즈 딕셔너리 사용 (이미 고유한 키들로 구성됨)
    series_dict = ALL_GDP_SERIES.copy()
    
    # include_contrib=False인 경우 기여도 시리즈 제외
    if not include_contrib:
        series_dict = {k: v for k, v in series_dict.items() if not k.endswith('_contrib')}
    
    print(f"📊 로드할 시리즈 개수: {len(series_dict)}개")
    print(f"   QoQ 시리즈: {len([k for k in series_dict.keys() if k.endswith('_qoq')])}개")
    print(f"   YoY 시리즈: {len([k for k in series_dict.keys() if k.endswith('_yoy')])}개") 
    print(f"   기여도 시리즈: {len([k for k in series_dict.keys() if k.endswith('_contrib')])}개")

    result = load_economic_data(
        series_dict=series_dict,
        data_source='FRED',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=10.0  # GDP 데이터 허용 오차
    )

    if result:
        GDP_DATA = result
        print_load_info()
        return True
    else:
        print("❌ GDP 데이터 로드 실패")
        return False

def print_load_info():
    """GDP 데이터 로드 정보 출력"""
    if not GDP_DATA or 'load_info' not in GDP_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = GDP_DATA['load_info']
    print(f"\n📊 GDP 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in GDP_DATA and not GDP_DATA['raw_data'].empty:
        latest_date = GDP_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_gdp_series_advanced(series_list, chart_type='multi_line', 
                            data_type='mom', periods=None, target_date=None,
                            left_ytitle=None, right_ytitle=None):
    """범용 GDP 시각화 함수 - plot_economic_series 활용"""
    if not GDP_DATA:
        print("⚠️ 먼저 load_gdp_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=GDP_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle,
        right_ytitle=right_ytitle,
        korean_names=GDP_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_gdp_data(series_list, data_type='mom', periods=None, 
                   target_date=None, export_path=None, file_format='excel'):
    """GDP 데이터 export 함수 - export_economic_data 활용"""
    if not GDP_DATA:
        print("⚠️ 먼저 load_gdp_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=GDP_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=GDP_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_gdp_data():
    """GDP 데이터 초기화"""
    global GDP_DATA
    GDP_DATA = {}
    print("🗑️ GDP 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not GDP_DATA or 'raw_data' not in GDP_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_gdp_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return GDP_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in GDP_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return GDP_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not GDP_DATA or 'mom_data' not in GDP_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_gdp_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return GDP_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in GDP_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return GDP_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not GDP_DATA or 'yoy_data' not in GDP_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_gdp_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return GDP_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in GDP_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return GDP_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not GDP_DATA or 'raw_data' not in GDP_DATA:
        return []
    return list(GDP_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 GDP 시리즈 표시"""
    print("=== 사용 가능한 GDP 시리즈 ===")
    
    # QoQ 시리즈
    print("\n📈 QoQ 변화율 시리즈:")
    qoq_series = {k: v for k, v in ALL_GDP_SERIES.items() if k.endswith('_qoq')}
    for series_id in sorted(qoq_series.keys()):
        korean_name = GDP_KOREAN_NAMES.get(series_id, series_id)
        print(f"  '{series_id}': {korean_name}")
    
    # YoY 시리즈
    print("\n📊 YoY 변화율 시리즈:")
    yoy_series = {k: v for k, v in ALL_GDP_SERIES.items() if k.endswith('_yoy')}
    for series_id in sorted(yoy_series.keys()):
        korean_name = GDP_KOREAN_NAMES.get(series_id, series_id)
        print(f"  '{series_id}': {korean_name}")
    
    # 기여도 시리즈
    print("\n🎯 기여도 시리즈:")
    contrib_series = {k: v for k, v in ALL_GDP_SERIES.items() if k.endswith('_contrib')}
    for series_id in sorted(contrib_series.keys()):
        korean_name = GDP_KOREAN_NAMES.get(series_id, series_id)
        print(f"  '{series_id}': {korean_name}")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not GDP_DATA or 'load_info' not in GDP_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': GDP_DATA['load_info']['loaded'],
        'series_count': GDP_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': GDP_DATA['load_info']
    }

# %%
# === GDP 전용 시각화/분석 함수들 (보존) ===

def create_gdp_contribution_chart():
    """
    GDP 기여도 차트 생성 (KPDS 포맷) - 로드된 기여도 시리즈 사용
    
    Returns:
        plotly figure
    """
    print("GDP 구성 요소별 기여도 (전분기대비)")
    if not GDP_DATA:
        print("⚠️ 먼저 load_gdp_data()를 실행하세요.")
        return None
    
    # 기여도 시리즈 확인 (_contrib 접미사가 붙은 시리즈들)
    contrib_columns = [col for col in GDP_DATA['raw_data'].columns if col.endswith('_contrib')]
    
    if not contrib_columns:
        print("⚠️ 기여도 데이터가 로드되지 않았습니다.")
        print("💡 해결방법: load_gdp_data(include_contrib=True, force_reload=True)를 실행해보세요.")
        return None
    
    # 주요 구성 요소들의 기여도 (새로운 키 구조 사용)
    main_components = ['consumption_contrib', 'investment_contrib', 'government_contrib', 'net_exports_contrib']
    available_components = [comp for comp in main_components if comp in GDP_DATA['raw_data'].columns]
    
    if not available_components:
        print("⚠️ 기여도 계산을 위한 데이터가 없습니다.")
        return None
    
    # vertical_bar 차트로 기여도 시각화 (raw 데이터 사용 - 이미 기여도 포인트)
    return plot_economic_series(
        data_dict=GDP_DATA,
        series_list=available_components,
        chart_type='vertical_bar',
        data_type='raw',  # 기여도 데이터는 이미 포인트 단위
        periods=8,
        left_ytitle="포인트",
        korean_names=GDP_KOREAN_NAMES
    )

# %%
# === 사용 예시 (업데이트된 키 구조) ===

print("=== 리팩토링된 GDP 분석 도구 사용법 (시리즈 키 중복 해결 완료!) ===")
print("1. 데이터 로드:")
print("   load_gdp_data()  # 스마트 업데이트 (기여도 시리즈 포함)")
print("   load_gdp_data(include_contrib=False)  # 기여도 시리즈 제외")
print("   load_gdp_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!) - 새로운 고유 키 사용:")
print("   plot_gdp_series_advanced(['gdp_qoq', 'gdp_yoy'], 'multi_line', 'raw')  # QoQ vs YoY 비교")
print("   plot_gdp_series_advanced(['consumption_qoq', 'investment_qoq'], 'multi_line', 'raw', left_ytitle='%')  # 소비 vs 투자 QoQ")
print("   plot_gdp_series_advanced(['consumption_contrib', 'investment_contrib'], 'vertical_bar', 'raw', left_ytitle='포인트')  # 기여도 비교")
print()
print("3. 🔥 기여도 차트 (KPDS 포맷 vertical_bar):")
print("   create_gdp_contribution_chart()  # GDP 구성요소별 기여도")
print()
print("4. 🔥 데이터 Export:")
print("   export_gdp_data(['gdp_qoq', 'gdp_yoy'], 'raw')")
print("   export_gdp_data(['consumption_contrib'], 'raw', periods=24, file_format='csv')")
print("   export_gdp_data(['investment_qoq'], 'raw', target_date='2024-06-01')")
print()
print("5. 시리즈 목록 확인:")
print("   show_available_series()  # 모든 사용 가능한 시리즈 보기")
print()
print("✅ 이제 QoQ, YoY, contrib 시리즈들이 고유한 키를 가져서 중복 문제 해결!")
print("✅ 같은 차트에서 gdp_qoq와 gdp_yoy를 동시에 비교 가능!")
print("✅ 모든 기능이 새로운 키 구조와 호환됨!")

# %%
print("\n🔥 테스트: 새로운 키 구조 확인")
show_available_series()
# %%
print("\n🔥 테스트: 데이터 로드 및 QoQ vs YoY 비교")
load_gdp_data(include_contrib=True)
plot_gdp_series_advanced(['gdp_qoq', 'gdp_yoy'], 'multi_line', 'raw')
# %%
print("\n🔥 테스트: 기여도 차트")
create_gdp_contribution_chart()
# %%
plot_gdp_series_advanced(['consumption_contrib', 'investment_contrib'], 'vertical_bar','raw', left_ytitle='포인트')
# %%
plot_gdp_series_advanced(['gdp_qoq', 'consumption_qoq'], 'multi_line', 'raw', left_ytitle='%')