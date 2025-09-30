# %%
"""
PPI 데이터 분석 (리팩토링 버전)
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
# === PPI 시리즈 정의 ===

# PPI 시리즈 매핑 (시리즈 이름: API ID)
PPI_SERIES = {
    # Final Demand (최종 수요)
    'final_demand_sa': 'WPSFD4',
    'final_demand_goods_sa': 'WPSFD41',
    'final_demand_foods_sa': 'WPSFD411',
    'final_demand_energy_sa': 'WPSFD412',
    'final_demand_core_sa': 'WPSFD49104',
    'final_demand_core_ex_trade_sa': 'WPSFD49116',
    'final_demand_services_sa': 'WPSFD42',
    'final_demand_transport_sa': 'WPSFD422',
    'final_demand_trade_sa': 'WPSFD423',
    'final_demand_services_ex_trade_sa': 'WPSFD421',
    'final_demand_construction_sa': 'WPSFD43',

    # Final Demand (최종 수요) - 계절미조정
    'final_demand': 'WPUFD4',
    'final_demand_goods': 'WPUFD41',
    'final_demand_foods': 'WPUFD411',
    'final_demand_energy': 'WPUFD412',
    'final_demand_core': 'WPUFD49104',
    'final_demand_core_ex_trade': 'WPUFD49116',
    'final_demand_services': 'WPUFD42',
    'final_demand_transport': 'WPUFD422',
    'final_demand_trade': 'WPUFD423',
    'final_demand_services_ex_trade': 'WPUFD421',
    'final_demand_construction': 'WPUFD43',
    
    # Intermediate Demand (중간 수요)
    'intermediate_processed_sa': 'WPSID61',
    'intermediate_unprocessed_sa': 'WPSID62',
    'intermediate_services_sa': 'WPSID63',
    'intermediate_stage4_sa': 'WPSID54',
    'intermediate_stage3_sa': 'WPSID53',
    'intermediate_stage2_sa': 'WPSID52',
    'intermediate_stage1_sa': 'WPSID51',
    
    # Specific Commodities (주요 품목)
    'motor_vehicles_sa': 'WPS1411',
    'pharmaceutical_sa': 'WPS0638',
    'gasoline_sa': 'WPS0571',
    'meats_sa': 'WPS0221',
    'industrial_chemicals_sa': 'WPS061',
    'lumber_sa': 'WPS081',
    # 일부 품목은 계절조정(SA) 시리즈가 없어 NSA로 대체
    'steel_products_sa': 'WPU1017',
    'diesel_fuel_sa': 'WPS057303',
    'animal_feeds_sa': 'WPS029',
    'crude_petroleum_sa': 'WPU0561',
    'grains_sa': 'WPS012',
    'carbon_steel_scrap_sa': 'WPS101211',
    
    # Services (서비스)
    'outpatient_healthcare_sa': 'WPS5111',
    'inpatient_healthcare_sa': 'WPS5121',
    'food_alcohol_retail_sa': 'WPS5811',
    'apparel_jewelry_retail_sa': 'WPS5831',
    'airline_services_sa': 'WPS3022',
    'securities_brokerage_sa': 'WPU4011',
    'business_loans_sa': 'WPS3911',
    'legal_services_sa': 'WPS4511',
    'truck_transport_sa': 'WPS301',
    'machinery_wholesale_sa': 'WPS057',
    
    # All Commodities (전체 상품)
    'all_commodities': 'WPU00000000',
    'industrial_commodities_sa': 'WPU03THRU15'
}

# 한국어 이름 매핑
PPI_KOREAN_NAMES = {
    # Final Demand (최종수요) - 계절조정
    'final_demand_sa': '최종수요 (계절조정)',
    'final_demand_goods_sa': '최종수요 재화 (계절조정)',
    'final_demand_foods_sa': '최종수요 식품 (계절조정)',
    'final_demand_energy_sa': '최종수요 에너지 (계절조정)',
    'final_demand_core_sa': '최종수요(식품·에너지 제외) (계절조정)',
    'final_demand_core_ex_trade_sa': '최종수요(식품·에너지·무역서비스 제외) (계절조정)',
    'final_demand_services_sa': '최종수요 서비스 (계절조정)',
    'final_demand_transport_sa': '최종수요 운송·창고업 (계절조정)',
    'final_demand_trade_sa': '최종수요 무역서비스 (계절조정)',
    'final_demand_services_ex_trade_sa': '최종수요 서비스(무역·운송·창고 제외) (계절조정)',
    'final_demand_construction_sa': '최종수요 건설업 (계절조정)',
    
    # Final Demand (최종수요) - 계절미조정
    'final_demand': '최종수요',
    'final_demand_goods': '최종수요 재화',
    'final_demand_foods': '최종수요 식품',
    'final_demand_energy': '최종수요 에너지',
    'final_demand_core': '최종수요(식품·에너지 제외)',
    'final_demand_core_ex_trade': '최종수요(식품·에너지·무역서비스 제외)',
    'final_demand_services': '최종수요 서비스',
    'final_demand_transport': '최종수요 운송·창고업',
    'final_demand_trade': '최종수요 무역서비스',
    'final_demand_services_ex_trade': '최종수요 서비스(무역·운송·창고 제외)',
    'final_demand_construction': '최종수요 건설업',
    
    # Intermediate Demand (중간수요) - 계절조정
    'intermediate_processed_sa': '중간수요 가공재 (계절조정)',
    'intermediate_unprocessed_sa': '중간수요 미가공재 (계절조정)',
    'intermediate_services_sa': '중간수요 서비스 (계절조정)',
    'intermediate_stage4_sa': '4단계 중간수요 (계절조정)',
    'intermediate_stage3_sa': '3단계 중간수요 (계절조정)',
    'intermediate_stage2_sa': '2단계 중간수요 (계절조정)',
    'intermediate_stage1_sa': '1단계 중간수요 (계절조정)',
    
    # Specific Commodities (주요 품목) - 계절조정
    'motor_vehicles_sa': '자동차 (계절조정)',
    'pharmaceutical_sa': '의약품 (계절조정)',
    'gasoline_sa': '가솔린 (계절조정)',
    'meats_sa': '육류 (계절조정)',
    'industrial_chemicals_sa': '산업화학 (계절조정)',
    'lumber_sa': '목재 (계절조정)',
    # NSA로 대체된 품목들 (계절조정 시리즈 부재)
    'steel_products_sa': '제철 제품 (계절미조정)',
    'diesel_fuel_sa': '디젤연료 (계절조정)',
    'animal_feeds_sa': '사료 (계절조정)',
    'crude_petroleum_sa': '원유 (계절미조정)',
    'grains_sa': '곡물 (계절조정)',
    'carbon_steel_scrap_sa': '탄소강 스크랩 (계절조정)',
    
    # Services (서비스) - 계절조정
    'outpatient_healthcare_sa': '외래 의료서비스 (계절조정)',
    'inpatient_healthcare_sa': '입원 의료서비스 (계절조정)',
    'food_alcohol_retail_sa': '식품·주류 소매 (계절조정)',
    'apparel_jewelry_retail_sa': '의류·보석 소매 (계절조정)',
    'airline_services_sa': '항공 승객 서비스 (계절조정)',
    'securities_brokerage_sa': '증권중개·투자 관련 (계절미조정)',
    'business_loans_sa': '기업 대출(부분) (계절조정)',
    'legal_services_sa': '법률 서비스 (계절조정)',
    'truck_transport_sa': '화물 트럭 운송 (계절조정)',
    'machinery_wholesale_sa': '기계·장비 도매 (계절조정)',
    
    # All Commodities (전체 상품) - 계절조정
    'all_commodities': '전체 상품',
    'industrial_commodities_sa': '산업 상품 (계절미조정)'
}

# PPI 카테고리 분류
PPI_CATEGORIES = {
    '최종수요_계절조정': {
        '최종수요 전체': ['final_demand_sa'],
        '최종수요 재화': ['final_demand_goods_sa', 'final_demand_foods_sa', 'final_demand_energy_sa'],
        '최종수요 서비스': ['final_demand_services_sa', 'final_demand_transport_sa', 'final_demand_trade_sa', 'final_demand_services_ex_trade_sa'],
        '최종수요 건설': ['final_demand_construction_sa'],
        '최종수요 코어': ['final_demand_core_sa', 'final_demand_core_ex_trade_sa']
    },
    '최종수요': {
        '최종수요 전체': ['final_demand'],
        '최종수요 재화': ['final_demand_goods', 'final_demand_foods', 'final_demand_energy'],
        '최종수요 서비스': ['final_demand_services', 'final_demand_transport', 'final_demand_trade', 'final_demand_services_ex_trade'],
        '최종수요 건설': ['final_demand_construction'],
        '최종수요 코어': ['final_demand_core', 'final_demand_core_ex_trade']
    },
    '중간수요_계절조정': {
        '중간수요 가공재': ['intermediate_processed_sa'],
        '중간수요 미가공재': ['intermediate_unprocessed_sa'],
        '중간수요 서비스': ['intermediate_services_sa'],
        '중간수요 단계별': ['intermediate_stage4_sa', 'intermediate_stage3_sa', 'intermediate_stage2_sa', 'intermediate_stage1_sa']
    },
    '주요품목_계절조정': {
        '에너지 관련': ['gasoline_sa', 'diesel_fuel_sa', 'crude_petroleum_sa'],
        '제조업': ['motor_vehicles_sa', 'pharmaceutical_sa', 'industrial_chemicals_sa', 'lumber_sa', 'steel_products_sa'],
        '식품 농업': ['meats_sa', 'animal_feeds_sa', 'grains_sa', 'carbon_steel_scrap_sa']
    },
    '서비스_계절조정': {
        '의료서비스': ['outpatient_healthcare_sa', 'inpatient_healthcare_sa'],
        '비즈니스서비스': ['securities_brokerage_sa', 'business_loans_sa', 'legal_services_sa'],
        '운송서비스': ['airline_services_sa', 'truck_transport_sa'],
        '소매서비스': ['food_alcohol_retail_sa', 'apparel_jewelry_retail_sa', 'machinery_wholesale_sa']
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/ppi_data.csv'
PPI_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_ppi_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 PPI 데이터 로드"""
    global PPI_DATA

    result = load_economic_data(
        series_dict=PPI_SERIES,
        data_source='BLS',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=10.0  # PPI 데이터 허용 오차
    )

    if result:
        # CSV에 ID 컬럼으로 저장된 이전 데이터와의 호환성 처리
        # 만약 raw_data 컬럼이 BLS 시리즈 ID(WP*)라면, 친숙한 시리즈 이름으로 리네임
        reverse_map = {v: k for k, v in PPI_SERIES.items()}
        try:
            raw_cols = list(result.get('raw_data', pd.DataFrame()).columns)
            if any(col in reverse_map for col in raw_cols):
                # 모든 데이터프레임에 동일한 리네임 적용
                for key in ['raw_data', 'mom_data', 'mom_change', 'yoy_data', 'yoy_change']:
                    if key in result and isinstance(result[key], pd.DataFrame):
                        result[key] = result[key].rename(columns=reverse_map)
                # 정규화된 컬럼으로 CSV 덮어쓰기 (향후 일관성)
                if 'raw_data' in result and not result['raw_data'].empty:
                    save_data_to_csv(result['raw_data'], CSV_FILE_PATH)
        except Exception:
            pass

        PPI_DATA = result
        print_load_info()
        return True
    else:
        print("❌ PPI 데이터 로드 실패")
        return False

def print_load_info():
    """PPI 데이터 로드 정보 출력"""
    if not PPI_DATA or 'load_info' not in PPI_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = PPI_DATA['load_info']
    print(f"\n📊 PPI 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in PPI_DATA and not PPI_DATA['raw_data'].empty:
        latest_date = PPI_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_ppi_series_advanced(series_list, chart_type='multi_line', 
                             data_type='mom', periods=None, target_date=None):
    """범용 PPI 시각화 함수 - plot_economic_series 활용"""
    if not PPI_DATA:
        print("⚠️ 먼저 load_ppi_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=PPI_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=PPI_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_ppi_data(series_list, data_type='mom', periods=None, 
                    target_date=None, export_path=None, file_format='excel'):
    """PPI 데이터 export 함수 - export_economic_data 활용"""
    if not PPI_DATA:
        print("⚠️ 먼저 load_ppi_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=PPI_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=PPI_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_ppi_data():
    """PPI 데이터 초기화"""
    global PPI_DATA
    PPI_DATA = {}
    print("🗑️ PPI 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not PPI_DATA or 'raw_data' not in PPI_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_ppi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PPI_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in PPI_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PPI_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not PPI_DATA or 'mom_data' not in PPI_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_ppi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PPI_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in PPI_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PPI_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not PPI_DATA or 'yoy_data' not in PPI_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_ppi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PPI_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in PPI_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PPI_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not PPI_DATA or 'raw_data' not in PPI_DATA:
        return []
    return list(PPI_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 PPI 시리즈 표시"""
    print("=== 사용 가능한 PPI 시리즈 ===")
    
    for series_name, series_id in PPI_SERIES.items():
        korean_name = PPI_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in PPI_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_id in series_list:
                korean_name = PPI_KOREAN_NAMES.get(series_id, series_id)
                print(f"    - {series_id}: {korean_name}")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not PPI_DATA or 'load_info' not in PPI_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': PPI_DATA['load_info']['loaded'],
        'series_count': PPI_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': PPI_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 PPI 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_ppi_data()  # 스마트 업데이트")
print("   load_ppi_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_ppi_series_advanced(['final_demand_sa', 'final_demand_core_sa'], 'multi_line', 'mom')")
print("   plot_ppi_series_advanced(['final_demand_sa'], 'horizontal_bar', 'yoy')")
print("   plot_ppi_series_advanced(['final_demand_sa'], 'single_line', 'mom', periods=24)")
print()
print("3. 🔥 데이터 Export:")
print("   export_ppi_data(['final_demand_sa', 'final_demand_core_sa'], 'mom')")
print("   export_ppi_data(['final_demand_sa'], 'raw', periods=24, file_format='csv')")
print("   export_ppi_data(['final_demand_sa'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_ppi_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_ppi_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_ppi_data()
plot_ppi_series_advanced(['final_demand_sa', 'final_demand_core_sa'], 'multi_line', 'mom')

# %%
plot_ppi_series_advanced(['final_demand_sa', 'final_demand_core_sa', 'final_demand_services_sa', 'final_demand_goods_sa', 'final_demand_foods_sa'], 'multi_line', 'yoy')

# %%
plot_ppi_series_advanced(['intermediate_processed_sa', 'intermediate_unprocessed_sa', 'intermediate_services_sa'], 'multi_line', 'yoy')
plot_ppi_series_advanced(['intermediate_stage4_sa', 'intermediate_stage3_sa', 'intermediate_stage2_sa', 'intermediate_stage1_sa'], 'multi_line', 'yoy')
# %%
plot_ppi_series_advanced(['motor_vehicles_sa', 'pharmaceutical_sa', 'gasoline_sa', 'meats_sa',
                          'industrial_chemicals_sa', 'lumber_sa', 'steel_products_sa',
                          'diesel_fuel_sa', 'animal_feeds_sa', 'crude_petroleum_sa', 'grains_sa', 'carbon_steel_scrap_sa'], 'horizontal_bar', 'yoy')
# %%
plot_ppi_series_advanced(['outpatient_healthcare_sa', 'inpatient_healthcare_sa',
                          'food_alcohol_retail_sa', 'apparel_jewelry_retail_sa',
                          'airline_services_sa', 'securities_brokerage_sa',
                          'business_loans_sa', 'legal_services_sa',
                          'truck_transport_sa', 'machinery_wholesale_sa'], 'horizontal_bar', 'yoy')
