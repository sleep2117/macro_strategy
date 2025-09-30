# %%
"""
미국 주택 가격 및 판매 데이터 분석 (리팩토링 버전)
- us_eco_utils를 사용한 통합 구조
- 5개 카테고리별 스마트 업데이트 지원
- 기존주택판매, 신규주택판매 데이터
- KPDS 포맷 시각화 지원
"""

import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# 통합 유틸리티 함수 불러오기
from us_eco_utils import *

# %%
# === FRED API 키 설정 ===
api_config.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'

print("✓ KPDS 시각화 포맷 로드됨")
print("✓ US Economic Data Utils 로드됨")

# %%
# === 주택 가격 및 판매 시리즈 정의 ===

# 기존 주택 판매 관련 시리즈 (NAR - Existing Home Sales)
EXISTING_HOME_SALES_SERIES = {
    # National Level - Sales Volume
    'ehs_sales_national_sa': 'EXHOSLUSM495S',        # 전체 기존 주택 판매량 (SA)
    'ehs_inventory_national': 'HOSINVUSM495N',       # 판매 가능 재고 (NSA)
    'ehs_months_supply': 'HOSSUPUSM673N',            # 재고 소진 개월수 (NSA)
    'ehs_sf_sales_national_sa': 'EXSFHSUSM495S',     # 단독주택 판매량 (SA)
    'ehs_sf_inventory_national': 'HSFINVUSM495N',    # 단독주택 재고 (SA)
    'ehs_sf_months_supply': 'HSFSUPUSM673N',         # 단독주택 재고 소진 개월수 (NSA)
    
    # Regional Level - Sales Volume
    'ehs_sales_northeast_sa': 'EXHOSLUSNEM495S',     # 동북부 판매량 (SA)
    'ehs_sales_midwest_sa': 'EXHOSLUSMWM495S',       # 중서부 판매량 (SA)
    'ehs_sales_south_sa': 'EXHOSLUSSOM495S',         # 남부 판매량 (SA)
    'ehs_sales_west_sa': 'EXHOSLUSWTM495S',          # 서부 판매량 (SA)
    
    'ehs_sf_sales_northeast_sa': 'EXSFHSUSNEM495S',  # 동북부 단독주택 판매량 (SA)
    'ehs_sf_sales_midwest_sa': 'EXSFHSUSMWM495S',    # 중서부 단독주택 판매량 (SA)
    'ehs_sf_sales_south_sa': 'EXSFHSUSSOM495S',      # 남부 단독주택 판매량 (SA)
    'ehs_sf_sales_west_sa': 'EXSFHSUSWTM495S',       # 서부 단독주택 판매량 (SA)
}

# 신규 주택 판매 관련 시리즈 (Census & HUD - New Residential Sales)
NEW_RESIDENTIAL_SALES_SERIES = {
    # National Level - Core Sales and Inventory
    'nrs_sales_national_sa': 'HSN1F',               # 신규 단독주택 판매량 (SAAR)
    'nrs_sales_national_nsa': 'HSN1FNSA',           # 신규 단독주택 판매량 (NSA)
    'nrs_sales_national_annual': 'HSN1FA',          # 신규 단독주택 판매량 (Annual)
    'nrs_inventory_national_sa': 'HNFSEPUSSA',      # 신규 주택 재고 (SA)
    'nrs_inventory_national_nsa': 'HNFSUSNSA',      # 신규 주택 재고 (NSA)
    'nrs_inventory_national_annual': 'HNFSUSNSAA',  # 신규 주택 재고 (Annual)
    'nrs_months_supply_sa': 'MSACSR',               # 재고 소진 개월수 (SA)
    'nrs_months_supply_nsa': 'MSACSRNSA',           # 재고 소진 개월수 (NSA)
    
    # Regional Level - Sales
    'nrs_sales_northeast_sa': 'HSN1FNE',            # 동북부 판매량 (SA)
    'nrs_sales_midwest_sa': 'HSN1FMW',              # 중서부 판매량 (SA)
    'nrs_sales_south_sa': 'HSN1FS',                 # 남부 판매량 (SA)
    'nrs_sales_west_sa': 'HSN1FW',                  # 서부 판매량 (SA)
    
    'nrs_sales_northeast_nsa': 'HSN1FNENSA',        # 동북부 판매량 (NSA)
    'nrs_sales_midwest_nsa': 'HSN1FMWNSA',          # 중서부 판매량 (NSA)
    'nrs_sales_south_nsa': 'HSN1FSNSA',             # 남부 판매량 (NSA)
    'nrs_sales_west_nsa': 'HSN1FWNSA',              # 서부 판매량 (NSA)
    
    'nrs_sales_northeast_annual': 'HSN1FNEA',       # 동북부 판매량 (Annual)
    'nrs_sales_midwest_annual': 'HSN1FMWA',         # 중서부 판매량 (Annual)
    'nrs_sales_south_annual': 'HSN1FSA',            # 남부 판매량 (Annual)
    'nrs_sales_west_annual': 'HSN1FWA',             # 서부 판매량 (Annual)
    
    # Regional Level - Inventory
    'nrs_inventory_northeast': 'HNFSNE',            # 동북부 재고
    'nrs_inventory_midwest': 'HNFSMW',              # 중서부 재고
    'nrs_inventory_south': 'HNFSS',                 # 남부 재고
    'nrs_inventory_west': 'HNFSW',                  # 서부 재고
    
    'nrs_inventory_northeast_annual': 'HNFSNEA',    # 동북부 재고 (Annual)
    'nrs_inventory_midwest_annual': 'HNFSMWA',      # 중서부 재고 (Annual)
    'nrs_inventory_south_annual': 'HNFSSA',         # 남부 재고 (Annual)
    'nrs_inventory_west_annual': 'HNFSWA',          # 서부 재고 (Annual)
    
    # Sales by Stage of Construction - SA Annual Rate
    'nrs_sales_total_stage': 'NHSDPTS',             # 전체 (단계별)
    'nrs_sales_completed': 'NHSDPCS',               # 완공
    'nrs_sales_under_construction': 'NHSDPUCS',     # 건설중
    'nrs_sales_not_started': 'NHSDPNSS',            # 미착공
    
    # Sales by Stage of Construction - NSA Monthly
    'nrs_sales_total_stage_nsa': 'NHSDPT',          # 전체 (단계별, NSA)
    'nrs_sales_completed_nsa': 'NHSDPC',            # 완공 (NSA)
    'nrs_sales_under_construction_nsa': 'NHSDPUC',  # 건설중 (NSA)
    'nrs_sales_not_started_nsa': 'NHSDPNS',         # 미착공 (NSA)
    
    # Sales by Stage of Construction - Annual
    'nrs_sales_total_stage_annual': 'NHSDPTA',      # 전체 (단계별, Annual)
    'nrs_sales_completed_annual': 'NHSDPCA',        # 완공 (Annual)
    'nrs_sales_under_construction_annual': 'NHSDPUCA', # 건설중 (Annual)
    'nrs_sales_not_started_annual': 'NHSDPNSA',     # 미착공 (Annual)
    
    # Inventory by Stage of Construction - SA Monthly
    'nrs_inventory_total_stage': 'NHFSEPTS',        # 전체 재고 (단계별)
    'nrs_inventory_completed_stage': 'NHFSEPCS',    # 완공 재고
    'nrs_inventory_under_construction_stage': 'NHFSEPUCS', # 건설중 재고
    'nrs_inventory_not_started_stage': 'NHFSEPNTS', # 미착공 재고
    
    # Inventory by Stage of Construction - NSA Monthly
    'nrs_inventory_total_stage_nsa': 'NHFSEPT',     # 전체 재고 (단계별, NSA)
    'nrs_inventory_completed_stage_nsa': 'NHFSEPC', # 완공 재고 (NSA)
    'nrs_inventory_under_construction_stage_nsa': 'NHFSEPUC', # 건설중 재고 (NSA)
    'nrs_inventory_not_started_stage_nsa': 'NHFSEPNT', # 미착공 재고 (NSA)
    
    # Inventory by Stage of Construction - Annual
    'nrs_inventory_total_stage_annual': 'NHFSEPTA', # 전체 재고 (단계별, Annual)
    'nrs_inventory_completed_stage_annual': 'NHFSEPCA', # 완공 재고 (Annual)
    'nrs_inventory_under_construction_stage_annual': 'NHFSEPUCA', # 건설중 재고 (Annual)
    'nrs_inventory_not_started_stage_annual': 'NHFSEPNTA', # 미착공 재고 (Annual)
    
    # Sales by Price Range (2020+ Price Ranges) - Monthly
    'nrs_sales_total_price': 'NHSUSSPT',            # 전체 (가격대별)
    'nrs_sales_under_300k': 'NHSUSSPU30',           # 30만달러 미만
    'nrs_sales_300k_to_399k': 'NHSUSSP30T39',       # 30-39.9만달러
    'nrs_sales_400k_to_499k': 'NHSUSSP40T49',       # 40-49.9만달러
    'nrs_sales_500k_to_599k': 'NHSUSSP50T59',       # 50-59.9만달러
    # Note: Monthly series are combined as 600k–799k on FRED
    'nrs_sales_600k_to_799k': 'NHSUSSP60T79',       # 60-79.9만달러 (월간은 통합 구간)
    'nrs_sales_800k_to_999k': 'NHSUSSP80T99',       # 80-99.9만달러
    'nrs_sales_over_1m': 'NHSUSSP100O',             # 100만달러 이상
    
    # Sales by Price Range - Quarterly
    'nrs_sales_total_price_q': 'NHSUSSPTQ',         # 전체 (가격대별, 분기)
    'nrs_sales_under_300k_q': 'NHSUSSPU30Q',        # 30만달러 미만 (분기)
    'nrs_sales_300k_to_399k_q': 'NHSUSSP30T39Q',    # 30-39.9만달러 (분기)
    'nrs_sales_400k_to_499k_q': 'NHSUSSP40T49Q',    # 40-49.9만달러 (분기)
    'nrs_sales_500k_to_599k_q': 'NHSUSSP50T59Q',    # 50-59.9만달러 (분기)
    'nrs_sales_600k_to_699k_q': 'NHSUSSP60T69Q',    # 60-69.9만달러 (분기)
    'nrs_sales_700k_to_799k_q': 'NHSUSSP70T79Q',    # 70-79.9만달러 (분기)
    'nrs_sales_800k_to_999k_q': 'NHSUSSP80T99Q',    # 80-99.9만달러 (분기)
    'nrs_sales_over_1m_q': 'NHSUSSP100OQ',          # 100만달러 이상 (분기)
    
    # Sales by Price Range - Annual
    'nrs_sales_total_price_annual': 'NHSUSSPTA',    # 전체 (가격대별, 연간)
    'nrs_sales_under_300k_annual': 'NHSUSSPU30A',   # 30만달러 미만 (연간)
    'nrs_sales_300k_to_399k_annual': 'NHSUSSP30T39A', # 30-39.9만달러 (연간)
    'nrs_sales_400k_to_499k_annual': 'NHSUSSP40T49A', # 40-49.9만달러 (연간)
    'nrs_sales_500k_to_599k_annual': 'NHSUSSP50T59A', # 50-59.9만달러 (연간)
    'nrs_sales_600k_to_699k_annual': 'NHSUSSP60T69A', # 60-69.9만달러 (연간)
    'nrs_sales_700k_to_799k_annual': 'NHSUSSP70T79A', # 70-79.9만달러 (연간)
    'nrs_sales_800k_to_999k_annual': 'NHSUSSP80T99A', # 80-99.9만달러 (연간)
    'nrs_sales_over_1m_annual': 'NHSUSSP100OA',     # 100만달러 이상 (연간)
    
    # Sales by Type of Financing (Quarterly)
    'nrs_sales_cash': 'HSTFC',                      # 현금 구매
    'nrs_sales_conventional': 'HSTFCM',             # 일반 융자
    'nrs_sales_fha': 'HSTFFHAI',                    # FHA 융자
    'nrs_sales_va': 'HSTFVAG',                      # VA 융자
    
    # Other Indicators
    'nrs_median_months_on_market': 'MNMFS',         # 시장 체류 기간 (중간값)
    'nrs_median_months_on_market_annual': 'MNMFSA', # 시장 체류 기간 (연간)
}

# 데이터 유형별로 분류
HOUSE_SALES_STOCK_DATA_CATEGORIES = {
    'existing_home_sales': EXISTING_HOME_SALES_SERIES,
    'new_residential_sales': NEW_RESIDENTIAL_SALES_SERIES
}

# 전체 시리즈 통합
ALL_HOUSE_SALES_STOCK_SERIES = {
    **EXISTING_HOME_SALES_SERIES,
    **NEW_RESIDENTIAL_SALES_SERIES
}

# 한국어 이름 매핑 (실제 컬럼명 기반 - category_indicator 형태)
HOUSE_SALES_STOCK_KOREAN_NAMES = {
    # Existing Home Sales (기존 주택 판매)
    'existing_home_sales_ehs_sales_national_sa': 'EHS 전국 판매량(SA)',
    'existing_home_sales_ehs_inventory_national': 'EHS 전국 재고',
    'existing_home_sales_ehs_months_supply': 'EHS 재고 소진율',
    'existing_home_sales_ehs_sf_sales_national_sa': 'EHS 단독주택 판매량(SA)',
    'existing_home_sales_ehs_sf_inventory_national': 'EHS 단독주택 재고',
    'existing_home_sales_ehs_sf_months_supply': 'EHS 단독주택 소진율',
    
    'existing_home_sales_ehs_sales_northeast_sa': 'EHS 동북부 판매량(SA)',
    'existing_home_sales_ehs_sales_midwest_sa': 'EHS 중서부 판매량(SA)',
    'existing_home_sales_ehs_sales_south_sa': 'EHS 남부 판매량(SA)',
    'existing_home_sales_ehs_sales_west_sa': 'EHS 서부 판매량(SA)',
    
    'existing_home_sales_ehs_sf_sales_northeast_sa': 'EHS 동북부 단독주택(SA)',
    'existing_home_sales_ehs_sf_sales_midwest_sa': 'EHS 중서부 단독주택(SA)',
    'existing_home_sales_ehs_sf_sales_south_sa': 'EHS 남부 단독주택(SA)',
    'existing_home_sales_ehs_sf_sales_west_sa': 'EHS 서부 단독주택(SA)',
    
    # New Residential Sales (신규 주택 판매) - Core Sales and Inventory
    'new_residential_sales_nrs_sales_national_sa': 'NRS 전국 판매량(SA)',
    'new_residential_sales_nrs_sales_national_nsa': 'NRS 전국 판매량(NSA)',
    'new_residential_sales_nrs_sales_national_annual': 'NRS 전국 판매량(연간)',
    'new_residential_sales_nrs_inventory_national_sa': 'NRS 전국 재고(SA)',
    'new_residential_sales_nrs_inventory_national_nsa': 'NRS 전국 재고(NSA)',
    'new_residential_sales_nrs_inventory_national_annual': 'NRS 전국 재고(연간)',
    'new_residential_sales_nrs_months_supply_sa': 'NRS 재고 소진율(SA)',
    'new_residential_sales_nrs_months_supply_nsa': 'NRS 재고 소진율(NSA)',
    
    # Regional Sales
    'new_residential_sales_nrs_sales_northeast_sa': 'NRS 동북부 판매량(SA)',
    'new_residential_sales_nrs_sales_midwest_sa': 'NRS 중서부 판매량(SA)',
    'new_residential_sales_nrs_sales_south_sa': 'NRS 남부 판매량(SA)',
    'new_residential_sales_nrs_sales_west_sa': 'NRS 서부 판매량(SA)',
    
    'new_residential_sales_nrs_sales_northeast_nsa': 'NRS 동북부 판매량(NSA)',
    'new_residential_sales_nrs_sales_midwest_nsa': 'NRS 중서부 판매량(NSA)',
    'new_residential_sales_nrs_sales_south_nsa': 'NRS 남부 판매량(NSA)',
    'new_residential_sales_nrs_sales_west_nsa': 'NRS 서부 판매량(NSA)',
    
    'new_residential_sales_nrs_sales_northeast_annual': 'NRS 동북부 판매량(연간)',
    'new_residential_sales_nrs_sales_midwest_annual': 'NRS 중서부 판매량(연간)',
    'new_residential_sales_nrs_sales_south_annual': 'NRS 남부 판매량(연간)',
    'new_residential_sales_nrs_sales_west_annual': 'NRS 서부 판매량(연간)',
    
    # Regional Inventory
    'new_residential_sales_nrs_inventory_northeast': 'NRS 동북부 재고',
    'new_residential_sales_nrs_inventory_midwest': 'NRS 중서부 재고',
    'new_residential_sales_nrs_inventory_south': 'NRS 남부 재고',
    'new_residential_sales_nrs_inventory_west': 'NRS 서부 재고',
    
    'new_residential_sales_nrs_inventory_northeast_annual': 'NRS 동북부 재고(연간)',
    'new_residential_sales_nrs_inventory_midwest_annual': 'NRS 중서부 재고(연간)',
    'new_residential_sales_nrs_inventory_south_annual': 'NRS 남부 재고(연간)',
    'new_residential_sales_nrs_inventory_west_annual': 'NRS 서부 재고(연간)',
    
    # Sales by Construction Stage - SA Annual Rate
    'new_residential_sales_nrs_sales_total_stage': 'NRS 전체 단계별',
    'new_residential_sales_nrs_sales_completed': 'NRS 완공',
    'new_residential_sales_nrs_sales_under_construction': 'NRS 건설중',
    'new_residential_sales_nrs_sales_not_started': 'NRS 미착공',
    
    # Sales by Construction Stage - NSA Monthly
    'new_residential_sales_nrs_sales_total_stage_nsa': 'NRS 전체 단계별(NSA)',
    'new_residential_sales_nrs_sales_completed_nsa': 'NRS 완공(NSA)',
    'new_residential_sales_nrs_sales_under_construction_nsa': 'NRS 건설중(NSA)',
    'new_residential_sales_nrs_sales_not_started_nsa': 'NRS 미착공(NSA)',
    
    # Sales by Construction Stage - Annual
    'new_residential_sales_nrs_sales_total_stage_annual': 'NRS 전체 단계별(연간)',
    'new_residential_sales_nrs_sales_completed_annual': 'NRS 완공(연간)',
    'new_residential_sales_nrs_sales_under_construction_annual': 'NRS 건설중(연간)',
    'new_residential_sales_nrs_sales_not_started_annual': 'NRS 미착공(연간)',
    
    # Inventory by Construction Stage - SA Monthly
    'new_residential_sales_nrs_inventory_total_stage': 'NRS 전체 재고 단계별',
    'new_residential_sales_nrs_inventory_completed_stage': 'NRS 완공 재고',
    'new_residential_sales_nrs_inventory_under_construction_stage': 'NRS 건설중 재고',
    'new_residential_sales_nrs_inventory_not_started_stage': 'NRS 미착공 재고',
    
    # Inventory by Construction Stage - NSA Monthly
    'new_residential_sales_nrs_inventory_total_stage_nsa': 'NRS 전체 재고 단계별(NSA)',
    'new_residential_sales_nrs_inventory_completed_stage_nsa': 'NRS 완공 재고(NSA)',
    'new_residential_sales_nrs_inventory_under_construction_stage_nsa': 'NRS 건설중 재고(NSA)',
    'new_residential_sales_nrs_inventory_not_started_stage_nsa': 'NRS 미착공 재고(NSA)',
    
    # Inventory by Construction Stage - Annual
    'new_residential_sales_nrs_inventory_total_stage_annual': 'NRS 전체 재고 단계별(연간)',
    'new_residential_sales_nrs_inventory_completed_stage_annual': 'NRS 완공 재고(연간)',
    'new_residential_sales_nrs_inventory_under_construction_stage_annual': 'NRS 건설중 재고(연간)',
    'new_residential_sales_nrs_inventory_not_started_stage_annual': 'NRS 미착공 재고(연간)',
    
    # Sales by Price Range - Monthly
    'new_residential_sales_nrs_sales_total_price': 'NRS 전체(가격대별)',
    'new_residential_sales_nrs_sales_under_300k': 'NRS <30만달러',
    'new_residential_sales_nrs_sales_300k_to_399k': 'NRS 30-39.9만달러',
    'new_residential_sales_nrs_sales_400k_to_499k': 'NRS 40-49.9만달러',
    'new_residential_sales_nrs_sales_500k_to_599k': 'NRS 50-59.9만달러',
    # 월간은 60-79.9만달러 구간이 통합되어 제공됨
    'new_residential_sales_nrs_sales_600k_to_799k': 'NRS 60-79.9만달러',
    'new_residential_sales_nrs_sales_800k_to_999k': 'NRS 80-99.9만달러',
    'new_residential_sales_nrs_sales_over_1m': 'NRS ≥100만달러',
    
    # Sales by Price Range - Quarterly
    'new_residential_sales_nrs_sales_total_price_q': 'NRS 전체(가격대별,분기)',
    'new_residential_sales_nrs_sales_under_300k_q': 'NRS <30만달러(분기)',
    'new_residential_sales_nrs_sales_300k_to_399k_q': 'NRS 30-39.9만달러(분기)',
    'new_residential_sales_nrs_sales_400k_to_499k_q': 'NRS 40-49.9만달러(분기)',
    'new_residential_sales_nrs_sales_500k_to_599k_q': 'NRS 50-59.9만달러(분기)',
    'new_residential_sales_nrs_sales_600k_to_699k_q': 'NRS 60-69.9만달러(분기)',
    'new_residential_sales_nrs_sales_700k_to_799k_q': 'NRS 70-79.9만달러(분기)',
    'new_residential_sales_nrs_sales_800k_to_999k_q': 'NRS 80-99.9만달러(분기)',
    'new_residential_sales_nrs_sales_over_1m_q': 'NRS ≥100만달러(분기)',
    
    # Sales by Price Range - Annual
    'new_residential_sales_nrs_sales_total_price_annual': 'NRS 전체(가격대별,연간)',
    'new_residential_sales_nrs_sales_under_300k_annual': 'NRS <30만달러(연간)',
    'new_residential_sales_nrs_sales_300k_to_399k_annual': 'NRS 30-39.9만달러(연간)',
    'new_residential_sales_nrs_sales_400k_to_499k_annual': 'NRS 40-49.9만달러(연간)',
    'new_residential_sales_nrs_sales_500k_to_599k_annual': 'NRS 50-59.9만달러(연간)',
    'new_residential_sales_nrs_sales_600k_to_699k_annual': 'NRS 60-69.9만달러(연간)',
    'new_residential_sales_nrs_sales_700k_to_799k_annual': 'NRS 70-79.9만달러(연간)',
    'new_residential_sales_nrs_sales_800k_to_999k_annual': 'NRS 80-99.9만달러(연간)',
    'new_residential_sales_nrs_sales_over_1m_annual': 'NRS ≥100만달러(연간)',
    
    # Sales by Financing Type (Quarterly)
    'new_residential_sales_nrs_sales_cash': 'NRS 현금구매',
    'new_residential_sales_nrs_sales_conventional': 'NRS 일반융자',
    'new_residential_sales_nrs_sales_fha': 'NRS FHA융자',
    'new_residential_sales_nrs_sales_va': 'NRS VA융자',
    
    # Other Indicators
    'new_residential_sales_nrs_median_months_on_market': 'NRS 시장체류기간',
    'new_residential_sales_nrs_median_months_on_market_annual': 'NRS 시장체류기간(연간)'
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/house_sales_stock_data_refactored.csv'
HOUSE_SALES_STOCK_DATA = {
    'raw_data': pd.DataFrame(),          # 원본 데이터
    'mom_data': pd.DataFrame(),          # 전월대비 변화
    'yoy_data': pd.DataFrame(),          # 전년동월대비 변화
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0,
        'categories_loaded': []
    }
}

# %%
# === 그룹별 스마트 업데이트를 위한 시리즈 그룹 정의 ===

def build_house_sales_stock_series_groups(enabled_categories=None):
    """
    주택 판매/재고 데이터 그룹화된 시리즈 딕셔너리 생성 (us_eco_utils 호환)
    
    Args:
        enabled_categories: 사용할 카테고리 리스트 (None이면 모든 카테고리)
    
    Returns:
        dict: {group_name: {series_name: series_id}} 형태의 그룹 딕셔너리
    """
    if enabled_categories is None:
        enabled_categories = list(HOUSE_SALES_STOCK_DATA_CATEGORIES.keys())
    
    series_groups = {}
    
    for category_name in enabled_categories:
        if category_name not in HOUSE_SALES_STOCK_DATA_CATEGORIES:
            continue
            
        category_series = HOUSE_SALES_STOCK_DATA_CATEGORIES[category_name]
        
        # 각 카테고리를 그룹으로 생성
        group_name = category_name
        
        # 시리즈명을 카테고리_지표명 형태로 변환
        group_series = {}
        for indicator_name, fred_id in category_series.items():
            series_name = f"{category_name}_{indicator_name}"
            group_series[series_name] = fred_id
        
        series_groups[group_name] = group_series
    
    return series_groups

# %%
# === 데이터 로드 함수 ===

def load_house_sales_stock_data(start_date='2020-01-01', force_reload=False, smart_update=True, enabled_categories=None):
    """
    모든 주택 판매/재고 데이터 로드 (그룹별 스마트 업데이트 지원)
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
        enabled_categories: 수집할 카테고리 리스트
    
    Returns:
        bool: 로드 성공 여부
    """
    global HOUSE_SALES_STOCK_DATA
    
    print("🚀 주택 판매/재고 데이터 로딩 시작 (그룹별 스마트 업데이트)")
    print("="*60)
    
    # 이미 로드된 경우 스킵 (강제 재로드가 아닌 경우)
    if HOUSE_SALES_STOCK_DATA['load_info']['loaded'] and not force_reload and not smart_update:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    try:
        # 카테고리별 시리즈 그룹 생성
        series_groups = build_house_sales_stock_series_groups(enabled_categories)
        
        print(f"📋 생성된 그룹:")
        for group_name, group_series in series_groups.items():
            print(f"   {group_name}: {len(group_series)}개 시리즈")
        
        # us_eco_utils의 그룹별 로드 함수 사용
        result = load_economic_data_grouped(
            series_groups=series_groups,
            data_source='FRED',
            csv_file_path=CSV_FILE_PATH,
            start_date=start_date,
            smart_update=smart_update,
            force_reload=force_reload,
            tolerance=10.0  # 주택 가격 지수용 허용 오차
        )
        
        if result is None:
            print("❌ 데이터 로딩 실패")
            return False
        
        # 전역 저장소에 결과 저장
        raw_data = result['raw_data']
        
        if raw_data.empty or len(raw_data.columns) < 3:
            print(f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data.columns)}개")
            return False
        
        # 전역 저장소 업데이트 (fed_pmi와 동일한 구조)
        HOUSE_SALES_STOCK_DATA['raw_data'] = raw_data
        HOUSE_SALES_STOCK_DATA['mom_data'] = result['mom_data']
        HOUSE_SALES_STOCK_DATA['yoy_data'] = result['yoy_data']
        
        # 로드 정보 업데이트 (그룹별 정보 추가)
        load_info = result['load_info']
        
        # 카테고리 이름으로 변환
        categories_loaded = []
        groups_checked = load_info.get('groups_checked', [])
        for group_name in groups_checked:
            if group_name not in categories_loaded:
                categories_loaded.append(group_name)
        
        HOUSE_SALES_STOCK_DATA['load_info'] = load_info
        HOUSE_SALES_STOCK_DATA['load_info']['categories_loaded'] = categories_loaded
        
        # CSV 저장 (그룹별 업데이트인 경우 이미 저장됨)
        if 'CSV' not in load_info.get('source', ''):
            # us_eco_utils의 save_data_to_csv 함수 사용
            save_data_to_csv(raw_data, CSV_FILE_PATH)
        
        print("\\n✅ 주택 판매/재고 데이터 로딩 완료!")
        print_load_info()
        
        # 그룹별 업데이트 결과 요약
        if 'groups_updated' in load_info and load_info['groups_updated']:
            print(f"\\n📝 업데이트된 그룹:")
            for group in load_info['groups_updated']:
                category_display = group.replace('_', ' ').title()
                print(f"   {category_display}")
        elif 'groups_checked' in load_info:
            print(f"\\n✅ 모든 그룹 데이터 일치 (업데이트 불필요)")
        
        return True
        
    except Exception as e:
        print(f"❌ 데이터 로딩 실패: {e}")
        import traceback
        print("상세 오류:")
        print(traceback.format_exc())
        return False

def print_load_info():
    """로드 정보 출력"""
    if not HOUSE_SALES_STOCK_DATA or 'load_info' not in HOUSE_SALES_STOCK_DATA:
        print("❌ 데이터가 로드되지 않음")
        return
        
    info = HOUSE_SALES_STOCK_DATA['load_info']
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   데이터 소스: {info.get('source', 'API')}")
    
    if info.get('categories_loaded'):
        categories_display = [cat.replace('_', ' ').title() for cat in info['categories_loaded']]
        print(f"   포함된 카테고리: {', '.join(categories_display)}")
    
    if not HOUSE_SALES_STOCK_DATA['raw_data'].empty:
        date_range = f"{HOUSE_SALES_STOCK_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {HOUSE_SALES_STOCK_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

# %%
# === 범용 시각화 함수 ===
def plot_house_sales_stock_series_advanced(series_list, chart_type='multi_line', 
                                           data_type='raw', periods=None, target_date=None):
    """범용 주택 판매/재고 시각화 함수 - plot_economic_series 활용"""
    if not HOUSE_SALES_STOCK_DATA:
        print("⚠️ 먼저 load_house_sales_stock_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=HOUSE_SALES_STOCK_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=HOUSE_SALES_STOCK_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_house_sales_stock_data(series_list, data_type='raw', periods=None, 
                                  target_date=None, export_path=None, file_format='excel'):
    """주택 판매/재고 데이터 export 함수 - export_economic_data 활용"""
    if not HOUSE_SALES_STOCK_DATA:
        print("⚠️ 먼저 load_house_sales_stock_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=HOUSE_SALES_STOCK_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=HOUSE_SALES_STOCK_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_house_sales_stock_data():
    """주택 판매/재고 데이터 초기화"""
    global HOUSE_SALES_STOCK_DATA
    HOUSE_SALES_STOCK_DATA = {}
    print("🗑️ 주택 판매/재고 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not HOUSE_SALES_STOCK_DATA or 'raw_data' not in HOUSE_SALES_STOCK_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_house_sales_stock_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return HOUSE_SALES_STOCK_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in HOUSE_SALES_STOCK_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return HOUSE_SALES_STOCK_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not HOUSE_SALES_STOCK_DATA or 'mom_data' not in HOUSE_SALES_STOCK_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_house_sales_stock_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return HOUSE_SALES_STOCK_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in HOUSE_SALES_STOCK_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return HOUSE_SALES_STOCK_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not HOUSE_SALES_STOCK_DATA or 'yoy_data' not in HOUSE_SALES_STOCK_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_house_sales_stock_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return HOUSE_SALES_STOCK_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in HOUSE_SALES_STOCK_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return HOUSE_SALES_STOCK_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not HOUSE_SALES_STOCK_DATA or 'raw_data' not in HOUSE_SALES_STOCK_DATA:
        return []
    return list(HOUSE_SALES_STOCK_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 주택 판매/재고 시리즈 표시"""
    if not HOUSE_SALES_STOCK_DATA or 'raw_data' not in HOUSE_SALES_STOCK_DATA:
        print("⚠️ 먼저 load_house_sales_stock_data()를 실행하여 데이터를 로드하세요.")
        return
    
    print("=== 사용 가능한 주택 판매/재고 시리즈 ===")
    print("="*60)
    
    all_columns = HOUSE_SALES_STOCK_DATA['raw_data'].columns.tolist()
    
    # 카테고리별로 그룹화 (판매/재고 데이터만)
    category_groups = {
        'existing_home_sales': [],
        'new_residential_sales': [],
        'other': []  # 분류되지 않은 기타 시리즈
    }
    
    # FRED 시리즈 ID를 카테고리별로 분류
    for col in all_columns:
        if col in EXISTING_HOME_SALES_SERIES.values():
            category_groups['existing_home_sales'].append(col)
        elif col in NEW_RESIDENTIAL_SALES_SERIES.values():
            category_groups['new_residential_sales'].append(col)
        else:
            category_groups['other'].append(col)
    
    # 카테고리별 출력
    category_names = {
        'existing_home_sales': '기존주택 판매/재고',
        'new_residential_sales': '신규주택 판매/재고',
        'other': '기타 시리즈'
    }
    
    for category_key, category_name in category_names.items():
        if category_groups[category_key]:
            print(f"\\n🏠 {category_name} ({len(category_groups[category_key])}개 시리즈)")
            print("-" * 40)
            for series in category_groups[category_key][:5]:  # 처음 5개만 표시
                korean_name = HOUSE_SALES_STOCK_KOREAN_NAMES.get(series, series)
                print(f"  • {series}")
                print(f"    → {korean_name}")
            if len(category_groups[category_key]) > 5:
                print(f"  ... 외 {len(category_groups[category_key])-5}개 더")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, series_dict in HOUSE_SALES_STOCK_DATA_CATEGORIES.items():
        category_display = category.replace('_', ' ').title()
        print(f"\\n{category_display}:")
        print(f"  시리즈 개수: {len(series_dict)}개")
        # 샘플 시리즈 몇 개 표시
        sample_series = list(series_dict.values())[:3]
        for fred_id in sample_series:
            korean_name = HOUSE_SALES_STOCK_KOREAN_NAMES.get(fred_id, fred_id)
            print(f"    - {fred_id}: {korean_name}")
        if len(series_dict) > 3:
            print(f"    ... 외 {len(series_dict)-3}개 더")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not HOUSE_SALES_STOCK_DATA or 'load_info' not in HOUSE_SALES_STOCK_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': HOUSE_SALES_STOCK_DATA['load_info']['loaded'],
        'series_count': HOUSE_SALES_STOCK_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': HOUSE_SALES_STOCK_DATA['load_info']
    }
# %%
# === 사용 예시 ===

print("=== 리팩토링된 주택 판매/재고 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_house_sales_stock_data()  # 그룹별 스마트 업데이트")
print("   load_house_price_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_house_sales_stock_series_advanced(['existing_home_sales_ehs_sales_national_sa'], 'multi_line', 'raw')")
print("   plot_house_sales_stock_series_advanced(['new_residential_sales_nrs_sales_national_sa'], 'horizontal_bar', 'mom')")
print("   plot_house_sales_stock_series_advanced(['existing_home_sales_ehs_inventory_national'], 'single_line', 'yoy', periods=24)")
print()
print("3. 🔥 데이터 Export:")
print("   export_house_sales_stock_data(['existing_home_sales_ehs_sales_national_sa'], 'raw')")
print("   export_house_sales_stock_data(['new_residential_sales_nrs_sales_national_sa'], 'mom', periods=24, file_format='csv')")
print("   export_house_sales_stock_data(['existing_home_sales_ehs_inventory_national'], 'yoy', target_date='2024-06-01')")
print()
print("4. 📋 데이터 확인:")
print("   show_available_series()  # 사용 가능한 모든 시리즈 목록")
print("   show_category_options()  # 카테고리별 옵션")
print("   get_raw_data()  # 원본 지수 데이터")
print("   get_mom_data()  # 전월대비 변화 데이터")
print("   get_yoy_data()  # 전년동월대비 변화 데이터")
print("   get_data_status()  # 현재 데이터 상태")

# %%
# 테스트 실행
print("테스트: 주택 가격 데이터 로딩...")
result = load_house_sales_stock_data()

plot_house_sales_stock_series_advanced(['existing_home_sales_ehs_sales_national_sa', 'existing_home_sales_ehs_inventory_national'], 'multi_line', 'raw')
# %%
