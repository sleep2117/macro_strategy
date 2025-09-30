# %%
"""
미국 주택 가격 데이터 수집, 저장, 스마트 업데이트, 분석 및 시각화 도구
- FRED API를 통한 주택 가격 데이터 수집
- Case-Shiller, FHFA, Zillow 지수 분석
- 데이터 유형별 스마트 업데이트 기능
- KPDS 포맷 시각화 지원
"""
import pandas as pd
from datetime import datetime
import sys
import warnings
import json
import os
warnings.filterwarnings('ignore')

# 필수 라이브러리들
try:
    import requests
    FRED_API_AVAILABLE = True
    print("✓ FRED API 연동 가능 (requests 라이브러리)")
except ImportError:
    print("⚠️ requests 라이브러리가 없습니다. 설치하세요: pip install requests")
    FRED_API_AVAILABLE = False

# FRED API 키 설정 
FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'  # https://fred.stlouisfed.org/docs/api/api_key.html 에서 발급

# KPDS 시각화 라이브러리 불러오기 (필수)
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

print("✓ KPDS 시각화 포맷 로드됨")

# %%
# === 주택 가격 시리즈 ID와 매핑 ===

# Case-Shiller 홈 프라이스 인덱스 (전체 시리즈)
CASE_SHILLER_SERIES = {
    # National & Composite Indices
    'cs_national_sa': 'CSUSHPISA',      # National (SA)
    'cs_national_nsa': 'CSUSHPINSA',    # National (NSA)
    'cs_10city_sa': 'SPCS10RSA',        # 10-City Composite (SA)
    'cs_10city_nsa': 'SPCS10RNSA',      # 10-City Composite (NSA)
    'cs_20city_sa': 'SPCS20RSA',        # 20-City Composite (SA)
    'cs_20city_nsa': 'SPCS20RNSA',      # 20-City Composite (NSA)
    
    # Main Metropolitan Areas (SA & NSA)
    'cs_atlanta_sa': 'ATXRSA',          'cs_atlanta_nsa': 'ATXRNSA',
    'cs_boston_sa': 'BOXRSA',           'cs_boston_nsa': 'BOXRNSA',
    'cs_charlotte_sa': 'CRXRSA',        'cs_charlotte_nsa': 'CRXRNSA',
    'cs_chicago_sa': 'CHXRSA',          'cs_chicago_nsa': 'CHXRNSA',
    'cs_cleveland_sa': 'CEXRSA',        'cs_cleveland_nsa': 'CEXRNSA',
    'cs_dallas_sa': 'DAXRSA',           'cs_dallas_nsa': 'DAXRNSA',
    'cs_denver_sa': 'DNXRSA',           'cs_denver_nsa': 'DNXRNSA',
    'cs_detroit_sa': 'DEXRSA',          'cs_detroit_nsa': 'DEXRNSA',
    'cs_las_vegas_sa': 'LVXRSA',        'cs_las_vegas_nsa': 'LVXRNSA',
    'cs_los_angeles_sa': 'LXXRSA',      'cs_los_angeles_nsa': 'LXXRNSA',
    'cs_miami_sa': 'MIXRSA',            'cs_miami_nsa': 'MIXRNSA',
    'cs_minneapolis_sa': 'MNXRSA',      'cs_minneapolis_nsa': 'MNXRNSA',
    'cs_new_york_sa': 'NYXRSA',         'cs_new_york_nsa': 'NYXRNSA',
    'cs_phoenix_sa': 'PHXRSA',          'cs_phoenix_nsa': 'PHXRNSA',
    'cs_portland_sa': 'POXRSA',         'cs_portland_nsa': 'POXRNSA',
    'cs_san_diego_sa': 'SDXRSA',        'cs_san_diego_nsa': 'SDXRNSA',
    'cs_san_francisco_sa': 'SFXRSA',    'cs_san_francisco_nsa': 'SFXRNSA',
    'cs_seattle_sa': 'SEXRSA',          'cs_seattle_nsa': 'SEXRNSA',
    'cs_tampa_sa': 'TPXRSA',            'cs_tampa_nsa': 'TPXRNSA',
    'cs_washington_sa': 'WDXRSA',       'cs_washington_nsa': 'WDXRNSA',
    
    # Tiered Indices - Los Angeles
    'cs_la_high_sa': 'LXXRHTSA',        'cs_la_high_nsa': 'LXXRHTNSA',
    'cs_la_mid_sa': 'LXXRMTSA',         'cs_la_mid_nsa': 'LXXRMTNSA',
    'cs_la_low_sa': 'LXXRLTSA',         'cs_la_low_nsa': 'LXXRLTNSA',
    
    # Tiered Indices - New York
    'cs_ny_high_sa': 'NYXRHTSA',        'cs_ny_high_nsa': 'NYXRHTNSA',
    'cs_ny_mid_sa': 'NYXRMTSA',         'cs_ny_mid_nsa': 'NYXRMTNSA',
    'cs_ny_low_sa': 'NYXRLTSA',         'cs_ny_low_nsa': 'NYXRLTNSA',
    
    # Tiered Indices - San Francisco
    'cs_sf_high_sa': 'SFXRHTSA',        'cs_sf_high_nsa': 'SFXRHTNSA',
    'cs_sf_mid_sa': 'SFXRMTSA',         'cs_sf_mid_nsa': 'SFXRMTNSA',
    'cs_sf_low_sa': 'SFXRLTSA',         'cs_sf_low_nsa': 'SFXRLTNSA',
    
    # Condo Indices
    'cs_boston_condo_sa': 'BOXRCSA',    'cs_boston_condo_nsa': 'BOXRCNSA',
    'cs_chicago_condo_sa': 'CHXRCSA',   'cs_chicago_condo_nsa': 'CHXRCNSA',
    'cs_la_condo_sa': 'LXXRCSA',        'cs_la_condo_nsa': 'LXXRCNSA',
    'cs_ny_condo_sa': 'NYXRCSA',        'cs_ny_condo_nsa': 'NYXRCNSA',
    'cs_sf_condo_sa': 'SFXRCSA',        'cs_sf_condo_nsa': 'SFXRCNSA',
}

# FHFA 홈 프라이스 인덱스 (전체 시리즈)
FHFA_SERIES = {
    # National Level
    'fhfa_national_sa': 'HPIPONM226S',    # US Purchase Only HPI (SA)
    'fhfa_national_nsa': 'HPIPONM226N',   # US Purchase Only HPI (NSA)
    
    # Census Divisions (SA & NSA)
    'fhfa_new_england_sa': 'PONHPI00101M226S',      'fhfa_new_england_nsa': 'PONHPI00101M226N',
    'fhfa_middle_atlantic_sa': 'PONHPI00102M226S',  'fhfa_middle_atlantic_nsa': 'PONHPI00102M226N',
    'fhfa_south_atlantic_sa': 'PONHPI00103M226S',   'fhfa_south_atlantic_nsa': 'PONHPI00103M226N',
    'fhfa_east_south_central_sa': 'PONHPI00104M226S', 'fhfa_east_south_central_nsa': 'PONHPI00104M226N',
    'fhfa_west_south_central_sa': 'PONHPI00105M226S', 'fhfa_west_south_central_nsa': 'PONHPI00105M226N',
    'fhfa_east_north_central_sa': 'PONHPI00106M226S', 'fhfa_east_north_central_nsa': 'PONHPI00106M226N',
    'fhfa_west_north_central_sa': 'PONHPI00107M226S', 'fhfa_west_north_central_nsa': 'PONHPI00107M226N',
    'fhfa_mountain_sa': 'PONHPI00108M226S',          'fhfa_mountain_nsa': 'PONHPI00108M226N',
    'fhfa_pacific_sa': 'PONHPI00109M226S',           'fhfa_pacific_nsa': 'PONHPI00109M226N',
}

# Zillow 홈 밸류 인덱스 (주요 주 선별)
ZILLOW_SERIES = {
    # National
    'zillow_us': 'USAUCSFRCONDOSMSAMID',
    
    # Major States 
    'zillow_california': 'CAUCSFRCONDOSMSAMID',
    'zillow_florida': 'FLUCSFRCONDOSMSAMID',
    'zillow_texas': 'TXUCSFRCONDOSMSAMID',
    'zillow_new_york': 'NYUCSFRCONDOSMSAMID',
    'zillow_washington': 'WAUCSFRCONDOSMSAMID',
    'zillow_massachusetts': 'MAUCSFRCONDOSMSAMID',
    'zillow_colorado': 'COUCSFRCONDOSMSAMID',
    'zillow_arizona': 'AZUCSFRCONDOSMSAMID',
    'zillow_nevada': 'NVUCSFRCONDOSMSAMID',
    'zillow_oregon': 'ORUCSFRCONDOSMSAMID',
    'zillow_georgia': 'GAUCSFRCONDOSMSAMID',
    'zillow_north_carolina': 'NCUCSFRCONDOSMSAMID',
    'zillow_illinois': 'ILUCSFRCONDOSMSAMID',
    'zillow_pennsylvania': 'PAUCSFRCONDOSMSAMID',
    'zillow_ohio': 'OHUCSFRCONDOSMSAMID',
    'zillow_michigan': 'MIUCSFRCONDOSMSAMID',
    'zillow_virginia': 'VAUCSFRCONDOSMSAMID',
}

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
    
    # National Level - Median Prices
    'ehs_median_price_national': 'HOSMEDUSM052N',    # 전국 중간 판매가격
    'ehs_sf_median_price_national': 'HSFMEDUSM052N', # 전국 단독주택 중간 판매가격
    
    # Regional Level - Median Prices
    'ehs_median_price_northeast': 'HOSMEDUSNEM052N', # 동북부 중간 판매가격
    'ehs_median_price_midwest': 'HOSMEDUSMWM052N',   # 중서부 중간 판매가격
    'ehs_median_price_south': 'HOSMEDUSSOM052N',     # 남부 중간 판매가격
    'ehs_median_price_west': 'HOSMEDUSWTM052N',      # 서부 중간 판매가격
    
    'ehs_sf_median_price_northeast': 'HSFMEDUSNEM052N', # 동북부 단독주택 중간 판매가격
    'ehs_sf_median_price_midwest': 'HSFMEDUSMWM052N',   # 중서부 단독주택 중간 판매가격
    'ehs_sf_median_price_south': 'HSFMEDUSSOM052N',     # 남부 단독주택 중간 판매가격
    'ehs_sf_median_price_west': 'HSFMEDUSWTM052N',      # 서부 단독주택 중간 판매가격
}

# 신규 주택 판매 관련 시리즈 (Census & HUD - New Residential Sales)
NEW_RESIDENTIAL_SALES_SERIES = {
    # National Level - Core Sales and Inventory
    'nrs_sales_national_sa': 'HSN1F',               # 신규 단독주택 판매량 (SAAR)
    'nrs_sales_national_nsa': 'HSN1FNSA',           # 신규 단독주택 판매량 (NSA)
    'nrs_inventory_national_sa': 'HNFSEPUSSA',      # 신규 주택 재고 (SA)
    'nrs_inventory_national_nsa': 'HNFSUSNSA',      # 신규 주택 재고 (NSA)
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
    
    # Regional Level - Inventory
    'nrs_inventory_northeast': 'HNFSNE',            # 동북부 재고
    'nrs_inventory_midwest': 'HNFSMW',              # 중서부 재고
    'nrs_inventory_south': 'HNFSS',                 # 남부 재고
    'nrs_inventory_west': 'HNFSW',                  # 서부 재고
    
    # Price Indicators - National
    'nrs_median_price_monthly': 'MSPNHSUS',         # 월별 중간 판매가격
    'nrs_median_price_quarterly': 'MSPUS',          # 분기별 중간 판매가격
    'nrs_average_price_monthly': 'ASPNHSUS',        # 월별 평균 판매가격
    'nrs_average_price_quarterly': 'ASPUS',         # 분기별 평균 판매가격
    
    # Price Indicators - Regional (Quarterly)
    'nrs_median_price_northeast_q': 'MSPNE',        # 동북부 중간 판매가격 (분기)
    'nrs_median_price_midwest_q': 'MSPMW',          # 중서부 중간 판매가격 (분기)
    'nrs_median_price_south_q': 'MSPS',             # 남부 중간 판매가격 (분기)
    'nrs_median_price_west_q': 'MSPW',              # 서부 중간 판매가격 (분기)
    
    'nrs_average_price_northeast_q': 'ASPNE',       # 동북부 평균 판매가격 (분기)
    'nrs_average_price_midwest_q': 'ASPMW',         # 중서부 평균 판매가격 (분기)
    'nrs_average_price_south_q': 'ASPS',            # 남부 평균 판매가격 (분기)
    'nrs_average_price_west_q': 'ASPW',             # 서부 평균 판매가격 (분기)
    
    # Sales by Stage of Construction
    'nrs_sales_total_stage': 'NHSDPTS',             # 전체 (단계별)
    'nrs_sales_completed': 'NHSDPCS',               # 완공
    'nrs_sales_under_construction': 'NHSDPUCS',     # 건설중
    'nrs_sales_not_started': 'NHSDPNSS',            # 미착공
    
    # Inventory by Stage of Construction
    'nrs_inventory_total_stage': 'NHFSEPTS',        # 전체 재고 (단계별)
    'nrs_inventory_completed_stage': 'NHFSEPCS',    # 완공 재고
    'nrs_inventory_under_construction_stage': 'NHFSEPUCS', # 건설중 재고
    'nrs_inventory_not_started_stage': 'NHFSEPNTS', # 미착공 재고
    
    # Sales by Type of Financing (Quarterly)
    'nrs_sales_cash': 'HSTFC',                      # 현금 구매
    'nrs_sales_conventional': 'HSTFCM',             # 일반 융자
    'nrs_sales_fha': 'HSTFFHAI',                    # FHA 융자
    'nrs_sales_va': 'HSTFVAG',                      # VA 융자
    
    # Other Indicators
    'nrs_median_months_on_market': 'MNMFS',         # 시장 체류 기간 (중간값)
}

# 데이터 유형별로 분류
HOUSE_PRICE_DATA_CATEGORIES = {
    'case_shiller': CASE_SHILLER_SERIES,
    'fhfa': FHFA_SERIES,
    'zillow': ZILLOW_SERIES,
    'existing_home_sales': EXISTING_HOME_SALES_SERIES,
    'new_residential_sales': NEW_RESIDENTIAL_SALES_SERIES
}

# 전체 시리즈 통합
ALL_HOUSE_PRICE_SERIES = {
    **CASE_SHILLER_SERIES,
    **FHFA_SERIES, 
    **ZILLOW_SERIES,
    **EXISTING_HOME_SALES_SERIES,
    **NEW_RESIDENTIAL_SALES_SERIES
}

# 한국어 이름 매핑
HOUSE_PRICE_KOREAN_NAMES = {
    # Case-Shiller National & Composite
    'cs_national_sa': 'CS 전국 지수(SA)',           'cs_national_nsa': 'CS 전국 지수(NSA)',
    'cs_10city_sa': 'CS 10도시 지수(SA)',         'cs_10city_nsa': 'CS 10도시 지수(NSA)',
    'cs_20city_sa': 'CS 20도시 지수(SA)',         'cs_20city_nsa': 'CS 20도시 지수(NSA)',
    
    # Case-Shiller Main Metro Areas
    'cs_atlanta_sa': 'CS 애트랜타(SA)',             'cs_atlanta_nsa': 'CS 애트랜타(NSA)',
    'cs_boston_sa': 'CS 보스턴(SA)',                'cs_boston_nsa': 'CS 보스턴(NSA)',
    'cs_charlotte_sa': 'CS 샬럿(SA)',               'cs_charlotte_nsa': 'CS 샬럿(NSA)',
    'cs_chicago_sa': 'CS 시카고(SA)',              'cs_chicago_nsa': 'CS 시카고(NSA)',
    'cs_cleveland_sa': 'CS 클리블랜드(SA)',           'cs_cleveland_nsa': 'CS 클리블랜드(NSA)',
    'cs_dallas_sa': 'CS 댈러스(SA)',               'cs_dallas_nsa': 'CS 댈러스(NSA)',
    'cs_denver_sa': 'CS 덴버(SA)',                'cs_denver_nsa': 'CS 덴버(NSA)',
    'cs_detroit_sa': 'CS 디트로이트(SA)',            'cs_detroit_nsa': 'CS 디트로이트(NSA)',
    'cs_las_vegas_sa': 'CS 라스베이거스(SA)',         'cs_las_vegas_nsa': 'CS 라스베이거스(NSA)',
    'cs_los_angeles_sa': 'CS 로스앤젤레스(SA)',       'cs_los_angeles_nsa': 'CS 로스앤젤레스(NSA)',
    'cs_miami_sa': 'CS 마이애미(SA)',              'cs_miami_nsa': 'CS 마이애미(NSA)',
    'cs_minneapolis_sa': 'CS 미니애폴리스(SA)',       'cs_minneapolis_nsa': 'CS 미니애폴리스(NSA)',
    'cs_new_york_sa': 'CS 뉴욕(SA)',              'cs_new_york_nsa': 'CS 뉴욕(NSA)',
    'cs_phoenix_sa': 'CS 피닉스(SA)',              'cs_phoenix_nsa': 'CS 피닉스(NSA)',
    'cs_portland_sa': 'CS 포틀랜드(SA)',            'cs_portland_nsa': 'CS 포틀랜드(NSA)',
    'cs_san_diego_sa': 'CS 샌디에이고(SA)',          'cs_san_diego_nsa': 'CS 샌디에이고(NSA)',
    'cs_san_francisco_sa': 'CS 샌프란시스코(SA)',     'cs_san_francisco_nsa': 'CS 샌프란시스코(NSA)',
    'cs_seattle_sa': 'CS 시애틀(SA)',              'cs_seattle_nsa': 'CS 시애틀(NSA)',
    'cs_tampa_sa': 'CS 탬파(SA)',                 'cs_tampa_nsa': 'CS 탬파(NSA)',
    'cs_washington_sa': 'CS 워싱턴DC(SA)',           'cs_washington_nsa': 'CS 워싱턴DC(NSA)',
    
    # Case-Shiller Tiered Indices
    'cs_la_high_sa': 'CS LA 고가(SA)',             'cs_la_high_nsa': 'CS LA 고가(NSA)',
    'cs_la_mid_sa': 'CS LA 중가(SA)',              'cs_la_mid_nsa': 'CS LA 중가(NSA)',
    'cs_la_low_sa': 'CS LA 저가(SA)',              'cs_la_low_nsa': 'CS LA 저가(NSA)',
    'cs_ny_high_sa': 'CS 뉴욕 고가(SA)',           'cs_ny_high_nsa': 'CS 뉴욕 고가(NSA)',
    'cs_ny_mid_sa': 'CS 뉴욕 중가(SA)',            'cs_ny_mid_nsa': 'CS 뉴욕 중가(NSA)',
    'cs_ny_low_sa': 'CS 뉴욕 저가(SA)',            'cs_ny_low_nsa': 'CS 뉴욕 저가(NSA)',
    'cs_sf_high_sa': 'CS SF 고가(SA)',             'cs_sf_high_nsa': 'CS SF 고가(NSA)',
    'cs_sf_mid_sa': 'CS SF 중가(SA)',              'cs_sf_mid_nsa': 'CS SF 중가(NSA)',
    'cs_sf_low_sa': 'CS SF 저가(SA)',              'cs_sf_low_nsa': 'CS SF 저가(NSA)',
    
    # Case-Shiller Condo Indices
    'cs_boston_condo_sa': 'CS 보스턴 콘도(SA)',      'cs_boston_condo_nsa': 'CS 보스턴 콘도(NSA)',
    'cs_chicago_condo_sa': 'CS 시카고 콘도(SA)',     'cs_chicago_condo_nsa': 'CS 시카고 콘도(NSA)',
    'cs_la_condo_sa': 'CS LA 콘도(SA)',           'cs_la_condo_nsa': 'CS LA 콘도(NSA)',
    'cs_ny_condo_sa': 'CS 뉴욕 콘도(SA)',          'cs_ny_condo_nsa': 'CS 뉴욕 콘도(NSA)',
    'cs_sf_condo_sa': 'CS SF 콘도(SA)',           'cs_sf_condo_nsa': 'CS SF 콘도(NSA)',
    
    # FHFA Indices
    'fhfa_national_sa': 'FHFA 전국(SA)',           'fhfa_national_nsa': 'FHFA 전국(NSA)',
    'fhfa_new_england_sa': 'FHFA 뉴잉글랜드(SA)',    'fhfa_new_england_nsa': 'FHFA 뉴잉글랜드(NSA)',
    'fhfa_middle_atlantic_sa': 'FHFA 중부대서양(SA)', 'fhfa_middle_atlantic_nsa': 'FHFA 중부대서양(NSA)',
    'fhfa_south_atlantic_sa': 'FHFA 남부대서양(SA)',  'fhfa_south_atlantic_nsa': 'FHFA 남부대서양(NSA)',
    'fhfa_east_south_central_sa': 'FHFA 동남중부(SA)', 'fhfa_east_south_central_nsa': 'FHFA 동남중부(NSA)',
    'fhfa_west_south_central_sa': 'FHFA 서남중부(SA)', 'fhfa_west_south_central_nsa': 'FHFA 서남중부(NSA)',
    'fhfa_east_north_central_sa': 'FHFA 동북중부(SA)', 'fhfa_east_north_central_nsa': 'FHFA 동북중부(NSA)',
    'fhfa_west_north_central_sa': 'FHFA 서북중부(SA)', 'fhfa_west_north_central_nsa': 'FHFA 서북중부(NSA)',
    'fhfa_mountain_sa': 'FHFA 산악지역(SA)',       'fhfa_mountain_nsa': 'FHFA 산악지역(NSA)',
    'fhfa_pacific_sa': 'FHFA 태평양(SA)',         'fhfa_pacific_nsa': 'FHFA 태평양(NSA)',
    
    # Zillow Indices
    'zillow_us': 'Zillow 전미',
    'zillow_california': 'Zillow 캘리포니아',
    'zillow_florida': 'Zillow 플로리다',
    'zillow_texas': 'Zillow 텍사스',
    'zillow_new_york': 'Zillow 뉴욕주',
    'zillow_washington': 'Zillow 워싱턴주',
    'zillow_massachusetts': 'Zillow 매사추세츠',
    'zillow_colorado': 'Zillow 콜로라도',
    'zillow_arizona': 'Zillow 애리조나',
    'zillow_nevada': 'Zillow 네바다',
    'zillow_oregon': 'Zillow 오리건',
    'zillow_georgia': 'Zillow 조지아',
    'zillow_north_carolina': 'Zillow 노스캐롤라이나',
    'zillow_illinois': 'Zillow 일리노이',
    'zillow_pennsylvania': 'Zillow 펜실베이니아',
    'zillow_ohio': 'Zillow 오하이오',
    'zillow_michigan': 'Zillow 미시간',
    'zillow_virginia': 'Zillow 버지니아',
    
    # Existing Home Sales (기존 주택 판매)
    'ehs_sales_national_sa': 'EHS 전국 판매량(SA)',
    'ehs_inventory_national': 'EHS 전국 재고',
    'ehs_months_supply': 'EHS 재고 소진율',
    'ehs_sf_sales_national_sa': 'EHS 단독주택 판매량(SA)',
    'ehs_sf_inventory_national': 'EHS 단독주택 재고',
    'ehs_sf_months_supply': 'EHS 단독주택 소진율',
    
    'ehs_sales_northeast_sa': 'EHS 동북부 판매량(SA)',
    'ehs_sales_midwest_sa': 'EHS 중서부 판매량(SA)',
    'ehs_sales_south_sa': 'EHS 남부 판매량(SA)',
    'ehs_sales_west_sa': 'EHS 서부 판매량(SA)',
    
    'ehs_sf_sales_northeast_sa': 'EHS 동북부 단독주택(SA)',
    'ehs_sf_sales_midwest_sa': 'EHS 중서부 단독주택(SA)',
    'ehs_sf_sales_south_sa': 'EHS 남부 단독주택(SA)',
    'ehs_sf_sales_west_sa': 'EHS 서부 단독주택(SA)',
    
    'ehs_median_price_national': 'EHS 전국 중간가격',
    'ehs_sf_median_price_national': 'EHS 단독주택 중간가격',
    
    'ehs_median_price_northeast': 'EHS 동북부 중간가격',
    'ehs_median_price_midwest': 'EHS 중서부 중간가격',
    'ehs_median_price_south': 'EHS 남부 중간가격',
    'ehs_median_price_west': 'EHS 서부 중간가격',
    
    'ehs_sf_median_price_northeast': 'EHS 동북부 단독주택 중간가격',
    'ehs_sf_median_price_midwest': 'EHS 중서부 단독주택 중간가격',
    'ehs_sf_median_price_south': 'EHS 남부 단독주택 중간가격',
    'ehs_sf_median_price_west': 'EHS 서부 단독주택 중간가격',
    
    # New Residential Sales (신규 주택 판매)
    'nrs_sales_national_sa': 'NRS 전국 판매량(SA)',
    'nrs_sales_national_nsa': 'NRS 전국 판매량(NSA)',
    'nrs_inventory_national_sa': 'NRS 전국 재고(SA)',
    'nrs_inventory_national_nsa': 'NRS 전국 재고(NSA)',
    'nrs_months_supply_sa': 'NRS 재고 소진율(SA)',
    'nrs_months_supply_nsa': 'NRS 재고 소진율(NSA)',
    
    'nrs_sales_northeast_sa': 'NRS 동북부 판매량(SA)',
    'nrs_sales_midwest_sa': 'NRS 중서부 판매량(SA)',
    'nrs_sales_south_sa': 'NRS 남부 판매량(SA)',
    'nrs_sales_west_sa': 'NRS 서부 판매량(SA)',
    
    'nrs_sales_northeast_nsa': 'NRS 동북부 판매량(NSA)',
    'nrs_sales_midwest_nsa': 'NRS 중서부 판매량(NSA)',
    'nrs_sales_south_nsa': 'NRS 남부 판매량(NSA)',
    'nrs_sales_west_nsa': 'NRS 서부 판매량(NSA)',
    
    'nrs_inventory_northeast': 'NRS 동북부 재고',
    'nrs_inventory_midwest': 'NRS 중서부 재고',
    'nrs_inventory_south': 'NRS 남부 재고',
    'nrs_inventory_west': 'NRS 서부 재고',
    
    'nrs_median_price_monthly': 'NRS 월별 중간가격',
    'nrs_median_price_quarterly': 'NRS 분기별 중간가격',
    'nrs_average_price_monthly': 'NRS 월별 평균가격',
    'nrs_average_price_quarterly': 'NRS 분기별 평균가격',
    
    'nrs_median_price_northeast_q': 'NRS 동북부 중간가격(분기)',
    'nrs_median_price_midwest_q': 'NRS 중서부 중간가격(분기)',
    'nrs_median_price_south_q': 'NRS 남부 중간가격(분기)',
    'nrs_median_price_west_q': 'NRS 서부 중간가격(분기)',
    
    'nrs_average_price_northeast_q': 'NRS 동북부 평균가격(분기)',
    'nrs_average_price_midwest_q': 'NRS 중서부 평균가격(분기)',
    'nrs_average_price_south_q': 'NRS 남부 평균가격(분기)',
    'nrs_average_price_west_q': 'NRS 서부 평균가격(분기)',
    
    'nrs_sales_total_stage': 'NRS 전체 단계별',
    'nrs_sales_completed': 'NRS 완공',
    'nrs_sales_under_construction': 'NRS 건설중',
    'nrs_sales_not_started': 'NRS 미착공',
    
    'nrs_inventory_total_stage': 'NRS 전체 재고 단계별',
    'nrs_inventory_completed_stage': 'NRS 완공 재고',
    'nrs_inventory_under_construction_stage': 'NRS 건설중 재고',
    'nrs_inventory_not_started_stage': 'NRS 미착공 재고',
    
    'nrs_sales_cash': 'NRS 현금구매',
    'nrs_sales_conventional': 'NRS 일반융자',
    'nrs_sales_fha': 'NRS FHA융자',
    'nrs_sales_va': 'NRS VA융자',
    
    'nrs_median_months_on_market': 'NRS 시장체류기간',
}

# CSV 파일 경로 및 전역 데이터 저장소 (fed_pmi.py 방식 참조)
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/house_price_data.csv'
META_FILE_PATH = '/home/jyp0615/us_eco/data/house_price_meta.json'

HOUSE_PRICE_DATA = {
    'raw_data': pd.DataFrame(),
    'mom_data': pd.DataFrame(),
    'yoy_data': pd.DataFrame(),
    'latest_values': {},
    'load_info': {
        'loaded': False,
        'last_update': None,
        'total_series': 0,
        'date_range': None,
        'data_categories': ['case_shiller', 'fhfa', 'zillow'],
        'start_date': None,
        'data_points': 0,
        'load_time': None,
        'series_count': 0
    }
}

# API 연결 정보
API_CONNECTED = False
CURRENT_API_KEY = FRED_API_KEY

# %%
# === CSV 데이터 관리 함수들 ===

def ensure_data_directory():
    """데이터 디렉터리 생성 확인"""
    data_dir = os.path.dirname(CSV_FILE_PATH)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"📁 데이터 디렉터리 생성: {data_dir}")

def save_house_price_data_to_csv():
    """현재 주택 가격 데이터를 CSV 파일로 저장"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("⚠️ 저장할 데이터가 없습니다. 먼저 데이터를 로드하세요.")
        return False
    
    ensure_data_directory()
    
    try:
        raw_data = HOUSE_PRICE_DATA['raw_data']
        
        # CSV 저장
        raw_data.to_csv(CSV_FILE_PATH, encoding='utf-8')
        
        # 메타데이터 저장
        with open(META_FILE_PATH, 'w', encoding='utf-8') as f:
            meta_data = {
                'load_time': HOUSE_PRICE_DATA['load_info']['load_time'].isoformat() if HOUSE_PRICE_DATA['load_info']['load_time'] else None,
                'last_update': HOUSE_PRICE_DATA['load_info']['last_update'].isoformat() if HOUSE_PRICE_DATA['load_info']['last_update'] else None,
                'start_date': HOUSE_PRICE_DATA['load_info']['start_date'],
                'total_series': HOUSE_PRICE_DATA['load_info']['total_series'],
                'data_points': HOUSE_PRICE_DATA['load_info']['data_points'],
                'data_categories': HOUSE_PRICE_DATA['load_info']['data_categories'],
                'latest_values': HOUSE_PRICE_DATA['latest_values']
            }
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 주택 가격 데이터 저장 완료: {CSV_FILE_PATH}")
        return True
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_house_price_data_from_csv():
    """CSV 파일에서 주택 가격 데이터 로드"""
    global HOUSE_PRICE_DATA
    
    if not os.path.exists(CSV_FILE_PATH):
        print(f"⚠️ CSV 파일이 없습니다: {CSV_FILE_PATH}")
        return False
    
    try:
        # CSV 데이터 로드
        df = pd.read_csv(CSV_FILE_PATH, index_col=0, parse_dates=True, encoding='utf-8')
        
        # 메타데이터 로드 (파생 데이터 업데이트에서 처리됨)

        if os.path.exists(META_FILE_PATH):
            with open(META_FILE_PATH, 'r', encoding='utf-8') as f:
                # 메타데이터는 파생 데이터 업데이트에서 처리됨
                json.load(f)

        
        # 전역 저장소에 저장
        HOUSE_PRICE_DATA['raw_data'] = df
        update_derived_data()
        
        print(f"✅ CSV에서 주택 가격 데이터 로드 완료: {CSV_FILE_PATH}")
        print(f"   시리즈 수: {len(df.columns)}개")
        print(f"   데이터 기간: {df.index[0].strftime('%Y-%m')} ~ {df.index[-1].strftime('%Y-%m')}")
        return True
        
    except Exception as e:
        print(f"❌ CSV 로드 실패: {e}")
        return False

def check_recent_data_consistency_enhanced():
    """최근 데이터 일치성 확인 (스마트 업데이트용)"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        return {
            'need_update': True,
            'reason': '기존 데이터 없음',
            'details': {}
        }
    
    print("🔍 최근 주택 가격 데이터 일치성 확인 중...")
    
    # API 연결 확인 및 초기화
    if not API_CONNECTED:
        print("🔄 FRED API 연결 초기화 중...")
        if not initialize_fred_api():
            print("⚠️ API 연결 실패, 전체 업데이트 필요")
            return {
                'need_update': True,
                'reason': 'API 연결 실패',
                'details': {}
            }
    
    # 주요 시리즈들을 카테고리별로 확인
    check_series = {
        'case_shiller': ['cs_national_sa', 'cs_20city_sa'],
        'fhfa': ['fhfa_national_sa'],
        'zillow': ['zillow_us'],
        'existing_home_sales': ['ehs_sales_national_sa', 'ehs_median_price_national'],
        'new_residential_sales': ['nrs_sales_national_sa', 'nrs_median_price_monthly']
    }
    
    total_mismatches = 0
    total_checked = 0
    category_results = {}
    
    for category, series_list in check_series.items():
        category_mismatches = 0
        category_checked = 0
        
        for series_key in series_list:
            if series_key not in HOUSE_PRICE_DATA['raw_data'].columns:
                category_mismatches += 1
                category_checked += 1
                continue
            
            # 기존 데이터의 최근 3개월 체크
            local_data = HOUSE_PRICE_DATA['raw_data'][series_key].dropna()
            if len(local_data) < 3:
                category_mismatches += 1
                category_checked += 1
                continue
            
            try:
                # API에서 최근 데이터 가져와서 비교 (더 긴 기간으로 확인)
                last_date = local_data.index[-1]
                start_check_date = (last_date - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
                
                api_data = get_series_data(series_key, start_date=start_check_date, min_points=3, is_update=True)
                
                if api_data is None:
                    category_mismatches += 1
                else:
                    # 최근 데이터 비교
                    recent_local = local_data.tail(3)
                    recent_api = api_data.tail(3)
                    
                    common_dates = recent_local.index.intersection(recent_api.index)
                    if len(common_dates) == 0:
                        category_mismatches += 1
                    else:
                        local_values = recent_local[common_dates].round(2)
                        api_values = recent_api[common_dates].round(2)
                        
                        if not local_values.equals(api_values):
                            category_mismatches += 1
                
                category_checked += 1
                
            except Exception as e:
                print(f"   ⚠️ {series_key} 확인 중 오류: {e}")
                category_mismatches += 1
                category_checked += 1
        
        category_results[category] = {
            'checked': category_checked,
            'mismatches': category_mismatches,
            'match_rate': (category_checked - category_mismatches) / category_checked if category_checked > 0 else 0
        }
        
        total_checked += category_checked
        total_mismatches += category_mismatches
    
    # 결과 출력
    for category, result in category_results.items():
        match_rate = result['match_rate'] * 100
        print(f"   📊 {category.upper()}: {result['checked'] - result['mismatches']}/{result['checked']} 일치 ({match_rate:.1f}%)")
    
    # 업데이트 필요성 판단 (더 관대하게 조정)
    overall_match_rate = (total_checked - total_mismatches) / total_checked if total_checked > 0 else 0
    
    if total_checked == 0:
        need_update = True
        reason = '확인할 시리즈 없음'
    elif overall_match_rate >= 0.7:  # 70% 이상 일치하면 업데이트 불필요
        need_update = False
        reason = '데이터 최신 상태'
    else:
        need_update = True
        reason = '데이터 불일치'
    
    print(f"📈 전체 일치율: {overall_match_rate*100:.1f}% ({total_checked - total_mismatches}/{total_checked})")
    print(f"🎯 업데이트 필요: {'예' if need_update else '아니오'} ({reason})")
    
    return {
        'need_update': need_update,
        'reason': reason,
        'overall_match_rate': overall_match_rate,
        'category_results': category_results,
        'total_checked': total_checked,
        'total_mismatches': total_mismatches
    }

# %%
# === FRED API 연동 함수들 ===

def initialize_fred_api():
    """FRED API 연결 초기화"""
    global API_CONNECTED, CURRENT_API_KEY
    
    if not FRED_API_AVAILABLE:
        print("❌ FRED API 사용 불가: requests 라이브러리 필요")
        return False
    
    # FRED API 키 시도 (단일 키만 사용)
    api_keys = [FRED_API_KEY]
    
    for i, api_key in enumerate(api_keys, 1):
        try:
            # 테스트 요청
            test_url = f"https://api.stlouisfed.org/fred/series?series_id=CSUSHPISA&api_key={api_key}&file_type=json"
            response = requests.get(test_url, timeout=10)
            
            if response.status_code == 200:
                CURRENT_API_KEY = api_key
                API_CONNECTED = True
                print(f"✅ FRED API 연결 성공 (API 키 #{i})")
                return True
            else:
                print(f"❌ API 키 #{i} 실패: {response.status_code}")
                
        except Exception as e:
            print(f"❌ API 키 #{i} 연결 오류: {e}")
    
    print("❌ 모든 FRED API 키 연결 실패")
    API_CONNECTED = False
    return False

def get_series_data(series_key, start_date='2020-01-01', end_date=None, min_points=6, is_update=False):
    """
    FRED API에서 특정 시리즈 데이터 가져오기
    """
    if not API_CONNECTED:
        print("❌ FRED API가 연결되지 않았습니다.")
        return None
    
    if series_key not in ALL_HOUSE_PRICE_SERIES:
        print(f"❌ 알 수 없는 시리즈: {series_key}")
        return None
    
    series_id = ALL_HOUSE_PRICE_SERIES[series_key]
    
    try:
        # FRED API 요청 URL
        url = f"https://api.stlouisfed.org/fred/series/observations"
        params = {
            'series_id': series_id,
            'api_key': CURRENT_API_KEY,
            'file_type': 'json',
            'observation_start': start_date,
            'frequency': 'm',  # 월별 데이터
            'aggregation_method': 'eop'  # 월말 기준
        }
        
        if end_date:
            params['observation_end'] = end_date
            
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ API 요청 실패: {response.status_code}")
            return None
        
        data = response.json()
        
        if 'observations' not in data:
            print(f"❌ 데이터 없음: {series_key}")
            return None
        
        observations = data['observations']
        
        # 데이터 파싱
        dates = []
        values = []
        
        for obs in observations:
            if obs['value'] not in ['.', '', None]:
                try:
                    dates.append(pd.to_datetime(obs['date']))
                    values.append(float(obs['value']))
                except:
                    continue
        
        if len(values) < min_points:
            if not is_update:
                print(f"⚠️ 데이터 부족: {series_key} ({len(values)}개 포인트)")
            return None
        
        # pandas Series 생성
        series = pd.Series(values, index=dates, name=series_key)
        series = series.sort_index()
        
        if not is_update:
            print(f"✅ {series_key}: {len(series)}개 데이터 ({series.index[0].strftime('%Y-%m')} ~ {series.index[-1].strftime('%Y-%m')})")
        
        return series
        
    except Exception as e:
        print(f"❌ 오류 발생: {series_key} - {e}")
        return None

# %%
# === 데이터 유형별 스마트 업데이트 기능 ===

def check_data_category_consistency(category_name, check_count=3):
    """
    특정 데이터 카테고리의 최근 데이터 일치성 확인
    
    Args:
        category_name: 'case_shiller', 'fhfa', 'zillow', 'existing_home_sales', 'new_residential_sales'
        check_count: 확인할 최근 데이터 개수
    """
    global HOUSE_PRICE_DATA
    
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        return {
            'need_update': True,
            'reason': '기존 데이터 없음',
            'details': {}
        }
    
    if category_name not in HOUSE_PRICE_DATA_CATEGORIES:
        print(f"❌ 알 수 없는 카테고리: {category_name}")
        return {'need_update': True, 'reason': '잘못된 카테고리', 'details': {}}
    
    # 해당 카테고리의 주요 시리즈 선택
    series_dict = HOUSE_PRICE_DATA_CATEGORIES[category_name]
    if category_name == 'case_shiller':
        check_series = ['cs_national_sa', 'cs_20city_sa']
    elif category_name == 'fhfa':
        check_series = ['fhfa_national_sa']
    elif category_name == 'zillow':
        check_series = ['zillow_us', 'zillow_california']
    elif category_name == 'existing_home_sales':
        check_series = ['ehs_sales_national_sa', 'ehs_median_price_national']
    elif category_name == 'new_residential_sales':
        check_series = ['nrs_sales_national_sa', 'nrs_median_price_monthly']
    else:
        check_series = list(series_dict.keys())[:2]
    
    print(f"🔍 {category_name.upper()} 카테고리 최근 {check_count}개 데이터 일치성 확인 중...")
    
    consistency_results = {}
    api_mismatch_count = 0
    
    for series_key in check_series:
        if series_key not in series_dict:
            continue
        
        if series_key not in HOUSE_PRICE_DATA['raw_data'].columns:
            consistency_results[series_key] = {'status': '로컬 데이터 없음', 'match': False}
            api_mismatch_count += 1
            continue
        
        # 기존 데이터
        local_data = HOUSE_PRICE_DATA['raw_data'][series_key].dropna()
        if len(local_data) < check_count:
            consistency_results[series_key] = {'status': '로컬 데이터 부족', 'match': False}
            api_mismatch_count += 1
            continue
        
        # API에서 최근 데이터 가져오기
        last_date = local_data.index[-1]
        start_check_date = (last_date - pd.DateOffset(months=check_count-1)).strftime('%Y-%m-%d')
        
        try:
            api_data = get_series_data(series_key, start_date=start_check_date, is_update=True)
            
            if api_data is None or len(api_data) == 0:
                consistency_results[series_key] = {'status': 'API 데이터 없음', 'match': False}
                api_mismatch_count += 1
                continue
            
            # 최근 데이터 비교
            recent_local = local_data.tail(check_count)
            recent_api = api_data.tail(check_count)
            
            # 날짜 맞춰서 비교
            common_dates = recent_local.index.intersection(recent_api.index)
            
            if len(common_dates) == 0:
                consistency_results[series_key] = {'status': '날짜 불일치', 'match': False}
                api_mismatch_count += 1
                continue
            
            # 값 비교 (소수점 2자리까지 허용)
            local_values = recent_local[common_dates].round(2)
            api_values = recent_api[common_dates].round(2)
            
            if local_values.equals(api_values):
                consistency_results[series_key] = {'status': '일치', 'match': True}
            else:
                consistency_results[series_key] = {
                    'status': '값 불일치', 
                    'match': False,
                    'local_latest': local_values.iloc[-1] if len(local_values) > 0 else None,
                    'api_latest': api_values.iloc[-1] if len(api_values) > 0 else None
                }
                api_mismatch_count += 1
                
        except Exception as e:
            consistency_results[series_key] = {'status': f'확인 오류: {e}', 'match': False}
            api_mismatch_count += 1
    
    # 결과 출력
    for series_key, result in consistency_results.items():
        status_emoji = "✅" if result['match'] else "❌"
        print(f"   {status_emoji} {series_key}: {result['status']}")
    
    # 업데이트 필요성 판단
    total_checked = len(consistency_results)
    if total_checked == 0:
        need_update = True
        reason = '확인할 시리즈 없음'
    elif api_mismatch_count == 0:
        need_update = False
        reason = '모든 데이터 일치'
    elif api_mismatch_count >= total_checked * 0.5:  # 50% 이상 불일치
        need_update = True
        reason = '데이터 불일치'
    else:
        need_update = True
        reason = '부분적 불일치'
    
    print(f"📊 {category_name.upper()} 일치성 결과: {total_checked - api_mismatch_count}/{total_checked} 일치")
    print(f"🎯 업데이트 필요: {'예' if need_update else '아니오'} ({reason})")
    
    return {
        'need_update': need_update,
        'reason': reason,
        'details': consistency_results,
        'mismatch_count': api_mismatch_count,
        'total_count': total_checked
    }

def update_data_category(category_name, start_date=None, smart_update=True):
    """
    특정 데이터 카테고리 업데이트
    
    Args:
        category_name: 'case_shiller', 'fhfa', 'zillow'
        start_date: 업데이트 시작 날짜
        smart_update: 스마트 업데이트 사용 여부
    """
    global HOUSE_PRICE_DATA
    
    if category_name not in HOUSE_PRICE_DATA_CATEGORIES:
        print(f"❌ 알 수 없는 카테고리: {category_name}")
        return False
    
    # 스마트 업데이트 확인
    if smart_update and HOUSE_PRICE_DATA['load_info']['loaded']:
        consistency_check = check_data_category_consistency(category_name)
        
        if not consistency_check['need_update']:
            print(f"🎯 {category_name.upper()} 데이터가 최신 상태입니다.")
            return True
        
        if consistency_check['reason'] == '데이터 불일치':
            print(f"⚠️ {category_name.upper()} 최근 데이터 불일치로 인한 재로드")
            if HOUSE_PRICE_DATA['raw_data'].empty:
                start_date = '2000-01-01'
            else:
                last_date = HOUSE_PRICE_DATA['raw_data'].index[-1]
                start_date = (last_date - pd.DateOffset(months=3)).strftime('%Y-%m-01')
    
    series_dict = HOUSE_PRICE_DATA_CATEGORIES[category_name]
    series_list = list(series_dict.keys())
    
    if start_date is None and not HOUSE_PRICE_DATA['raw_data'].empty:
        last_date = HOUSE_PRICE_DATA['raw_data'].index[-1]
        start_date = last_date.strftime('%Y-%m-01')
    elif start_date is None:
        start_date = '2000-01-01'
    
    print(f"📊 {category_name.upper()} 데이터 업데이트 시작: {start_date}부터")
    
    # FRED API 초기화
    if not initialize_fred_api():
        print("❌ FRED API 초기화 실패")
        return False
    
    # 새로운 데이터 수집
    new_data = {}
    successful_updates = 0
    failed_updates = 0
    
    for series_key in series_list:
        try:
            print(f"🔄 {category_name.upper()} 업데이트 중: {series_key}")
            
            new_series_data = get_series_data(series_key, start_date=start_date, is_update=True)
            
            if new_series_data is not None and len(new_series_data) > 0:
                new_data[series_key] = new_series_data
                successful_updates += 1
                print(f"✅ 성공: {series_key} ({len(new_series_data)}개 포인트)")
            else:
                failed_updates += 1
                print(f"❌ 실패: {series_key}")
                
        except Exception as e:
            failed_updates += 1
            print(f"❌ 오류: {series_key} - {e}")
    
    # 기존 데이터와 병합
    if new_data:
        print(f"📈 {category_name.upper()} 데이터 병합 중... ({len(new_data)}개 시리즈)")
        
        if HOUSE_PRICE_DATA['raw_data'].empty:
            # 새로운 DataFrame 생성
            all_dates = set()
            for series in new_data.values():
                all_dates.update(series.index)
            
            date_index = pd.Index(sorted(all_dates))
            HOUSE_PRICE_DATA['raw_data'] = pd.DataFrame(index=date_index)
        
        # 기존 데이터 확장
        existing_df = HOUSE_PRICE_DATA['raw_data'].copy()
        
        # 새로운 인덱스 수집
        all_new_dates = set()
        for new_series in new_data.values():
            all_new_dates.update(new_series.index)
        
        # DataFrame 인덱스 확장
        if all_new_dates:
            new_index = existing_df.index.union(pd.Index(all_new_dates)).sort_values()
            existing_df = existing_df.reindex(new_index)
        
        # 새 데이터 병합
        for series_key, new_series in new_data.items():
            for date, value in new_series.items():
                existing_df.loc[date, series_key] = value
        
        HOUSE_PRICE_DATA['raw_data'] = existing_df
        
        print(f"✅ {category_name.upper()} 업데이트 완료: 성공 {successful_updates}, 실패 {failed_updates}")
        return True
    else:
        print(f"❌ {category_name.upper()} 업데이트할 새 데이터 없음")
        return False

def load_all_house_price_data_enhanced(start_date='2000-01-01', force_reload=False, smart_update=True, enabled_categories=None):
    """
    모든 주택 가격 데이터 로드 (스마트 업데이트 포함)
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드
        smart_update: 스마트 업데이트 사용
        enabled_categories: 로드할 카테고리 리스트
    """
    global HOUSE_PRICE_DATA
    
    print("🏠 미국 주택 가격 데이터 로드 시작...")
    
    # 로드할 카테고리 결정
    if enabled_categories is None:
        enabled_categories = ['case_shiller', 'fhfa', 'zillow']
    
    print(f"🎯 대상 카테고리: {', '.join(enabled_categories)}")
    
    # 1. 먼저 CSV에서 기존 데이터 로드 시도
    if not force_reload and not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("📁 기존 CSV 데이터 확인 중...")
        if load_house_price_data_from_csv():
            print("✅ 기존 데이터 로드 완료")
        else:
            print("⚠️ 기존 데이터 없음, 새로 수집 필요")
    
    # 2. 스마트 업데이트 확인
    if smart_update and HOUSE_PRICE_DATA['load_info']['loaded'] and not force_reload:
        print("🔍 스마트 업데이트 확인 중...")
        consistency_check = check_recent_data_consistency_enhanced()
        
        if not consistency_check['need_update']:
            print("✅ 데이터가 최신 상태입니다.")
            return True
        else:
            print(f"🔄 업데이트 필요: {consistency_check['reason']}")
    
    # 3. 데이터가 이미 로드되어 있고 강제 재로드가 아닌 경우
    if HOUSE_PRICE_DATA['load_info']['loaded'] and not force_reload and not smart_update:
        print("✅ 주택 가격 데이터가 이미 로드되었습니다.")
        return True
    
    # 4. API를 통한 데이터 수집 및 업데이트
    print("🌐 FRED API를 통한 데이터 수집 시작...")
    
    if not initialize_fred_api():
        print("❌ FRED API 초기화 실패")
        return False
    
    all_data = {}
    total_successful = 0
    total_failed = 0
    
    # 스마트 업데이트 모드에서는 최근 데이터만 수집
    if smart_update and HOUSE_PRICE_DATA['load_info']['loaded'] and not force_reload:
        # 최근 24개월 데이터로 업데이트 (충분한 데이터 확보)
        if not HOUSE_PRICE_DATA['raw_data'].empty:
            last_date = HOUSE_PRICE_DATA['raw_data'].index[-1]
            start_date = (last_date - pd.DateOffset(months=24)).strftime('%Y-%m-%d')
            print(f"📅 스마트 업데이트: {start_date}부터 수집")
    
    for category in enabled_categories:
        print(f"\n📊 {category.upper()} 카테고리 로딩 중...")
        series_dict = HOUSE_PRICE_DATA_CATEGORIES[category]
        
        for series_key in series_dict.keys():
            try:
                # 스마트 업데이트 시 더 적은 데이터로도 허용
                min_points = 3 if smart_update else 6
                series_data = get_series_data(series_key, start_date=start_date, min_points=min_points)
                
                if series_data is not None:
                    all_data[series_key] = series_data
                    total_successful += 1
                    print(f"✅ {series_key}: {len(series_data)}개 데이터")
                else:
                    total_failed += 1
                    print(f"❌ {series_key}: 데이터 없음")
                    
            except Exception as e:
                print(f"❌ 오류: {series_key} - {e}")
                total_failed += 1
    
    # 5. DataFrame 생성 및 병합
    if all_data:
        print(f"\n📊 데이터 병합 중...")
        
        if HOUSE_PRICE_DATA['raw_data'].empty or force_reload:
            # 새로운 DataFrame 생성
            all_dates = set()
            for series in all_data.values():
                all_dates.update(series.index)
            
            date_index = pd.Index(sorted(all_dates))
            df = pd.DataFrame(index=date_index)
            
            for series_key, series_data in all_data.items():
                df[series_key] = series_data
        else:
            # 기존 데이터와 병합
            df = HOUSE_PRICE_DATA['raw_data'].copy()
            
            # 새로운 날짜들 추가
            all_new_dates = set()
            for series in all_data.values():
                all_new_dates.update(series.index)
            
            if all_new_dates:
                new_index = df.index.union(pd.Index(all_new_dates)).sort_values()
                df = df.reindex(new_index)
            
            # 새 데이터 병합
            for series_key, series_data in all_data.items():
                for date, value in series_data.items():
                    df.loc[date, series_key] = value
        
        # 전역 변수에 저장
        HOUSE_PRICE_DATA['raw_data'] = df
        
        # 파생 데이터 계산
        update_derived_data()
        
        # CSV 저장
        save_house_price_data_to_csv()
        
        print(f"✅ 주택 가격 데이터 처리 완료:")
        print(f"   - 성공: {total_successful}개 시리즈")
        print(f"   - 실패: {total_failed}개 시리즈") 
        print(f"   - 날짜 범위: {df.index[0].strftime('%Y-%m')} ~ {df.index[-1].strftime('%Y-%m')}")
        print(f"   - 데이터 포인트: {len(df)} x {len(df.columns)}")
        
        return True
    else:
        print("❌ 로드된 데이터가 없습니다.")
        return False

def update_derived_data():
    """파생 데이터 업데이트 (MoM, YoY, 최신값 등)"""
    global HOUSE_PRICE_DATA
    
    if HOUSE_PRICE_DATA['raw_data'].empty:
        return
    
    df = HOUSE_PRICE_DATA['raw_data']
    
    # MoM, YoY 계산
    HOUSE_PRICE_DATA['mom_data'] = df.pct_change(periods=1) * 100
    HOUSE_PRICE_DATA['yoy_data'] = df.pct_change(periods=12) * 100
    
    # 최신 값
    latest_values = {}
    for col in df.columns:
        if not df[col].isna().all():
            latest_values[col] = df[col].dropna().iloc[-1]
    
    HOUSE_PRICE_DATA['latest_values'] = latest_values
    
    # 로드 정보 업데이트
    HOUSE_PRICE_DATA['load_info'] = {
        'loaded': True,
        'last_update': datetime.datetime.now(),
        'total_series': len(df.columns),
        'date_range': f"{df.index[0].strftime('%Y-%m')} ~ {df.index[-1].strftime('%Y-%m')}",
        'data_categories': ['case_shiller', 'fhfa', 'zillow'],
        'start_date': df.index[0].strftime('%Y-%m-%d'),
        'data_points': len(df),
        'load_time': datetime.datetime.now(),
        'series_count': len(df.columns)
    }

# %%
# === 데이터 조회 함수들 ===

def show_available_series():
    """사용 가능한 모든 주택 가격 시리즈 표시"""
    print("🏠 사용 가능한 주택 가격 시리즈:")
    print("=" * 80)
    
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_house_price_data_enhanced()를 먼저 실행하세요.")
        return
    
    # 실제 로드된 데이터 기준으로 표시
    loaded_series = list(HOUSE_PRICE_DATA['raw_data'].columns)
    
    for category_name, series_dict in HOUSE_PRICE_DATA_CATEGORIES.items():
        available_in_category = [key for key in series_dict.keys() if key in loaded_series]
        
        if available_in_category:
            print(f"\n📊 {category_name.upper()} 시리즈 ({len(available_in_category)}개):")
            print("-" * 60)
            
            for key in sorted(available_in_category):
                korean_name = HOUSE_PRICE_KOREAN_NAMES.get(key, key)
                series_id = series_dict[key]
                print(f"  {key:25} │ {korean_name:30} │ {series_id}")
    
    print(f"\n📈 총 {len(loaded_series)}개 시리즈 로드됨")

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        return []
    return list(HOUSE_PRICE_DATA['raw_data'].columns)

def show_available_components():
    """카테고리별 주택가격 지표 표시"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_house_price_data_enhanced()를 먼저 실행하세요.")
        return
    
    loaded_series = list(HOUSE_PRICE_DATA['raw_data'].columns)
    
    print("🏠 카테고리별 주택가격 지표")
    print("=" * 70)
    
    categories = {
        'national_indices': {
            'title': '📊 전국 지수',
            'series': ['cs_national_sa', 'cs_national_nsa', 'fhfa_national_sa', 'fhfa_national_nsa', 'zillow_us']
        },
        'composite_indices': {
            'title': '📊 종합 지수',
            'series': ['cs_10city_sa', 'cs_10city_nsa', 'cs_20city_sa', 'cs_20city_nsa']
        },
        'major_metros': {
            'title': '🏙️ 주요 도시',
            'series': ['cs_new_york_sa', 'cs_los_angeles_sa', 'cs_chicago_sa', 'cs_san_francisco_sa', 'cs_miami_sa', 'cs_boston_sa']
        },
        'regional_indices': {
            'title': '🗺️ 지역별 지수 (FHFA)',
            'series': ['fhfa_new_england_sa', 'fhfa_middle_atlantic_sa', 'fhfa_south_atlantic_sa', 'fhfa_pacific_sa']
        },
        'state_indices': {
            'title': '🏘️ 주별 지수 (Zillow)',
            'series': ['zillow_california', 'zillow_florida', 'zillow_texas', 'zillow_new_york', 'zillow_washington']
        }
    }
    
    for category, info in categories.items():
        available_series = [s for s in info['series'] if s in loaded_series]
        if available_series:
            print(f"\n{info['title']} ({len(available_series)}개)")
            print("-" * 50)
            for series_key in available_series:
                korean_name = HOUSE_PRICE_KOREAN_NAMES.get(series_key, series_key)
                latest_value = HOUSE_PRICE_DATA['latest_values'].get(series_key, 'N/A')
                if isinstance(latest_value, (int, float)):
                    latest_str = f"{latest_value:.1f}"
                else:
                    latest_str = str(latest_value)
                print(f"  {series_key:20} │ {korean_name:25} │ {latest_str:>8}")

def get_house_price_data(series_list, start_date=None, end_date=None):
    """주택 가격 데이터 조회"""
    global HOUSE_PRICE_DATA
    
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("❌ 데이터가 로드되지 않았습니다. load_all_house_price_data_enhanced()를 먼저 실행하세요.")
        return None
    
    if isinstance(series_list, str):
        series_list = [series_list]
    
    # 유효한 시리즈만 필터링
    valid_series = [s for s in series_list if s in HOUSE_PRICE_DATA['raw_data'].columns]
    
    if not valid_series:
        print("❌ 조회할 유효한 시리즈가 없습니다.")
        return None
    
    # 데이터 추출
    df = HOUSE_PRICE_DATA['raw_data'][valid_series].copy()
    
    # 날짜 범위 적용
    if start_date:
        df = df[df.index >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df.index <= pd.to_datetime(end_date)]
    
    return df

def calculate_mom_changes(series_list):
    """전월비 변화율 계산"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("❌ 데이터가 로드되지 않았습니다.")
        return None
    
    if isinstance(series_list, str):
        series_list = [series_list]
    
    valid_series = [s for s in series_list if s in HOUSE_PRICE_DATA['mom_data'].columns]
    
    if not valid_series:
        return None
    
    return HOUSE_PRICE_DATA['mom_data'][valid_series]

def calculate_yoy_changes(series_list):
    """전년비 변화율 계산"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("❌ 데이터가 로드되지 않았습니다.")
        return None
    
    if isinstance(series_list, str):
        series_list = [series_list]
    
    valid_series = [s for s in series_list if s in HOUSE_PRICE_DATA['yoy_data'].columns]
    
    if not valid_series:
        return None
    
    return HOUSE_PRICE_DATA['yoy_data'][valid_series]

# %%
# === 시각화 함수들 ===

def plot_house_price_series(series_list, chart_type='multi_line', data_type='level', 
                           periods=36, labels=None, left_ytitle=None, right_ytitle=None, target_date=None):
    """주택 가격 시리즈 시각화 (KPDS 포맷 사용)"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_house_price_data_enhanced()를 먼저 실행하세요.")
        return None
    
    if isinstance(series_list, str):
        series_list = [series_list]
    
    # 유효한 시리즈만 필터링
    available_series = list_available_series()
    valid_series = [s for s in series_list if s in available_series]
    
    if not valid_series:
        print("❌ 유효한 시리즈가 없습니다.")
        print(f"요청된 시리즈: {series_list}")
        print(f"사용 가능한 시리즈: {available_series[:5]}..." if len(available_series) > 5 else f"사용 가능한 시리즈: {available_series}")
        return None
    
    # 데이터 타입별 데이터 준비
    if data_type == 'level':
        df = HOUSE_PRICE_DATA['raw_data'][valid_series].copy()
        default_ytitle = "지수"
        title_prefix = "주택 가격 지수"
    elif data_type == 'mom':
        df = HOUSE_PRICE_DATA['mom_data'][valid_series].copy()
        default_ytitle = "%"
        title_prefix = "주택 가격 전월비"
    elif data_type == 'yoy':
        df = HOUSE_PRICE_DATA['yoy_data'][valid_series].copy()
        default_ytitle = "%"
        title_prefix = "주택 가격 전년비"
    else:
        print("❌ data_type은 'level', 'mom', 'yoy' 중 하나여야 합니다.")
        return None
    
    # 날짜 범위 적용
    if target_date:
        df = df[df.index <= pd.to_datetime(target_date)]
    
    # 기간 제한
    df = df.tail(periods).dropna(how='all')
    
    if df.empty:
        print("❌ 표시할 데이터가 없습니다.")
        return None
    
    # Y축 제목 설정
    if left_ytitle is None:
        left_ytitle = default_ytitle
    if right_ytitle is None:
        right_ytitle = default_ytitle
    
    # 라벨 준비
    if labels is None:
        labels = {col: HOUSE_PRICE_KOREAN_NAMES.get(col, col) for col in valid_series}
    
    # 제목 생성 및 출력 (CLAUDE.md 규칙)
    latest_date = df.index[-1].strftime('%b-%y') if not df.empty else 'N/A'
    main_title = f"{title_prefix} (기준: {latest_date})"
    print(main_title)
    
    # 차트 타입별 시각화
    if chart_type == 'multi_line':
        # 다중 라인 차트
        display_df = df.copy()
        display_df.columns = [labels.get(col, col) for col in df.columns]
        return df_multi_line_chart(display_df, ytitle=left_ytitle)
        
    elif chart_type == 'single_line':
        # 단일 라인 차트
        if len(valid_series) > 1:
            print("⚠️ single_line 차트는 하나의 시리즈만 지원합니다. 첫 번째 시리즈를 사용합니다.")
        col = valid_series[0]
        label = labels.get(col, col)
        return df_line_chart(df, column=col, ytitle=left_ytitle, label=label)
        
    elif chart_type == 'dual_axis':
        # 이중축 차트
        if len(valid_series) < 2:
            print("❌ dual_axis 차트는 최소 2개 시리즈가 필요합니다.")
            return None
        
        # 시리즈를 왼쪽/오른쪽으로 분할
        mid_point = len(valid_series) // 2
        left_cols = valid_series[:mid_point] if mid_point > 0 else [valid_series[0]]
        right_cols = valid_series[mid_point:] if mid_point < len(valid_series) else [valid_series[-1]]
        
        left_labels = [labels.get(col, col) for col in left_cols]
        right_labels = [labels.get(col, col) for col in right_cols]
        
        return df_dual_axis_chart(
            df=df,
            left_cols=left_cols,
            right_cols=right_cols,
            left_labels=left_labels,
            right_labels=right_labels,
            left_title=left_ytitle,
            right_title=right_ytitle
        )
        
    elif chart_type == 'horizontal_bar':
        # 가로 바 차트 (최신 값 기준)
        latest_values = df.iloc[-1].to_dict()
        data_dict = {labels.get(col, col): val for col, val in latest_values.items() if pd.notna(val)}
        
        if not data_dict:
            print("❌ 가로 바 차트에 표시할 데이터가 없습니다.")
            return None
        
        # 기준일 정보 출력
        print(f"기준일: {latest_date}")
        
        unit = "" if data_type == 'level' else "%"
        return create_horizontal_bar_chart(data_dict, unit=unit)
    
    else:
        print("❌ chart_type은 'multi_line', 'single_line', 'dual_axis', 'horizontal_bar' 중 하나여야 합니다.")
        return None

def create_multiline_chart(df, series_list, title, ylabel):
    """멀티라인 차트 생성 (KPDS 포맷 사용)"""
    if title:
        print(title)
    
    # 열 이름을 한국어로 변환
    display_df = df[series_list].copy()
    display_df.columns = [HOUSE_PRICE_KOREAN_NAMES.get(col, col) for col in series_list]
    
    return df_multi_line_chart(display_df, ytitle=ylabel)

def create_dual_axis_chart(df, series_list, title, ylabel):
    """듀얼 축 차트 생성 (KPDS 포맷 사용)"""
    if len(series_list) < 2:
        print("❌ dual_axis 차트는 최소 2개 시리즈가 필요합니다.")
        return None
    
    if title:
        print(title)
    
    # 왼쪽/오른쪽 축 데이터 준비
    left_cols = [series_list[0]]
    right_cols = [series_list[1]]
    
    # 한국어 라벨 준비
    left_labels = [HOUSE_PRICE_KOREAN_NAMES.get(series_list[0], series_list[0])]
    right_labels = [HOUSE_PRICE_KOREAN_NAMES.get(series_list[1], series_list[1])]
    
    return df_dual_axis_chart(
        df, 
        left_cols=left_cols, 
        right_cols=right_cols,
        left_labels=left_labels,
        right_labels=right_labels,
        left_title=ylabel,
        right_title=ylabel
    )

def create_horizontal_bar_chart(df, series_list, title, ylabel):
    """수평 바 차트 생성 (KPDS 포맷 사용)"""
    if title:
        print(title)
    
    # 최신 값 추출
    latest_values = df[series_list].iloc[-1]
    latest_date = df.index[-1]
    
    # 한국어 이름으로 변환하여 딕셔너리 생성
    data_dict = {}
    for series in series_list:
        korean_name = HOUSE_PRICE_KOREAN_NAMES.get(series, series)
        data_dict[korean_name] = latest_values[series]
    
    # 기준일 정보 출력
    print(f"기준일: {latest_date.strftime('%Y년 %m월')}")
    
    # KPDS 스타일 가로 바 차트 생성
    from kpds_fig_format_enhanced import create_horizontal_bar_chart
    return create_horizontal_bar_chart(data_dict, unit=ylabel if ylabel != "지수" else "")

# %%
# === 편의 함수들 ===

# %%
# === 편의 함수들 ===

def quick_house_price_chart(series_list, chart_type='multi_line', periods=24):
    """빠른 주택 가격 차트 (레벨)"""
    return plot_house_price_series(series_list, chart_type, 'level', periods)

def quick_mom_chart(series_list, chart_type='multi_line', periods=24):
    """빠른 전월비 차트"""
    return plot_house_price_series(series_list, chart_type, 'mom', periods)

def quick_yoy_chart(series_list, chart_type='multi_line', periods=24):
    """빠른 전년비 차트"""
    return plot_house_price_series(series_list, chart_type, 'yoy', periods)

def quick_single_line(series_key, data_type='level', periods=36):
    """빠른 단일 라인 차트"""
    return plot_house_price_series([series_key], 'single_line', data_type, periods)

def quick_dual_axis(left_series, right_series, data_type='level', periods=36):
    """빠른 이중축 차트"""
    series_list = [left_series, right_series] if isinstance(left_series, str) else left_series + [right_series]
    return plot_house_price_series(series_list, 'dual_axis', data_type, periods)

def quick_horizontal_bar(series_list, data_type='yoy'):
    """빠른 가로 바 차트 (최신값 기준)"""
    return plot_house_price_series(series_list, 'horizontal_bar', data_type, periods=1)

# %%
# === 특화된 비교 분석 함수들 ===

def compare_house_price_indices(index_type='national', chart_type='multi_line', data_type='yoy', periods=36):
    """주택 가격 지수 비교"""
    comparison_sets = {
        'national': {
            'series': ['cs_national_sa', 'fhfa_national_sa', 'zillow_us'],
            'title': '전국 지수 비교'
        },
        'composite': {
            'series': ['cs_national_sa', 'cs_10city_sa', 'cs_20city_sa'],
            'title': 'Case-Shiller 종합 지수 비교'
        },
        'west_coast': {
            'series': ['cs_los_angeles_sa', 'cs_san_francisco_sa', 'cs_seattle_sa', 'cs_portland_sa'],
            'title': '서부 주요 도시 비교'
        },
        'east_coast': {
            'series': ['cs_new_york_sa', 'cs_boston_sa', 'cs_washington_sa', 'cs_miami_sa'],
            'title': '동부 주요 도시 비교'
        },
        'sunbelt': {
            'series': ['cs_miami_sa', 'cs_atlanta_sa', 'cs_phoenix_sa', 'cs_dallas_sa', 'cs_tampa_sa'],
            'title': '선벨트 도시 비교'
        },
        'tier_la': {
            'series': ['cs_la_high_sa', 'cs_la_mid_sa', 'cs_la_low_sa'],
            'title': 'LA 가격대별 비교'
        },
        'tier_ny': {
            'series': ['cs_ny_high_sa', 'cs_ny_mid_sa', 'cs_ny_low_sa'],
            'title': '뉴욕 가격대별 비교'
        },
        'tier_sf': {
            'series': ['cs_sf_high_sa', 'cs_sf_mid_sa', 'cs_sf_low_sa'],
            'title': '샌프란시스코 가격대별 비교'
        },
        'zillow_states': {
            'series': ['zillow_california', 'zillow_florida', 'zillow_texas', 'zillow_new_york', 'zillow_washington'],
            'title': 'Zillow 주요 주 비교'
        },
        'existing_vs_new_sales': {
            'series': ['ehs_sales_national_sa', 'nrs_sales_national_sa'],
            'title': '기존주택 vs 신규주택 판매량 비교'
        },
        'home_prices_comparison': {
            'series': ['ehs_median_price_national', 'nrs_median_price_monthly'],
            'title': '기존주택 vs 신규주택 가격 비교'
        },
        'regional_existing_sales': {
            'series': ['ehs_sales_northeast_sa', 'ehs_sales_midwest_sa', 'ehs_sales_south_sa', 'ehs_sales_west_sa'],
            'title': '지역별 기존주택 판매량'
        },
        'regional_new_sales': {
            'series': ['nrs_sales_northeast_sa', 'nrs_sales_midwest_sa', 'nrs_sales_south_sa', 'nrs_sales_west_sa'],
            'title': '지역별 신규주택 판매량'
        },
        'inventory_comparison': {
            'series': ['ehs_inventory_national', 'nrs_inventory_national_sa'],
            'title': '기존주택 vs 신규주택 재고 비교'
        },
        'months_supply_comparison': {
            'series': ['ehs_months_supply', 'nrs_months_supply_sa'],
            'title': '재고 소진율 비교 (기존주택 vs 신규주택)'
        }
    }
    
    if index_type not in comparison_sets:
        available_types = list(comparison_sets.keys())
        print(f"❌ index_type은 다음 중 하나여야 합니다: {', '.join(available_types)}")
        return None
    
    config = comparison_sets[index_type]
    print(f"📊 {config['title']}")
    
    return plot_house_price_series(
        series_list=config['series'], 
        chart_type=chart_type, 
        data_type=data_type, 
        periods=periods
    )

def create_house_price_overview():
    """주택 가격 종합 개요 차트"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_house_price_data_enhanced()를 먼저 실행하세요.")
        return None
    
    print("🏠 미국 주택 가격 지수 종합 개요")
    
    # 전국 지수 비교 (레벨)
    compare_house_price_indices('national', 'multi_line', 'level', 60)
    
    # 전국 지수 전년비 비교
    compare_house_price_indices('national', 'multi_line', 'yoy', 36)
    
    return True

def create_regional_comparison():
    """지역별 주택 가격 비교"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return None
    
    print("🗺️ 지역별 주택 가격 비교 분석")
    
    # 서부 vs 동부 주요 도시
    compare_house_price_indices('west_coast', 'multi_line', 'yoy', 36)
    compare_house_price_indices('east_coast', 'multi_line', 'yoy', 36)
    
    # 선벨트 지역
    compare_house_price_indices('sunbelt', 'multi_line', 'yoy', 36)
    
    return True

def create_tier_analysis(city='la'):
    """도시별 가격대 분석"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return None
    
    city_map = {
        'la': 'tier_la',
        'ny': 'tier_ny', 
        'sf': 'tier_sf'
    }
    
    if city not in city_map:
        print(f"❌ city는 다음 중 하나여야 합니다: {', '.join(city_map.keys())}")
        return None
    
    return compare_house_price_indices(city_map[city], 'multi_line', 'yoy', 36)

def create_house_price_momentum_chart(periods=24):
    """주택 가격 모멘텀 차트 (전월비 vs 전년비)"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return None
    
    # 전국 지수 사용
    series_key = 'cs_national_sa'
    if series_key not in HOUSE_PRICE_DATA['raw_data'].columns:
        print(f"❌ {series_key} 시리즈를 찾을 수 없습니다.")
        return None
    
    # MoM과 YoY 데이터 준비
    mom_data = HOUSE_PRICE_DATA['mom_data'][series_key].tail(periods)
    yoy_data = HOUSE_PRICE_DATA['yoy_data'][series_key].tail(periods)
    
    # DataFrame 생성
    df = pd.DataFrame({
        '전월비': mom_data,
        '전년비': yoy_data
    }).dropna()
    
    if df.empty:
        print("❌ 표시할 데이터가 없습니다.")
        return None
    
    latest_date = df.index[-1].strftime('%b-%y')
    print(f"주택 가격 모멘텀 분석 (기준: {latest_date})")
    
    return df_dual_axis_chart(
        df=df,
        left_cols=['전월비'],
        right_cols=['전년비'],
        left_title='%',
        right_title='%'
    )

# %%
# === 릴리즈 일정 관련 함수들 ===

def get_house_price_release_schedule():
    """주택 가격 관련 데이터의 릴리즈 일정 정보"""
    release_schedule = {
        'case_shiller': {
            'name': 'S&P CoreLogic Case-Shiller Home Price Index',
            'agency': 'S&P Dow Jones Indices',
            'frequency': 'Monthly',
            'release_lag': '약 2개월 (2-month lag)',
            'release_day': '매월 마지막 화요일',
            'data_reference': '2개월 전 데이터',
            'example': '3월 말 발표 → 1월 데이터',
            'url': 'https://www.spglobal.com/spdji/en/index-family/indicators/sp-corelogic-case-shiller/',
        },
        'fhfa': {
            'name': 'FHFA House Price Index',
            'agency': 'Federal Housing Finance Agency',
            'frequency': 'Monthly',
            'release_lag': '약 2개월 (2-month lag)',
            'release_day': '매월 마지막 화요일 또는 수요일',
            'data_reference': '2개월 전 데이터',
            'example': '3월 말 발표 → 1월 데이터',
            'url': 'https://www.fhfa.gov/DataTools/Downloads/Pages/House-Price-Index.aspx',
        },
        'existing_home_sales': {
            'name': 'Existing Home Sales',
            'agency': 'National Association of Realtors (NAR)',
            'frequency': 'Monthly',
            'release_lag': '약 3주 (3-week lag)',
            'release_day': '매월 셋째주 금요일',
            'data_reference': '1개월 전 데이터',
            'example': '2월 20일경 발표 → 1월 데이터',
            'url': 'https://www.nar.realtor/newsroom/existing-home-sales',
        },
        'new_residential_sales': {
            'name': 'New Residential Sales',
            'agency': 'U.S. Census Bureau & HUD',
            'frequency': 'Monthly',
            'release_lag': '약 4주 (4-week lag)',
            'release_day': '매월 넷째주',
            'data_reference': '1개월 전 데이터',
            'example': '2월 25일경 발표 → 1월 데이터',
            'url': 'https://www.census.gov/construction/nrs/',
        },
        'zillow': {
            'name': 'Zillow Home Value Index (ZHVI)',
            'agency': 'Zillow',
            'frequency': 'Monthly',
            'release_lag': '약 2주 (2-week lag)',
            'release_day': '매월 중순',
            'data_reference': '1개월 전 데이터',
            'example': '2월 15일경 발표 → 1월 데이터',
            'url': 'https://www.zillow.com/research/data/',
        }
    }
    
    return release_schedule

def print_release_schedule():
    """주택 가격 데이터 릴리즈 일정 출력"""
    schedule = get_house_price_release_schedule()
    
    print("📅 주택 가격 관련 데이터 릴리즈 일정")
    print("=" * 60)
    
    for category, info in schedule.items():
        print(f"\n🏠 {info['name']}")
        print(f"   발행기관: {info['agency']}")
        print(f"   발표주기: {info['frequency']}")
        print(f"   발표시점: {info['release_day']}")
        print(f"   데이터지연: {info['release_lag']}")
        print(f"   참조기간: {info['data_reference']}")
        print(f"   예시: {info['example']}")
        print(f"   URL: {info['url']}")
    
    print("\n💡 스마트 업데이트 활용 팁:")
    print("   • NAR(기존주택)과 Census(신규주택)은 매월 중순~말에 발표")
    print("   • Case-Shiller와 FHFA는 2개월 지연으로 매월 말 발표")
    print("   • Zillow는 가장 빠른 업데이트 (2주 지연)")
    print("   • 각 기관별로 smart_update=True로 개별 업데이트 가능")

def check_expected_release_dates(target_month=None):
    """특정 월의 예상 릴리즈 날짜 확인"""
    import calendar
    from datetime import datetime, timedelta
    
    if target_month is None:
        target_month = datetime.now().strftime('%Y-%m')
    
    try:
        year, month = map(int, target_month.split('-'))
    except:
        print("❌ target_month 형식: 'YYYY-MM' (예: '2025-03')")
        return
    
    print(f"📅 {year}년 {month}월 주택 데이터 예상 릴리즈 일정")
    print("=" * 50)
    
    # 해당 월의 날짜 계산
    _, last_day = calendar.monthrange(year, month)
    
    # NAR (Existing Home Sales) - 셋째주 금요일
    third_friday = None
    day = 1
    friday_count = 0
    while day <= last_day:
        if calendar.weekday(year, month, day) == 4:  # 금요일
            friday_count += 1
            if friday_count == 3:
                third_friday = day
                break
        day += 1
    
    # Census (New Residential Sales) - 넷째주 (대략 20-25일)
    fourth_week_range = "20-25일"
    
    # Case-Shiller & FHFA - 마지막 화요일
    last_tuesday = None
    day = last_day
    while day >= 1:
        if calendar.weekday(year, month, day) == 1:  # 화요일
            last_tuesday = day
            break
        day -= 1
    
    # Zillow - 중순 (대략 15일)
    zillow_date = 15
    
    print(f"🏠 Zillow ZHVI: {month}월 {zillow_date}일경 (1개월 전 데이터)")
    if third_friday:
        print(f"🏠 NAR 기존주택 판매: {month}월 {third_friday}일 (1개월 전 데이터)")
    print(f"🏠 Census 신규주택 판매: {month}월 {fourth_week_range} (1개월 전 데이터)")
    if last_tuesday:
        print(f"🏠 Case-Shiller 지수: {month}월 {last_tuesday}일 (2개월 전 데이터)")
        print(f"🏠 FHFA 지수: {month}월 {last_tuesday}일 (2개월 전 데이터)")
    
    print(f"\n💡 {year}년 {month}월에 업데이트되는 실제 데이터:")
    print(f"   • Zillow: {year}년 {month-1 if month > 1 else 12}월 데이터")
    print(f"   • NAR/Census: {year}년 {month-1 if month > 1 else 12}월 데이터") 
    print(f"   • Case-Shiller/FHFA: {year}년 {month-2 if month > 2 else 12}월 데이터")

def smart_update_by_schedule(category=None, force_check=False):
    """릴리즈 일정에 따른 스마트 업데이트"""
    from datetime import datetime
    
    current_date = datetime.now()
    current_day = current_date.day
    current_weekday = current_date.weekday()  # 0=월요일
    
    print(f"📅 오늘 날짜: {current_date.strftime('%Y년 %m월 %d일 (%A)')}")
    
    if category is None:
        print("🔍 모든 카테고리에 대해 릴리즈 일정 기반 스마트 체크 수행...")
        categories_to_check = list(HOUSE_PRICE_DATA_CATEGORIES.keys())
    else:
        categories_to_check = [category] if isinstance(category, str) else category
    
    results = {}
    
    for cat in categories_to_check:
        if cat not in HOUSE_PRICE_DATA_CATEGORIES:
            print(f"⚠️ 알 수 없는 카테고리: {cat}")
            continue
        
        should_check = force_check
        reason = ""
        
        # 카테고리별 릴리즈 일정 체크
        if not should_check:
            if cat == 'zillow' and current_day >= 14:
                should_check = True
                reason = "Zillow 중순 릴리즈 예정일"
            elif cat == 'existing_home_sales' and current_day >= 15 and current_weekday == 4:
                should_check = True
                reason = "NAR 셋째주 금요일 릴리즈"
            elif cat == 'new_residential_sales' and current_day >= 20:
                should_check = True
                reason = "Census 넷째주 릴리즈 예정"
            elif cat in ['case_shiller', 'fhfa'] and current_day >= 25 and current_weekday == 1:
                should_check = True
                reason = "Case-Shiller/FHFA 월말 화요일 릴리즈"
        
        if should_check:
            print(f"\n🔄 {cat.upper()} 스마트 업데이트 수행 중... ({reason})")
            result = load_house_price_data_by_category(cat, smart_update=True)
            results[cat] = {'updated': result, 'reason': reason}
        else:
            print(f"⏰ {cat.upper()}: 릴리즈 일정상 업데이트 불필요")
            results[cat] = {'updated': False, 'reason': 'No release expected'}
    
    return results

def create_house_price_heatmap(data_type='yoy', num_series=10):
    """주택 가격 히트맵 (가로 바 차트 형태)"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return None
    
    # 데이터 선택
    if data_type == 'yoy':
        data = HOUSE_PRICE_DATA['yoy_data']
        title_suffix = "전년비"
    elif data_type == 'mom':
        data = HOUSE_PRICE_DATA['mom_data']
        title_suffix = "전월비"
    else:
        print("❌ data_type은 'yoy' 또는 'mom'이어야 합니다.")
        return None
    
    # 최신 데이터 추출
    latest_data = data.iloc[-1].dropna()
    
    # 상위/하위 시리즈 선택
    if len(latest_data) > num_series:
        latest_data = latest_data.nlargest(num_series)
    
    # 한국어 라벨 매핑
    data_dict = {}
    for series_key, value in latest_data.items():
        korean_name = HOUSE_PRICE_KOREAN_NAMES.get(series_key, series_key)
        data_dict[korean_name] = value
    
    latest_date = data.index[-1].strftime('%b-%y')
    print(f"주택 가격 {title_suffix} 순위 (기준: {latest_date})")
    
    unit = "%" if data_type in ['yoy', 'mom'] else ""
    return create_horizontal_bar_chart(data_dict, unit=unit)

def analyze_house_price_trends():
    """주택 가격 트렌드 종합 분석"""
    print("🏠 미국 주택 가격 트렌드 종합 분석")
    print("=" * 50)
    
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("먼저 load_all_house_price_data_enhanced()를 실행하세요.")
        return
    
    # 1. 종합 개요
    print("\n📊 종합 개요")
    create_house_price_overview()
    
    # 2. 지역별 비교
    print("\n🗺️ 지역별 비교")
    create_regional_comparison()
    
    # 3. 모멘텀 분석
    print("\n📈 모멘텀 분석")
    create_house_price_momentum_chart()
    
    # 4. 순위 분석
    print("\n🏆 전년비 순위")
    create_house_price_heatmap('yoy', 15)

# %%
# === 통합 실행 함수들 ===

def run_complete_house_price_analysis(start_date='2000-01-01', force_reload=False, smart_update=True, 
                                     target_category='all'):
    """
    완전한 주택 가격 분석 실행
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드
        smart_update: 스마트 업데이트 
        target_category: 'all', 'case_shiller', 'fhfa', 'zillow'
    """
    
    # 데이터 로드
    if target_category == 'all':
        enabled_categories = ['case_shiller', 'fhfa', 'zillow']
    else:
        enabled_categories = [target_category]
    
    success = load_all_house_price_data_enhanced(
        start_date=start_date, 
        force_reload=force_reload,
        smart_update=smart_update,
        enabled_categories=enabled_categories
    )
    
    if not success:
        print("❌ 데이터 로드 실패")
        return False
    
    print("\n📊 시리즈 정보:")
    show_available_series()
    
    print("\n📈 전국 지수 비교:")
    compare_house_price_indices('national', 'multi_line')
    
    print("\n📈 주요 지역 비교:")
    compare_house_price_indices('west_coast', 'multi_line')
    
    
    return True

def update_specific_category(category_name, smart_update=True):
    """특정 카테고리만 업데이트"""
    print(f"🔄 {category_name.upper()} 카테고리 업데이트 시작")
    
    success = update_data_category(category_name, smart_update=smart_update)
    
    if success:
        update_derived_data()  # 파생 데이터 업데이트
        print(f"✅ {category_name.upper()} 업데이트 완료")
    else:
        print(f"❌ {category_name.upper()} 업데이트 실패")
    
    return success

# %%
# === 편의 함수들 ===

def analyze_house_price_trends():
    """주택 가격 트렌드 종합 분석"""
    print("🏠 미국 주택 가격 트렌드 분석")
    
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("먼저 load_all_house_price_data_enhanced()를 실행하세요.")
        return
    
    # 전국 지수 비교
    print("\n📈 전국 지수 비교 (전년비)")
    compare_house_price_indices('national', 'multi_line')
    
    # 주요 지역 비교
    print("\n📈 서부 주요 도시 비교 (전년비)")
    compare_house_price_indices('west_coast', 'multi_line')
    
    print("\n📈 동부 주요 도시 비교 (전년비)")
    compare_house_price_indices('east_coast', 'multi_line')

def quick_house_price_update():
    """빠른 주택 가격 데이터 업데이트"""
    return load_all_house_price_data_enhanced(smart_update=True)

def compare_sectors(series_list, chart_type='horizontal_bar', data_type='yoy'):
    """섹터/지역 비교 (CES_employ.py의 compare_sectors와 유사)"""
    return plot_house_price_series(series_list, chart_type, data_type)

# %%
# === 시스템 상태 확인 함수 ===

def check_system_status():
    """house_price.py 시스템 상태 확인"""
    print("🏠 House Price Analysis System Status")
    print("=" * 50)
    
    # 1. 데이터 로드 상태
    print(f"📊 데이터 로드됨: {HOUSE_PRICE_DATA['load_info']['loaded']}")
    if HOUSE_PRICE_DATA['load_info']['loaded']:
        info = HOUSE_PRICE_DATA['load_info']
        print(f"   └ 시리즈 수: {info['total_series']}개")
        print(f"   └ 데이터 기간: {info.get('date_range', 'N/A')}")
        print(f"   └ 마지막 업데이트: {info.get('last_update', 'N/A')}")
    
    # 2. 주요 함수 정의 상태
    functions = [
        ('show_available_series', '시리즈 목록 조회'),
        ('plot_house_price_series', '범용 시각화'),
        ('compare_house_price_indices', '지수 비교'),
        ('analyze_house_price_trends', '종합 분석'),
        ('quick_yoy_chart', '빠른 전년비 차트'),
        ('create_house_price_heatmap', '순위 차트')
    ]
    
    print("\n🔧 함수 정의 상태:")
    for func_name, description in functions:
        if func_name in globals():
            print(f"   ✅ {func_name}: {description}")
        else:
            print(f"   ❌ {func_name}: {description}")
    
    # 3. KPDS 라이브러리 연동 상태
    try:
        from kpds_fig_format_enhanced import df_multi_line_chart
        print("\n🎨 KPDS 시각화 라이브러리: ✅ 연동됨")
    except ImportError:
        print("\n🎨 KPDS 시각화 라이브러리: ❌ 연동 실패")
    
    # 4. 권장 다음 단계
    print("\n📋 권장 다음 단계:")
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("   1. load_all_house_price_data_enhanced() 실행")
        print("   2. show_available_series() 로 데이터 확인")
        print("   3. analyze_house_price_trends() 로 종합 분석")
    else:
        print("   1. show_available_components() 로 카테고리별 시리즈 확인")
        print("   2. compare_house_price_indices('national') 로 전국 지수 비교")
        print("   3. analyze_house_price_trends() 로 종합 트렌드 분석")
        print("   4. create_house_price_heatmap('yoy', 15) 로 순위 확인")

def show_house_price_summary():
    """주택 가격 데이터 요약 정보 출력"""
    if not HOUSE_PRICE_DATA['load_info']['loaded']:
        print("먼저 데이터를 로드하세요.")
        return
    
    info = HOUSE_PRICE_DATA['load_info']
    print("🏠 주택 가격 데이터 현황")
    print(f"   로드된 시리즈: {info['total_series']}개")
    print(f"   데이터 기간: {info['date_range']}")
    print(f"   마지막 업데이트: {info['last_update'].strftime('%Y-%m-%d %H:%M')}")
    print(f"   데이터 포인트: {info['data_points']}개")

# %%
# === 사용 예제 ===

# 데이터 로드 및 기본 분석
# run_complete_house_price_analysis()

# %%
# === 함수 사용 예제 ===
"""
사용 예제:

1. 데이터 로드:
   load_all_house_price_data_enhanced()

2. 사용 가능한 시리즈 확인:
   show_available_series()
   show_available_components()

3. 기본 시각화:
   # 전국 지수 비교 (멀티라인)
   plot_house_price_series(['cs_national_sa', 'fhfa_national_sa', 'zillow_us'], 'multi_line', 'yoy')
   
   # 이중축 차트
   plot_house_price_series(['cs_national_sa', 'cs_20city_sa'], 'dual_axis', 'level')
   
   # 가로 바 차트 (최신값 기준)
   plot_house_price_series(['cs_los_angeles_sa', 'cs_san_francisco_sa', 'cs_seattle_sa'], 'horizontal_bar', 'yoy')

4. 편의 함수들:
   # 빠른 차트
   quick_yoy_chart(['cs_national_sa', 'fhfa_national_sa'])
   quick_horizontal_bar(['cs_new_york_sa', 'cs_los_angeles_sa', 'cs_chicago_sa'])
   
   # 지역별 비교
   compare_house_price_indices('national')  # 전국 지수 비교
   compare_house_price_indices('west_coast')  # 서부 도시 비교
   compare_house_price_indices('tier_la')  # LA 가격대별

5. 종합 분석:
   analyze_house_price_trends()  # 전체 트렌드 분석
   create_house_price_overview()  # 종합 개요
   create_regional_comparison()  # 지역별 비교
   
6. 특화 분석:
   create_house_price_momentum_chart()  # 모멘텀 분석
   create_house_price_heatmap('yoy', 15)  # 순위 차트
   create_tier_analysis('la')  # 가격대별 분석
"""

# 시스템 상태 확인 실행
check_system_status()

# %%
# 시스템 준비 완료 메시지
print("")
print("🎉 House Price Analysis System 준비 완료!")
print("CES_employ.py와 동일한 방식으로 사용할 수 있습니다.")
print("")
print("주요 사용법:")
print("• plot_house_price_series(['cs_national_sa'], 'multi_line', 'yoy')")
print("• compare_house_price_indices('national')")
print("• analyze_house_price_trends()")
print("• show_available_series()")
print("")

# %%
load_all_house_price_data_enhanced()
# %%
plot_house_price_series(['cs_national_nsa', 'fhfa_national_nsa', 'zillow_us'], 'multi_line', 'yoy')
# %%
