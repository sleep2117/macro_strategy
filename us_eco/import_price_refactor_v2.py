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
# === FRED 데이터 소스 사용 ===
# BLS 대신 FRED 데이터를 사용합니다

# %%
# === Import Prices 시리즈 정의 (FRED 기반) ===

# ===== A. Import Price Index by End Use 시리즈 =====
IMPORT_SERIES_END_USE = {
    # 주요 전체 지표
    'all_commodities': 'IR',
    'all_excluding_petroleum': 'IREXPET',
    'all_excluding_fuels': 'IREXFUELS',
    'all_excluding_food_fuels': 'IREXFDFLS',
    
    # 주요 카테고리
    'foods_feeds_beverages': 'IR0',
    'industrial_supplies': 'IR1',
    'capital_goods': 'IR2',
    'automotive_vehicles': 'IR3',
    'consumer_goods': 'IR4',
    
    # 연료 및 에너지
    'fuels_lubricants': 'IR10',
    'crude_oil': 'IR10000',
    
    # 산업용 원자재 세부
    'industrial_supplies_excl_fuel': 'IR1EXFUEL',
    'industrial_supplies_excl_petroleum': 'IR1EXPET',
    'industrial_supplies_durable': 'IR1DUR',
    'unfinished_metals_durable': 'IR14',
    'iron_steel_products': 'IR150',
    'iron_steel_semifinished': 'IR141',
    'bauxite_aluminum': 'IR14200',
    'copper': 'IR14220',
    'zinc': 'IR14260',
    'other_precious_metals': 'IR14280',
    'building_materials_excl_metals': 'IR13',
    'stone_sand_cement': 'IR13020',
    'chemicals_fertilizers': 'IR12510',
    
    # 자본재 세부
    'semiconductors': 'IR21320',
    
    # 소비재 세부  
    'consumer_durables_excl_auto': 'IR41',
    'apparel_footwear_household': 'IR400',
    'pharmaceutical_preparations': 'IR40100',
    'furniture_household_goods': 'IR41000',
    'household_appliances': 'IR41030',
    'gem_diamonds': 'IR42100',
    
    # 식품 세부
    'green_coffee_cocoa_sugar': 'IR000',
    'green_coffee': 'IR00000',
    'fish_shellfish': 'IR01000',
    
    # 특수 항목
    'nonmonetary_gold': 'IR14270'
}

# ===== B. Import Price Index by Locality of Origin 시리즈 =====
IMPORT_SERIES_ORIGIN = {
    # 주요 국가/지역 - 전체 산업
    'china_total': 'CHNTOT',
    'canada_total': 'CANTOT',
    'japan_total': 'JPNTOT',
    'european_union_total': 'EECTOT',
    'germany_total': 'GERTOT',
    'united_kingdom_total': 'UKTOT',
    'mexico_total': 'MEXTOT',
    'latin_america_total': 'LATTOT',
    'france_total': 'FRNTOT',
    'industrialized_countries_total': 'INDUSTOT',
    'asian_newly_industrialized_total': 'OASTOT',
    'asean_total': 'ASEANTOT',
    'asia_near_east_total': 'ASNRETOT',
    'pacific_rim_total': 'PRIMTOT',
    
    # 제조업 vs 비제조업 구분 (존재하는 시리즈만)
    # 참고: 중국, 일본은 전체 제조업/비제조업 시리즈가 없고 세부 부문만 제공
    'canada_manufacturing': 'CANMANU',
    'canada_nonmanufacturing': 'CANNONM',
    'european_union_manufacturing': 'EECMANU',
    'european_union_nonmanufacturing': 'EECNONM',
    'mexico_manufacturing': 'MEXMANU',
    'mexico_nonmanufacturing': 'MEXNONM',
    'latin_america_manufacturing': 'LATMANU',
    'latin_america_nonmanufacturing': 'LATNONM',
    'industrialized_manufacturing': 'INDUSMANU',
    'industrialized_nonmanufacturing': 'INDUSNONM',
    
    # 중국 - 세부 제조업
    'china_manufacturing_part1': 'COCHNZ31',
    'china_manufacturing_part2': 'COCHNZ32',
    'china_manufacturing_part3': 'COCHNZ33',
    'china_apparel_manufacturing': 'COCHNZ315',
    'china_leather_manufacturing': 'COCHNZ316',
    'china_footwear_manufacturing': 'COCHNZ3162',
    'china_chemical_manufacturing': 'COCHNZ325',
    'china_plastics_rubber_manufacturing': 'COCHNZ326',
    'china_plastics_manufacturing': 'COCHNZ3261',
    'china_fabricated_metal_manufacturing': 'COCHNZ332',
    'china_cutlery_handtool_manufacturing': 'COCHNZ3322',
    'china_machinery_manufacturing': 'COCHNZ333',
    'china_computer_electronic_manufacturing': 'COCHNZ334',
    'china_communications_equipment': 'COCHNZ3342',
    'china_audio_video_equipment': 'COCHNZ3343',
    'china_measuring_instruments': 'COCHNZ3345',
    'china_electrical_equipment_manufacturing': 'COCHNZ335',
    'china_electric_lighting_manufacturing': 'COCHNZ3351',
    'china_household_appliance_manufacturing': 'COCHNZ3352',
    'china_furniture_manufacturing': 'COCHNZ337',
    'china_household_furniture_manufacturing': 'COCHNZ3371',
    'china_miscellaneous_manufacturing': 'COCHNZ339',
    'china_other_miscellaneous_manufacturing': 'COCHNZ3399',
    
    # 유럽연합 - 세부 제조업
    'eu_manufacturing_part1': 'COEECZ31',
    'eu_manufacturing_part2': 'COEECZ32',
    'eu_manufacturing_part3': 'COEECZ33',
    'eu_food_manufacturing': 'COEECZ311',
    'eu_beverage_tobacco_manufacturing': 'COEECZ312',
    'eu_chemical_manufacturing': 'COEECZ325',
    'eu_pharmaceutical_manufacturing': 'COEECZ3254',
    'eu_primary_metal_manufacturing': 'COEECZ331',
    'eu_fabricated_metal_manufacturing': 'COEECZ332',
    'eu_other_fabricated_metal_manufacturing': 'COEECZ3329',
    'eu_machinery_manufacturing': 'COEECZ333',
    'eu_agriculture_machinery_manufacturing': 'COEECZ3331',
    'eu_industrial_machinery_manufacturing': 'COEECZ3332',
    'eu_other_machinery_manufacturing': 'COEECZ3339',
    'eu_computer_electronic_manufacturing': 'COEECZ334',
    'eu_electrical_equipment_manufacturing': 'COEECZ335',
    'eu_transportation_equipment_manufacturing': 'COEECZ336',
    'eu_miscellaneous_manufacturing': 'COEECZ339',
    
    # 캐나다 - 세부 제조업
    'canada_mining_oil_gas': 'COCANZ21',
    'canada_manufacturing_part1': 'COCANZ31',
    'canada_manufacturing_part2': 'COCANZ32',
    'canada_manufacturing_part3': 'COCANZ33',
    'canada_food_manufacturing': 'COCANZ311',
    'canada_paper_manufacturing': 'COCANZ322',
    'canada_chemical_manufacturing': 'COCANZ325',
    'canada_machinery_manufacturing': 'COCANZ333',
    
    # 독일 - 세부 제조업
    'germany_manufacturing_part3': 'COGERZ33',
    'germany_machinery_manufacturing': 'COGERZ333',
    'germany_computer_electronic_manufacturing': 'COGERZ334',
    
    # 일본 - 세부 제조업
    'japan_manufacturing_part2': 'COJPNZ32',
    'japan_manufacturing_part3': 'COJPNZ33',
    'japan_machinery_manufacturing': 'COJPNZ333',
    'japan_computer_electronic_manufacturing': 'COJPNZ334',
    
    # 선진국 - 세부 제조업
    'industrialized_mining_oil_gas': 'COINDUSZ21',
    'industrialized_manufacturing_part1': 'COINDUSZ31',
    'industrialized_manufacturing_part2': 'COINDUSZ32',
    'industrialized_food_manufacturing': 'COINDUSZ311',
    'industrialized_animal_processing': 'COINDUSZ3116',
    'industrialized_beverage_tobacco': 'COINDUSZ312',
    'industrialized_paper_manufacturing': 'COINDUSZ322',
    'industrialized_chemical_manufacturing': 'COINDUSZ325',
    'industrialized_basic_chemical_manufacturing': 'COINDUSZ3251',
    'industrialized_pharmaceutical_manufacturing': 'COINDUSZ3254',
    'industrialized_plastics_rubber_manufacturing': 'COINDUSZ326',
    'industrialized_semiconductor_manufacturing': 'COINDUSZ3344',
    'industrialized_motor_vehicle_manufacturing': 'COINDUSZ3361',
    'industrialized_aerospace_manufacturing': 'COINDUSZ3364',
    'industrialized_fabricated_metal_manufacturing': 'COINDUSZ3329',
    'industrialized_electrical_equipment_manufacturing': 'COINDUSZ335',
    'industrialized_medical_equipment_manufacturing': 'COINDUSZ3391'
}

# 통합 시리즈 딕셔너리 (하위 호환성을 위해 유지)
IMPORT_SERIES = {
    **IMPORT_SERIES_END_USE,
    **IMPORT_SERIES_ORIGIN
}

# ===== 한국어 이름 매핑 =====
IMPORT_KOREAN_NAMES_END_USE = {
    # 주요 전체 지표
    'all_commodities': '수입품 - 전체',
    'all_excluding_petroleum': '수입품 - 석유 제외 전체',
    'all_excluding_fuels': '수입품 - 연료 제외 전체',
    'all_excluding_food_fuels': '수입품 - 식품·연료 제외 전체',
    
    # 주요 카테고리
    'foods_feeds_beverages': '수입품 - 식품, 사료 및 음료',
    'industrial_supplies': '수입품 - 산업용 원자재',
    'capital_goods': '수입품 - 자본재',
    'automotive_vehicles': '수입품 - 자동차',
    'consumer_goods': '수입품 - 소비재',
    
    # 연료 및 에너지
    'fuels_lubricants': '수입품 - 연료 및 윤활유',
    'crude_oil': '수입품 - 원유',
    
    # 산업용 원자재 세부
    'industrial_supplies_excl_fuel': '산업용 원자재 - 연료 제외',
    'industrial_supplies_excl_petroleum': '산업용 원자재 - 석유 제외',
    'industrial_supplies_durable': '산업용 원자재 - 내구재',
    'unfinished_metals_durable': '미완성 금속 - 내구재용',
    'iron_steel_products': '철강 제품',
    'iron_steel_semifinished': '철강 반제품',
    'bauxite_aluminum': '보크사이트 및 알루미늄',
    'copper': '구리',
    'zinc': '아연',
    'other_precious_metals': '기타 귀금속',
    'building_materials_excl_metals': '건설자재 - 금속 제외',
    'stone_sand_cement': '석재, 모래 및 시멘트',
    'chemicals_fertilizers': '화학제품 - 비료',
    
    # 자본재 세부
    'semiconductors': '반도체',
    
    # 소비재 세부
    'consumer_durables_excl_auto': '소비재 내구재 - 자동차 제외',
    'apparel_footwear_household': '의류, 신발 및 가정용품',
    'pharmaceutical_preparations': '의약품',
    'furniture_household_goods': '가구 및 가정용품',
    'household_appliances': '가전제품',
    'gem_diamonds': '보석 다이아몬드',
    
    # 식품 세부
    'green_coffee_cocoa_sugar': '생두, 코코아, 설탕',
    'green_coffee': '생두',
    'fish_shellfish': '생선 및 조개류',
    
    # 특수 항목
    'nonmonetary_gold': '비화폐용 금'
}

IMPORT_KOREAN_NAMES_ORIGIN = {
    # 주요 국가/지역 - 전체 산업
    'china_total': '중국 - 전체',
    'canada_total': '캐나다 - 전체',
    'japan_total': '일본 - 전체',
    'european_union_total': '유럽연합 - 전체',
    'germany_total': '독일 - 전체',
    'united_kingdom_total': '영국 - 전체',
    'mexico_total': '멕시코 - 전체',
    'latin_america_total': '라틴아메리카 - 전체',
    'france_total': '프랑스 - 전체',
    'industrialized_countries_total': '선진국 - 전체',
    'asian_newly_industrialized_total': '아시아 신흥공업국 - 전체',
    'asean_total': '아세안 - 전체',
    'asia_near_east_total': '아시아 근동 - 전체',
    'pacific_rim_total': '환태평양 - 전체',
    
    # 제조업 vs 비제조업 (존재하는 시리즈만)
    'canada_manufacturing': '캐나다 - 제조업',
    'canada_nonmanufacturing': '캐나다 - 비제조업',
    'european_union_manufacturing': '유럽연합 - 제조업',
    'european_union_nonmanufacturing': '유럽연합 - 비제조업',
    'mexico_manufacturing': '멕시코 - 제조업',
    'mexico_nonmanufacturing': '멕시코 - 비제조업',
    'latin_america_manufacturing': '라틴아메리카 - 제조업',
    'latin_america_nonmanufacturing': '라틴아메리카 - 비제조업',
    'industrialized_manufacturing': '선진국 - 제조업',
    'industrialized_nonmanufacturing': '선진국 - 비제조업',
    
    # 참고: 중국과 일본은 전체 제조업 시리즈가 없어 세부 부문별로만 분석 가능
    
    # 중국 - 세부 제조업
    'china_manufacturing_part1': '중국 - 제조업 1부',
    'china_manufacturing_part2': '중국 - 제조업 2부',
    'china_manufacturing_part3': '중국 - 제조업 3부',
    'china_apparel_manufacturing': '중국 - 의류 제조업',
    'china_leather_manufacturing': '중국 - 가죽 제조업',
    'china_footwear_manufacturing': '중국 - 신발 제조업',
    'china_chemical_manufacturing': '중국 - 화학 제조업',
    'china_plastics_rubber_manufacturing': '중국 - 플라스틱·고무 제조업',
    'china_plastics_manufacturing': '중국 - 플라스틱 제조업',
    'china_fabricated_metal_manufacturing': '중국 - 금속가공 제조업',
    'china_cutlery_handtool_manufacturing': '중국 - 칼·공구 제조업',
    'china_machinery_manufacturing': '중국 - 기계 제조업',
    'china_computer_electronic_manufacturing': '중국 - 컴퓨터·전자 제조업',
    'china_communications_equipment': '중국 - 통신장비',
    'china_audio_video_equipment': '중국 - 음향·영상장비',
    'china_measuring_instruments': '중국 - 측정기기',
    'china_electrical_equipment_manufacturing': '중국 - 전기장비 제조업',
    'china_electric_lighting_manufacturing': '중국 - 전기조명 제조업',
    'china_household_appliance_manufacturing': '중국 - 가전제품 제조업',
    'china_furniture_manufacturing': '중국 - 가구 제조업',
    'china_household_furniture_manufacturing': '중국 - 가정용 가구 제조업',
    'china_miscellaneous_manufacturing': '중국 - 기타 제조업',
    'china_other_miscellaneous_manufacturing': '중국 - 기타 잡제조업',
    
    # 유럽연합 - 세부 제조업
    'eu_manufacturing_part1': '유럽연합 - 제조업 1부',
    'eu_manufacturing_part2': '유럽연합 - 제조업 2부',
    'eu_manufacturing_part3': '유럽연합 - 제조업 3부',
    'eu_food_manufacturing': '유럽연합 - 식품 제조업',
    'eu_beverage_tobacco_manufacturing': '유럽연합 - 음료·담배 제조업',
    'eu_chemical_manufacturing': '유럽연합 - 화학 제조업',
    'eu_pharmaceutical_manufacturing': '유럽연합 - 의약품 제조업',
    'eu_primary_metal_manufacturing': '유럽연합 - 1차 금속 제조업',
    'eu_fabricated_metal_manufacturing': '유럽연합 - 금속가공 제조업',
    'eu_other_fabricated_metal_manufacturing': '유럽연합 - 기타 금속가공 제조업',
    'eu_machinery_manufacturing': '유럽연합 - 기계 제조업',
    'eu_agriculture_machinery_manufacturing': '유럽연합 - 농업·건설·광업 기계',
    'eu_industrial_machinery_manufacturing': '유럽연합 - 산업용 기계',
    'eu_other_machinery_manufacturing': '유럽연합 - 기타 범용 기계',
    'eu_computer_electronic_manufacturing': '유럽연합 - 컴퓨터·전자 제조업',
    'eu_electrical_equipment_manufacturing': '유럽연합 - 전기장비 제조업',
    'eu_transportation_equipment_manufacturing': '유럽연합 - 운송장비 제조업',
    'eu_miscellaneous_manufacturing': '유럽연합 - 기타 제조업',
    
    # 기타 국가별 세부 제조업 (일부만 표시)
    'canada_mining_oil_gas': '캐나다 - 광업·석유·가스',
    'canada_manufacturing_part1': '캐나다 - 제조업 1부',
    'canada_manufacturing_part2': '캐나다 - 제조업 2부',
    'canada_manufacturing_part3': '캐나다 - 제조업 3부',
    'canada_food_manufacturing': '캐나다 - 식품 제조업',
    'canada_paper_manufacturing': '캐나다 - 제지 제조업',
    'canada_chemical_manufacturing': '캐나다 - 화학 제조업',
    'canada_machinery_manufacturing': '캐나다 - 기계 제조업',
    
    'germany_manufacturing_part3': '독일 - 제조업 3부',
    'germany_machinery_manufacturing': '독일 - 기계 제조업',
    'germany_computer_electronic_manufacturing': '독일 - 컴퓨터·전자 제조업',
    
    'japan_manufacturing_part2': '일본 - 제조업 2부',
    'japan_manufacturing_part3': '일본 - 제조업 3부',
    'japan_machinery_manufacturing': '일본 - 기계 제조업',
    'japan_computer_electronic_manufacturing': '일본 - 컴퓨터·전자 제조업',
    
    'industrialized_mining_oil_gas': '선진국 - 광업·석유·가스',
    'industrialized_manufacturing_part1': '선진국 - 제조업 1부',
    'industrialized_manufacturing_part2': '선진국 - 제조업 2부',
    'industrialized_food_manufacturing': '선진국 - 식품 제조업',
    'industrialized_animal_processing': '선진국 - 축산가공업',
    'industrialized_beverage_tobacco': '선진국 - 음료·담배',
    'industrialized_paper_manufacturing': '선진국 - 제지 제조업',
    'industrialized_chemical_manufacturing': '선진국 - 화학 제조업',
    'industrialized_basic_chemical_manufacturing': '선진국 - 기초화학 제조업',
    'industrialized_pharmaceutical_manufacturing': '선진국 - 의약품 제조업',
    'industrialized_plastics_rubber_manufacturing': '선진국 - 플라스틱·고무 제조업',
    'industrialized_semiconductor_manufacturing': '선진국 - 반도체 제조업',
    'industrialized_motor_vehicle_manufacturing': '선진국 - 자동차 제조업',
    'industrialized_aerospace_manufacturing': '선진국 - 항공우주 제조업',
    'industrialized_fabricated_metal_manufacturing': '선진국 - 기타 금속가공 제조업',
    'industrialized_electrical_equipment_manufacturing': '선진국 - 전기장비 제조업',
    'industrialized_medical_equipment_manufacturing': '선진국 - 의료기기 제조업'
}

# 통합 한국어 이름 매핑
IMPORT_KOREAN_NAMES = {
    **IMPORT_KOREAN_NAMES_END_USE,
    **IMPORT_KOREAN_NAMES_ORIGIN
}

# Import Prices 계층 구조 (카테고리)
IMPORT_CATEGORIES = {
    '주요 전체 지표': {
        '전체 수입품': ['all_commodities', 'all_excluding_petroleum', 'all_excluding_fuels', 'all_excluding_food_fuels']
    },
    '용도별(End Use) - 주요 카테고리': {
        '식품·음료': ['foods_feeds_beverages', 'green_coffee_cocoa_sugar', 'green_coffee', 'fish_shellfish'],
        '산업용 원자재': ['industrial_supplies', 'industrial_supplies_excl_fuel', 'industrial_supplies_excl_petroleum', 'industrial_supplies_durable'],
        '자본재': ['capital_goods', 'semiconductors'],
        '자동차': ['automotive_vehicles'],
        '소비재': ['consumer_goods', 'consumer_durables_excl_auto', 'apparel_footwear_household']
    },
    '용도별(End Use) - 에너지': {
        '연료': ['fuels_lubricants', 'crude_oil']
    },
    '용도별(End Use) - 금속·원자재': {
        '금속': ['unfinished_metals_durable', 'iron_steel_products', 'iron_steel_semifinished', 'bauxite_aluminum', 'copper', 'zinc', 'other_precious_metals'],
        '건설자재': ['building_materials_excl_metals', 'stone_sand_cement'],
        '화학제품': ['chemicals_fertilizers']
    },
    '용도별(End Use) - 소비재 세부': {
        '의약품·건강': ['pharmaceutical_preparations'],
        '가정용품': ['furniture_household_goods', 'household_appliances'],
        '귀금속·보석': ['gem_diamonds', 'nonmonetary_gold']
    },
    '원산지별 - 주요 국가': {
        '동아시아': ['china_total', 'japan_total', 'asian_newly_industrialized_total', 'asean_total'],
        '북미': ['canada_total', 'mexico_total'],
        '유럽': ['european_union_total', 'germany_total', 'united_kingdom_total', 'france_total'],
        '지역권': ['latin_america_total', 'asia_near_east_total', 'pacific_rim_total', 'industrialized_countries_total']
    },
    '원산지별 - 중국 세부': {
        '제조업 부문별': ['china_manufacturing_part1', 'china_manufacturing_part2', 'china_manufacturing_part3'],
        '전자·기계': ['china_computer_electronic_manufacturing', 'china_machinery_manufacturing', 'china_electrical_equipment_manufacturing'],
        '소비재': ['china_apparel_manufacturing', 'china_leather_manufacturing', 'china_footwear_manufacturing', 'china_furniture_manufacturing']
    },
    '원산지별 - 유럽연합 세부': {
        '제조업': ['eu_manufacturing_part1', 'eu_manufacturing_part2', 'eu_manufacturing_part3'],
        '첨단산업': ['eu_computer_electronic_manufacturing', 'eu_machinery_manufacturing', 'eu_pharmaceutical_manufacturing'],
        '기초산업': ['eu_food_manufacturing', 'eu_chemical_manufacturing', 'eu_primary_metal_manufacturing']
    },
    '원산지별 - 선진국 세부': {
        '제조업': ['industrialized_manufacturing', 'industrialized_manufacturing_part1', 'industrialized_manufacturing_part2'],
        '첨단기술': ['industrialized_semiconductor_manufacturing', 'industrialized_pharmaceutical_manufacturing', 'industrialized_aerospace_manufacturing'],
        '자동차·기계': ['industrialized_motor_vehicle_manufacturing', 'industrialized_electrical_equipment_manufacturing']
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
        data_source='FRED',
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

print("=== FRED 기반 Import Prices 분석 도구 사용법 ===")
print("📈 데이터 소스: BLS → FRED로 변경 (1,400+ 시리즈 지원)")
print()
print("1. 데이터 로드:")
print("   load_import_data()  # 스마트 업데이트")
print("   load_import_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (End Use 카테고리):")
print("   plot_import_series_advanced(['all_commodities', 'fuels_lubricants'], 'multi_line', 'mom')")
print("   plot_import_series_advanced(['industrial_supplies', 'capital_goods', 'consumer_goods'], 'multi_line', 'yoy')")
print("   plot_import_series_advanced(['semiconductors', 'pharmaceutical_preparations'], 'multi_line', 'mom')")
print()
print("3. 🔥 원산지별 분석:")
print("   plot_import_series_advanced(['china_total', 'japan_total', 'european_union_total'], 'multi_line', 'yoy')")
print("   plot_import_series_advanced(['china_computer_electronic_manufacturing'], 'single_line', 'mom')")
print("   plot_import_series_advanced(['industrialized_semiconductor_manufacturing'], 'single_line', 'yoy')")
print()
print("4. 🔥 데이터 Export:")
print("   export_import_data(['all_commodities', 'china_total'], 'mom')")
print("   export_import_data(['semiconductors', 'pharmaceutical_preparations'], 'yoy', periods=24)")
print()
print("✅ FRED 데이터로 더 풍부하고 세밀한 분석 가능!")
print("✅ End Use + Origin 조합으로 다각적 분석!")
print("✅ 130+ 시리즈로 확장된 분석 범위!")
print("⚠️ 참고: 중국/일본은 전체 제조업 시리즈가 없어 세부 부문별로만 분석 가능")

# %%
# FRED 데이터로 테스트 (기존 데이터 강제 재로드)
load_import_data()

# %%
# End Use 카테고리 테스트
plot_import_series_advanced(['all_commodities', 'fuels_lubricants'], 'multi_line', 'mom')

# %%
# 원산지별 분석 테스트
plot_import_series_advanced(['china_total', 'japan_total', 'european_union_total'], 'multi_line', 'yoy')

# %%
# 세부 카테고리 테스트
plot_import_series_advanced(['semiconductors', 'pharmaceutical_preparations'], 'multi_line', 'mom')

# %%
plot_import_series_advanced(['all_commodities', 'foods_feeds_beverages',
                             'industrial_supplies', 'capital_goods',
                             'automotive_vehicles', 'consumer_goods'], 'multi_line', 'yoy')
# %%
plot_import_series_advanced(['industrial_supplies_excl_fuel', 'industrial_supplies_excl_petroleum',
                             'industrial_supplies_durable', 'unfinished_metals_durable',
                             'iron_steel_products', 'iron_steel_semifinished',
                             'bauxite_aluminum', 'copper', 'zinc', 'other_precious_metals',
                             'building_materials_excl_metals', 'stone_sand_cement',
                             'chemicals_fertilizers', 'semiconductors',
                             'consumer_durables_excl_auto', 'apparel_footwear_household',
                             'pharmaceutical_preparations', 'furniture_household_goods',
                             'household_appliances', 'gem_diamonds'], 'horizontal_bar', 'yoy')
# %%
