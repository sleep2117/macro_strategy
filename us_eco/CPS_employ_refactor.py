# %%
"""
CPS(Current Population Survey) 데이터 분석 (리팩토링 버전)
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
# === CPS 시리즈 정의 ===

CPS_SERIES = {
    # 주요 노동시장 지표
    'civilian_labor_force': 'LNS11000000',
    'participation_rate': 'LNS11300000',
    'employment_level': 'LNS12000000',
    'employment_population_ratio': 'LNS12300000',
    'employed_full_time': 'LNS12500000',
    'employed_part_time': 'LNS12600000',
    'unemployment_level': 'LNS13000000',
    'unemployment_rate': 'LNS14000000',
    
    # 연령별 실업률
    'unemployment_rate_16_19': 'LNS14000012',
    'unemployment_rate_men_20_over': 'LNS14000025',
    'unemployment_rate_women_20_over': 'LNS14000026',
    
    # 인종별 실업률
    'unemployment_rate_white': 'LNS14000003',
    'unemployment_rate_black': 'LNS14000006',
    'unemployment_rate_asian': 'LNS14032183',
    'unemployment_rate_hispanic': 'LNS14000009',
    
    # 교육수준별 실업률
    'unemployment_rate_less_hs': 'LNS14027659',
    'unemployment_rate_hs_grad': 'LNS14027660',
    'unemployment_rate_some_college': 'LNS14027689',
    'unemployment_rate_bachelor_higher': 'LNS14027662',
    
    # 실업 기간
    'unemployed_less_5_weeks': 'LNS13008396',
    'unemployed_5_14_weeks': 'LNS13008756',
    'unemployed_15_weeks_over': 'LNS13008516',
    'unemployed_27_weeks_over': 'LNS13008636',
    'average_weeks_unemployed': 'LNS13008275',
    'median_weeks_unemployed': 'LNS13008276',
    
    # 실업 유형
    'unemployment_job_losers': 'LNS13023621',
    'unemployment_job_losers_layoff': 'LNS13023653',
    'unemployment_job_losers_not_layoff': 'LNS13025699',
    'unemployment_job_leavers': 'LNS13023705',
    'unemployment_reentrants': 'LNS13023557',
    'unemployment_new_entrants': 'LNS13023569',
    
    # 기타 노동시장 지표
    'part_time_economic_reasons': 'LNS12032194',
    'not_in_labor_force': 'LNS15000000',
    'marginally_attached': 'LNS15026642',
    'discouraged_workers': 'LNS15026645',
    'u6_unemployment_rate': 'LNS13327709',
    'multiple_jobholders_level': 'LNS12026619',
    'multiple_jobholders_percent': 'LNS12026620',
    
    # 날씨 영향
    'not_at_work_bad_weather': 'LNU02036012',
    'at_work_1_34hrs_bad_weather': 'LNU02033224',
    
    # 20세 이상 주요 지표
    'labor_force_20_over': 'LNS11000024',
    'participation_rate_20_over': 'LNS11300024',
    'employment_20_over': 'LNS12000024',
    'unemployment_20_over': 'LNS13000024',
    'unemployment_rate_20_over': 'LNS14000024',
    
    # 25세 이상 주요 지표
    'labor_force_25_over': 'LNS11000048',
    'participation_rate_25_over': 'LNS11300048',
    'employment_25_over': 'LNS12000048',
    'unemployment_25_over': 'LNS13000048',
    'unemployment_rate_25_over': 'LNS14000048',
    
    # 55세 이상 주요 지표
    'labor_force_55_over': 'LNS11024230',
    'participation_rate_55_over': 'LNS11324230',
    'employment_55_over': 'LNS12024230',
    'unemployment_55_over': 'LNS13024230',
    'unemployment_rate_55_over': 'LNS14024230',
    
    # 히스패닉/라티노 전체 노동시장 지표
    'labor_force_hispanic': 'LNS11000009',
    'participation_rate_hispanic': 'LNS11300009',
    'employment_hispanic': 'LNS12000009',
    'unemployment_hispanic': 'LNS13000009',
    'not_in_labor_force_hispanic': 'LNS15000009',
    
    # === 추가 연령별 노동시장 지표 (Not Seasonally Adjusted) ===
    'labor_force_18_over_nsa': 'LNU01076975',
    'participation_rate_18_over_nsa': 'LNU01376975',
    'employment_18_over_nsa': 'LNU02076975',
    'unemployment_18_over_nsa': 'LNU03076975',
    'unemployment_rate_18_over_nsa': 'LNU04076975',
    'not_in_labor_force_18_over_nsa': 'LNU05076975',
    
    # 25세 이상 (Not Seasonally Adjusted)
    'labor_force_25_over_nsa': 'LNU01000048',
    'participation_rate_25_over_nsa': 'LNU01300048',
    'employment_25_over_nsa': 'LNU02000048',
    'employed_full_time_25_over_nsa': 'LNU02500048',
    'employed_part_time_25_over_nsa': 'LNU02600048',
    'unemployment_25_over_nsa': 'LNU03000048',
    'unemployed_fulltime_seeking_25_over_nsa': 'LNU03100048',
    'unemployed_parttime_seeking_25_over_nsa': 'LNU03200048',
    'unemployment_rate_25_over_nsa': 'LNU04000048',
    
    # 45세 이상 (Not Seasonally Adjusted)
    'labor_force_45_over_nsa': 'LNU01000092',
    'participation_rate_45_over_nsa': 'LNU01300092',
    'employment_45_over_nsa': 'LNU02000092',
    'unemployment_45_over_nsa': 'LNU03000092',
    'unemployment_rate_45_over_nsa': 'LNU04000092',
    'not_in_labor_force_45_over_nsa': 'LNU05000092',
    
    # 55세 이상 (Not Seasonally Adjusted)
    'labor_force_55_over_nsa': 'LNU01024230',
    'participation_rate_55_over_nsa': 'LNU01324230',
    'employment_55_over_nsa': 'LNU02024230',
    'employed_full_time_55_over_nsa': 'LNU02524230',
    'employed_part_time_55_over_nsa': 'LNU02624230',
    'unemployment_55_over_nsa': 'LNU03024230',
    'unemployed_fulltime_seeking_55_over_nsa': 'LNU03124230',
    'unemployed_parttime_seeking_55_over_nsa': 'LNU03224230',
    'unemployment_rate_55_over_nsa': 'LNU04024230',
    'not_in_labor_force_55_over_nsa': 'LNU05024230',
    
    # 65세 이상 (Not Seasonally Adjusted)
    'labor_force_65_over_nsa': 'LNU01000097',
    'participation_rate_65_over_nsa': 'LNU01300097',
    'employment_65_over_nsa': 'LNU02000097',
    'unemployment_65_over_nsa': 'LNU03000097',
    'unemployment_rate_65_over_nsa': 'LNU04000097',
    'not_in_labor_force_65_over_nsa': 'LNU05000097',
    
    # === 추가 인종별 노동시장 지표 (Not Seasonally Adjusted) ===
    # 백인 (White)
    'labor_force_white_nsa': 'LNU01000003',
    'participation_rate_white_nsa': 'LNU01300003',
    'employment_white_nsa': 'LNU02000003',
    'employed_full_time_white_nsa': 'LNU02500003',
    'employed_part_time_white_nsa': 'LNU02600003',
    'unemployment_white_nsa': 'LNU03000003',
    'unemployment_rate_white_nsa': 'LNU04000003',
    'not_in_labor_force_white_nsa': 'LNU05000003',
    
    # 흑인 (Black or African American)
    'labor_force_black_nsa': 'LNU01000006',
    'participation_rate_black_nsa': 'LNU01300006',
    'employment_black_nsa': 'LNU02000006',
    'employed_full_time_black_nsa': 'LNU02500006',
    'employed_part_time_black_nsa': 'LNU02600006',
    'unemployment_black_nsa': 'LNU03000006',
    'unemployment_rate_black_nsa': 'LNU04000006',
    'not_in_labor_force_black_nsa': 'LNU05000006',
    
    # 아시아계 (Asian)
    'labor_force_asian_nsa': 'LNU01032183',
    'participation_rate_asian_nsa': 'LNU01332183',
    'employment_asian_nsa': 'LNU02032183',
    'employed_full_time_asian_nsa': 'LNU02532183',
    'employed_part_time_asian_nsa': 'LNU02632183',
    'unemployment_asian_nsa': 'LNU03032183',
    'unemployment_rate_asian_nsa': 'LNU04032183',
    'not_in_labor_force_asian_nsa': 'LNU05032183',
    
    # 아메리카 원주민 (American Indian or Alaska Native)
    'labor_force_native_american_nsa': 'LNU01035243',
    'participation_rate_native_american_nsa': 'LNU01335243',
    'employment_native_american_nsa': 'LNU02035243',
    'unemployment_native_american_nsa': 'LNU03035243',
    'unemployment_rate_native_american_nsa': 'LNU04035243',
    'not_in_labor_force_native_american_nsa': 'LNU05035243',
    
    # 하와이 원주민 및 태평양 섬 주민 (Native Hawaiian or Other Pacific Islander)
    'labor_force_pacific_islander_nsa': 'LNU01035553',
    'participation_rate_pacific_islander_nsa': 'LNU01335553',
    'employment_pacific_islander_nsa': 'LNU02035553',
    'unemployment_pacific_islander_nsa': 'LNU03035553',
    'unemployment_rate_pacific_islander_nsa': 'LNU04035553',
    'not_in_labor_force_pacific_islander_nsa': 'LNU05035553',
    
    # === 기본 지표들의 Not Seasonally Adjusted 버전 ===
    'civilian_labor_force_nsa': 'LNU01000000',
    'participation_rate_nsa': 'LNU01300000',
    'employment_level_nsa': 'LNU02000000',
    'employed_full_time_nsa': 'LNU02500000',
    'employed_part_time_nsa': 'LNU02600000',
    'unemployment_level_nsa': 'LNU03000000',
    'unemployed_fulltime_seeking_nsa': 'LNU03100000',
    'unemployed_parttime_seeking_nsa': 'LNU03200000',
    'unemployment_rate_nsa': 'LNU04000000',
    'unemployment_rate_fulltime_force_nsa': 'LNU04100000',
    'unemployment_rate_parttime_force_nsa': 'LNU04200000',
    'not_in_labor_force_nsa': 'LNU05000000'
}

# 한국어 이름 매핑
CPS_KOREAN_NAMES = {
    # 주요 노동시장 지표
    'civilian_labor_force': '경제활동인구',
    'participation_rate': '경제활동참가율',
    'employment_level': '취업자수',
    'employment_population_ratio': '고용률',
    'employed_full_time': '풀타임 취업자',
    'employed_part_time': '파트타임 취업자',
    'unemployment_level': '실업자수',
    'unemployment_rate': '실업률',
    
    # 연령별 실업률
    'unemployment_rate_16_19': '실업률 - 16-19세',
    'unemployment_rate_men_20_over': '실업률 - 20세 이상 남성',
    'unemployment_rate_women_20_over': '실업률 - 20세 이상 여성',
    
    # 인종별 실업률
    'unemployment_rate_white': '실업률 - 백인',
    'unemployment_rate_black': '실업률 - 흑인',
    'unemployment_rate_asian': '실업률 - 아시아계',
    'unemployment_rate_hispanic': '실업률 - 히스패닉',
    
    # 교육수준별 실업률
    'unemployment_rate_less_hs': '실업률 - 고졸 미만',
    'unemployment_rate_hs_grad': '실업률 - 고졸',
    'unemployment_rate_some_college': '실업률 - 대학 중퇴/전문대',
    'unemployment_rate_bachelor_higher': '실업률 - 대졸 이상',
    
    # 실업 기간
    'LNS13008396': '실업자 - 5주 미만',
    'LNS13008756': '실업자 - 5-14주',
    'LNS13008516': '실업자 - 15주 이상',
    'LNS13008636': '실업자 - 27주 이상',
    'LNS13008275': '평균 실업 기간',
    'LNS13008276': '중간값 실업 기간',
    
    # 실업 유형
    'LNS13023621': '실직자',
    'LNS13023653': '일시해고자',
    'LNS13025699': '영구해고자',
    'LNS13023705': '자발적 이직자',
    'LNS13023557': '재진입자',
    'LNS13023569': '신규진입자',
    
    # 기타 노동시장 지표
    'LNS12032194': '경제적 이유 파트타임',
    'LNS15000000': '비경제활동인구',
    'LNS15026642': '한계노동력',
    'LNS15026645': '구직단념자',
    'LNS13327709': 'U-6 실업률 (광의)',
    'LNS12026619': '복수직업 보유자',
    'LNS12026620': '복수직업 보유율',
    
    # 날씨 영향
    'LNU02036012': '악천후 결근',
    'LNU02033224': '악천후 단축근무',
    
    # 20세 이상 주요 지표
    'LNS11000024': '경제활동인구 - 20세 이상',
    'LNS11300024': '경제활동참가율 - 20세 이상',
    'LNS12000024': '취업자수 - 20세 이상',
    'LNS13000024': '실업자수 - 20세 이상',
    'LNS14000024': '실업률 - 20세 이상',
    
    # 25세 이상 주요 지표
    'LNS11000048': '경제활동인구 - 25세 이상',
    'LNS11300048': '경제활동참가율 - 25세 이상',
    'LNS12000048': '취업자수 - 25세 이상',
    'LNS13000048': '실업자수 - 25세 이상',
    'LNS14000048': '실업률 - 25세 이상',
    
    # 55세 이상 주요 지표
    'LNS11024230': '경제활동인구 - 55세 이상',
    'LNS11324230': '경제활동참가율 - 55세 이상',
    'LNS12024230': '취업자수 - 55세 이상',
    'LNS13024230': '실업자수 - 55세 이상',
    'LNS14024230': '실업률 - 55세 이상',
    
    # 히스패닉/라티노 전체 노동시장 지표
    'LNS11000009': '경제활동인구 - 히스패닉/라티노',
    'LNS11300009': '경제활동참가율 - 히스패닉/라티노',
    'LNS12000009': '취업자수 - 히스패닉/라티노',
    'LNS13000009': '실업자수 - 히스패닉/라티노',
    'LNS15000009': '비경제활동인구 - 히스패닉/라티노',
    
    # === 추가 연령별 노동시장 지표 한국어 매핑 (Not Seasonally Adjusted) ===
    # 18세 이상
    'LNU01076975': '경제활동인구 - 18세 이상 (비계절조정)',
    'LNU01376975': '경제활동참가율 - 18세 이상 (비계절조정)',
    'LNU02076975': '취업자수 - 18세 이상 (비계절조정)',
    'LNU03076975': '실업자수 - 18세 이상 (비계절조정)',
    'LNU04076975': '실업률 - 18세 이상 (비계절조정)',
    'LNU05076975': '비경제활동인구 - 18세 이상 (비계절조정)',
    
    # 25세 이상 (비계절조정)
    'LNU01000048': '경제활동인구 - 25세 이상 (비계절조정)',
    'LNU01300048': '경제활동참가율 - 25세 이상 (비계절조정)',
    'LNU02000048': '취업자수 - 25세 이상 (비계절조정)',
    'LNU02500048': '풀타임 취업자 - 25세 이상 (비계절조정)',
    'LNU02600048': '파트타임 취업자 - 25세 이상 (비계절조정)',
    'LNU03000048': '실업자수 - 25세 이상 (비계절조정)',
    'LNU03100048': '풀타임 구직자 - 25세 이상 (비계절조정)',
    'LNU03200048': '파트타임 구직자 - 25세 이상 (비계절조정)',
    'LNU04000048': '실업률 - 25세 이상 (비계절조정)',
    
    # 45세 이상 (비계절조정)
    'LNU01000092': '경제활동인구 - 45세 이상 (비계절조정)',
    'LNU01300092': '경제활동참가율 - 45세 이상 (비계절조정)',
    'LNU02000092': '취업자수 - 45세 이상 (비계절조정)',
    'LNU03000092': '실업자수 - 45세 이상 (비계절조정)',
    'LNU04000092': '실업률 - 45세 이상 (비계절조정)',
    'LNU05000092': '비경제활동인구 - 45세 이상 (비계절조정)',
    
    # 55세 이상 (비계절조정)
    'LNU01024230': '경제활동인구 - 55세 이상 (비계절조정)',
    'LNU01324230': '경제활동참가율 - 55세 이상 (비계절조정)',
    'LNU02024230': '취업자수 - 55세 이상 (비계절조정)',
    'LNU02524230': '풀타임 취업자 - 55세 이상 (비계절조정)',
    'LNU02624230': '파트타임 취업자 - 55세 이상 (비계절조정)',
    'LNU03024230': '실업자수 - 55세 이상 (비계절조정)',
    'LNU03124230': '풀타임 구직자 - 55세 이상 (비계절조정)',
    'LNU03224230': '파트타임 구직자 - 55세 이상 (비계절조정)',
    'LNU04024230': '실업률 - 55세 이상 (비계절조정)',
    'LNU05024230': '비경제활동인구 - 55세 이상 (비계절조정)',
    
    # 65세 이상 (비계절조정)
    'LNU01000097': '경제활동인구 - 65세 이상 (비계절조정)',
    'LNU01300097': '경제활동참가율 - 65세 이상 (비계절조정)',
    'LNU02000097': '취업자수 - 65세 이상 (비계절조정)',
    'LNU03000097': '실업자수 - 65세 이상 (비계절조정)',
    'LNU04000097': '실업률 - 65세 이상 (비계절조정)',
    'LNU05000097': '비경제활동인구 - 65세 이상 (비계절조정)',
    
    # === 추가 인종별 노동시장 지표 한국어 매핑 (Not Seasonally Adjusted) ===
    # 백인 (비계절조정)
    'LNU01000003': '경제활동인구 - 백인 (비계절조정)',
    'LNU01300003': '경제활동참가율 - 백인 (비계절조정)',
    'LNU02000003': '취업자수 - 백인 (비계절조정)',
    'LNU02500003': '풀타임 취업자 - 백인 (비계절조정)',
    'LNU02600003': '파트타임 취업자 - 백인 (비계절조정)',
    'LNU03000003': '실업자수 - 백인 (비계절조정)',
    'LNU04000003': '실업률 - 백인 (비계절조정)',
    'LNU05000003': '비경제활동인구 - 백인 (비계절조정)',
    
    # 흑인 (비계절조정)
    'LNU01000006': '경제활동인구 - 흑인 (비계절조정)',
    'LNU01300006': '경제활동참가율 - 흑인 (비계절조정)',
    'LNU02000006': '취업자수 - 흑인 (비계절조정)',
    'LNU02500006': '풀타임 취업자 - 흑인 (비계절조정)',
    'LNU02600006': '파트타임 취업자 - 흑인 (비계절조정)',
    'LNU03000006': '실업자수 - 흑인 (비계절조정)',
    'LNU04000006': '실업률 - 흑인 (비계절조정)',
    'LNU05000006': '비경제활동인구 - 흑인 (비계절조정)',
    
    # 아시아계 (비계절조정)
    'LNU01032183': '경제활동인구 - 아시아계 (비계절조정)',
    'LNU01332183': '경제활동참가율 - 아시아계 (비계절조정)',
    'LNU02032183': '취업자수 - 아시아계 (비계절조정)',
    'LNU02532183': '풀타임 취업자 - 아시아계 (비계절조정)',
    'LNU02632183': '파트타임 취업자 - 아시아계 (비계절조정)',
    'LNU03032183': '실업자수 - 아시아계 (비계절조정)',
    'LNU04032183': '실업률 - 아시아계 (비계절조정)',
    'LNU05032183': '비경제활동인구 - 아시아계 (비계절조정)',
    
    # 아메리카 원주민 (비계절조정)
    'LNU01035243': '경제활동인구 - 아메리카 원주민 (비계절조정)',
    'LNU01335243': '경제활동참가율 - 아메리카 원주민 (비계절조정)',
    'LNU02035243': '취업자수 - 아메리카 원주민 (비계절조정)',
    'LNU03035243': '실업자수 - 아메리카 원주민 (비계절조정)',
    'LNU04035243': '실업률 - 아메리카 원주민 (비계절조정)',
    'LNU05035243': '비경제활동인구 - 아메리카 원주민 (비계절조정)',
    
    # 하와이 원주민 및 태평양 섬 주민 (비계절조정)
    'LNU01035553': '경제활동인구 - 하와이/태평양 원주민 (비계절조정)',
    'LNU01335553': '경제활동참가율 - 하와이/태평양 원주민 (비계절조정)',
    'LNU02035553': '취업자수 - 하와이/태평양 원주민 (비계절조정)',
    'LNU03035553': '실업자수 - 하와이/태평양 원주민 (비계절조정)',
    'LNU04035553': '실업률 - 하와이/태평양 원주민 (비계절조정)',
    'LNU05035553': '비경제활동인구 - 하와이/태평양 원주민 (비계절조정)',
    
    # === 기본 지표들의 비계절조정 버전 한국어 매핑 ===
    'LNU01000000': '경제활동인구 (비계절조정)',
    'LNU01300000': '경제활동참가율 (비계절조정)',
    'LNU02000000': '취업자수 (비계절조정)',
    'LNU02500000': '풀타임 취업자 (비계절조정)',
    'LNU02600000': '파트타임 취업자 (비계절조정)',
    'LNU03000000': '실업자수 (비계절조정)',
    'LNU03100000': '풀타임 구직자 (비계절조정)',
    'LNU03200000': '파트타임 구직자 (비계절조정)',
    'LNU04000000': '실업률 (비계절조정)',
    'LNU04100000': '풀타임 실업률 (비계절조정)',
    'LNU04200000': '파트타임 실업률 (비계절조정)',
    'LNU05000000': '비경제활동인구 (비계절조정)'
}

# CPS 카테고리 분류
CPS_CATEGORIES = {
    '핵심지표': {
        '주요 지표': ['LNS14000000', 'LNS11300000', 'LNS12300000'],
        '고용 현황': ['LNS12000000', 'LNS13000000', 'LNS11000000'],
        '광의 실업률': ['LNS14000000', 'LNS13327709']
    },
    '인구통계별': {
        '연령별': ['LNS14000012', 'LNS14000025', 'LNS14000026'],
        '인종별': ['LNS14000003', 'LNS14000006', 'LNS14032183', 'LNS14000009'],
        '교육수준별': ['LNS14027659', 'LNS14027660', 'LNS14027689', 'LNS14027662']
    },
    '연령대별분석': {
        '16세이상전체': ['LNS11000000', 'LNS11300000', 'LNS12000000', 'LNS13000000', 'LNS14000000', 'LNS15000000'],
        '18세이상': ['LNU01076975', 'LNU01376975', 'LNU02076975', 'LNU03076975', 'LNU04076975', 'LNU05076975'],
        '20세이상': ['LNS11000024', 'LNS11300024', 'LNS12000024', 'LNS13000024', 'LNS14000024'],
        '25세이상': ['LNS11000048', 'LNS11300048', 'LNS12000048', 'LNS13000048', 'LNS14000048'],
        '25세이상비계절조정': ['LNU01000048', 'LNU01300048', 'LNU02000048', 'LNU03000048', 'LNU04000048'],
        '45세이상': ['LNU01000092', 'LNU01300092', 'LNU02000092', 'LNU03000092', 'LNU04000092', 'LNU05000092'],
        '55세이상': ['LNS11024230', 'LNS11324230', 'LNS12024230', 'LNS13024230', 'LNS14024230'],
        '55세이상비계절조정': ['LNU01024230', 'LNU01324230', 'LNU02024230', 'LNU03024230', 'LNU04024230', 'LNU05024230'],
        '65세이상': ['LNU01000097', 'LNU01300097', 'LNU02000097', 'LNU03000097', 'LNU04000097', 'LNU05000097']
    },
    '인종별분석': {
        '백인': ['LNS14000003', 'LNU01000003', 'LNU01300003', 'LNU02000003', 'LNU03000003', 'LNU04000003', 'LNU05000003'],
        '흑인': ['LNS14000006', 'LNU01000006', 'LNU01300006', 'LNU02000006', 'LNU03000006', 'LNU04000006', 'LNU05000006'],
        '아시아계': ['LNS14032183', 'LNU01032183', 'LNU01332183', 'LNU02032183', 'LNU03032183', 'LNU04032183', 'LNU05032183'],
        '아메리카원주민': ['LNU01035243', 'LNU01335243', 'LNU02035243', 'LNU03035243', 'LNU04035243', 'LNU05035243'],
        '하와이태평양원주민': ['LNU01035553', 'LNU01335553', 'LNU02035553', 'LNU03035553', 'LNU04035553', 'LNU05035553'],
        '히스패닉라티노': ['LNS14000009', 'LNS11000009', 'LNS11300009', 'LNS12000009', 'LNS13000009', 'LNS15000009']
    },
    '계절조정비교': {
        '경제활동인구': ['LNS11000000', 'LNU01000000'],
        '경제활동참가율': ['LNS11300000', 'LNU01300000'],
        '취업자수': ['LNS12000000', 'LNU02000000'],
        '실업자수': ['LNS13000000', 'LNU03000000'],
        '실업률': ['LNS14000000', 'LNU04000000'],
        '비경제활동인구': ['LNS15000000', 'LNU05000000']
    },
    '실업분석': {
        '실업기간': ['LNS13008396', 'LNS13008756', 'LNS13008516', 'LNS13008636'],
        '실업기간통계': ['LNS13008275', 'LNS13008276'],
        '실업유형': ['LNS13023621', 'LNS13023653', 'LNS13025699', 'LNS13023705']
    },
    '고용형태': {
        '풀타임/파트타임': ['LNS12500000', 'LNS12600000', 'LNS12032194'],
        '복수직업': ['LNS12026619', 'LNS12026620']
    },
    '노동시장참여': {
        '비경제활동': ['LNS15000000', 'LNS15026642', 'LNS15026645'],
        '진입/이탈': ['LNS13023557', 'LNS13023569']
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/cps_data.csv'
CPS_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_cps_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 CPS 데이터 로드"""
    global CPS_DATA

    result = load_economic_data(
        series_dict=CPS_SERIES,
        data_source='BLS',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=1000.0  # 고용 데이터 허용 오차
    )

    if result:
        CPS_DATA = result
        print_load_info()
        return True
    else:
        print("❌ CPS 데이터 로드 실패")
        return False

def print_load_info():
    """CPS 데이터 로드 정보 출력"""
    if not CPS_DATA or 'load_info' not in CPS_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = CPS_DATA['load_info']
    print(f"\n📊 CPS 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in CPS_DATA and not CPS_DATA['raw_data'].empty:
        latest_date = CPS_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_cps_series_advanced(series_list, chart_type='multi_line', 
                            data_type='mom', periods=None, target_date=None):
    """범용 CPS 시각화 함수 - plot_economic_series 활용"""
    if not CPS_DATA:
        print("⚠️ 먼저 load_cps_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=CPS_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=CPS_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_cps_data(series_list, data_type='mom', periods=None, 
                   target_date=None, export_path=None, file_format='excel'):
    """CPS 데이터 export 함수 - export_economic_data 활용"""
    if not CPS_DATA:
        print("⚠️ 먼저 load_cps_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=CPS_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=CPS_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_cps_data():
    """CPS 데이터 초기화"""
    global CPS_DATA
    CPS_DATA = {}
    print("🗑️ CPS 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not CPS_DATA or 'raw_data' not in CPS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_cps_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPS_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in CPS_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPS_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not CPS_DATA or 'mom_data' not in CPS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_cps_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPS_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in CPS_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPS_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not CPS_DATA or 'yoy_data' not in CPS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_cps_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPS_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in CPS_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPS_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not CPS_DATA or 'raw_data' not in CPS_DATA:
        return []
    return list(CPS_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 CPS 시리즈 표시"""
    print("=== 사용 가능한 CPS 시리즈 ===")
    
    for series_name, series_id in CPS_SERIES.items():
        korean_name = CPS_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in CPS_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_name in series_list:
                korean_name = CPS_KOREAN_NAMES.get(series_name, series_name)
                api_id = CPS_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not CPS_DATA or 'load_info' not in CPS_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': CPS_DATA['load_info']['loaded'],
        'series_count': CPS_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': CPS_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 CPS 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_cps_data()  # 스마트 업데이트")
print("   load_cps_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_cps_series_advanced(['unemployment_rate', 'unemployment_rate_white'], 'multi_line', 'mom')")
print("   plot_cps_series_advanced(['unemployment_rate'], 'horizontal_bar', 'yoy')")
print("   plot_cps_series_advanced(['civilian_labor_force'], 'single_line', 'mom', periods=24)")
print()
print("3. 🔥 데이터 Export:")
print("   export_cps_data(['unemployment_rate', 'unemployment_rate_white'], 'mom')")
print("   export_cps_data(['civilian_labor_force'], 'raw', periods=24, file_format='csv')")
print("   export_cps_data(['unemployment_rate'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_cps_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_cps_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_cps_data()
plot_cps_series_advanced(['unemployment_rate', 'unemployment_rate_white'], 'multi_line', 'mom')

# %%
plot_cps_series_advanced(['employment_population_ratio', 'participation_rate'], 'multi_line', 'raw')

# %%
plot_cps_series_advanced(['civilian_labor_force', 'unemployment_level'], 'dual_axis', 'raw')

# %%
