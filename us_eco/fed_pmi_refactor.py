# %%
"""
연준별 PMI 데이터 수집, 저장, 스마트 업데이트, 분석 및 시각화 도구
- FRED API를 사용하여 연준별 PMI 데이터 수집
- 필라델피아 연준부터 시작, 다른 연준 확장 가능
- 스마트 업데이트 기능으로 최신 데이터 자동 감지
- KPDS 포맷 시각화 지원
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import warnings
import os
import json
warnings.filterwarnings('ignore')

# 통합 유틸리티 함수 불러오기
from us_eco_utils import *

# %%
# === FRED API 키 설정 ===
api_config.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'  # ADP 파일에서 가져온 키

print("✓ KPDS 시각화 포맷 로드됨")
print("✓ US Economic Data Utils 로드됨")

# %%
# === 연준별 PMI 데이터 시리즈 맵 ===

# 필라델피아 연준 - 제조업 PMI 시리즈 (Manufacturing Business Outlook Survey - MBOS)
# 주요 Diffusion Index 시리즈만 선별 (가이드 파일 기준)
PHILADELPHIA_MANUFACTURING_SERIES = {
    # 현재 지표 (Current Diffusion Indexes)
    'general_activity': 'GACDFSA066MSFRBPHI',        # 일반 활동 지수 (메인 PMI)
    'new_orders': 'NOCDFSA066MSFRBPHI',              # 신규 주문
    'shipments': 'SHCDFSA066MSFRBPHI',               # 출하
    'employment': 'NECDFSA066MSFRBPHI',              # 고용 (가이드 파일 기준)
    'inventories': 'IVCDFSA066MSFRBPHI',             # 재고 (가이드 파일 기준)
    'delivery_time': 'DTCDFSA066MSFRBPHI',           # 배송 시간
    'prices_paid': 'PPCDFSA066MSFRBPHI',             # 지불 가격 (가이드 파일 기준)
    'prices_received': 'PRCDFSA066MSFRBPHI',         # 수취 가격 (가이드 파일 기준)
    'unfilled_orders': 'UOCDFSA066MSFRBPHI',         # 미완료 주문
    'work_hours': 'AWCDFSA066MSFRBPHI',              # 근무 시간 (가이드 파일 기준)
    
    # 미래 지표 (Future Diffusion Indexes)
    'future_general_activity': 'GAFDFSA066MSFRBPHI', # 미래 일반 활동
    'future_new_orders': 'NOFDFSA066MSFRBPHI',       # 미래 신규 주문
    'future_shipments': 'SHFDFSA066MSFRBPHI',        # 미래 출하
    'future_employment': 'NEFDFSA066MSFRBPHI',       # 미래 고용 (가이드 파일 기준)
    'future_inventories': 'IVFDFSA066MSFRBPHI',      # 미래 재고 (가이드 파일 기준)
    'future_delivery_time': 'DTFDFSA066MSFRBPHI',    # 미래 배송 시간
    'future_prices_paid': 'PPFDFSA066MSFRBPHI',      # 미래 지불 가격 (가이드 파일 기준)
    'future_prices_received': 'PRFDFSA066MSFRBPHI',  # 미래 수취 가격 (가이드 파일 기준)
    'future_unfilled_orders': 'UOFDFSA066MSFRBPHI',  # 미래 미완료 주문
    'future_work_hours': 'AWFDFSA066MSFRBPHI',       # 미래 근무 시간 (가이드 파일 기준)
    'future_capital_expenditures': 'CEFDFSA066MSFRBPHI', # 미래 자본 지출 (가이드 파일 기준)
}

# 필라델피아 연준 - 비제조업 PMI 시리즈 (Nonmanufacturing Business Outlook Survey - NBOS)
# 주요 Diffusion Index 시리즈만 선별 (가이드 파일 기준)
PHILADELPHIA_NONMANUFACTURING_SERIES = {
    # 현재 지표 (Current Diffusion Indexes)
    'general_activity_firm': 'GABNDIF066MSFRBPHI',     # 기업별 일반 활동 (메인 PMI)
    'new_orders': 'NOBNDIF066MSFRBPHI',                # 신규 주문 (가이드 파일 기준)
    'sales_revenues': 'SRBNDIF066MSFRBPHI',            # 매출/수익 (가이드 파일 기준)
    'unfilled_orders': 'UOBNDIF066MSFRBPHI',           # 미완료 주문 (가이드 파일 기준)
    'full_time_employees': 'NFBNDIF066MSFRBPHI',       # 정규직 고용 (가이드 파일 기준)
    'part_time_employees': 'NPBNDIF066MSFRBPHI',       # 비정규직 고용 (가이드 파일 기준)
    'average_workweek': 'AWBNDIF066MSFRBPHI',          # 평균 근무시간 (가이드 파일 기준)
    'wages_benefits': 'WBBNDIF066MSFRBPHI',            # 임금 및 혜택 (가이드 파일 기준)
    'prices_paid': 'PPBNDIF066MSFRBPHI',               # 지불 가격 (가이드 파일 기준)
    'prices_received': 'PRBNDIF066MSFRBPHI',           # 수취 가격 (가이드 파일 기준)
    'inventories': 'IVBNDIF066MSFRBPHI',               # 재고 (가이드 파일 추가)
    'capital_expenditures_plant': 'CPBNDIF066MSFRBPHI',     # 물리적 자본 지출 (가이드 파일 기준)
    'capital_expenditures_equipment': 'CEBNDIF066MSFRBPHI', # 장비/소프트웨어 자본지출 (가이드 파일 기준)
    
    # 미래 지표 (Future Diffusion Indexes) - 기업별 인식
    'future_general_activity_firm': 'GAFBNDIF066MSFRBPHI',     # 미래 기업별 일반 활동 (가이드 파일 기준)
    'future_general_activity_region': 'GARFBNDIF066MSFRBPHI',  # 미래 지역별 일반 활동 (가이드 파일 기준)
}

# 모든 필라델피아 연준 시리즈 통합
ALL_PHILADELPHIA_SERIES = {
    'manufacturing': PHILADELPHIA_MANUFACTURING_SERIES,
    'nonmanufacturing': PHILADELPHIA_NONMANUFACTURING_SERIES
}

# 뉴욕 연준 - Empire State Manufacturing Survey (제조업 PMI) 주요 Diffusion Index 시리즈
NEW_YORK_MANUFACTURING_SERIES = {
    # 현재 지표 (Current Diffusion Indexes) - 핵심 선별
    'general_business_conditions': 'GACDISA066MSFRBNY',      # 일반 비즈니스 상황 (메인 PMI)
    'new_orders': 'NOCDISA066MSFRBNY',                       # 신규 주문
    'shipments': 'SHCDISA066MSFRBNY',                        # 출하
    'number_of_employees': 'NECDISA066MSFRBNY',              # 고용
    'inventories': 'IVCDISA066MSFRBNY',                      # 재고
    'delivery_time': 'DTCDISA066MSFRBNY',                    # 배송 시간
    'prices_paid': 'PPCDISA066MSFRBNY',                      # 지불 가격
    'prices_received': 'PRCDISA066MSFRBNY',                  # 수취 가격
    'unfilled_orders': 'UOCDISA066MSFRBNY',                  # 미완료 주문
    'average_workweek': 'AWCDISA066MSFRBNY',                 # 평균 근무시간
    
    # 미래 지표 (Future Diffusion Indexes)
    'future_general_business_conditions': 'GAFDISA066MSFRBNY',  # 미래 일반 비즈니스 상황
    'future_new_orders': 'NOFDISA066MSFRBNY',                   # 미래 신규 주문
    'future_shipments': 'SHFDISA066MSFRBNY',                    # 미래 출하
    'future_number_of_employees': 'NEFDISA066MSFRBNY',          # 미래 고용
    'future_inventories': 'IVFDISA066MSFRBNY',                  # 미래 재고
    'future_delivery_time': 'DTFDISA066MSFRBNY',                # 미래 배송 시간
    'future_prices_paid': 'PPFDISA066MSFRBNY',                  # 미래 지불 가격
    'future_prices_received': 'PRFDISA066MSFRBNY',              # 미래 수취 가격
    'future_unfilled_orders': 'UOFDISA066MSFRBNY',              # 미래 미완료 주문
    'future_workweek': 'AWFDISA066MSFRBNY',                     # 미래 평균 근무시간
    'future_capital_expenditures': 'CEFDISA066MSFRBNY',         # 미래 자본 지출
}

# 뉴욕 연준 - Business Leaders Survey (서비스업 PMI) 주요 Diffusion Index 시리즈
NEW_YORK_SERVICES_SERIES = {
    # 현재 지표 (Current Diffusion Indexes) - 핵심 선별
    'business_activity': 'BACDINA066MNFRBNY',                # 비즈니스 활동 (메인 PMI)
    'employment': 'EMCDINA066MNFRBNY',                       # 고용
    'business_climate': 'BCCDINA066MNFRBNY',                 # 비즈니스 환경
    'prices_paid': 'PPCDINA066MNFRBNY',                      # 지불 가격
    'prices_received': 'PRCDINA066MNFRBNY',                  # 수취 가격
    'wages': 'WPCDINA066MNFRBNY',                            # 임금
    'capital_spending': 'CSCDINA066MNFRBNY',                 # 자본 지출
    
    # 미래 지표 (Future Diffusion Indexes)
    'future_business_activity': 'BAFDINA066MNFRBNY',         # 미래 비즈니스 활동
    'future_employment': 'EMFDINA066MNFRBNY',                # 미래 고용
    'future_business_climate': 'BCFDINA066MNFRBNY',          # 미래 비즈니스 환경
    'future_prices_paid': 'PPFDINA066MNFRBNY',               # 미래 지불 가격
    'future_prices_received': 'PRFDINA066MNFRBNY',           # 미래 수취 가격
    'future_wages': 'WPFDINA066MNFRBNY',                     # 미래 임금
    'future_capital_spending': 'CSFDINA066MNFRBNY',          # 미래 자본 지출
}

# 모든 뉴욕 연준 시리즈 통합
ALL_NEW_YORK_SERIES = {
    'manufacturing': NEW_YORK_MANUFACTURING_SERIES,
    'services': NEW_YORK_SERVICES_SERIES
}

# 시카고 연준 - Chicago Fed Survey of Economic Conditions (핵심 활성 지표만 선별)
CHICAGO_ECONOMIC_SERIES = {
    # 현재 지표 (활성 시리즈만)
    'activity_index': 'CFSBCACTIVITY',                    # 일반 활동 지수 (메인)
    'manufacturing_activity': 'CFSBCACTIVITYMFG',         # 제조업 활동 지수
    'nonmanufacturing_activity': 'CFSBCACTIVITYNMFG',     # 비제조업 활동 지수
    'current_hiring': 'CFSBCHIRING',                      # 현재 고용
    'labor_costs': 'CFSBCLABORCOSTS',                     # 인건비 지수
    'nonlabor_costs': 'CFSBCNONLABORCOSTS',               # 비인건비 지수
    
    # 미래 전망 (활성 시리즈만)
    'hiring_expectations': 'CFSBCHIRINGEXP',              # 향후 12개월 고용 전망
    'capx_expectations': 'CFSBCCAPXEXP',                  # 향후 12개월 자본지출 전망
    'us_economy_outlook': 'CFSBCOUTLOOK',                 # 향후 12개월 미국 경제 전망
}

# 모든 시카고 연준 시리즈 통합
ALL_CHICAGO_SERIES = {
    'economic_conditions': CHICAGO_ECONOMIC_SERIES
}

# 텍사스(댈러스) 연준 - Texas Manufacturing Outlook Survey (핵심 Diffusion Index만 선별)
TEXAS_MANUFACTURING_SERIES = {
    # 현재 지표 (Current Diffusion Indexes) - 핵심 선별
    'general_business_activity': 'BACTSAMFRBDAL',         # 일반 비즈니스 활동 (메인 PMI)
    'production': 'PRODSAMFRBDAL',                        # 생산
    'new_orders': 'VNWOSAMFRBDAL',                        # 신규 주문
    'shipments': 'VSHPSAMFRBDAL',                         # 출하
    'employment': 'NEMPSAMFRBDAL',                        # 고용
    'finished_goods_inventories': 'FGISAMFRBDAL',         # 완제품 재고
    'unfilled_orders': 'UFILSAMFRBDAL',                   # 미완료 주문
    'delivery_time': 'DTMSAMFRBDAL',                      # 배송 시간
    'prices_paid_raw_materials': 'PRMSAMFRBDAL',          # 원자재 지불 가격
    'prices_received_finished_goods': 'PFGSAMFRBDAL',     # 완제품 수취 가격
    'wages_benefits': 'WGSSAMFRBDAL',                     # 임금 및 혜택
    'hours_worked': 'AVGWKSAMFRBDAL',                     # 근무 시간
    'capacity_utilization': 'CAPUSAMFRBDAL',             # 가동률
    'capital_expenditures': 'CEXPSAMFRBDAL',             # 자본 지출
    'company_outlook': 'COLKSAMFRBDAL',                   # 회사 전망
    'growth_rate_orders': 'GROSAMFRBDAL',                # 주문 증가율
    
    # 미래 지표 (Future Diffusion Indexes) - 핵심 선별
    'future_general_business_activity': 'FBACTSAMFRBDAL', # 미래 일반 비즈니스 활동
    'future_production': 'FPRODSAMFRBDAL',                # 미래 생산
    'future_new_orders': 'FVNWOSAMFRBDAL',                # 미래 신규 주문
    'future_shipments': 'FVSHPSAMFRBDAL',                 # 미래 출하
    'future_employment': 'FNEMPSAMFRBDAL',                # 미래 고용
    'future_finished_goods_inventories': 'FFGISAMFRBDAL', # 미래 완제품 재고
    'future_unfilled_orders': 'FUFILSAMFRBDAL',           # 미래 미완료 주문
    'future_delivery_time': 'FDTMSAMFRBDAL',              # 미래 배송 시간
    'future_prices_paid_raw_materials': 'FPRMSAMFRBDAL',  # 미래 원자재 지불 가격
    'future_prices_received_finished_goods': 'FPFGSAMFRBDAL', # 미래 완제품 수취 가격
    'future_wages_benefits': 'FWGSSAMFRBDAL',             # 미래 임금 및 혜택
    'future_hours_worked': 'FAVGWKSAMFRBDAL',             # 미래 근무 시간
    'future_capacity_utilization': 'FCAPUSAMFRBDAL',      # 미래 가동률
    'future_capital_expenditures': 'FCEXPSAMFRBDAL',      # 미래 자본 지출
    'future_company_outlook': 'FCOLKSAMFRBDAL',           # 미래 회사 전망
    'future_growth_rate_orders': 'FGROSAMFRBDAL',         # 미래 주문 증가율
}

# 텍사스(댈러스) 연준 - 소매업 PMI 시리즈 (Texas Retail Outlook Survey - TROS)
TEXAS_RETAIL_SERIES = {
    # 현재 지표 (Current Diffusion Indexes)
    'general_business_activity': 'TROSBACTSAMFRBDAL',        # 일반 비즈니스 활동 (메인 PMI)
    'sales': 'TROSREVSAMFRBDAL',                             # 매출
    'company_outlook': 'TROSCOLKSAMFRBDAL',                  # 회사 전망
    'employment': 'TROSEMPSAMFRBDAL',                        # 고용
    'selling_prices': 'TROSSELLSAMFRBDAL',                   # 판매 가격
    'companywide_sales': 'TROSTREVSAMFRBDAL',               # 회사 전체 매출
    'input_prices': 'TROSINPSAMFRBDAL',                      # 투입 가격
    'companywide_internet_sales': 'TROSINTREVSAMFRBDAL',     # 회사 전체 인터넷 매출
    'hours_worked': 'TROSAVGWKSAMFRBDAL',                    # 근무 시간
    'inventories': 'TROSINVSAMFRBDAL',                       # 재고
    'wages_benefits': 'TROSWGSSAMFRBDAL',                    # 임금 및 혜택
    'part_time_employment': 'TROSPEMPSAMFRBDAL',             # 파트타임 고용
    'capital_expenditures': 'TROSCEXPSAMFRBDAL',             # 자본 지출
    
    # 미래 지표 (Future Diffusion Indexes)
    'future_general_business_activity': 'TROSFBACTSAMFRBDAL',    # 미래 일반 비즈니스 활동
    'future_sales': 'TROSFREVSAMFRBDAL',                         # 미래 매출
    'future_company_outlook': 'TROSFCOLKSAMFRBDAL',              # 미래 회사 전망
    'future_companywide_internet_sales': 'TROSFINTREVSAMFRBDAL', # 미래 회사 전체 인터넷 매출
    'future_hours_worked': 'TROSFAVGWKSAMFRBDAL',                # 미래 근무 시간
    'future_inventories': 'TROSFINVSAMFRBDAL',                   # 미래 재고
    'future_employment': 'TROSFEMPSAMFRBDAL',                    # 미래 고용
    'future_input_prices': 'TROSFINPSAMFRBDAL',                  # 미래 투입 가격
    'future_wages_benefits': 'TROSFWGSSAMFRBDAL',                # 미래 임금 및 혜택
    'future_part_time_employment': 'TROSFPEMPSAMFRBDAL',         # 미래 파트타임 고용
    'future_selling_prices': 'TROSFSELLSAMFRBDAL',               # 미래 판매 가격
    'future_capital_expenditures': 'TROSFCEXPSAMFRBDAL',         # 미래 자본 지출
}

# 모든 텍사스(댈러스) 연준 시리즈 통합
ALL_TEXAS_SERIES = {
    'manufacturing': TEXAS_MANUFACTURING_SERIES,
    'retail': TEXAS_RETAIL_SERIES
}

# 한국어 이름 매핑
FED_PMI_KOREAN_NAMES = {
    # 제조업 현재 지표
    'general_activity': '일반 활동 지수',
    'new_orders': '신규 주문',
    'shipments': '출하',
    'employment': '고용',
    'inventories': '재고',
    'delivery_time': '배송 시간',
    'prices_paid': '지불 가격',
    'prices_received': '수취 가격',
    'unfilled_orders': '미완료 주문',
    'work_hours': '근무 시간',
    
    # 제조업 미래 지표
    'future_general_activity': '미래 일반 활동',
    'future_new_orders': '미래 신규 주문',
    'future_shipments': '미래 출하',
    'future_employment': '미래 고용',
    'future_inventories': '미래 재고',
    'future_delivery_time': '미래 배송 시간',
    'future_prices_paid': '미래 지불 가격',
    'future_prices_received': '미래 수취 가격',
    'future_unfilled_orders': '미래 미완료 주문',
    'future_work_hours': '미래 근무 시간',
    'future_capital_expenditures': '미래 자본 지출',
    
    # 비제조업 현재 지표
    'general_activity_firm': '기업별 일반 활동',
    'general_activity_region': '지역별 일반 활동',
    'sales_revenues': '매출/수익',
    'unfilled_orders': '미완료 주문',
    'full_time_employees': '정규직 고용',
    'part_time_employees': '비정규직 고용',
    'average_workweek': '평균 근무시간',
    'wages_benefits': '임금 및 혜택',
    'inventories': '재고',
    'capital_expenditures_plant': '물리적 자본 지출',
    'capital_expenditures_equipment': '장비/소프트웨어 자본 지출',
    
    # 비제조업 미래 지표
    'future_general_activity_firm': '미래 기업별 일반 활동',
    'future_general_activity_region': '미래 지역별 일반 활동',
    
    # 뉴욕 연준 제조업 현재 지표
    'general_business_conditions': '일반 비즈니스 상황',
    'number_of_employees': '고용 인원수',
    'average_workweek_ny': '뉴욕 평균 근무시간',
    
    # 뉴욕 연준 제조업 미래 지표
    'future_general_business_conditions': '미래 일반 비즈니스 상황',
    'future_number_of_employees': '미래 고용 인원수',
    'future_workweek': '미래 뉴욕 평균 근무시간',
    
    # 뉴욕 연준 서비스업 현재 지표
    'business_activity': '비즈니스 활동',
    'business_climate': '비즈니스 환경',
    'wages': '뉴욕 임금',
    'capital_spending': '뉴욕 자본 지출',
    
    # 뉴욕 연준 서비스업 미래 지표
    'future_business_activity': '미래 비즈니스 활동',
    'future_business_climate': '미래 비즈니스 환경',
    'future_wages': '미래 뉴욕 임금',
    'future_capital_spending': '미래 뉴욕 자본 지출',
    
    # 시카고 연준 지표
    'activity_index': '일반 활동 지수',
    'manufacturing_activity': '제조업 활동',
    'nonmanufacturing_activity': '비제조업 활동',
    'current_hiring': '현재 고용',
    'labor_costs': '인건비',
    'nonlabor_costs': '비인건비',
    'hiring_expectations': '고용 전망',
    'capx_expectations': '자본지출 전망',
    'us_economy_outlook': '미국 경제 전망',
    
    # 텍사스 연준 현재 지표
    'general_business_activity': '일반 비즈니스 활동',
    'production': '생산',
    'finished_goods_inventories': '완제품 재고',
    'prices_paid_raw_materials': '원자재 지불 가격',
    'prices_received_finished_goods': '완제품 수취 가격',
    'wages_benefits_tx': '텍사스 임금 및 혜택',
    'hours_worked': '근무 시간',
    'capacity_utilization': '가동률',
    'capital_expenditures_tx': '텍사스 자본 지출',
    'company_outlook': '회사 전망',
    'growth_rate_orders': '주문 증가율',
    
    # 텍사스 연준 미래 지표
    'future_general_business_activity': '미래 일반 비즈니스 활동',
    'future_production': '미래 생산',
    'future_finished_goods_inventories': '미래 완제품 재고',
    'future_prices_paid_raw_materials': '미래 원자재 지불 가격',
    'future_prices_received_finished_goods': '미래 완제품 수취 가격',
    'future_wages_benefits': '미래 임금 및 혜택',
    'future_hours_worked': '미래 근무 시간',
    'future_capacity_utilization': '미래 가동률',
    'future_capital_expenditures': '미래 자본 지출',
    'future_company_outlook': '미래 회사 전망',
    'future_growth_rate_orders': '미래 주문 증가율',
    
    # 텍사스 연준 소매업 지표 (TROS)
    'sales': '매출',
    'selling_prices': '판매 가격',
    'companywide_sales': '회사 전체 매출',
    'input_prices': '투입 가격',
    'companywide_internet_sales': '회사 전체 인터넷 매출',
    'wages_benefits': '임금 및 혜택',
    'part_time_employment': '파트타임 고용',
    'capital_expenditures': '자본 지출',
    'future_sales': '미래 매출',
    'future_selling_prices': '미래 판매 가격',
    'future_companywide_internet_sales': '미래 회사 전체 인터넷 매출',
    'future_input_prices': '미래 투입 가격',
    'future_wages_benefits': '미래 임금 및 혜택',
    'future_part_time_employment': '미래 파트타임 고용',
    'future_capital_expenditures': '미래 자본 지출',
}

FED_PMI_SECTOR_LABELS = {
    'manufacturing': '제조업',
    'nonmanufacturing': '비제조업',
    'services': '서비스업',
    'economic_conditions': '경제 상황',
    'retail': '소매업',
}


def _augment_fed_korean_names() -> None:
    base_names = dict(FED_PMI_KOREAN_NAMES)
    for bank_key, bank_config in FEDERAL_RESERVE_BANKS.items():
        bank_label = bank_config.get('name', bank_key.title())
        for sector_key, sector_series in bank_config.get('series', {}).items():
            sector_label = FED_PMI_SECTOR_LABELS.get(
                sector_key,
                sector_key.replace('_', ' ').title()
            )
            for indicator_key, fred_id in sector_series.items():
                alias = f"{bank_key}_{sector_key}_{indicator_key}"
                indicator_label = base_names.get(
                    indicator_key,
                    indicator_key.replace('_', ' ')
                )
                display_label = f"{bank_label} {sector_label} · {indicator_label}"
                FED_PMI_KOREAN_NAMES.setdefault(indicator_key, indicator_label)
                FED_PMI_KOREAN_NAMES[alias] = display_label
                FED_PMI_KOREAN_NAMES[fred_id] = display_label


# 연준별 설정 (확장 가능)
FEDERAL_RESERVE_BANKS = {
    'philadelphia': {
        'name': '필라델피아 연준',
        'district': 3,
        'series': ALL_PHILADELPHIA_SERIES,
        'enabled': True
    },
    'new_york': {
        'name': '뉴욕 연준',
        'district': 2,
        'series': ALL_NEW_YORK_SERIES,
        'enabled': True
    },
    'chicago': {
        'name': '시카고 연준',
        'district': 7,
        'series': ALL_CHICAGO_SERIES,
        'enabled': True
    },
    'dallas': {
        'name': '댈러스 연준',
        'district': 11,
        'series': ALL_TEXAS_SERIES,
        'enabled': True
    },
    # 향후 다른 연준 추가 예정
    # 'richmond': {
    #     'name': '리치몬드 연준',
    #     'district': 5,
    #     'series': {},  # 향후 추가
    #     'enabled': False
    # }
}

_augment_fed_korean_names()

# %%
# === 그룹별 스마트 업데이트를 위한 시리즈 그룹 정의 ===

def build_fed_series_groups(enabled_banks=None):
    """
    연준별 그룹화된 시리즈 딕셔너리 생성 (us_eco_utils 호환)
    
    Args:
        enabled_banks: 사용할 연준 리스트 (None이면 모든 활성화된 연준)
    
    Returns:
        dict: {group_name: {series_name: series_id}} 형태의 그룹 딕셔너리
    """
    if enabled_banks is None:
        enabled_banks = [bank for bank, config in FEDERAL_RESERVE_BANKS.items() if config['enabled']]
    
    series_groups = {}
    
    for bank_name in enabled_banks:
        if bank_name not in FEDERAL_RESERVE_BANKS:
            continue
            
        bank_config = FEDERAL_RESERVE_BANKS[bank_name]
        bank_series = bank_config['series']
        
        # 각 연준의 섹터별로 그룹 생성
        for sector_name, sector_series in bank_series.items():
            group_name = f"{bank_name}_{sector_name}"
            
            # 시리즈명을 연준_섹터_지표명 형태로 변환
            group_series = {}
            for indicator_name, fred_id in sector_series.items():
                series_name = f"{bank_name}_{sector_name}_{indicator_name}"
                group_series[series_name] = fred_id
            
            series_groups[group_name] = group_series
    
    return series_groups

# %%
# === 전역 데이터 저장소 ===

# FRED 세션
FRED_SESSION = None

# CSV 파일 경로
CSV_FILE_PATH = data_path('fed_pmi_data.csv')
META_FILE_PATH = data_path('fed_pmi_meta.json')

# 전역 데이터 저장소
FED_PMI_DATA = {
    'raw_data': pd.DataFrame(),          # 원본 확산지수 데이터
    'diffusion_data': pd.DataFrame(),    # 0 기준선 대비 확산지수
    'mom_data': pd.DataFrame(),          # 전월대비 변화
    'latest_values': {},                 # 최신 값들
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0,
        'fed_banks': []
    }
}


# %%
# === 연준 PMI 전용 계산 함수들 ===

def calculate_diffusion_index(data):
    """확산지수 계산 (0 기준선 대비 - PMI는 50이 기준이지만 연준 확산지수는 0이 기준)"""
    return data.copy()  # 연준 확산지수는 이미 0 기준선 대비로 계산됨

# %%
# === Enhanced 데이터 로드 함수 ===

def load_all_fed_pmi_data_enhanced(start_date='2020-01-01', force_reload=False, smart_update=True, enabled_banks=None):
    """
    모든 연준 PMI 데이터 로드 (그룹별 스마트 업데이트 지원)
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
        enabled_banks: 수집할 연준 리스트
    
    Returns:
        bool: 로드 성공 여부
    """
    global FED_PMI_DATA
    
    print("🚀 연준 PMI 데이터 로딩 시작 (그룹별 스마트 업데이트)")
    print("="*60)
    
    # 이미 로드된 경우 스킵 (강제 재로드가 아닌 경우)
    if FED_PMI_DATA['load_info']['loaded'] and not force_reload and not smart_update:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    try:
        # 연준별 시리즈 그룹 생성
        series_groups = build_fed_series_groups(enabled_banks)
        
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
            tolerance=5.0  # 확산지수용 허용 오차
        )
        
        if result is None:
            print("❌ 데이터 로딩 실패")
            return False
        
        # 전역 저장소에 결과 저장
        raw_data = result['raw_data']
        
        if raw_data.empty or len(raw_data.columns) < 3:
            print(f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data.columns)}개")
            return False
        
        # 전역 저장소 업데이트
        FED_PMI_DATA['raw_data'] = raw_data
        FED_PMI_DATA['diffusion_data'] = calculate_diffusion_index(raw_data)
        FED_PMI_DATA['mom_data'] = calculate_mom_change(raw_data)
        
        # 최신 값 저장
        latest_values = {}
        for col in raw_data.columns:
            if not raw_data[col].isna().all():
                latest_values[col] = raw_data[col].dropna().iloc[-1]
        FED_PMI_DATA['latest_values'] = latest_values
        
        # 로드 정보 업데이트 (그룹별 정보 추가)
        load_info = result['load_info']
        
        # 연준 이름으로 변환 (올바른 매핑)
        fed_banks = []
        groups_checked = load_info.get('groups_checked', [])
        for group_name in groups_checked:
            # new_york의 경우 특별 처리
            if group_name.startswith('new_york_'):
                fed_name = 'new_york'
            else:
                fed_name = group_name.split('_')[0]  # philadelphia_manufacturing -> philadelphia
            
            # FEDERAL_RESERVE_BANKS에 존재하는지 확인하고 추가
            if fed_name in FEDERAL_RESERVE_BANKS and fed_name not in fed_banks:
                fed_banks.append(fed_name)
        
        FED_PMI_DATA['load_info'] = {
            'loaded': True,
            'load_time': load_info['load_time'],
            'start_date': load_info['start_date'],
            'series_count': load_info['series_count'],
            'data_points': load_info['data_points'],
            'fed_banks': fed_banks,
            'source': load_info['source'],
            'groups_checked': groups_checked,
            'groups_updated': load_info.get('groups_updated', []),
            'consistency_results': load_info.get('consistency_results', {})
        }
        
        # CSV 저장 (그룹별 업데이트인 경우 이미 저장됨)
        if 'CSV' not in load_info.get('source', ''):
            # us_eco_utils의 save_data_to_csv 함수 사용
            save_data_to_csv(raw_data, CSV_FILE_PATH)
        
        print("\n✅ 연준 PMI 데이터 로딩 완료!")
        print_load_info()
        
        # 그룹별 업데이트 결과 요약
        if 'groups_updated' in load_info and load_info['groups_updated']:
            print(f"\n📝 업데이트된 그룹:")
            for group in load_info['groups_updated']:
                fed_name = group.split('_')[0]
                sector_name = '_'.join(group.split('_')[1:])
                fed_display = FEDERAL_RESERVE_BANKS.get(fed_name, {}).get('name', fed_name)
                print(f"   {fed_display} ({sector_name})")
        elif 'groups_checked' in load_info:
            print(f"\n✅ 모든 그룹 데이터 일치 (업데이트 불필요)")
        
        return True
        
    except Exception as e:
        print(f"❌ 데이터 로딩 실패: {e}")
        import traceback
        print("상세 오류:")
        print(traceback.format_exc())
        return False

def print_load_info():
    """로드 정보 출력"""
    info = FED_PMI_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
    else:
        print(f"📊 로드된 데이터 정보:")
        print(f"   시리즈 개수: {info['series_count']}")
        print(f"   데이터 포인트: {info['data_points']}")
        print(f"   시작 날짜: {info['start_date']}")
        print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   데이터 소스: {info.get('source', 'API')}")
        
        if info.get('fed_banks'):
            # KeyError 방지를 위한 안전한 접근
            bank_names = []
            for bank in info['fed_banks']:
                if bank in FEDERAL_RESERVE_BANKS:
                    bank_names.append(FEDERAL_RESERVE_BANKS[bank]['name'])
                else:
                    print(f"⚠️ 알 수 없는 연준 코드: {bank}")
            
            if bank_names:
                print(f"   포함된 연준: {', '.join(bank_names)}")
        
        if not FED_PMI_DATA['raw_data'].empty:
            date_range = f"{FED_PMI_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {FED_PMI_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
            print(f"   데이터 기간: {date_range}")

# %%
# === 유틸리티 함수들 ===

def get_series_korean_label(series_name):
    """
    시리즈명을 한국어 라벨로 변환
    
    Args:
        series_name: 시리즈명 (예: 'chicago_economic_conditions_capx_expectations')
    
    Returns:
        str: 한국어 라벨
    """
    # 기본 패턴들 정의
    patterns = {
        # 시카고 연준 패턴
        'chicago_economic_conditions_': '시카고 연준 ',
        'chicago_': '시카고 연준 ',
        
        # 필라델피아 연준 패턴  
        'philadelphia_manufacturing_': '필라델피아 연준 제조업 ',
        'philadelphia_nonmanufacturing_': '필라델피아 연준 비제조업 ',
        'philadelphia_': '필라델피아 연준 ',
        
        # 뉴욕 연준 패턴
        'new_york_manufacturing_': '뉴욕 연준 제조업 ',
        'new_york_services_': '뉴욕 연준 서비스업 ',
        'new_york_': '뉴욕 연준 ',
        
        # 텍사스(댈러스) 연준 패턴
        'dallas_manufacturing_': '댈러스 연준 제조업 ',
        'dallas_retail_': '댈러스 연준 소매업 ',
        'texas_manufacturing_': '텍사스 연준 제조업 ',
        'dallas_': '댈러스 연준 ',
        'texas_': '텍사스 연준 '
    }
    
    # 패턴 매칭으로 접두사 찾기
    prefix = ''
    indicator = series_name
    
    for pattern, korean_prefix in patterns.items():
        if series_name.startswith(pattern):
            prefix = korean_prefix
            indicator = series_name[len(pattern):]
            break
    
    # 지표명 한국어 변환
    korean_indicator = FED_PMI_KOREAN_NAMES.get(indicator, indicator)
    
    # 최종 라벨 생성
    if prefix:
        return f"{prefix}{korean_indicator}"
    else:
        return korean_indicator

# %%
# === 데이터 접근 함수들 ===

def get_raw_data(series_names=None):
    """원본 확산지수 데이터 반환"""
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_fed_pmi_data_enhanced()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return FED_PMI_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in FED_PMI_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return FED_PMI_DATA['raw_data'][available_series].copy()

def get_diffusion_data(series_names=None):
    """확산지수 데이터 반환 (0 기준선 대비)"""
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_fed_pmi_data_enhanced()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return FED_PMI_DATA['diffusion_data'].copy()
    
    available_series = [s for s in series_names if s in FED_PMI_DATA['diffusion_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return FED_PMI_DATA['diffusion_data'][available_series].copy()

# %%
# === 사용자 정의 시각화 함수들 ===

def list_available_series():
    """
    사용 가능한 모든 시리즈 목록 출력
    
    Returns:
        dict: 연준별 사용 가능한 시리즈 정보
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return {}
    
    print("📊 사용 가능한 연준 PMI 시리즈 목록")
    print("="*60)
    
    series_info = {}
    all_columns = FED_PMI_DATA['raw_data'].columns.tolist()
    
    # 연준별로 그룹화
    fed_groups = {
        'philadelphia': [],
        'new_york': [],
        'chicago': [],
        'dallas': []
    }
    
    for col in all_columns:
        for fed_key in fed_groups.keys():
            if col.startswith(f"{fed_key}_"):
                fed_groups[fed_key].append(col)
                break
    
    # 연준별 출력
    fed_names = {
        'philadelphia': '필라델피아 연준',
        'new_york': '뉴욕 연준', 
        'chicago': '시카고 연준',
        'dallas': '댈러스 연준'
    }
    
    for fed_key, fed_name in fed_names.items():
        if fed_groups[fed_key]:
            print(f"\n🏦 {fed_name} ({len(fed_groups[fed_key])}개 시리즈)")
            print("-" * 40)
            
            # 섹터별로 분류
            manufacturing_series = [s for s in fed_groups[fed_key] if 'manufacturing' in s]
            nonmanufacturing_series = [s for s in fed_groups[fed_key] if 'nonmanufacturing' in s or 'services' in s]
            retail_series = [s for s in fed_groups[fed_key] if 'retail' in s]
            economic_series = [s for s in fed_groups[fed_key] if 'economic' in s]
            
            if manufacturing_series:
                print(f"  📈 제조업 ({len(manufacturing_series)}개):")
                for series in manufacturing_series[:5]:  # 처음 5개만 표시
                    korean_name = get_series_korean_label(series)
                    print(f"    • {series}")
                    print(f"      → {korean_name}")
                if len(manufacturing_series) > 5:
                    print(f"    ... 외 {len(manufacturing_series)-5}개 더")
            
            if nonmanufacturing_series:
                print(f"  🏢 비제조업/서비스업 ({len(nonmanufacturing_series)}개):")
                for series in nonmanufacturing_series[:3]:
                    korean_name = get_series_korean_label(series)
                    print(f"    • {series}")
                    print(f"      → {korean_name}")
                if len(nonmanufacturing_series) > 3:
                    print(f"    ... 외 {len(nonmanufacturing_series)-3}개 더")
            
            if retail_series:
                print(f"  🛒 소매업 ({len(retail_series)}개):")
                for series in retail_series[:3]:
                    korean_name = get_series_korean_label(series)
                    print(f"    • {series}")
                    print(f"      → {korean_name}")
                if len(retail_series) > 3:
                    print(f"    ... 외 {len(retail_series)-3}개 더")
            
            if economic_series:
                print(f"  💼 경제 종합 ({len(economic_series)}개):")
                for series in economic_series[:3]:
                    korean_name = get_series_korean_label(series)
                    print(f"    • {series}")
                    print(f"      → {korean_name}")
                if len(economic_series) > 3:
                    print(f"    ... 외 {len(economic_series)-3}개 더")
            
            series_info[fed_key] = {
                'manufacturing': manufacturing_series,
                'nonmanufacturing': nonmanufacturing_series,
                'retail': retail_series,
                'economic': economic_series
            }
    
    print(f"\n📋 전체 요약:")
    print(f"   총 {len(all_columns)}개 시리즈 사용 가능")
    print(f"   데이터 기간: {FED_PMI_DATA['raw_data'].index.min().strftime('%Y-%m')} ~ {FED_PMI_DATA['raw_data'].index.max().strftime('%Y-%m')}")
    
    print(f"\n💡 사용법:")
    print(f"   create_custom_multi_line_chart(['series1', 'series2', ...])")
    print(f"   create_custom_dual_axis_chart(['left_series'], ['right_series'])")
    
    return series_info

# %%
# === 범용 시각화 함수 ===
def plot_fed_pmi_series_advanced(series_list, chart_type='multi_line', 
                                  data_type='raw', periods=None, target_date=None):
    """범용 연준 PMI 시각화 함수 - plot_economic_series 활용"""
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하세요.")
        return None

    # us_eco_utils의 plot_economic_series 함수 사용
    from us_eco_utils import plot_economic_series
    
    # 전체 시리즈명 기반 한국어 매핑 생성
    full_korean_names = {}
    if not FED_PMI_DATA['raw_data'].empty:
        for col in FED_PMI_DATA['raw_data'].columns:
            full_korean_names[col] = get_series_korean_label(col)
    
    return plot_economic_series(
        data_dict=FED_PMI_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=full_korean_names
    )

# %%
# === 데이터 Export 함수 ===
def export_fed_pmi_data(series_list, data_type='raw', periods=None, 
                        target_date=None, export_path=None, file_format='excel'):
    """연준 PMI 데이터 export 함수 - export_economic_data 활용"""
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하세요.")
        return None

    # us_eco_utils의 export_economic_data 함수 사용
    from us_eco_utils import export_economic_data
    
    # 전체 시리즈명 기반 한국어 매핑 생성
    full_korean_names = {}
    if not FED_PMI_DATA['raw_data'].empty:
        for col in FED_PMI_DATA['raw_data'].columns:
            full_korean_names[col] = get_series_korean_label(col)
    
    return export_economic_data(
        data_dict=FED_PMI_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=full_korean_names,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 추가 데이터 접근 함수들 ===

def clear_fed_pmi_data():
    """연준 PMI 데이터 초기화"""
    global FED_PMI_DATA
    FED_PMI_DATA = {
        'raw_data': pd.DataFrame(),
        'diffusion_data': pd.DataFrame(),
        'mom_data': pd.DataFrame(),
        'latest_values': {},
        'load_info': {
            'loaded': False,
            'load_time': None,
            'start_date': None,
            'series_count': 0,
            'data_points': 0,
            'fed_banks': []
        }
    }
    print("🗑️ 연준 PMI 데이터가 초기화되었습니다")

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not FED_PMI_DATA['load_info']['loaded'] or 'mom_data' not in FED_PMI_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_fed_pmi_data_enhanced()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return FED_PMI_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in FED_PMI_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return FED_PMI_DATA['mom_data'][available_series].copy()

def get_data_status():
    """현재 데이터 상태 반환"""
    if not FED_PMI_DATA['load_info']['loaded']:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': FED_PMI_DATA['load_info']['loaded'],
        'series_count': FED_PMI_DATA['load_info']['series_count'],
        'available_series': list(FED_PMI_DATA['raw_data'].columns) if not FED_PMI_DATA['raw_data'].empty else [],
        'load_info': FED_PMI_DATA['load_info']
    }

def show_available_series():
    """사용 가능한 연준 PMI 시리즈 표시"""
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return
    
    print("=== 사용 가능한 연준 PMI 시리즈 ===")
    
    all_columns = FED_PMI_DATA['raw_data'].columns.tolist()
    
    # 연준별로 그룹화
    fed_groups = {
        'philadelphia': [],
        'new_york': [],
        'chicago': [],
        'dallas': []
    }
    
    for col in all_columns:
        for fed_key in fed_groups.keys():
            if col.startswith(f"{fed_key}_"):
                fed_groups[fed_key].append(col)
                break
    
    # 연준별 출력
    fed_names = {
        'philadelphia': '필라델피아 연준',
        'new_york': '뉴욕 연준', 
        'chicago': '시카고 연준',
        'dallas': '댈러스 연준'
    }
    
    for fed_key, fed_name in fed_names.items():
        if fed_groups[fed_key]:
            print(f"\n🏦 {fed_name} ({len(fed_groups[fed_key])}개 시리즈)")
            for series in fed_groups[fed_key][:5]:  # 처음 5개만 표시
                korean_name = get_series_korean_label(series)
                print(f"  • {series}")
                print(f"    → {korean_name}")
            if len(fed_groups[fed_key]) > 5:
                print(f"  ... 외 {len(fed_groups[fed_key])-5}개 더")

# %%
# === 사용 예시 및 설명 ===

print("=== 리팩토링된 연준 PMI 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_all_fed_pmi_data_enhanced()  # 그룹별 스마트 업데이트")
print("   load_all_fed_pmi_data_enhanced(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_fed_pmi_series_advanced(['philadelphia_manufacturing_general_activity', 'chicago_economic_conditions_activity_index'], 'multi_line', 'raw')")
print("   plot_fed_pmi_series_advanced(['new_york_manufacturing_general_business_conditions'], 'horizontal_bar', 'mom')")
print("   plot_fed_pmi_series_advanced(['dallas_manufacturing_general_business_activity'], 'single_line', 'raw', periods=24)")
print()
print("3. 🔥 데이터 Export:")
print("   export_fed_pmi_data(['philadelphia_manufacturing_general_activity', 'chicago_economic_conditions_activity_index'], 'raw')")
print("   export_fed_pmi_data(['new_york_manufacturing_general_business_conditions'], 'mom', periods=24, file_format='csv')")
print("   export_fed_pmi_data(['dallas_manufacturing_general_business_activity'], 'raw', target_date='2024-06-01')")
print()
print("4. 📋 데이터 확인:")
print("   show_available_series()  # 사용 가능한 모든 시리즈 목록")
print("   get_raw_data()  # 원본 확산지수 데이터")
print("   get_diffusion_data()  # 0 기준선 대비 확산지수")
print("   get_mom_data()  # 전월대비 변화 데이터")
print("   get_data_status()  # 현재 데이터 상태")
print()
print("✅ plot_fed_pmi_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_fed_pmi_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")
print()
print("🔥 그룹별 스마트 업데이트 특징:")
print("   • 필라델피아 연준만 새 데이터가 있으면 필라델피아만 업데이트")
print("   • 시카고 연준만 새 데이터가 있으면 시카고만 업데이트")
print("   • 전체 재로드 없이 부분 업데이트로 효율성 극대화")
print("   • 각 연준의 발표 일정에 맞춰 개별 스마트 업데이트")
print()
print("🏦 지원되는 연준:")
for bank_name, config in FEDERAL_RESERVE_BANKS.items():
    if config['enabled']:
        sectors = list(config['series'].keys())
        print(f"   • {config['name']}: {', '.join(sectors)}")
print()
print("💡 연준별 발표 일정:")
print("   • 필라델피아: 매월 17일경 (제조업), 매월 23일경 (비제조업)")
print("   • 뉴욕: 매월 15일경 (제조업), 매월 20일경 (서비스업)")
print("   • 시카고: 매월 마지막 금요일")
print("   • 댈러스: 매월 마지막 월요일 (제조업), 격월 (소매업)")
print()
print("🎯 최적화된 워크플로:")
print("   1. 매일 load_all_fed_pmi_data_enhanced() 실행")
print("   2. 새로운 데이터가 있는 연준만 자동 업데이트")
print("   3. plot_fed_pmi_series_advanced()로 시각화")
print("   4. export_fed_pmi_data()로 데이터 내보내기")
print("   5. 효율적이고 빠른 데이터 관리!")

# %%
# === 수정 내역 (2025-08-22) ===
# 1. KeyError: 'new' 오류 수정
#    - fed_banks 파싱 로직에서 'new_york' 특별 처리 추가
#    - print_load_info 함수에 안전성 체크 추가
# 2. 한국어 매핑 문제 수정  
#    - plot_fed_pmi_series_advanced에서 full_korean_names 생성
#    - get_series_korean_label 활용한 동적 매핑
#    - export_fed_pmi_data에도 동일한 방식 적용

# %%
load_all_fed_pmi_data_enhanced()
plot_fed_pmi_series_advanced(['philadelphia_manufacturing_general_activity', 'chicago_economic_conditions_activity_index'], 'multi_line', 'raw')

# %%
