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

# 필수 라이브러리들
try:
    import requests
    FRED_API_AVAILABLE = True
    print("✓ FRED API 연동 가능 (requests 라이브러리)")
except ImportError:
    print("⚠️ requests 라이브러리가 없습니다. 설치하세요: pip install requests")
    FRED_API_AVAILABLE = False

# FRED API 키 설정
FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'  # ADP 파일에서 가져온 키

# KPDS 시각화 라이브러리 불러오기 (필수)
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

print("✓ KPDS 시각화 포맷 로드됨")

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

# %%
# === 전역 데이터 저장소 ===

# FRED 세션
FRED_SESSION = None

# CSV 파일 경로
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/fed_pmi_data.csv'
META_FILE_PATH = '/home/jyp0615/us_eco/data/fed_pmi_meta.json'

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
# === CSV 데이터 관리 함수들 ===

def ensure_data_directory():
    """데이터 디렉터리 생성 확인"""
    data_dir = os.path.dirname(CSV_FILE_PATH)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"📁 데이터 디렉터리 생성: {data_dir}")

def save_fed_pmi_data_to_csv():
    """
    현재 연준 PMI 데이터를 CSV 파일로 저장
    
    Returns:
        bool: 저장 성공 여부
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 저장할 데이터가 없습니다. 먼저 데이터를 로드하세요.")
        return False
    
    ensure_data_directory()
    
    try:
        # raw_data와 메타데이터를 함께 저장
        raw_data = FED_PMI_DATA['raw_data']
        
        # 저장할 데이터 준비
        save_data = {
            'timestamp': raw_data.index.tolist(),
            **{col: raw_data[col].tolist() for col in raw_data.columns}
        }
        
        # DataFrame으로 변환하여 저장
        save_df = pd.DataFrame(save_data)
        save_df.to_csv(CSV_FILE_PATH, index=False, encoding='utf-8')
        
        # 메타데이터도 별도 저장
        with open(META_FILE_PATH, 'w', encoding='utf-8') as f:
            meta_data = {
                'load_time': FED_PMI_DATA['load_info']['load_time'].isoformat() if FED_PMI_DATA['load_info']['load_time'] else None,
                'start_date': FED_PMI_DATA['load_info']['start_date'],
                'series_count': FED_PMI_DATA['load_info']['series_count'],
                'data_points': FED_PMI_DATA['load_info']['data_points'],
                'fed_banks': FED_PMI_DATA['load_info']['fed_banks'],
                'latest_values': FED_PMI_DATA['latest_values']
            }
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 연준 PMI 데이터 저장 완료: {CSV_FILE_PATH}")
        print(f"✅ 메타데이터 저장 완료: {META_FILE_PATH}")
        return True
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_fed_pmi_data_from_csv():
    """
    CSV 파일에서 연준 PMI 데이터 로드
    
    Returns:
        bool: 로드 성공 여부
    """
    global FED_PMI_DATA
    
    if not os.path.exists(CSV_FILE_PATH):
        print(f"⚠️ CSV 파일이 없습니다: {CSV_FILE_PATH}")
        return False
    
    try:
        # CSV 데이터 로드
        df = pd.read_csv(CSV_FILE_PATH, encoding='utf-8')
        
        # timestamp 컬럼을 날짜 인덱스로 변환
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        # 메타데이터 로드
        latest_values = {}
        fed_banks = []
        if os.path.exists(META_FILE_PATH):
            with open(META_FILE_PATH, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
                latest_values = meta_data.get('latest_values', {})
                fed_banks = meta_data.get('fed_banks', [])
        
        # 전역 저장소에 저장
        FED_PMI_DATA['raw_data'] = df
        FED_PMI_DATA['diffusion_data'] = calculate_diffusion_index(df)
        FED_PMI_DATA['mom_data'] = calculate_mom_change(df)
        FED_PMI_DATA['latest_values'] = latest_values
        FED_PMI_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': df.index[0].strftime('%Y-%m-%d') if len(df) > 0 else None,
            'series_count': len(df.columns),
            'data_points': len(df),
            'fed_banks': fed_banks
        }
        
        print(f"✅ CSV에서 연준 PMI 데이터 로드 완료: {CSV_FILE_PATH}")
        print_load_info()
        return True
        
    except Exception as e:
        print(f"❌ CSV 로드 실패: {e}")
        return False

def check_recent_data_consistency_enhanced():
    """
    연준별 개별 일치성 확인 - 각 연준의 발표 시점이 다르므로 개별 확인
    
    Returns:
        dict: 일치성 확인 결과 (need_update, reason, fed_updates)
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        return {'need_update': True, 'reason': '기존 데이터 없음', 'fed_updates': {}}
    
    print("🔍 연준별 개별 데이터 일치성 확인 중...")
    
    try:
        existing_data = FED_PMI_DATA['raw_data']
        fed_updates = {}
        overall_need_update = False
        update_reasons = []
        
        # 각 연준별 메인 지표 정의
        fed_main_indicators = {
            'philadelphia': {
                'series': 'philadelphia_manufacturing_general_activity',
                'fred_id': PHILADELPHIA_MANUFACTURING_SERIES['general_activity'],
                'name': '필라델피아 연준'
            },
            'new_york': {
                'series': 'new_york_manufacturing_general_business_conditions', 
                'fred_id': NEW_YORK_MANUFACTURING_SERIES['general_business_conditions'],
                'name': '뉴욕 연준'
            },
            'chicago': {
                'series': 'chicago_economic_conditions_activity_index',
                'fred_id': CHICAGO_ECONOMIC_SERIES['activity_index'], 
                'name': '시카고 연준'
            },
            'dallas': {
                'series': 'dallas_manufacturing_general_business_activity',
                'fred_id': TEXAS_MANUFACTURING_SERIES['general_business_activity'],
                'name': '댈러스 연준'
            }
        }
        
        print("\n📊 각 연준별 일치성 확인:")
        
        for fed_name, indicator in fed_main_indicators.items():
            series_name = indicator['series']
            fred_id = indicator['fred_id']
            display_name = indicator['name']
            
            print(f"\n🏦 {display_name} 확인 중...")
            
            # 기존 데이터 확인
            if series_name not in existing_data.columns:
                print(f"   ⚠️ 기존 데이터 없음")
                fed_updates[fed_name] = {
                    'need_update': True,
                    'reason': '기존 데이터 없음',
                    'existing_date': None,
                    'api_date': None
                }
                overall_need_update = True
                update_reasons.append(f"{fed_name}(데이터없음)")
                continue
            
            # 기존 데이터 최신 날짜
            existing_series = existing_data[series_name].dropna()
            if existing_series.empty:
                print(f"   ⚠️ 기존 데이터 비어있음")
                fed_updates[fed_name] = {
                    'need_update': True, 
                    'reason': '기존 데이터 비어있음',
                    'existing_date': None,
                    'api_date': None
                }
                overall_need_update = True
                update_reasons.append(f"{fed_name}(빈데이터)")
                continue
                
            existing_latest_date = existing_series.index[-1]
            print(f"   기존 데이터 최신: {existing_latest_date.strftime('%Y-%m')}")
            
            # 최근 3개월 데이터만 API에서 가져와서 확인 - 경량화
            recent_start = (datetime.datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            recent_api_data = get_fred_data(fred_id, recent_start)
            
            if recent_api_data is None or recent_api_data.empty:
                print(f"   ⚠️ API 데이터 조회 실패")
                fed_updates[fed_name] = {
                    'need_update': True,
                    'reason': 'API 조회 실패', 
                    'existing_date': existing_latest_date,
                    'api_date': None
                }
                overall_need_update = True
                update_reasons.append(f"{fed_name}(API실패)")
                continue
            
            api_latest_date = recent_api_data.index[-1]
            print(f"   API 데이터 최신: {api_latest_date.strftime('%Y-%m')}")
            
            # 날짜 비교 (월 단위로 비교)
            existing_month = existing_latest_date.to_period('M')
            api_month = api_latest_date.to_period('M')
            
            print(f"   비교: 기존 {existing_month} vs API {api_month}")
            
            if api_month > existing_month:
                print(f"   🆕 새로운 데이터 발견: {existing_latest_date.strftime('%Y-%m')} → {api_latest_date.strftime('%Y-%m')}")
                fed_updates[fed_name] = {
                    'need_update': True,
                    'reason': f'새로운 데이터 ({api_latest_date.strftime("%Y-%m")})',
                    'existing_date': existing_latest_date,
                    'api_date': api_latest_date
                }
                overall_need_update = True
                update_reasons.append(f"{fed_name}({api_latest_date.strftime('%Y-%m')})")
                continue
            
            # 값 비교 (확산지수 허용 오차: 5.0)
            common_dates = existing_series.index.intersection(recent_api_data.index)
            value_mismatch = False
            
            if len(common_dates) > 0:
                tolerance = 5.0  # 확산지수 허용 오차
                for date in common_dates[-3:]:  # 최근 3개 데이터만 확인
                    existing_val = existing_series[date]
                    api_val = recent_api_data[date]
                    
                    if pd.notna(existing_val) and pd.notna(api_val):
                        diff = abs(existing_val - api_val)
                        if diff > tolerance:
                            print(f"   🚨 값 불일치 발견 ({date.strftime('%Y-%m')}): {existing_val:.1f} vs {api_val:.1f} (diff: {diff:.1f})")
                            fed_updates[fed_name] = {
                                'need_update': True,
                                'reason': f'값 불일치 ({date.strftime("%Y-%m")}): {diff:.1f} 차이',
                                'existing_date': existing_latest_date,
                                'api_date': api_latest_date
                            }
                            overall_need_update = True
                            update_reasons.append(f"{fed_name}(값불일치)")
                            value_mismatch = True
                            break
            
            if not value_mismatch:
                print(f"   ✅ 데이터 일치 (같은 월, 값도 동일)")
                fed_updates[fed_name] = {
                    'need_update': False,
                    'reason': '데이터 일치',
                    'existing_date': existing_latest_date, 
                    'api_date': api_latest_date
                }
        
        # 전체 결과 요약
        if overall_need_update:
            reason = f"연준별 업데이트 필요: {', '.join(update_reasons)}"
            print(f"\n🚨 전체 결과: 업데이트 필요 - {reason}")
            print(f"🔧 디버그: overall_need_update={overall_need_update}, update_reasons={update_reasons}")
        else:
            reason = "모든 연준 데이터 일치"
            print(f"\n✅ 전체 결과: {reason}")
            print(f"🔧 디버그: overall_need_update={overall_need_update}, fed_updates 수={len(fed_updates)}")
        
        return {
            'need_update': overall_need_update,
            'reason': reason,
            'fed_updates': fed_updates
        }
    
    except Exception as e:
        print(f"⚠️ 일치성 확인 중 오류: {e}")
        return {
            'need_update': True, 
            'reason': f'확인 오류: {str(e)}',
            'fed_updates': {}
        }

# %%
# === 분석 계산 함수들 ===

def calculate_diffusion_index(data):
    """확산지수 계산 (0 기준선 대비 - PMI는 50이 기준이지만 연준 확산지수는 0이 기준)"""
    return data.copy()  # 연준 확산지수는 이미 0 기준선 대비로 계산됨

def calculate_mom_change(data):
    """전월대비 변화량 계산"""
    return data.diff()

# %%
# === FRED API 초기화 및 데이터 가져오기 함수 ===

def initialize_fred_api():
    """FRED API 세션 초기화"""
    global FRED_SESSION
    
    if not FRED_API_AVAILABLE:
        print("⚠️ FRED API 사용 불가 (requests 라이브러리 없음)")
        return False
    
    if not FRED_API_KEY or FRED_API_KEY == 'YOUR_FRED_API_KEY_HERE':
        print("⚠️ FRED API 키가 설정되지 않았습니다. FRED_API_KEY를 설정하세요.")
        print("  https://fred.stlouisfed.org/docs/api/api_key.html 에서 무료로 발급 가능합니다.")
        return False
    
    try:
        FRED_SESSION = requests.Session()
        print("✓ FRED API 세션 초기화 성공")
        return True
    except Exception as e:
        print(f"⚠️ FRED API 초기화 실패: {e}")
        return False

def get_fred_data(series_id, start_date='2020-01-01', end_date=None):
    """
    FRED API에서 데이터 가져오기
    
    Args:
        series_id: FRED 시리즈 ID
        start_date: 시작 날짜
        end_date: 종료 날짜 (None이면 현재)
    
    Returns:
        pandas.Series: 날짜를 인덱스로 하는 시리즈 데이터
    """
    if not FRED_API_AVAILABLE or FRED_SESSION is None:
        print(f"❌ FRED API 사용 불가 - {series_id}")
        return None
    
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # FRED API URL 구성
    url = f'https://api.stlouisfed.org/fred/series/observations'
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': start_date,
        'observation_end': end_date,
        'sort_order': 'asc'
    }
    
    try:
        print(f"📊 FRED에서 로딩: {series_id}")
        response = FRED_SESSION.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'observations' in data:
            observations = data['observations']
            
            # 데이터 정리
            dates = []
            values = []
            
            for obs in observations:
                try:
                    date = pd.to_datetime(obs['date'])
                    # '.' 값은 NaN으로 처리
                    if obs['value'] == '.':
                        value = np.nan
                    else:
                        value = float(obs['value'])
                    
                    dates.append(date)
                    values.append(value)
                except (ValueError, KeyError):
                    continue
            
            if dates and values:
                series = pd.Series(values, index=dates, name=series_id)
                series = series.sort_index()
                
                print(f"✓ FRED 성공: {series_id} ({len(series.dropna())}개 포인트)")
                return series
            else:
                print(f"❌ FRED 데이터 없음: {series_id}")
                return None
        else:
            print(f"❌ FRED 응답에 데이터 없음: {series_id}")
            return None
            
    except Exception as e:
        print(f"❌ FRED 요청 실패: {series_id} - {e}")
        return None

# %%
# === 연준별 데이터 수집 함수들 ===

def fetch_philadelphia_fed_data(start_date='2020-01-01', sector='both'):
    """
    필라델피아 연준 PMI 데이터 수집
    
    Args:
        start_date: 시작 날짜
        sector: 'manufacturing', 'nonmanufacturing', 'both'
    
    Returns:
        pd.DataFrame: 수집된 데이터
    """
    all_data = pd.DataFrame()
    
    # 수집할 섹터 결정
    sectors_to_fetch = []
    if sector in ['manufacturing', 'both']:
        sectors_to_fetch.append('manufacturing')
    if sector in ['nonmanufacturing', 'both']:
        sectors_to_fetch.append('nonmanufacturing')
    
    print(f"필라델피아 연준 데이터 수집 시작 (섹터: {sector})...")
    
    for sector_name in sectors_to_fetch:
        print(f"\n📊 {sector_name} 데이터 수집 중...")
        
        sector_series = ALL_PHILADELPHIA_SERIES[sector_name]
        
        for indicator_name, series_id in sector_series.items():
            try:
                # 데이터 가져오기
                series_data = get_fred_data(series_id, start_date)
                
                if series_data is not None and not series_data.empty:
                    # 컬럼명 설정 (연준_섹터_지표명)
                    column_name = f"philadelphia_{sector_name}_{indicator_name}"
                    
                    # 기존 데이터프레임과 병합
                    if all_data.empty:
                        all_data = pd.DataFrame({column_name: series_data})
                    else:
                        all_data = all_data.join(pd.DataFrame({column_name: series_data}), how='outer')
                    
                    print(f"  ✓ {indicator_name}: {len(series_data.dropna())} 행 수집완료")
                else:
                    print(f"  ❌ {indicator_name}: 데이터 없음")
                    
            except Exception as e:
                print(f"  ❌ {indicator_name} 데이터 수집 실패: {str(e)}")
                continue
    
    print(f"\n필라델피아 연준 데이터 수집 완료: {len(all_data.columns)}개 시리즈, {len(all_data)} 행")
    return all_data

def fetch_new_york_fed_data(start_date='2020-01-01', sector='both'):
    """
    뉴욕 연준 PMI 데이터 수집
    
    Args:
        start_date: 시작 날짜
        sector: 'manufacturing', 'services', 'both'
    
    Returns:
        pd.DataFrame: 수집된 데이터
    """
    all_data = pd.DataFrame()
    
    # 수집할 섹터 결정
    sectors_to_fetch = []
    if sector in ['manufacturing', 'both']:
        sectors_to_fetch.append('manufacturing')
    if sector in ['services', 'both']:
        sectors_to_fetch.append('services')
    
    print(f"뉴욕 연준 데이터 수집 시작 (섹터: {sector})...")
    
    for sector_name in sectors_to_fetch:
        print(f"\n📊 {sector_name} 데이터 수집 중...")
        
        sector_series = ALL_NEW_YORK_SERIES[sector_name]
        
        for indicator_name, series_id in sector_series.items():
            try:
                # 데이터 가져오기
                series_data = get_fred_data(series_id, start_date)
                
                if series_data is not None and not series_data.empty:
                    # 컬럼명 설정 (연준_섹터_지표명)
                    column_name = f"new_york_{sector_name}_{indicator_name}"
                    
                    # 기존 데이터프레임과 병합
                    if all_data.empty:
                        all_data = pd.DataFrame({column_name: series_data})
                    else:
                        all_data = all_data.join(pd.DataFrame({column_name: series_data}), how='outer')
                    
                    print(f"  ✓ {indicator_name}: {len(series_data.dropna())} 행 수집완료")
                else:
                    print(f"  ❌ {indicator_name}: 데이터 없음")
                    
            except Exception as e:
                print(f"  ❌ {indicator_name} 데이터 수집 실패: {str(e)}")
                continue
    
    print(f"\n뉴욕 연준 데이터 수집 완료: {len(all_data.columns)}개 시리즈, {len(all_data)} 행")
    return all_data

def fetch_chicago_fed_data(start_date='2020-01-01'):
    """
    시카고 연준 경제 상황 조사 데이터 수집
    
    Args:
        start_date: 시작 날짜
    
    Returns:
        pd.DataFrame: 수집된 데이터
    """
    all_data = pd.DataFrame()
    
    print(f"시카고 연준 데이터 수집 시작...")
    
    print(f"\n📊 경제 상황 조사 데이터 수집 중...")
    
    sector_series = ALL_CHICAGO_SERIES['economic_conditions']
    
    for indicator_name, series_id in sector_series.items():
        try:
            # 데이터 가져오기
            series_data = get_fred_data(series_id, start_date)
            
            if series_data is not None and not series_data.empty:
                # 컬럼명 설정 (연준_섹터_지표명)
                column_name = f"chicago_economic_conditions_{indicator_name}"
                
                # 기존 데이터프레임과 병합
                if all_data.empty:
                    all_data = pd.DataFrame({column_name: series_data})
                else:
                    all_data = all_data.join(pd.DataFrame({column_name: series_data}), how='outer')
                
                print(f"  ✓ {indicator_name}: {len(series_data.dropna())} 행 수집완료")
            else:
                print(f"  ❌ {indicator_name}: 데이터 없음")
                
        except Exception as e:
            print(f"  ❌ {indicator_name} 데이터 수집 실패: {str(e)}")
            continue
    
    print(f"\n시카고 연준 데이터 수집 완료: {len(all_data.columns)}개 시리즈, {len(all_data)} 행")
    return all_data

def fetch_dallas_fed_data(start_date='2020-01-01'):
    """
    댈러스(텍사스) 연준 제조업 전망 조사 데이터 수집
    
    Args:
        start_date: 시작 날짜
    
    Returns:
        pd.DataFrame: 수집된 데이터
    """
    all_data = pd.DataFrame()
    
    print(f"댈러스 연준 데이터 수집 시작...")
    
    print(f"\n📊 제조업 전망 조사 데이터 수집 중...")
    
    sector_series = ALL_TEXAS_SERIES['manufacturing']
    
    for indicator_name, series_id in sector_series.items():
        try:
            # 데이터 가져오기
            series_data = get_fred_data(series_id, start_date)
            
            if series_data is not None and not series_data.empty:
                # 컬럼명 설정 (연준_섹터_지표명)
                column_name = f"dallas_manufacturing_{indicator_name}"
                
                # 기존 데이터프레임과 병합
                if all_data.empty:
                    all_data = pd.DataFrame({column_name: series_data})
                else:
                    all_data = all_data.join(pd.DataFrame({column_name: series_data}), how='outer')
                
                print(f"  ✓ {indicator_name}: {len(series_data.dropna())} 행 수집완료")
            else:
                print(f"  ❌ {indicator_name}: 데이터 없음")
                
        except Exception as e:
            print(f"  ❌ {indicator_name} 데이터 수집 실패: {str(e)}")
            continue
    
    # 소매업 전망 조사 데이터 수집 (TROS)
    print(f"\n📊 소매업 전망 조사 데이터 수집 중...")
    
    retail_series = ALL_TEXAS_SERIES['retail']
    
    for indicator_name, series_id in retail_series.items():
        try:
            # 데이터 가져오기
            series_data = get_fred_data(series_id, start_date)
            
            if series_data is not None and not series_data.empty:
                # 컬럼명 설정 (연준_섹터_지표명)
                column_name = f"dallas_retail_{indicator_name}"
                
                # 기존 데이터프레임과 병합
                if all_data.empty:
                    all_data = pd.DataFrame({column_name: series_data})
                else:
                    all_data = all_data.join(pd.DataFrame({column_name: series_data}), how='outer')
                
                print(f"  ✓ {indicator_name}: {len(series_data.dropna())} 행 수집완료")
            else:
                print(f"  ❌ {indicator_name}: 데이터 없음")
                
        except Exception as e:
            print(f"  ❌ {indicator_name} 데이터 수집 실패: {str(e)}")
            continue
    
    print(f"\n댈러스 연준 데이터 수집 완료: {len(all_data.columns)}개 시리즈, {len(all_data)} 행")
    return all_data

def fetch_all_fed_pmi_data(start_date='2020-01-01', enabled_banks=None):
    """
    모든 활성화된 연준의 PMI 데이터 수집
    
    Args:
        start_date: 시작 날짜
        enabled_banks: 수집할 연준 리스트 (None이면 모든 활성화된 연준)
    
    Returns:
        pd.DataFrame: 통합된 데이터
    """
    all_fed_data = pd.DataFrame()
    collected_banks = []
    
    # 활성화된 연준들 확인
    if enabled_banks is None:
        enabled_banks = [bank for bank, config in FEDERAL_RESERVE_BANKS.items() if config['enabled']]
    
    print("연준 PMI 데이터 수집 시작...")
    print(f"수집 대상 연준: {enabled_banks}")
    print("="*50)
    
    for bank_name in enabled_banks:
        if bank_name not in FEDERAL_RESERVE_BANKS:
            print(f"⚠️ 알 수 없는 연준: {bank_name}")
            continue
        
        bank_config = FEDERAL_RESERVE_BANKS[bank_name]
        print(f"\n🏦 {bank_config['name']} (District {bank_config['district']}) 데이터 수집...")
        
        try:
            if bank_name == 'philadelphia':
                bank_data = fetch_philadelphia_fed_data(start_date, 'both')
            elif bank_name == 'new_york':
                bank_data = fetch_new_york_fed_data(start_date, 'both')
            elif bank_name == 'chicago':
                bank_data = fetch_chicago_fed_data(start_date)
            elif bank_name == 'dallas':
                bank_data = fetch_dallas_fed_data(start_date)
            else:
                print(f"⚠️ {bank_name} 연준은 아직 구현되지 않았습니다.")
                continue
            
            if not bank_data.empty:
                # 전체 데이터프레임에 병합
                if all_fed_data.empty:
                    all_fed_data = bank_data
                else:
                    all_fed_data = all_fed_data.join(bank_data, how='outer')
                
                collected_banks.append(bank_name)
                print(f"✅ {bank_config['name']} 데이터 수집 완료")
            else:
                print(f"❌ {bank_config['name']} 데이터 수집 실패")
                
        except Exception as e:
            print(f"❌ {bank_config['name']} 데이터 수집 중 오류: {e}")
            continue
    
    print(f"\n총 {len(collected_banks)}개 연준에서 {len(all_fed_data.columns)}개 시리즈, {len(all_fed_data)} 행 수집완료")
    if collected_banks:
        print(f"수집된 연준: {', '.join([FEDERAL_RESERVE_BANKS[bank]['name'] for bank in collected_banks])}")
    
    return all_fed_data, collected_banks

# %%
# === Enhanced 데이터 로드 함수 ===

def load_all_fed_pmi_data_enhanced(start_date='2020-01-01', force_reload=False, smart_update=True, enabled_banks=None):
    """
    모든 연준 PMI 데이터 로드 (스마트 업데이트 지원)
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
        enabled_banks: 수집할 연준 리스트
    
    Returns:
        bool: 로드 성공 여부
    """
    global FED_PMI_DATA
    
    # 이미 로드된 경우 스킵 (강제 재로드가 아닌 경우)
    if FED_PMI_DATA['load_info']['loaded'] and not force_reload and not smart_update:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    # 스마트 업데이트 로직
    if smart_update and not force_reload:
        print("🤖 스마트 업데이트 모드 활성화")
        
        # 먼저 CSV에서 데이터 로드 시도
        csv_loaded = load_fed_pmi_data_from_csv()
        
        if csv_loaded and not FED_PMI_DATA['raw_data'].empty:
            # CSV 데이터가 이미 전역 저장소에 로드됨
            
            # FRED API 초기화
            if initialize_fred_api():
                # 고도화된 일치성 확인 (ADP 방식 적용)
                consistency_result = check_recent_data_consistency_enhanced()
                
                # 일치성 결과 출력
                print(f"🔍 일치성 확인 결과: {consistency_result.get('reason', '알 수 없음')}")
                
                needs_api_call = consistency_result.get('need_update', True)
                
                if not needs_api_call:
                    print("✅ 최근 데이터가 일치함 - API 호출 건너뛰기")
                    print("🔧 디버그: 스마트 업데이트 성공, 함수 조기 종료")
                    # CSV 데이터를 그대로 사용 
                    # 메타데이터 업데이트
                    latest_values = {}
                    for col in FED_PMI_DATA['raw_data'].columns:
                        if not FED_PMI_DATA['raw_data'][col].isna().all():
                            latest_values[col] = FED_PMI_DATA['raw_data'][col].dropna().iloc[-1]
                    
                    FED_PMI_DATA['latest_values'] = latest_values
                    FED_PMI_DATA['load_info'].update({
                        'load_time': datetime.datetime.now(),
                        'source': 'CSV (스마트 업데이트)',
                        'consistency_check': consistency_result
                    })
                    
                    print("💾 CSV 데이터 사용 (일치성 확인됨)")
                    print_load_info()
                    return True
                else:
                    print("📡 데이터 불일치 감지 - 전체 API 호출 진행")
            else:
                print("⚠️ FRED API 초기화 실패 - 전체 API 호출로 진행")
        else:
            print("⚠️ CSV 로드 실패 - 전체 API 호출로 진행")
    
    # API를 통한 전체 데이터 로드 (스마트 업데이트 실패하거나 비활성화된 경우)
    print("🔧 디버그: 스마트 업데이트 우회, 전체 API 호출 진행")
    print("🚀 연준 PMI 데이터 로딩 시작... (FRED API)")
    print("="*50)
    
    # FRED API 초기화
    if not initialize_fred_api():
        print("❌ FRED API 초기화 실패")
        return False
        
    try:
        # 모든 연준 데이터 수집
        combined_data, collected_banks = fetch_all_fed_pmi_data(start_date, enabled_banks)
        
        if combined_data.empty or len(combined_data.columns) < 3:  # 최소 3개 시리즈는 있어야 함
            error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(combined_data.columns)}개"
            print(error_msg)
            return False
        
        # 날짜 필터링
        if start_date:
            combined_data = combined_data[combined_data.index >= start_date]
        
        # 전역 저장소에 저장
        FED_PMI_DATA['raw_data'] = combined_data
        FED_PMI_DATA['diffusion_data'] = calculate_diffusion_index(combined_data)
        FED_PMI_DATA['mom_data'] = calculate_mom_change(combined_data)
        
        # 최신 값 저장
        latest_values = {}
        for col in combined_data.columns:
            if not combined_data[col].isna().all():
                latest_values[col] = combined_data[col].dropna().iloc[-1]
        FED_PMI_DATA['latest_values'] = latest_values
        
        FED_PMI_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': start_date,
            'series_count': len(combined_data.columns),
            'data_points': len(combined_data),
            'fed_banks': collected_banks,
            'source': 'API (전체 로드)'
        }
        
        if consistency_result:
            FED_PMI_DATA['load_info']['consistency_check'] = consistency_result
        
        # CSV에 저장
        save_fed_pmi_data_to_csv()
        
        print("\n✅ 데이터 로딩 완료!")
        print_load_info()
        
        return True
        
    except Exception as e:
        print(f"❌ 데이터 로딩 실패: {e}")
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
            bank_names = [FEDERAL_RESERVE_BANKS[bank]['name'] for bank in info['fed_banks']]
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
# === 시각화 함수들 (KPDS 포맷) ===

def create_fed_pmi_timeseries_chart(series_names=None, start_date=None):
    """
    연준 PMI 시계열 차트 생성 (KPDS 포맷)
    
    Args:
        series_names: 차트에 표시할 시리즈 리스트
        start_date: 시작 날짜 (예: '2021-01-01')
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    if series_names is None:
        # 필라델피아 연준 주요 지표들 기본 선택
        series_names = [
            'philadelphia_manufacturing_general_activity',
            'philadelphia_nonmanufacturing_general_activity_firm'
        ]
    
    # 데이터 가져오기 (원본 확산지수 데이터)
    df = get_raw_data(series_names)
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 시작 날짜 필터링
    if start_date:
        df = df[df.index >= start_date]
    
    # 라벨 생성 (시리즈명을 한국어로 변환)
    labels = {}
    for col in df.columns:
        korean_label = get_series_korean_label(col)
        labels[col] = korean_label
    
    # 타이틀은 print()로 별도 출력
    print("연준별 PMI 확산지수")
    
    # KPDS 포맷 사용하여 차트 생성
    if len(df.columns) == 1:
        fig = df_line_chart(df, df.columns[0], ytitle="확산지수")
    else:
        fig = df_multi_line_chart(df, df.columns.tolist(), ytitle="확산지수", labels=labels)
    
    # 0선 추가 (확산지수는 0이 기준선)
    fig.add_hline(y=0, line_width=2, line_color="black", opacity=0.8)
    
    return fig

def create_fed_comparison_by_indicator(indicator_type, start_date=None):
    """
    지표별 연준 간 비교 차트 생성 (공통 지표들을 연준별로 비교)
    
    Args:
        indicator_type: 비교할 지표 유형 ('general_activity', 'new_orders', 'employment', 'prices_paid', etc.)
        start_date: 시작 날짜
    
    Returns:
        plotly figure
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 지표별 연준 시리즈 매핑
    indicator_mappings = {
        'general_activity': {
            'philadelphia': 'philadelphia_manufacturing_general_activity',
            'new_york': 'new_york_manufacturing_general_business_conditions', 
            'chicago': 'chicago_economic_conditions_activity_index',
            'dallas': 'dallas_manufacturing_general_business_activity'
        },
        'new_orders': {
            'philadelphia': 'philadelphia_manufacturing_new_orders',
            'new_york': 'new_york_manufacturing_new_orders',
            'dallas': 'dallas_manufacturing_new_orders'
        },
        'employment': {
            'philadelphia': 'philadelphia_manufacturing_employment',
            'new_york': 'new_york_manufacturing_number_of_employees',
            'chicago': 'chicago_economic_conditions_current_hiring',
            'dallas': 'dallas_manufacturing_employment'
        },
        'prices_paid': {
            'philadelphia': 'philadelphia_manufacturing_prices_paid',
            'new_york': 'new_york_manufacturing_prices_paid',
            'chicago': 'chicago_economic_conditions_nonlabor_costs',
            'dallas': 'dallas_manufacturing_prices_paid_raw_materials'
        },
        'production': {
            'philadelphia': 'philadelphia_manufacturing_shipments',
            'new_york': 'new_york_manufacturing_shipments',
            'dallas': 'dallas_manufacturing_production'
        },
        'future_outlook': {
            'philadelphia': 'philadelphia_manufacturing_future_general_activity',
            'new_york': 'new_york_manufacturing_future_general_business_conditions',
            'chicago': 'chicago_economic_conditions_us_economy_outlook',
            'dallas': 'dallas_manufacturing_future_general_business_activity'
        }
    }
    
    if indicator_type not in indicator_mappings:
        print(f"⚠️ 지원되지 않는 지표 유형: {indicator_type}")
        print(f"지원되는 지표: {list(indicator_mappings.keys())}")
        return None
    
    # 해당 지표의 시리즈들 가져오기
    series_mapping = indicator_mappings[indicator_type]
    available_series = []
    
    for fed_name, series_name in series_mapping.items():
        if series_name in FED_PMI_DATA['raw_data'].columns:
            available_series.append(series_name)
    
    if not available_series:
        print(f"⚠️ {indicator_type} 지표에 대한 데이터가 없습니다.")
        return None
    
    # 데이터 가져오기
    df = get_raw_data(available_series)
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 시작 날짜 필터링
    if start_date:
        df = df[df.index >= start_date]
    
    # 라벨 생성
    labels = {}
    for col in df.columns:
        korean_label = get_series_korean_label(col)
        # 지표 유형 제거하고 연준명만 표시
        for fed_name in ['필라델피아 연준', '뉴욕 연준', '시카고 연준', '댈러스 연준', '텍사스 연준']:
            if korean_label.startswith(fed_name):
                labels[col] = fed_name
                break
        else:
            labels[col] = korean_label
    
    # 지표별 한국어 이름
    indicator_korean_names = {
        'general_activity': '일반 활동 지수',
        'new_orders': '신규 주문',
        'employment': '고용',
        'prices_paid': '지불 가격',
        'production': '생산/출하',
        'future_outlook': '미래 전망'
    }
    
    indicator_korean = indicator_korean_names.get(indicator_type, indicator_type)
    
    # 타이틀 출력
    print(f"연준별 {indicator_korean} 비교")
    
    # KPDS 포맷 사용하여 차트 생성
    if len(df.columns) == 1:
        fig = df_line_chart(df, df.columns[0], ytitle="확산지수")
    else:
        fig = df_multi_line_chart(df, df.columns.tolist(), ytitle="확산지수", labels=labels)
    
    # 0선 추가 (확산지수는 0이 기준선)
    fig.add_hline(y=0, line_width=2, line_color="black", opacity=0.8)
    
    return fig

def create_fed_pmi_comparison_chart(fed_banks=None, sector='manufacturing'):
    """
    연준별 PMI 비교 차트 (KPDS 포맷 가로 바 차트)
    
    Args:
        fed_banks: 비교할 연준 리스트
        sector: 'manufacturing' 또는 'nonmanufacturing'
        title: 차트 제목
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    if fed_banks is None:
        fed_banks = FED_PMI_DATA['load_info'].get('fed_banks', ['philadelphia'])
    
    # 최신 날짜 (향후 사용을 위해 유지)
    # latest_date = FED_PMI_DATA['raw_data'].index[-1]
    
    # 메인 지표들 선택 (general_activity)
    series_names = []
    for fed_bank in fed_banks:
        series_name = f"{fed_bank}_{sector}_general_activity"
        if series_name in FED_PMI_DATA['raw_data'].columns:
            series_names.append(series_name)
    
    if not series_names:
        print("⚠️ 비교할 수 있는 시리즈가 없습니다.")
        return None
    
    # 최신 데이터
    latest_data = FED_PMI_DATA['raw_data'][series_names].iloc[-1].dropna()
    
    # 데이터 정렬
    sorted_data = latest_data.sort_values(ascending=True)
    
    # 라벨 및 색상
    categories = []
    values = []
    colors = []
    
    for series_name, value in sorted_data.items():
        # 시리즈명 파싱
        parts = series_name.split('_')
        fed_name = parts[0]
        
        fed_korean = FEDERAL_RESERVE_BANKS.get(fed_name, {}).get('name', fed_name)
        sector_korean = '제조업' if sector == 'manufacturing' else '비제조업'
        
        categories.append(f"{fed_korean}\n{sector_korean}")
        values.append(value)
        
        # 색상: 양수면 상승색, 음수면 하락색
        colors.append(deepred_pds if value >= 0 else deepblue_pds)
    
    # 가로 바 차트 생성 (KPDS 포맷)
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=list(categories),
        x=list(values),
        orientation='h',
        marker_color=list(colors),
        text=[f'{v:+.1f}' for v in values],
        textposition='outside',
        showlegend=False,
        textfont=dict(family='NanumGothic', size=FONT_SIZE_GENERAL)
    ))
    
    # 레이아웃 설정 (KPDS 포맷 준수) - 타이틀 제거
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,  # KPDS 표준 너비
        height=max(300, len(categories) * 80),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            title=dict(text="확산지수", font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.1f',
            showgrid=True, gridwidth=1, gridcolor="lightgrey"
        ),
        yaxis=dict(
            title=dict(text="", font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=False,
            tickcolor='white',
            showgrid=False
        ),
        margin=dict(l=150, r=80, t=80, b=60)
    )
    
    # 0선 추가
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    return fig

def create_new_york_fed_dashboard(start_date='2022-01-01'):
    """
    뉴욕 연준 PMI 대시보드 생성
    
    Args:
        start_date: 분석 시작 날짜
    
    Returns:
        dict: 생성된 차트들
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("🏦 뉴욕 연준 PMI 대시보드 생성 중...")
    print("="*50)
    
    results = {}
    
    try:
        # 1. 제조업 vs 서비스업 메인 지표
        print("📊 1. 제조업 vs 서비스업 일반 활동 지수...")
        main_series = [
            'new_york_manufacturing_general_business_conditions',
            'new_york_services_business_activity'
        ]
        print("뉴욕 연준 - 제조업 vs 서비스업 일반 활동")
        results['main_comparison'] = create_fed_pmi_timeseries_chart(
            main_series, start_date)
        
        # 2. 제조업 세부 지표들
        print("📊 2. 제조업 세부 지표...")
        manufacturing_series = [
            'new_york_manufacturing_new_orders',
            'new_york_manufacturing_shipments',
            'new_york_manufacturing_number_of_employees'
        ]
        print("뉴욕 연준 제조업 - 세부 지표")
        results['manufacturing_details'] = create_fed_pmi_timeseries_chart(
            manufacturing_series, start_date)
        
        # 3. 서비스업 세부 지표들
        print("📊 3. 서비스업 세부 지표...")
        services_series = [
            'new_york_services_employment',
            'new_york_services_business_climate',
            'new_york_services_wages'
        ]
        print("뉴욕 연준 서비스업 - 세부 지표")
        results['services_details'] = create_fed_pmi_timeseries_chart(
            services_series, start_date)
        
        # 4. 가격 관련 지표들
        print("📊 4. 가격 관련 지표...")
        price_series = [
            'new_york_manufacturing_prices_paid',
            'new_york_manufacturing_prices_received',
            'new_york_services_prices_paid',
            'new_york_services_prices_received'
        ]
        print("뉴욕 연준 - 가격 관련 지표")
        results['price_indicators'] = create_fed_pmi_timeseries_chart(
            price_series, start_date)
        
        # 5. 미래 전망 지표들
        print("📊 5. 미래 전망 지표...")
        future_series = [
            'new_york_manufacturing_future_new_orders',
            'new_york_manufacturing_future_number_of_employees',
            'new_york_services_future_business_activity'
        ]
        print("뉴욕 연준 - 미래 전망 지표")
        results['future_outlook'] = create_fed_pmi_timeseries_chart(
            future_series, start_date)
        
    except Exception as e:
        print(f"⚠️ 차트 생성 중 오류: {e}")
    
    print(f"\n✅ 대시보드 완료! 생성된 차트: {len(results)}개")
    return results

def create_philadelphia_fed_dashboard(start_date='2022-01-01'):
    """
    필라델피아 연준 PMI 대시보드 생성
    
    Args:
        start_date: 분석 시작 날짜
    
    Returns:
        dict: 생성된 차트들
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("🏦 필라델피아 연준 PMI 대시보드 생성 중...")
    print("="*50)
    
    results = {}
    
    try:
        # 1. 제조업 vs 비제조업 메인 지표
        print("📊 1. 제조업 vs 비제조업 일반 활동 지수...")
        main_series = [
            'philadelphia_manufacturing_general_activity',
            'philadelphia_nonmanufacturing_general_activity_firm'
        ]
        print("필라델피아 연준 - 제조업 vs 비제조업 일반 활동")
        results['main_comparison'] = create_fed_pmi_timeseries_chart(
            main_series, start_date)
        
        # 2. 제조업 세부 지표들
        print("📊 2. 제조업 세부 지표...")
        manufacturing_series = [
            'philadelphia_manufacturing_new_orders',
            'philadelphia_manufacturing_shipments',
            'philadelphia_manufacturing_employment'
        ]
        print("필라델피아 연준 제조업 - 세부 지표")
        results['manufacturing_details'] = create_fed_pmi_timeseries_chart(
            manufacturing_series, start_date)
        
        # 3. 비제조업 세부 지표들
        print("📊 3. 비제조업 세부 지표...")
        nonmanufacturing_series = [
            'philadelphia_nonmanufacturing_sales_revenues',
            'philadelphia_nonmanufacturing_full_time_employees',
            'philadelphia_nonmanufacturing_average_workweek'
        ]
        print("필라델피아 연준 비제조업 - 세부 지표")
        results['nonmanufacturing_details'] = create_fed_pmi_timeseries_chart(
            nonmanufacturing_series, start_date)
        
        # 4. 가격 관련 지표들
        print("📊 4. 가격 관련 지표...")
        price_series = [
            'philadelphia_manufacturing_prices_paid',
            'philadelphia_manufacturing_prices_received',
            'philadelphia_nonmanufacturing_prices_paid',
            'philadelphia_nonmanufacturing_prices_received'
        ]
        print("필라델피아 연준 - 가격 관련 지표")
        results['price_indicators'] = create_fed_pmi_timeseries_chart(
            price_series, start_date)
        
        # 5. 미래 전망 지표들
        print("📊 5. 미래 전망 지표...")
        future_series = [
            'philadelphia_manufacturing_future_new_orders',
            'philadelphia_manufacturing_future_employment',
            'philadelphia_nonmanufacturing_future_general_activity_firm'
        ]
        print("필라델피아 연준 - 미래 전망 지표")
        results['future_outlook'] = create_fed_pmi_timeseries_chart(
            future_series, start_date)
        
    except Exception as e:
        print(f"⚠️ 차트 생성 중 오류: {e}")
    
    print(f"\n✅ 대시보드 완료! 생성된 차트: {len(results)}개")
    return results

def create_chicago_fed_dashboard(start_date='2022-01-01'):
    """
    시카고 연준 경제 조사 대시보드 생성
    
    Args:
        start_date: 분석 시작 날짜
    
    Returns:
        dict: 생성된 차트들
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("🏦 시카고 연준 경제 조사 대시보드 생성 중...")
    print("="*50)
    
    results = {}
    
    try:
        # 1. 종합 활동 지수들
        print("📊 1. 종합 활동 지수들...")
        activity_series = [
            'chicago_economic_conditions_activity_index',
            'chicago_economic_conditions_manufacturing_activity',
            'chicago_economic_conditions_nonmanufacturing_activity'
        ]
        print("시카고 연준 - 활동 지수")
        results['activity_comparison'] = create_fed_pmi_timeseries_chart(
            activity_series, start_date)
        
        # 2. 고용 관련 지표들
        print("📊 2. 고용 관련 지표들...")
        hiring_series = [
            'chicago_economic_conditions_current_hiring',
            'chicago_economic_conditions_hiring_expectations'
        ]
        print("시카고 연준 - 고용 지표")
        results['hiring_indicators'] = create_fed_pmi_timeseries_chart(
            hiring_series, start_date)
        
        # 3. 비용 관련 지표들
        print("📊 3. 비용 관련 지표들...")
        cost_series = [
            'chicago_economic_conditions_labor_costs',
            'chicago_economic_conditions_nonlabor_costs'
        ]
        print("시카고 연준 - 비용 지표")
        results['cost_indicators'] = create_fed_pmi_timeseries_chart(
            cost_series, start_date)
        
        # 4. 미래 전망 지표들
        print("📊 4. 미래 전망 지표들...")
        outlook_series = [
            'chicago_economic_conditions_capx_expectations',
            'chicago_economic_conditions_us_economy_outlook'
        ]
        print("시카고 연준 - 미래 전망")
        results['outlook_indicators'] = create_fed_pmi_timeseries_chart(
            outlook_series, start_date)
        
    except Exception as e:
        print(f"⚠️ 차트 생성 중 오류: {e}")
    
    print(f"\n✅ 대시보드 완료! 생성된 차트: {len(results)}개")
    return results

def create_dallas_fed_dashboard(start_date='2022-01-01'):
    """
    댈러스 연준 제조업 전망 조사 대시보드 생성
    
    Args:
        start_date: 분석 시작 날짜
    
    Returns:
        dict: 생성된 차트들
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("🏦 댈러스 연준 제조업 전망 조사 대시보드 생성 중...")
    print("="*50)
    
    results = {}
    
    try:
        # 1. 핵심 제조업 지표들
        print("📊 1. 핵심 제조업 지표들...")
        core_series = [
            'dallas_manufacturing_general_business_activity',
            'dallas_manufacturing_production',
            'dallas_manufacturing_new_orders'
        ]
        print("댈러스 연준 - 핵심 제조업 지표")
        results['core_indicators'] = create_fed_pmi_timeseries_chart(
            core_series, start_date)
        
        # 2. 생산 관련 지표들
        print("📊 2. 생산 관련 지표들...")
        production_series = [
            'dallas_manufacturing_shipments',
            'dallas_manufacturing_finished_goods_inventories',
            'dallas_manufacturing_capacity_utilization'
        ]
        results['production_indicators'] = create_fed_pmi_timeseries_chart(
            production_series, start_date)
        
        # 3. 고용 및 노동 지표들
        print("📊 3. 고용 및 노동 지표들...")
        employment_series = [
            'dallas_manufacturing_employment',
            'dallas_manufacturing_hours_worked',
            'dallas_manufacturing_wages_benefits'
        ]
        results['employment_indicators'] = create_fed_pmi_timeseries_chart(
            employment_series, start_date)
        
        # 4. 가격 관련 지표들
        print("📊 4. 가격 관련 지표들...")
        price_series = [
            'dallas_manufacturing_prices_paid_raw_materials',
            'dallas_manufacturing_prices_received_finished_goods'
        ]
        results['price_indicators'] = create_fed_pmi_timeseries_chart(
            price_series, start_date)
        
        # 5. 미래 전망 지표들
        print("📊 5. 미래 전망 지표들...")
        future_series = [
            'dallas_manufacturing_future_general_business_activity',
            'dallas_manufacturing_future_production',
            'dallas_manufacturing_future_employment'
        ]
        results['future_outlook'] = create_fed_pmi_timeseries_chart(
            future_series, start_date)
        
    except Exception as e:
        print(f"⚠️ 차트 생성 중 오류: {e}")
    
    print(f"\n✅ 대시보드 완료! 생성된 차트: {len(results)}개")
    return results

def create_multi_fed_comparison_dashboard(start_date='2022-01-01'):
    """
    다중 연준 비교 대시보드 생성 (4개 연준)
    
    Args:
        start_date: 분석 시작 날짜
    
    Returns:
        dict: 생성된 차트들
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("🏦 4개 연준 종합 비교 대시보드 생성 중...")
    print("="*50)
    
    results = {}
    
    try:
        # 1. 제조업 메인 지표 비교 (4개 연준)
        print("📊 1. 제조업 메인 지표 비교...")
        manufacturing_main = [
            'philadelphia_manufacturing_general_activity',
            'new_york_manufacturing_general_business_conditions',
            'dallas_manufacturing_general_business_activity'
        ]
        print("연준별 제조업 PMI 비교")
        results['manufacturing_comparison'] = create_fed_pmi_timeseries_chart(
            manufacturing_main, start_date)
        
        # 2. 전체 경제 활동 지표 비교
        print("📊 2. 전체 경제 활동 지표 비교...")
        activity_main = [
            'philadelphia_manufacturing_general_activity',
            'philadelphia_nonmanufacturing_general_activity_firm',
            'new_york_manufacturing_general_business_conditions',
            'new_york_services_business_activity',
            'chicago_economic_conditions_activity_index',
            'dallas_manufacturing_general_business_activity'
        ]
        print("연준별 경제 활동 비교")
        results['activity_comparison'] = create_fed_pmi_timeseries_chart(
            activity_main, start_date)
        
        # 3. 고용 지표 비교
        print("📊 3. 고용 지표 비교...")
        employment_series = [
            'philadelphia_manufacturing_employment',
            'new_york_manufacturing_number_of_employees',
            'chicago_economic_conditions_current_hiring',
            'dallas_manufacturing_employment'
        ]
        print("연준별 고용 지표 비교")
        results['employment_comparison'] = create_fed_pmi_timeseries_chart(
            employment_series, start_date)
        
        # 4. 가격 압력 비교
        print("📊 4. 가격 압력 비교...")
        price_paid_series = [
            'philadelphia_manufacturing_prices_paid',
            'new_york_manufacturing_prices_paid',
            'dallas_manufacturing_prices_paid_raw_materials'
        ]
        print("연준별 가격 압력 비교")
        results['price_pressure_comparison'] = create_fed_pmi_timeseries_chart(
            price_paid_series, start_date)
        
        # 5. 미래 전망 비교
        print("📊 5. 미래 전망 비교...")
        future_series = [
            'philadelphia_manufacturing_future_employment',
            'new_york_manufacturing_future_new_orders',
            'chicago_economic_conditions_hiring_expectations',
            'dallas_manufacturing_future_general_business_activity'
        ]
        print("연준별 미래 전망 비교")
        results['future_outlook_comparison'] = create_fed_pmi_timeseries_chart(
            future_series, start_date)
        
    except Exception as e:
        print(f"⚠️ 차트 생성 중 오류: {e}")
    
    print(f"\n✅ 4개 연준 종합 비교 대시보드 완료! 생성된 차트: {len(results)}개")
    return results

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
                for i, series in enumerate(manufacturing_series[:5]):  # 처음 5개만 표시
                    korean_name = get_series_korean_label(series)
                    print(f"    • {series}")
                    print(f"      → {korean_name}")
                if len(manufacturing_series) > 5:
                    print(f"    ... 외 {len(manufacturing_series)-5}개 더")
            
            if nonmanufacturing_series:
                print(f"  🏢 비제조업/서비스업 ({len(nonmanufacturing_series)}개):")
                for i, series in enumerate(nonmanufacturing_series[:3]):
                    korean_name = get_series_korean_label(series)
                    print(f"    • {series}")
                    print(f"      → {korean_name}")
                if len(nonmanufacturing_series) > 3:
                    print(f"    ... 외 {len(nonmanufacturing_series)-3}개 더")
            
            if retail_series:
                print(f"  🛒 소매업 ({len(retail_series)}개):")
                for i, series in enumerate(retail_series[:3]):
                    korean_name = get_series_korean_label(series)
                    print(f"    • {series}")
                    print(f"      → {korean_name}")
                if len(retail_series) > 3:
                    print(f"    ... 외 {len(retail_series)-3}개 더")
            
            if economic_series:
                print(f"  💼 경제 종합 ({len(economic_series)}개):")
                for i, series in enumerate(economic_series[:3]):
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

def create_custom_multi_line_chart(series_names, start_date=None, title=None):
    """
    사용자가 선택한 시리즈들로 멀티라인 차트 생성
    
    Args:
        series_names (list): 차트에 표시할 시리즈 이름 리스트
        start_date (str): 시작 날짜 (예: '2022-01-01')
        title (str): 차트 제목 (None이면 자동 생성)
    
    Returns:
        plotly.graph_objects.Figure: 생성된 차트
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    if not isinstance(series_names, list) or len(series_names) == 0:
        print("⚠️ series_names는 비어있지 않은 리스트여야 합니다.")
        return None
    
    # 데이터 가져오기
    df = get_raw_data(series_names)
    
    if df.empty:
        print("⚠️ 요청한 시리즈 데이터가 없습니다.")
        print("💡 list_available_series()를 실행하여 사용 가능한 시리즈를 확인하세요.")
        return None
    
    # 시작 날짜 필터링
    if start_date:
        df = df[df.index >= start_date]
    
    # 한국어 라벨 생성
    labels = {}
    for col in df.columns:
        labels[col] = get_series_korean_label(col)
    
    # 제목 설정
    if title is None:
        if len(series_names) <= 3:
            title = f"연준 PMI 비교 ({len(series_names)}개 시리즈)"
        else:
            title = f"연준 PMI 종합 비교 ({len(series_names)}개 시리즈)"
    
    # 타이틀 출력
    print(title)
    
    # KPDS 포맷 멀티라인 차트 생성
    if len(df.columns) == 1:
        fig = df_line_chart(df, df.columns[0], ytitle="확산지수")
    else:
        fig = df_multi_line_chart(df, df.columns.tolist(), ytitle="확산지수", labels=labels)
    
    # 0선 추가 (확산지수는 0이 기준선)
    fig.add_hline(y=0, line_width=2, line_color="black", opacity=0.8)
    
    return fig

def create_custom_dual_axis_chart(left_series, right_series, start_date=None, left_title="왼쪽축", right_title="오른쪽축", main_title=None):
    """
    사용자가 선택한 시리즈들로 이중축 차트 생성
    
    Args:
        left_series (list): 왼쪽 축에 표시할 시리즈 리스트
        right_series (list): 오른쪽 축에 표시할 시리즈 리스트
        start_date (str): 시작 날짜 (예: '2022-01-01')
        left_title (str): 왼쪽 축 제목
        right_title (str): 오른쪽 축 제목
        main_title (str): 차트 제목 (None이면 자동 생성)
    
    Returns:
        plotly.graph_objects.Figure: 생성된 이중축 차트
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    if not isinstance(left_series, list) or not isinstance(right_series, list):
        print("⚠️ left_series와 right_series는 모두 리스트여야 합니다.")
        return None
    
    if len(left_series) == 0 and len(right_series) == 0:
        print("⚠️ 최소 하나의 시리즈는 지정해야 합니다.")
        return None
    
    # 모든 시리즈 데이터 가져오기
    all_series = left_series + right_series
    df = get_raw_data(all_series)
    
    if df.empty:
        print("⚠️ 요청한 시리즈 데이터가 없습니다.")
        print("💡 list_available_series()를 실행하여 사용 가능한 시리즈를 확인하세요.")
        return None
    
    # 시작 날짜 필터링
    if start_date:
        df = df[df.index >= start_date]
    
    # 제목 설정
    if main_title is None:
        main_title = f"연준 PMI 이중축 비교 (좌: {len(left_series)}개, 우: {len(right_series)}개)"
    
    # 타이틀 출력
    print(main_title)
    
    # 왼쪽축과 오른쪽축 데이터 분리
    left_df = df[left_series] if left_series else pd.DataFrame()
    right_df = df[right_series] if right_series else pd.DataFrame()
    
    # 한국어 라벨 생성
    left_labels = {}
    right_labels = {}
    
    for col in left_df.columns:
        left_labels[col] = get_series_korean_label(col)
    
    for col in right_df.columns:
        right_labels[col] = get_series_korean_label(col)
    
    # KPDS 포맷 이중축 차트 생성
    fig = df_dual_axis_chart(
        left_df, 
        right_df, 
        left_ytitle=left_title, 
        right_ytitle=right_title,
        left_labels=left_labels,
        right_labels=right_labels
    )
    
    # 0선 추가 (확산지수는 0이 기준선)
    fig.add_hline(y=0, line_width=2, line_color="black", opacity=0.8)
    
    return fig

def create_fed_comparison_chart(indicator_type='general_activity', chart_type='multi_line', start_date=None):
    """
    특정 지표의 연준별 비교 차트 생성 (간편 함수)
    
    Args:
        indicator_type (str): 비교할 지표 유형 
            - 'general_activity': 일반 활동 지수
            - 'new_orders': 신규 주문
            - 'employment': 고용
            - 'prices_paid': 지불 가격
            - 'production': 생산/출하
            - 'future_outlook': 미래 전망
        chart_type (str): 차트 유형 ('multi_line' 또는 'dual_axis')
        start_date (str): 시작 날짜
    
    Returns:
        plotly.graph_objects.Figure: 생성된 차트
    """
    if indicator_type == 'general_activity':
        series_list = [
            'philadelphia_manufacturing_general_activity',
            'new_york_manufacturing_general_business_conditions',
            'chicago_economic_conditions_activity_index',
            'dallas_manufacturing_general_business_activity'
        ]
    elif indicator_type == 'new_orders':
        series_list = [
            'philadelphia_manufacturing_new_orders',
            'new_york_manufacturing_new_orders',
            'dallas_manufacturing_new_orders'
        ]
    elif indicator_type == 'employment':
        series_list = [
            'philadelphia_manufacturing_employment',
            'new_york_manufacturing_number_of_employees',
            'chicago_economic_conditions_current_hiring',
            'dallas_manufacturing_employment'
        ]
    elif indicator_type == 'prices_paid':
        series_list = [
            'philadelphia_manufacturing_prices_paid',
            'new_york_manufacturing_prices_paid',
            'chicago_economic_conditions_nonlabor_costs',
            'dallas_manufacturing_prices_paid_raw_materials'
        ]
    elif indicator_type == 'production':
        series_list = [
            'philadelphia_manufacturing_shipments',
            'new_york_manufacturing_shipments',
            'dallas_manufacturing_production'
        ]
    elif indicator_type == 'future_outlook':
        series_list = [
            'philadelphia_manufacturing_future_general_activity',
            'new_york_manufacturing_future_general_business_conditions',
            'chicago_economic_conditions_us_economy_outlook',
            'dallas_manufacturing_future_general_business_activity'
        ]
    else:
        print(f"⚠️ 지원하지 않는 지표 유형: {indicator_type}")
        print("💡 사용 가능한 지표: general_activity, new_orders, employment, prices_paid, production, future_outlook")
        return None
    
    if chart_type == 'multi_line':
        return create_custom_multi_line_chart(series_list, start_date=start_date, title=f"연준별 {indicator_type} 비교")
    elif chart_type == 'dual_axis':
        # 이중축의 경우 첫 2개는 왼쪽, 나머지는 오른쪽
        mid_point = len(series_list) // 2
        left_series = series_list[:mid_point]
        right_series = series_list[mid_point:]
        return create_custom_dual_axis_chart(
            left_series, 
            right_series, 
            start_date=start_date,
            left_title=f"{indicator_type} (그룹1)",
            right_title=f"{indicator_type} (그룹2)",
            main_title=f"연준별 {indicator_type} 이중축 비교"
        )
    else:
        print(f"⚠️ 지원하지 않는 차트 유형: {chart_type}")
        print("💡 사용 가능한 차트 유형: multi_line, dual_axis")
        return None

# %%
# === 분석 리포트 함수 ===

def generate_new_york_fed_report():
    """
    뉴욕 연준 PMI 분석 리포트 생성
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("📝 뉴욕 연준 PMI 분석 리포트")
    print("="*50)
    
    # 최신 데이터 가져오기
    latest_date = FED_PMI_DATA['raw_data'].index[-1]
    latest_raw = FED_PMI_DATA['raw_data'].iloc[-1]
    
    print(f"📅 분석 기준일: {latest_date.strftime('%Y년 %m월')}")
    print()
    
    # 1. 주요 지표 현황
    print("📊 주요 지표 현황:")
    
    key_indicators = [
        ('new_york_manufacturing_general_business_conditions', '제조업 일반 비즈니스 상황'),
        ('new_york_services_business_activity', '서비스업 비즈니스 활동'),
        ('new_york_manufacturing_new_orders', '제조업 신규 주문'),
        ('new_york_manufacturing_number_of_employees', '제조업 고용'),
        ('new_york_services_employment', '서비스업 고용')
    ]
    
    for indicator, korean_name in key_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            
            # 상태 판정 (확산지수는 0이 기준)
            status = "확장" if value > 0 else "수축"
            status_emoji = "🟢" if value > 0 else "🔴"
            
            print(f"  {status_emoji} {korean_name}: {value:.1f} ({status})")
    
    print()
    
    # 2. 제조업 vs 서비스업 비교
    print("🏭 제조업 vs 서비스업 비교:")
    manu_activity = latest_raw.get('new_york_manufacturing_general_business_conditions', None)
    services_activity = latest_raw.get('new_york_services_business_activity', None)
    
    if manu_activity is not None and not pd.isna(manu_activity):
        print(f"  제조업 일반 비즈니스 상황: {manu_activity:.1f}")
    if services_activity is not None and not pd.isna(services_activity):
        print(f"  서비스업 비즈니스 활동: {services_activity:.1f}")
    
    if manu_activity is not None and services_activity is not None and not pd.isna(manu_activity) and not pd.isna(services_activity):
        gap = manu_activity - services_activity
        if abs(gap) < 5:
            print(f"  → 제조업과 서비스업이 비슷한 상태 (격차: {gap:+.1f}p)")
        elif gap > 0:
            print(f"  → 제조업이 서비스업보다 강세 (+{gap:.1f}p)")
        else:
            print(f"  → 서비스업이 제조업보다 강세 ({gap:.1f}p)")
    
    print()
    
    # 3. 가격 압력 분석
    print("💰 가격 압력 분석:")
    price_indicators = [
        ('new_york_manufacturing_prices_paid', '제조업 지불 가격'),
        ('new_york_manufacturing_prices_received', '제조업 수취 가격'),
        ('new_york_services_prices_paid', '서비스업 지불 가격'),
        ('new_york_services_prices_received', '서비스업 수취 가격')
    ]
    
    for indicator, name in price_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            
            if value > 10:
                pressure = "높음"
                emoji = "🔴"
            elif value > 0:
                pressure = "보통"
                emoji = "🟡"
            else:
                pressure = "낮음"
                emoji = "🟢"
            
            print(f"  {emoji} {name}: {value:.1f} (압력 {pressure})")
    
    print()
    
    # 4. 미래 전망
    print("🔮 미래 전망:")
    future_indicators = [
        ('new_york_manufacturing_future_new_orders', '제조업 미래 신규 주문'),
        ('new_york_manufacturing_future_number_of_employees', '제조업 미래 고용'),
        ('new_york_services_future_business_activity', '서비스업 미래 비즈니스 활동')
    ]
    
    positive_outlook = 0
    total_outlook = 0
    
    for indicator, name in future_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            total_outlook += 1
            
            if value > 0:
                outlook = "긍정적"
                emoji = "🟢"
                positive_outlook += 1
            else:
                outlook = "부정적"
                emoji = "🔴"
            
            print(f"  {emoji} {name}: {value:.1f} ({outlook})")
    
    # 종합 전망
    if total_outlook > 0:
        outlook_ratio = positive_outlook / total_outlook
        print()
        if outlook_ratio >= 0.7:
            print("  📈 종합 전망: 긍정적 (경기 확장 전망)")
        elif outlook_ratio >= 0.3:
            print("  📊 종합 전망: 혼조 (신중한 관찰 필요)")
        else:
            print("  📉 종합 전망: 부정적 (경기 둔화 우려)")
    
    print()
    print("="*50)
    print("💡 본 분석은 뉴욕 연준 PMI 데이터를 기반으로 한 기계적 해석입니다.")
    print("   투자 결정시에는 추가적인 경제지표와 전문가 분석을 참고하시기 바랍니다.")

def generate_philadelphia_fed_report():
    """
    필라델피아 연준 PMI 분석 리포트 생성
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("📝 필라델피아 연준 PMI 분석 리포트")
    print("="*50)
    
    # 최신 데이터 가져오기
    latest_date = FED_PMI_DATA['raw_data'].index[-1]
    latest_raw = FED_PMI_DATA['raw_data'].iloc[-1]
    
    print(f"📅 분석 기준일: {latest_date.strftime('%Y년 %m월')}")
    print()
    
    # 1. 주요 지표 현황
    print("📊 주요 지표 현황:")
    
    key_indicators = [
        ('philadelphia_manufacturing_general_activity', '제조업 일반 활동'),
        ('philadelphia_nonmanufacturing_general_activity_firm', '비제조업 일반 활동'),
        ('philadelphia_manufacturing_new_orders', '제조업 신규 주문'),
        ('philadelphia_manufacturing_employment', '제조업 고용'),
        ('philadelphia_nonmanufacturing_full_time_employees', '비제조업 고용')
    ]
    
    for indicator, korean_name in key_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            
            # 상태 판정 (확산지수는 0이 기준)
            status = "확장" if value > 0 else "수축"
            status_emoji = "🟢" if value > 0 else "🔴"
            
            print(f"  {status_emoji} {korean_name}: {value:.1f} ({status})")
    
    print()
    
    # 2. 제조업 vs 비제조업 비교
    print("🏭 제조업 vs 비제조업 비교:")
    manu_activity = latest_raw.get('philadelphia_manufacturing_general_activity', None)
    services_activity = latest_raw.get('philadelphia_nonmanufacturing_general_activity_firm', None)
    
    if manu_activity is not None and not pd.isna(manu_activity):
        print(f"  제조업 일반 활동: {manu_activity:.1f}")
    if services_activity is not None and not pd.isna(services_activity):
        print(f"  비제조업 일반 활동: {services_activity:.1f}")
    
    if manu_activity is not None and services_activity is not None and not pd.isna(manu_activity) and not pd.isna(services_activity):
        gap = manu_activity - services_activity
        if abs(gap) < 5:
            print(f"  → 제조업과 비제조업이 비슷한 상태 (격차: {gap:+.1f}p)")
        elif gap > 0:
            print(f"  → 제조업이 비제조업보다 강세 (+{gap:.1f}p)")
        else:
            print(f"  → 비제조업이 제조업보다 강세 ({gap:.1f}p)")
    
    print()
    
    # 3. 가격 압력 분석
    print("💰 가격 압력 분석:")
    price_indicators = [
        ('philadelphia_manufacturing_prices_paid', '제조업 지불 가격'),
        ('philadelphia_manufacturing_prices_received', '제조업 수취 가격'),
        ('philadelphia_nonmanufacturing_prices_paid', '비제조업 지불 가격'),
        ('philadelphia_nonmanufacturing_prices_received', '비제조업 수취 가격')
    ]
    
    for indicator, name in price_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            
            if value > 10:
                pressure = "높음"
                emoji = "🔴"
            elif value > 0:
                pressure = "보통"
                emoji = "🟡"
            else:
                pressure = "낮음"
                emoji = "🟢"
            
            print(f"  {emoji} {name}: {value:.1f} (압력 {pressure})")
    
    print()
    
    # 4. 미래 전망
    print("🔮 미래 전망:")
    future_indicators = [
        ('philadelphia_manufacturing_future_new_orders', '제조업 미래 신규 주문'),
        ('philadelphia_manufacturing_future_employment', '제조업 미래 고용'),
        ('philadelphia_nonmanufacturing_future_general_activity_firm', '비제조업 미래 일반 활동')
    ]
    
    positive_outlook = 0
    total_outlook = 0
    
    for indicator, name in future_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            total_outlook += 1
            
            if value > 0:
                outlook = "긍정적"
                emoji = "🟢"
                positive_outlook += 1
            else:
                outlook = "부정적"
                emoji = "🔴"
            
            print(f"  {emoji} {name}: {value:.1f} ({outlook})")
    
    # 종합 전망
    if total_outlook > 0:
        outlook_ratio = positive_outlook / total_outlook
        print()
        if outlook_ratio >= 0.7:
            print("  📈 종합 전망: 긍정적 (경기 확장 전망)")
        elif outlook_ratio >= 0.3:
            print("  📊 종합 전망: 혼조 (신중한 관찰 필요)")
        else:
            print("  📉 종합 전망: 부정적 (경기 둔화 우려)")
    
    print()
    print("="*50)
    print("💡 본 분석은 필라델피아 연준 PMI 데이터를 기반으로 한 기계적 해석입니다.")
    print("   투자 결정시에는 추가적인 경제지표와 전문가 분석을 참고하시기 바랍니다.")

def generate_chicago_fed_report():
    """
    시카고 연준 경제 조사 분석 리포트 생성
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("📝 시카고 연준 경제 조사 분석 리포트")
    print("="*50)
    
    # 최신 데이터 가져오기
    latest_date = FED_PMI_DATA['raw_data'].index[-1]
    latest_raw = FED_PMI_DATA['raw_data'].iloc[-1]
    
    print(f"📅 분석 기준일: {latest_date.strftime('%Y년 %m월')}")
    print()
    
    # 1. 주요 지표 현황
    print("📊 주요 지표 현황:")
    
    key_indicators = [
        ('chicago_economic_conditions_activity_index', '전체 활동 지수'),
        ('chicago_economic_conditions_manufacturing_activity', '제조업 활동'),
        ('chicago_economic_conditions_nonmanufacturing_activity', '비제조업 활동'),
        ('chicago_economic_conditions_current_hiring', '현재 고용'),
        ('chicago_economic_conditions_labor_costs', '인건비'),
        ('chicago_economic_conditions_nonlabor_costs', '비인건비')
    ]
    
    for indicator, korean_name in key_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            
            # 상태 판정 (확산지수는 0이 기준, 하지만 시카고 연준은 다를 수 있음)
            status = "확장" if value > 0 else "수축"
            status_emoji = "🟢" if value > 0 else "🔴"
            
            print(f"  {status_emoji} {korean_name}: {value:.1f} ({status})")
    
    print()
    
    # 2. 제조업 vs 비제조업 비교
    print("🏭 제조업 vs 비제조업 비교:")
    manu_activity = latest_raw.get('chicago_economic_conditions_manufacturing_activity', None)
    nonmanu_activity = latest_raw.get('chicago_economic_conditions_nonmanufacturing_activity', None)
    
    if manu_activity is not None and not pd.isna(manu_activity):
        print(f"  제조업 활동: {manu_activity:.1f}")
    if nonmanu_activity is not None and not pd.isna(nonmanu_activity):
        print(f"  비제조업 활동: {nonmanu_activity:.1f}")
    
    if manu_activity is not None and nonmanu_activity is not None and not pd.isna(manu_activity) and not pd.isna(nonmanu_activity):
        gap = manu_activity - nonmanu_activity
        if abs(gap) < 5:
            print(f"  → 제조업과 비제조업이 비슷한 상태 (격차: {gap:+.1f}p)")
        elif gap > 0:
            print(f"  → 제조업이 비제조업보다 강세 (+{gap:.1f}p)")
        else:
            print(f"  → 비제조업이 제조업보다 강세 ({gap:.1f}p)")
    
    print()
    
    # 3. 고용 전망 분석
    print("👥 고용 전망 분석:")
    current_hiring = latest_raw.get('chicago_economic_conditions_current_hiring', None)
    hiring_expectations = latest_raw.get('chicago_economic_conditions_hiring_expectations', None)
    
    if current_hiring is not None and not pd.isna(current_hiring):
        status = "확장" if current_hiring > 0 else "수축"
        emoji = "🟢" if current_hiring > 0 else "🔴"
        print(f"  {emoji} 현재 고용: {current_hiring:.1f} ({status})")
    
    if hiring_expectations is not None and not pd.isna(hiring_expectations):
        status = "긍정적" if hiring_expectations > 0 else "부정적"
        emoji = "🟢" if hiring_expectations > 0 else "🔴"
        print(f"  {emoji} 고용 전망: {hiring_expectations:.1f} ({status})")
    
    if current_hiring is not None and hiring_expectations is not None and not pd.isna(current_hiring) and not pd.isna(hiring_expectations):
        gap = hiring_expectations - current_hiring
        if gap > 5:
            print(f"  → 고용 전망이 현재보다 크게 개선될 것으로 예상 (+{gap:.1f}p)")
        elif gap < -5:
            print(f"  → 고용 전망이 현재보다 크게 악화될 것으로 예상 ({gap:.1f}p)")
        else:
            print(f"  → 고용 상황이 현재와 비슷하게 유지될 전망 ({gap:+.1f}p)")
    
    print()
    
    # 4. 미래 전망
    print("🔮 미래 전망:")
    future_indicators = [
        ('chicago_economic_conditions_capx_expectations', '자본지출 전망'),
        ('chicago_economic_conditions_us_economy_outlook', '미국 경제 전망')
    ]
    
    positive_outlook = 0
    total_outlook = 0
    
    for indicator, name in future_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            total_outlook += 1
            
            if value > 0:
                outlook = "긍정적"
                emoji = "🟢"
                positive_outlook += 1
            else:
                outlook = "부정적"
                emoji = "🔴"
            
            print(f"  {emoji} {name}: {value:.1f} ({outlook})")
    
    # 종합 전망
    if total_outlook > 0:
        outlook_ratio = positive_outlook / total_outlook
        print()
        if outlook_ratio >= 0.7:
            print("  📈 종합 전망: 긍정적 (경기 확장 전망)")
        elif outlook_ratio >= 0.3:
            print("  📊 종합 전망: 혼조 (신중한 관찰 필요)")
        else:
            print("  📉 종합 전망: 부정적 (경기 둔화 우려)")
    
    print()
    print("="*50)
    print("💡 본 분석은 시카고 연준 경제 조사 데이터를 기반으로 한 기계적 해석입니다.")
    print("   투자 결정시에는 추가적인 경제지표와 전문가 분석을 참고하시기 바랍니다.")

def generate_dallas_fed_report():
    """
    댈러스 연준 제조업 전망 조사 분석 리포트 생성
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("📝 댈러스 연준 제조업 전망 조사 분석 리포트")
    print("="*50)
    
    # 최신 데이터 가져오기
    latest_date = FED_PMI_DATA['raw_data'].index[-1]
    latest_raw = FED_PMI_DATA['raw_data'].iloc[-1]
    
    print(f"📅 분석 기준일: {latest_date.strftime('%Y년 %m월')}")
    print()
    
    # 1. 주요 지표 현황
    print("📊 주요 지표 현황:")
    
    key_indicators = [
        ('dallas_manufacturing_general_business_activity', '일반 비즈니스 활동'),
        ('dallas_manufacturing_production', '생산'),
        ('dallas_manufacturing_new_orders', '신규 주문'),
        ('dallas_manufacturing_shipments', '출하'),
        ('dallas_manufacturing_employment', '고용')
    ]
    
    for indicator, korean_name in key_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            
            # 상태 판정 (확산지수는 0이 기준)
            status = "확장" if value > 0 else "수축"
            status_emoji = "🟢" if value > 0 else "🔴"
            
            print(f"  {status_emoji} {korean_name}: {value:.1f} ({status})")
    
    print()
    
    # 2. 생산 동향 분석
    print("🏭 생산 동향 분석:")
    production = latest_raw.get('dallas_manufacturing_production', None)
    capacity_util = latest_raw.get('dallas_manufacturing_capacity_utilization', None)
    inventory = latest_raw.get('dallas_manufacturing_finished_goods_inventories', None)
    
    if production is not None and not pd.isna(production):
        status = "확장" if production > 0 else "수축"
        emoji = "🟢" if production > 0 else "🔴"
        print(f"  {emoji} 생산: {production:.1f} ({status})")
    
    if capacity_util is not None and not pd.isna(capacity_util):
        status = "상승" if capacity_util > 0 else "하락"
        emoji = "🟢" if capacity_util > 0 else "🔴"
        print(f"  {emoji} 가동률: {capacity_util:.1f} ({status})")
    
    if inventory is not None and not pd.isna(inventory):
        status = "증가" if inventory > 0 else "감소"
        # 재고는 감소가 긍정적일 수 있음
        emoji = "🟡" if inventory > 5 else "🟢" if inventory < -5 else "🟠"
        print(f"  {emoji} 완제품 재고: {inventory:.1f} ({status})")
    
    print()
    
    # 3. 가격 압력 분석
    print("💰 가격 압력 분석:")
    prices_paid = latest_raw.get('dallas_manufacturing_prices_paid_raw_materials', None)
    prices_received = latest_raw.get('dallas_manufacturing_prices_received_finished_goods', None)
    
    if prices_paid is not None and not pd.isna(prices_paid):
        if prices_paid > 10:
            pressure = "높음"
            emoji = "🔴"
        elif prices_paid > 0:
            pressure = "보통"
            emoji = "🟡"
        else:
            pressure = "낮음"
            emoji = "🟢"
        print(f"  {emoji} 원자재 가격 압력: {prices_paid:.1f} (압력 {pressure})")
    
    if prices_received is not None and not pd.isna(prices_received):
        if prices_received > 10:
            margin = "개선"
            emoji = "🟢"
        elif prices_received > 0:
            margin = "보통"
            emoji = "🟡"
        else:
            margin = "악화"
            emoji = "🔴"
        print(f"  {emoji} 완제품 가격: {prices_received:.1f} (마진 {margin})")
    
    print()
    
    # 4. 미래 전망
    print("🔮 미래 전망:")
    future_indicators = [
        ('dallas_manufacturing_future_general_business_activity', '미래 일반 비즈니스 활동'),
        ('dallas_manufacturing_future_production', '미래 생산'),
        ('dallas_manufacturing_future_employment', '미래 고용')
    ]
    
    positive_outlook = 0
    total_outlook = 0
    
    for indicator, name in future_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            total_outlook += 1
            
            if value > 0:
                outlook = "긍정적"
                emoji = "🟢"
                positive_outlook += 1
            else:
                outlook = "부정적"
                emoji = "🔴"
            
            print(f"  {emoji} {name}: {value:.1f} ({outlook})")
    
    # 종합 전망
    if total_outlook > 0:
        outlook_ratio = positive_outlook / total_outlook
        print()
        if outlook_ratio >= 0.7:
            print("  📈 종합 전망: 긍정적 (제조업 확장 전망)")
        elif outlook_ratio >= 0.3:
            print("  📊 종합 전망: 혼조 (신중한 관찰 필요)")
        else:
            print("  📉 종합 전망: 부정적 (제조업 둔화 우려)")
    
    print()
    print("="*50)
    print("💡 본 분석은 댈러스 연준 제조업 전망 조사 데이터를 기반으로 한 기계적 해석입니다.")
    print("   투자 결정시에는 추가적인 경제지표와 전문가 분석을 참고하시기 바랍니다.")

def generate_multi_fed_comparison_report():
    """
    4개 연준 PMI 비교 분석 리포트 생성
    """
    if not FED_PMI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_fed_pmi_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("📝 4개 연준 PMI 종합 비교 분석 리포트")
    print("="*50)
    
    # 최신 데이터 가져오기
    latest_date = FED_PMI_DATA['raw_data'].index[-1]
    latest_raw = FED_PMI_DATA['raw_data'].iloc[-1]
    
    print(f"📅 분석 기준일: {latest_date.strftime('%Y년 %m월')}")
    print()
    
    # 1. 제조업 PMI 비교
    print("🏭 제조업 PMI 비교:")
    manufacturing_indicators = [
        ('philadelphia_manufacturing_general_activity', '필라델피아 연준'),
        ('new_york_manufacturing_general_business_conditions', '뉴욕 연준'),
        ('dallas_manufacturing_general_business_activity', '댈러스 연준')
    ]
    
    manu_values = []
    for indicator, fed_name in manufacturing_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            manu_values.append((fed_name, value))
            
            status = "확장" if value > 0 else "수축"
            emoji = "🟢" if value > 0 else "🔴"
            print(f"  {emoji} {fed_name}: {value:.1f} ({status})")
    
    if len(manu_values) >= 2:
        # 최고/최저 성과 연준 찾기
        sorted_values = sorted(manu_values, key=lambda x: x[1], reverse=True)
        best_fed = sorted_values[0]
        worst_fed = sorted_values[-1]
        print(f"  → 제조업 최고 성과: {best_fed[0]} ({best_fed[1]:+.1f})")
        print(f"  → 제조업 최저 성과: {worst_fed[0]} ({worst_fed[1]:+.1f})")
    
    print()
    
    # 2. 종합 경제 활동 비교
    print("🌎 종합 경제 활동 비교:")
    
    # 각 연준별 대표 지표
    fed_main_indicators = [
        ('philadelphia_manufacturing_general_activity', '필라델피아 연준 (제조업)'),
        ('philadelphia_nonmanufacturing_general_activity_firm', '필라델피아 연준 (비제조업)'),
        ('new_york_manufacturing_general_business_conditions', '뉴욕 연준 (제조업)'),
        ('new_york_services_business_activity', '뉴욕 연준 (서비스업)'),
        ('chicago_economic_conditions_activity_index', '시카고 연준 (종합)'),
        ('dallas_manufacturing_general_business_activity', '댈러스 연준 (제조업)')
    ]
    
    all_values = []
    for indicator, description in fed_main_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            all_values.append((description, value))
            
            status = "확장" if value > 0 else "수축"
            emoji = "🟢" if value > 0 else "🔴"
            print(f"  {emoji} {description}: {value:.1f} ({status})")
    
    print()
    
    # 3. 지역별 경기 동향 종합 (연준별 평균)
    print("📊 지역별 경기 동향 종합:")
    
    fed_scores = {}
    
    # 필라델피아 지역
    phila_indicators = ['philadelphia_manufacturing_general_activity', 'philadelphia_nonmanufacturing_general_activity_firm']
    phila_scores = [latest_raw[ind] for ind in phila_indicators if ind in latest_raw.index and not pd.isna(latest_raw[ind])]
    if phila_scores:
        fed_scores['필라델피아'] = sum(phila_scores) / len(phila_scores)
    
    # 뉴욕 지역
    ny_indicators = ['new_york_manufacturing_general_business_conditions', 'new_york_services_business_activity']
    ny_scores = [latest_raw[ind] for ind in ny_indicators if ind in latest_raw.index and not pd.isna(latest_raw[ind])]
    if ny_scores:
        fed_scores['뉴욕'] = sum(ny_scores) / len(ny_scores)
    
    # 시카고 지역
    chicago_indicator = 'chicago_economic_conditions_activity_index'
    if chicago_indicator in latest_raw.index and not pd.isna(latest_raw[chicago_indicator]):
        fed_scores['시카고'] = latest_raw[chicago_indicator]
    
    # 댈러스 지역
    dallas_indicator = 'dallas_manufacturing_general_business_activity'
    if dallas_indicator in latest_raw.index and not pd.isna(latest_raw[dallas_indicator]):
        fed_scores['댈러스'] = latest_raw[dallas_indicator]
    
    # 지역별 점수 출력
    for region, score in fed_scores.items():
        status = "확장" if score > 0 else "수축"
        emoji = "🟢" if score > 0 else "🔴"
        print(f"  {emoji} {region} 지역 종합: {score:.1f} ({status})")
    
    # 최고/최저 지역 찾기
    if fed_scores:
        sorted_regions = sorted(fed_scores.items(), key=lambda x: x[1], reverse=True)
        best_region = sorted_regions[0]
        worst_region = sorted_regions[-1]
        print(f"  → 최고 성과 지역: {best_region[0]} ({best_region[1]:+.1f})")
        print(f"  → 최저 성과 지역: {worst_region[0]} ({worst_region[1]:+.1f})")
    
    # 4. 고용 동향 비교
    print()
    print("👥 고용 동향 비교:")
    employment_indicators = [
        ('philadelphia_manufacturing_employment', '필라델피아 연준 (제조업)'),
        ('new_york_manufacturing_number_of_employees', '뉴욕 연준 (제조업)'),
        ('chicago_economic_conditions_current_hiring', '시카고 연준 (전체)'),
        ('dallas_manufacturing_employment', '댈러스 연준 (제조업)')
    ]
    
    for indicator, description in employment_indicators:
        if indicator in latest_raw.index and not pd.isna(latest_raw[indicator]):
            value = latest_raw[indicator]
            
            status = "확장" if value > 0 else "수축"
            emoji = "🟢" if value > 0 else "🔴"
            print(f"  {emoji} {description}: {value:.1f} ({status})")
    
    print()
    print("="*50)
    print("💡 본 분석은 4개 연준 PMI 데이터를 기반으로 한 기계적 해석입니다.")
    print("   투자 결정시에는 추가적인 경제지표와 전문가 분석을 참고하시기 바랍니다.")

# %%
# === 통합 분석 함수 ===

def run_complete_fed_pmi_analysis_enhanced(start_date='2020-01-01', force_reload=False, smart_update=True, target_fed='all'):
    """
    완전한 연준 PMI 분석 실행 - 데이터 로드 후 모든 차트 생성 (4개 연준 지원)
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
        target_fed: 분석 대상 연준 ('philadelphia', 'new_york', 'chicago', 'dallas', 'all')
    
    Returns:
        dict: 생성된 차트들
    """
    print("🚀 완전한 연준 PMI 분석 시작 (4개 연준 지원)")
    print("="*50)
    
    # 1. 데이터 로드 (스마트 업데이트!)
    print("\n1️⃣ 데이터 로딩 (스마트 업데이트)")
    success = load_all_fed_pmi_data_enhanced(start_date=start_date, force_reload=force_reload, smart_update=smart_update)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    results = {}
    step = 2
    
    # 2. 연준별 대시보드 생성
    if target_fed in ['philadelphia', 'all']:
        print(f"\n{step}️⃣ 필라델피아 연준 대시보드 생성")
        results['philadelphia'] = create_philadelphia_fed_dashboard(start_date='2022-01-01')
        step += 1
        
        print(f"\n{step}️⃣ 필라델피아 연준 분석 리포트")
        generate_philadelphia_fed_report()
        step += 1
    
    if target_fed in ['new_york', 'all']:
        print(f"\n{step}️⃣ 뉴욕 연준 대시보드 생성")
        results['new_york'] = create_new_york_fed_dashboard(start_date='2022-01-01')
        step += 1
        
        print(f"\n{step}️⃣ 뉴욕 연준 분석 리포트")
        generate_new_york_fed_report()
        step += 1
    
    if target_fed in ['chicago', 'all']:
        print(f"\n{step}️⃣ 시카고 연준 대시보드 생성")
        results['chicago'] = create_chicago_fed_dashboard(start_date='2022-01-01')
        step += 1
        
        print(f"\n{step}️⃣ 시카고 연준 분석 리포트")
        generate_chicago_fed_report()
        step += 1
    
    if target_fed in ['dallas', 'all']:
        print(f"\n{step}️⃣ 댈러스 연준 대시보드 생성")
        results['dallas'] = create_dallas_fed_dashboard(start_date='2022-01-01')
        step += 1
        
        print(f"\n{step}️⃣ 댈러스 연준 분석 리포트")
        generate_dallas_fed_report()
        step += 1
    
    if target_fed == 'all':
        print(f"\n{step}️⃣ 4개 연준 종합 비교 대시보드 생성")
        results['comparison'] = create_multi_fed_comparison_dashboard(start_date='2022-01-01')
        step += 1
        
        print(f"\n{step}️⃣ 4개 연준 종합 비교 분석 리포트")
        generate_multi_fed_comparison_report()
    
    return results

def run_new_york_fed_analysis(start_date='2020-01-01', force_reload=False, smart_update=True):
    """
    뉴욕 연준 PMI 분석 실행 (단독 분석)
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🗽 뉴욕 연준 PMI 분석 시작")
    print("="*50)
    
    # 데이터 로드
    success = load_all_fed_pmi_data_enhanced(start_date=start_date, force_reload=force_reload, smart_update=smart_update)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 뉴욕 연준 대시보드 생성
    results = create_new_york_fed_dashboard(start_date='2022-01-01')
    
    # 뉴욕 연준 분석 리포트
    generate_new_york_fed_report()
    
    return results

def run_chicago_fed_analysis(start_date='2020-01-01', force_reload=False, smart_update=True):
    """
    시카고 연준 경제 조사 분석 실행 (단독 분석)
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🏙️ 시카고 연준 경제 조사 분석 시작")
    print("="*50)
    
    # 데이터 로드
    success = load_all_fed_pmi_data_enhanced(start_date=start_date, force_reload=force_reload, smart_update=smart_update)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 시카고 연준 대시보드 생성
    results = create_chicago_fed_dashboard(start_date='2022-01-01')
    
    # 시카고 연준 분석 리포트
    generate_chicago_fed_report()
    
    return results

def run_dallas_fed_analysis(start_date='2020-01-01', force_reload=False, smart_update=True):
    """
    댈러스 연준 제조업 전망 조사 분석 실행 (단독 분석)
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🤠 댈러스 연준 제조업 전망 조사 분석 시작")
    print("="*50)
    
    # 데이터 로드
    success = load_all_fed_pmi_data_enhanced(start_date=start_date, force_reload=force_reload, smart_update=smart_update)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 댈러스 연준 대시보드 생성
    results = create_dallas_fed_dashboard(start_date='2022-01-01')
    
    # 댈러스 연준 분석 리포트
    generate_dallas_fed_report()
    
    return results

def run_multi_fed_comparison_analysis(start_date='2020-01-01', force_reload=False, smart_update=True):
    """
    4개 연준 종합 비교 분석 실행
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🏛️ 4개 연준 종합 비교 분석 시작")
    print("="*50)
    
    # 데이터 로드
    success = load_all_fed_pmi_data_enhanced(start_date=start_date, force_reload=force_reload, smart_update=smart_update)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 4개 연준 비교 대시보드 생성
    results = create_multi_fed_comparison_dashboard(start_date='2022-01-01')
    
    # 4개 연준 비교 분석 리포트
    generate_multi_fed_comparison_report()
    
    return results

# %%
# === 사용 예시 ===

print("\n" + "="*80)
print("🎯 권장 사용법 (4개 연준 완전 지원!):")
print("   🏆 run_complete_fed_pmi_analysis_enhanced(target_fed='all')  # 모든 연준 분석!")
print("   🗽 run_new_york_fed_analysis()  # 뉴욕 연준만 분석")
print("   🏙️ run_chicago_fed_analysis()  # 시카고 연준만 분석")
print("   🤠 run_dallas_fed_analysis()  # 댈러스 연준만 분석")
print("   🏛️ run_multi_fed_comparison_analysis()  # 4개 연준 종합 비교")
print()
print("📊 개별 함수 사용법:")
print("1. 데이터 로드 (스마트 업데이트):")
print("   load_all_fed_pmi_data_enhanced(smart_update=True)  # 기본값, 일치 시 API 건너뛰기")
print("   load_all_fed_pmi_data_enhanced(force_reload=True)  # 강제 전체 재로드")
print("   load_fed_pmi_data_from_csv()  # CSV에서만 로드")
print()
print("2. 💾 데이터 저장:")
print("   save_fed_pmi_data_to_csv()  # 현재 데이터를 CSV로 저장 (자동 호출됨)")
print()
print("3. 🤖 스마트 업데이트 기능:")
print("   check_recent_data_consistency_enhanced()  # 최근 데이터 일치성 확인 (ADP 방식)")
print("   - 허용 오차: 확산지수 기준")
print("   - CSV 데이터와 API 데이터 비교")
print("   - 불일치 시에만 전체 API 호출")
print()
print("4. 📈 차트 생성 (연준별):")
print("   # 필라델피아 연준")
print("   create_philadelphia_fed_dashboard()  # 필라델피아 연준 종합 대시보드")
print("   generate_philadelphia_fed_report()  # 필라델피아 연준 분석 리포트")
print()
print("   # 뉴욕 연준")
print("   create_new_york_fed_dashboard()  # 뉴욕 연준 종합 대시보드")
print("   generate_new_york_fed_report()  # 뉴욕 연준 분석 리포트")
print()
print("   # 시카고 연준 (NEW!)")
print("   create_chicago_fed_dashboard()  # 시카고 연준 경제 조사 대시보드")
print("   generate_chicago_fed_report()  # 시카고 연준 분석 리포트")
print()
print("   # 댈러스 연준 (NEW!)")
print("   create_dallas_fed_dashboard()  # 댈러스 연준 제조업 전망 대시보드")
print("   generate_dallas_fed_report()  # 댈러스 연준 분석 리포트")
print()
print("   # 지표별 연준 간 비교 (NEW!)")
print("   create_fed_comparison_by_indicator('general_activity')  # 일반 활동 지수 연준별 비교")
print("   create_fed_comparison_by_indicator('new_orders')  # 신규 주문 연준별 비교")
print("   create_fed_comparison_by_indicator('employment')  # 고용 연준별 비교")
print("   create_fed_comparison_by_indicator('prices_paid')  # 지불 가격 연준별 비교")
print("   create_fed_comparison_by_indicator('production')  # 생산/출하 연준별 비교")
print("   create_fed_comparison_by_indicator('future_outlook')  # 미래 전망 연준별 비교")
print()
print("   # 4개 연준 종합 비교 (NEW!)")
print("   create_multi_fed_comparison_dashboard()  # 4개 연준 종합 비교 대시보드")
print("   generate_multi_fed_comparison_report()  # 4개 연준 비교 분석 리포트")
print()
print("5. 🎯 통합 분석:")
print("   run_complete_fed_pmi_analysis_enhanced(target_fed='all')  # 모든 연준 전체 분석")
print("   run_complete_fed_pmi_analysis_enhanced(target_fed='philadelphia')  # 필라델피아만")
print("   run_complete_fed_pmi_analysis_enhanced(target_fed='new_york')  # 뉴욕만")
print("   run_complete_fed_pmi_analysis_enhanced(target_fed='chicago')  # 시카고만")
print("   run_complete_fed_pmi_analysis_enhanced(target_fed='dallas')  # 댈러스만")
print()
print("6. 📋 사용 가능한 시리즈 (총 109개!):")
print("   - 필라델피아 연준 제조업 (21개): general_activity, new_orders, shipments, employment, etc.")
print("   - 필라델피아 연준 비제조업 (15개): general_activity_firm, sales_revenues, full_time_employees, etc.")
print("   - 뉴욕 연준 제조업 (21개): general_business_conditions, new_orders, shipments, etc.")
print("   - 뉴욕 연준 서비스업 (14개): business_activity, employment, business_climate, etc.")
print("   - 시카고 연준 경제 조사 (9개): activity_index, manufacturing_activity, hiring_expectations, etc.")
print("   - 댈러스 연준 제조업 (29개): general_business_activity, production, capacity_utilization, etc.")
print("   - 총 109개 주요 Diffusion Index 시리즈 (4개 연준 핵심 선별)")
print("   - 향후 다른 연준 추가 예정 (리치몬드, 캔자스시티, 애틀랜타 등)")
print()
print("7. 🆕 새로 추가된 연준 특징:")
print("   📍 시카고 연준:")
print("     - Chicago Fed Survey of Economic Conditions")
print("     - 제조업/비제조업 통합 경제 조사")
print("     - 고용, 비용, 미래 전망 지표")
print()
print("   📍 댈러스 연준:")
print("     - Texas Manufacturing Outlook Survey")
print("     - 포괄적인 제조업 전망 조사")
print("     - 생산, 가동률, 재고, 가격 등 세부 지표")
print()
print("   📍 4개 연준 종합 비교:")
print("     - 지역별 경기 동향 분석")
print("     - 제조업 PMI 종합 비교")
print("     - 고용, 가격 압력 지역별 차이 분석")
print()
print("8. 🎨 사용자 정의 시각화 (NEW!):")
print("   # 사용 가능한 시리즈 확인")
print("   list_available_series()  # 모든 연준의 사용 가능한 시리즈 목록")
print()
print("   # 멀티라인 차트 (원하는 시리즈 선택)")
print("   create_custom_multi_line_chart([")
print("       'philadelphia_manufacturing_general_activity',")
print("       'new_york_manufacturing_general_business_conditions',")
print("       'dallas_manufacturing_general_business_activity'")
print("   ], start_date='2022-01-01')")
print()
print("   # 이중축 차트 (좌측/우측 축 따로 지정)")
print("   create_custom_dual_axis_chart(")
print("       left_series=['philadelphia_manufacturing_general_activity', 'new_york_manufacturing_general_business_conditions'],")
print("       right_series=['chicago_economic_conditions_activity_index', 'dallas_manufacturing_general_business_activity'],")
print("       left_title='동부 연준', right_title='중서부 연준'")
print("   )")
print()
print("   # 간편 연준별 비교 (지표별 미리 정의된 차트)")
print("   create_fed_comparison_chart('general_activity', 'multi_line')  # 일반 활동 지수 비교")
print("   create_fed_comparison_chart('employment', 'dual_axis')  # 고용 지표 이중축 비교")
print("   create_fed_comparison_chart('prices_paid', 'multi_line')  # 가격 압력 비교")
print()
print("   💡 지원 지표 유형: general_activity, new_orders, employment, prices_paid, production, future_outlook")
print("   💡 차트 유형: multi_line, dual_axis")
print("="*80)

# %%
# 🆕 스마트 업데이트 활성화하여 4개 연준 통합 분석 실행
print("🚀 4개 연준 (필라델피아 + 뉴욕 + 시카고 + 댈러스) 통합 분석 시작!")
run_complete_fed_pmi_analysis_enhanced(target_fed='all', smart_update=True)

# %%
check_recent_data_consistency_enhanced()
# %%
