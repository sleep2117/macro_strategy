# %%
"""
BLS API 전용 CPS(Current Population Survey) 분석 및 시각화 도구
- BLS API만 사용하여 CPS 노동시장 데이터 수집
- 스마트 업데이트로 API 호출 최소화
- CSV 저장/로드 및 자동 업데이트
- 투자은행/이코노미스트를 위한 전문 분석 기능
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sys
import warnings
warnings.filterwarnings('ignore')

# 필수 라이브러리들
try:
    import requests
    import json
    BLS_API_AVAILABLE = True
    print("✓ BLS API 연동 가능 (requests 라이브러리)")
except ImportError:
    print("⚠️ requests 라이브러리가 없습니다. 설치하세요: pip install requests")
    BLS_API_AVAILABLE = False

# BLS API 키 설정
BLS_API_KEY = '56b193612b614cdc9416359fd1c73a74'
BLS_API_KEY2 = '0450ef37363c48b5bedd2ae6fc92dd6e'
BLS_API_KEY3 = 'daf1ca7970b74e81b6a5c7a80a8b8a7f'
CURRENT_API_KEY = BLS_API_KEY2

# 시각화 라이브러리 불러오기
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

# %%
# === CPS 시리즈 ID와 설명 매핑 ===

CPS_SERIES = {
    # 주요 노동시장 지표
    'LNS11000000': 'Civilian Labor Force Level',
    'LNS11300000': 'Civilian Labor Force Participation Rate',
    'LNS12000000': 'Employment Level',
    'LNS12300000': 'Employment-Population Ratio',
    'LNS12500000': 'Employed, Usually Work Full Time',
    'LNS12600000': 'Employed, Usually Work Part Time',
    'LNS13000000': 'Unemployment Level',
    'LNS14000000': 'Unemployment Rate',
    
    # 연령별 실업률
    'LNS14000012': 'Unemployment Rate - 16-19 Years',
    'LNS14000025': 'Unemployment Rate - 20 Years & Over Men',
    'LNS14000026': 'Unemployment Rate - 20 Years & Over Women',
    
    # 인종별 실업률
    'LNS14000003': 'Unemployment Rate - White',
    'LNS14000006': 'Unemployment Rate - Black or African American',
    'LNS14032183': 'Unemployment Rate - Asian',
    'LNS14000009': 'Unemployment Rate - Hispanic or Latino',
    
    # 교육수준별 실업률
    'LNS14027659': 'Unemployment Rate - Less than High School',
    'LNS14027660': 'Unemployment Rate - High School Graduates',
    'LNS14027689': 'Unemployment Rate - Some College',
    'LNS14027662': 'Unemployment Rate - Bachelor\'s Degree and Higher',
    
    # 실업 기간
    'LNS13008396': 'Unemployed Less Than 5 weeks',
    'LNS13008756': 'Unemployed 5-14 Weeks',
    'LNS13008516': 'Unemployed 15 Weeks & Over',
    'LNS13008636': 'Unemployed 27 Weeks & Over',
    'LNS13008275': 'Average Weeks Unemployed',
    'LNS13008276': 'Median Weeks Unemployed',
    
    # 실업 유형
    'LNS13023621': 'Unemployment - Job Losers',
    'LNS13023653': 'Unemployment - Job Losers On Layoff',
    'LNS13025699': 'Unemployment - Job Losers Not on Layoff',
    'LNS13023705': 'Unemployment - Job Leavers',
    'LNS13023557': 'Unemployment - Reentrants',
    'LNS13023569': 'Unemployment - New Entrants',
    
    # 기타 노동시장 지표
    'LNS12032194': 'Part Time for Economic Reasons',
    'LNS15000000': 'Not in Labor Force',
    'LNS15026642': 'Marginally Attached to Labor Force',
    'LNS15026645': 'Discouraged Workers',
    'LNS13327709': 'U-6 Unemployment Rate',
    'LNS12026619': 'Multiple Jobholders Level',
    'LNS12026620': 'Multiple Jobholders as Percent of Employed',
    
    # 날씨 영향
    'LNU02036012': 'Not at Work - Bad Weather',
    'LNU02033224': 'At Work 1-34 Hrs - Bad Weather',
    
    # 20세 이상 주요 지표
    'LNS11000024': 'Labor Force - 20 Years & Over',
    'LNS11300024': 'Participation Rate - 20 Years & Over',
    'LNS12000024': 'Employment - 20 Years & Over',
    'LNS13000024': 'Unemployment - 20 Years & Over',
    'LNS14000024': 'Unemployment Rate - 20 Years & Over',
    
    # 25세 이상 주요 지표
    'LNS11000048': 'Labor Force - 25 Years & Over',
    'LNS11300048': 'Participation Rate - 25 Years & Over',
    'LNS12000048': 'Employment - 25 Years & Over',
    'LNS13000048': 'Unemployment - 25 Years & Over',
    'LNS14000048': 'Unemployment Rate - 25 Years & Over',
    
    # 55세 이상 주요 지표
    'LNS11024230': 'Labor Force - 55 Years & Over',
    'LNS11324230': 'Participation Rate - 55 Years & Over',
    'LNS12024230': 'Employment - 55 Years & Over',
    'LNS13024230': 'Unemployment - 55 Years & Over',
    'LNS14024230': 'Unemployment Rate - 55 Years & Over',
    
    # 히스패닉/라티노 전체 노동시장 지표
    'LNS11000009': 'Labor Force - Hispanic or Latino',
    'LNS11300009': 'Participation Rate - Hispanic or Latino',
    'LNS12000009': 'Employment - Hispanic or Latino',
    'LNS13000009': 'Unemployment - Hispanic or Latino',
    'LNS15000009': 'Not in Labor Force - Hispanic or Latino',
    
    # === 추가 연령별 노동시장 지표 (Not Seasonally Adjusted) ===
    'LNU01076975': 'Civilian Labor Force - 18 Years & Over (NSA)',
    'LNU01376975': 'Participation Rate - 18 Years & Over (NSA)',
    'LNU02076975': 'Employment - 18 Years & Over (NSA)',
    'LNU03076975': 'Unemployment - 18 Years & Over (NSA)',
    'LNU04076975': 'Unemployment Rate - 18 Years & Over (NSA)',
    'LNU05076975': 'Not in Labor Force - 18 Years & Over (NSA)',
    
    # 25세 이상 (Not Seasonally Adjusted)
    'LNU01000048': 'Civilian Labor Force - 25 Years & Over (NSA)',
    'LNU01300048': 'Participation Rate - 25 Years & Over (NSA)',
    'LNU02000048': 'Employment - 25 Years & Over (NSA)',
    'LNU02500048': 'Employed Full Time - 25 Years & Over (NSA)',
    'LNU02600048': 'Employed Part Time - 25 Years & Over (NSA)',
    'LNU03000048': 'Unemployment - 25 Years & Over (NSA)',
    'LNU03100048': 'Unemployed Looking for Full-time - 25 Years & Over (NSA)',
    'LNU03200048': 'Unemployed Looking for Part-time - 25 Years & Over (NSA)',
    'LNU04000048': 'Unemployment Rate - 25 Years & Over (NSA)',
    
    # 45세 이상 (Not Seasonally Adjusted)
    'LNU01000092': 'Civilian Labor Force - 45 Years & Over (NSA)',
    'LNU01300092': 'Participation Rate - 45 Years & Over (NSA)',
    'LNU02000092': 'Employment - 45 Years & Over (NSA)',
    'LNU03000092': 'Unemployment - 45 Years & Over (NSA)',
    'LNU04000092': 'Unemployment Rate - 45 Years & Over (NSA)',
    'LNU05000092': 'Not in Labor Force - 45 Years & Over (NSA)',
    
    # 55세 이상 (Not Seasonally Adjusted)
    'LNU01024230': 'Civilian Labor Force - 55 Years & Over (NSA)',
    'LNU01324230': 'Participation Rate - 55 Years & Over (NSA)',
    'LNU02024230': 'Employment - 55 Years & Over (NSA)',
    'LNU02524230': 'Employed Full Time - 55 Years & Over (NSA)',
    'LNU02624230': 'Employed Part Time - 55 Years & Over (NSA)',
    'LNU03024230': 'Unemployment - 55 Years & Over (NSA)',
    'LNU03124230': 'Unemployed Looking for Full-time - 55 Years & Over (NSA)',
    'LNU03224230': 'Unemployed Looking for Part-time - 55 Years & Over (NSA)',
    'LNU04024230': 'Unemployment Rate - 55 Years & Over (NSA)',
    'LNU05024230': 'Not in Labor Force - 55 Years & Over (NSA)',
    
    # 65세 이상 (Not Seasonally Adjusted)
    'LNU01000097': 'Civilian Labor Force - 65 Years & Over (NSA)',
    'LNU01300097': 'Participation Rate - 65 Years & Over (NSA)',
    'LNU02000097': 'Employment - 65 Years & Over (NSA)',
    'LNU03000097': 'Unemployment - 65 Years & Over (NSA)',
    'LNU04000097': 'Unemployment Rate - 65 Years & Over (NSA)',
    'LNU05000097': 'Not in Labor Force - 65 Years & Over (NSA)',
    
    # === 추가 인종별 노동시장 지표 (Not Seasonally Adjusted) ===
    # 백인 (White)
    'LNU01000003': 'Civilian Labor Force - White (NSA)',
    'LNU01300003': 'Participation Rate - White (NSA)',
    'LNU02000003': 'Employment - White (NSA)',
    'LNU02500003': 'Employed Full Time - White (NSA)',
    'LNU02600003': 'Employed Part Time - White (NSA)',
    'LNU03000003': 'Unemployment - White (NSA)',
    'LNU04000003': 'Unemployment Rate - White (NSA)',
    'LNU05000003': 'Not in Labor Force - White (NSA)',
    
    # 흑인 (Black or African American)
    'LNU01000006': 'Civilian Labor Force - Black or African American (NSA)',
    'LNU01300006': 'Participation Rate - Black or African American (NSA)',
    'LNU02000006': 'Employment - Black or African American (NSA)',
    'LNU02500006': 'Employed Full Time - Black or African American (NSA)',
    'LNU02600006': 'Employed Part Time - Black or African American (NSA)',
    'LNU03000006': 'Unemployment - Black or African American (NSA)',
    'LNU04000006': 'Unemployment Rate - Black or African American (NSA)',
    'LNU05000006': 'Not in Labor Force - Black or African American (NSA)',
    
    # 아시아계 (Asian)
    'LNU01032183': 'Civilian Labor Force - Asian (NSA)',
    'LNU01332183': 'Participation Rate - Asian (NSA)',
    'LNU02032183': 'Employment - Asian (NSA)',
    'LNU02532183': 'Employed Full Time - Asian (NSA)',
    'LNU02632183': 'Employed Part Time - Asian (NSA)',
    'LNU03032183': 'Unemployment - Asian (NSA)',
    'LNU04032183': 'Unemployment Rate - Asian (NSA)',
    'LNU05032183': 'Not in Labor Force - Asian (NSA)',
    
    # 아메리카 원주민 (American Indian or Alaska Native)
    'LNU01035243': 'Civilian Labor Force - American Indian or Alaska Native (NSA)',
    'LNU01335243': 'Participation Rate - American Indian or Alaska Native (NSA)',
    'LNU02035243': 'Employment - American Indian or Alaska Native (NSA)',
    'LNU03035243': 'Unemployment - American Indian or Alaska Native (NSA)',
    'LNU04035243': 'Unemployment Rate - American Indian or Alaska Native (NSA)',
    'LNU05035243': 'Not in Labor Force - American Indian or Alaska Native (NSA)',
    
    # 하와이 원주민 및 태평양 섬 주민 (Native Hawaiian or Other Pacific Islander)
    'LNU01035553': 'Civilian Labor Force - Native Hawaiian or Other Pacific Islander (NSA)',
    'LNU01335553': 'Participation Rate - Native Hawaiian or Other Pacific Islander (NSA)',
    'LNU02035553': 'Employment - Native Hawaiian or Other Pacific Islander (NSA)',
    'LNU03035553': 'Unemployment - Native Hawaiian or Other Pacific Islander (NSA)',
    'LNU04035553': 'Unemployment Rate - Native Hawaiian or Other Pacific Islander (NSA)',
    'LNU05035553': 'Not in Labor Force - Native Hawaiian or Other Pacific Islander (NSA)',
    
    # === 기본 지표들의 Not Seasonally Adjusted 버전 ===
    'LNU01000000': 'Civilian Labor Force (NSA)',
    'LNU01300000': 'Civilian Labor Force Participation Rate (NSA)',
    'LNU02000000': 'Employment Level (NSA)',
    'LNU02500000': 'Employed, Usually Work Full Time (NSA)',
    'LNU02600000': 'Employed, Usually Work Part Time (NSA)',
    'LNU03000000': 'Unemployment Level (NSA)',
    'LNU03100000': 'Unemployed Looking for Full-time Work (NSA)',
    'LNU03200000': 'Unemployed Looking for Part-time Work (NSA)',
    'LNU04000000': 'Unemployment Rate (NSA)',
    'LNU04100000': 'Unemployment Rate of the Full-time Labor Force (NSA)',
    'LNU04200000': 'Unemployment Rate of the Part-time Labor Force (NSA)',
    'LNU05000000': 'Not in Labor Force (NSA)'
}

# 한국어 이름 매핑
CPS_KOREAN_NAMES = {
    # 주요 노동시장 지표
    'LNS11000000': '경제활동인구',
    'LNS11300000': '경제활동참가율',
    'LNS12000000': '취업자수',
    'LNS12300000': '고용률',
    'LNS12500000': '풀타임 취업자',
    'LNS12600000': '파트타임 취업자',
    'LNS13000000': '실업자수',
    'LNS14000000': '실업률',
    
    # 연령별 실업률
    'LNS14000012': '실업률 - 16-19세',
    'LNS14000025': '실업률 - 20세 이상 남성',
    'LNS14000026': '실업률 - 20세 이상 여성',
    
    # 인종별 실업률
    'LNS14000003': '실업률 - 백인',
    'LNS14000006': '실업률 - 흑인',
    'LNS14032183': '실업률 - 아시아계',
    'LNS14000009': '실업률 - 히스패닉',
    
    # 교육수준별 실업률
    'LNS14027659': '실업률 - 고졸 미만',
    'LNS14027660': '실업률 - 고졸',
    'LNS14027689': '실업률 - 대학 중퇴/전문대',
    'LNS14027662': '실업률 - 대졸 이상',
    
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
# === 전역 데이터 저장소 ===

# API 설정
BLS_SESSION = None
API_INITIALIZED = False

# 전역 데이터 저장소
CPS_DATA = {
    'raw_data': pd.DataFrame(),      # 원본 레벨 데이터
    'mom_data': pd.DataFrame(),      # 전월대비 변화 데이터
    'yoy_data': pd.DataFrame(),      # 전년동월대비 변화 데이터
    'latest_values': {},             # 최신 값들
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0
    }
}

# %%
# === API 초기화 및 데이터 수집 함수들 ===

def initialize_bls_api():
    """BLS API 세션 초기화"""
    global BLS_SESSION
    
    if not BLS_API_AVAILABLE:
        print("⚠️ BLS API 사용 불가 (requests 라이브러리 없음)")
        return False
    
    try:
        BLS_SESSION = requests.Session()
        print("✓ BLS API 세션 초기화 성공")
        return True
    except Exception as e:
        print(f"⚠️ BLS API 초기화 실패: {e}")
        return False

def switch_api_key():
    """API 키를 전환하는 함수"""
    global CURRENT_API_KEY
    if CURRENT_API_KEY == BLS_API_KEY:
        CURRENT_API_KEY = BLS_API_KEY2
        print("🔄 BLS API 키를 BLS_API_KEY2로 전환했습니다.")
    elif CURRENT_API_KEY == BLS_API_KEY2:
        CURRENT_API_KEY = BLS_API_KEY3
        print("🔄 BLS API 키를 BLS_API_KEY3로 전환했습니다.")
    else:
        CURRENT_API_KEY = BLS_API_KEY
        print("🔄 BLS API 키를 BLS_API_KEY로 전환했습니다.")

def get_bls_data(series_id, start_year=2020, end_year=None):
    """
    BLS API에서 데이터 가져오기 (API 키 이중화 지원)
    
    Args:
        series_id: BLS 시리즈 ID
        start_year: 시작 연도
        end_year: 종료 연도 (None이면 현재 연도)
    
    Returns:
        pandas.Series: 날짜를 인덱스로 하는 시리즈 데이터
    """
    global CURRENT_API_KEY
    
    if not BLS_API_AVAILABLE or BLS_SESSION is None:
        print(f"❌ BLS API 사용 불가 - {series_id}")
        return None
    
    if end_year is None:
        end_year = datetime.datetime.now().year
    
    # BLS API 요청 데이터
    data = {
        'seriesid': [series_id],
        'startyear': str(start_year),
        'endyear': str(end_year),
        'catalog': False,
        'calculations': False,
        'annualaverage': False
    }
    
    # 현재 사용 중인 API 키 추가
    if CURRENT_API_KEY:
        data['registrationkey'] = CURRENT_API_KEY
    
    url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
    headers = {'Content-type': 'application/json'}
    
    try:
        current_key_name = "주요" if CURRENT_API_KEY == BLS_API_KEY else "백업"
        print(f"📊 BLS에서 로딩 ({current_key_name}): {series_id}")
        response = BLS_SESSION.post(url, data=json.dumps(data), headers=headers, timeout=30)
        response.raise_for_status()
        
        json_data = response.json()
        
        if json_data.get('status') == 'REQUEST_SUCCEEDED':
            series_data = json_data['Results']['series'][0]['data']
            
            # 데이터 정리
            dates = []
            values = []
            
            for item in series_data:
                try:
                    year = int(item['year'])
                    period = item['period']
                    if period.startswith('M'):
                        month = int(period[1:])
                        date = pd.Timestamp(year, month, 1)
                        value = float(item['value'])
                        
                        dates.append(date)
                        values.append(value)
                except (ValueError, KeyError):
                    continue
            
            if dates and values:
                # 날짜순 정렬
                df = pd.DataFrame({'date': dates, 'value': values})
                df = df.sort_values('date')
                series = pd.Series(df['value'].values, index=df['date'], name=series_id)
                
                print(f"✓ BLS 성공: {series_id} ({len(series)}개 포인트)")
                return series
            else:
                print(f"❌ BLS 데이터 없음: {series_id}")
                return None
        else:
            error_msg = json_data.get('message', 'Unknown error')
            print(f"⚠️ BLS API 오류: {error_msg}")
            
            # Daily threshold 초과인 경우 API 키 전환 시도
            if 'daily threshold' in error_msg.lower() or 'daily quota' in error_msg.lower():
                print("📈 Daily threshold 초과 감지 - API 키 전환 시도")
                switch_api_key()
                
                # 새로운 API 키로 재시도
                data['registrationkey'] = CURRENT_API_KEY
                try:
                    print(f"🔄 새 API 키로 재시도: {series_id}")
                    response = BLS_SESSION.post(url, data=json.dumps(data), headers=headers, timeout=30)
                    response.raise_for_status()
                    
                    json_data = response.json()
                    if json_data.get('status') == 'REQUEST_SUCCEEDED':
                        series_data = json_data['Results']['series'][0]['data']
                        
                        dates = []
                        values = []
                        
                        for item in series_data:
                            try:
                                year = int(item['year'])
                                period = item['period']
                                if period.startswith('M'):
                                    month = int(period[1:])
                                    date = pd.Timestamp(year, month, 1)
                                    value = float(item['value'])
                                    
                                    dates.append(date)
                                    values.append(value)
                            except (ValueError, KeyError):
                                continue
                        
                        if dates and values:
                            df = pd.DataFrame({'date': dates, 'value': values})
                            df = df.sort_values('date')
                            series = pd.Series(df['value'].values, index=df['date'], name=series_id)
                            
                            print(f"✅ 백업 API 키로 성공: {series_id} ({len(series)}개 포인트)")
                            return series
                    
                    print(f"❌ 백업 API 키로도 실패: {series_id}")
                    return None
                except Exception as e:
                    print(f"❌ 백업 API 키 요청 실패: {series_id} - {e}")
                    return None
            else:
                return None
            
    except Exception as e:
        print(f"❌ BLS 요청 실패: {series_id} - {e}")
        return None

# %%
# === 데이터 계산 및 로드 함수들 ===

def calculate_mom_change(data):
    """전월대비 변화 계산 (레벨 데이터는 차이, 비율 데이터는 차이)"""
    if data.name and ('Rate' in data.name or 'Ratio' in data.name or data.name.startswith('LNS14')):
        # 비율 데이터는 차이 계산
        return data.diff()
    else:
        # 레벨 데이터는 변화율 계산
        return ((data / data.shift(1)) - 1) * 100

def calculate_yoy_change(data):
    """전년동월대비 변화 계산"""
    if data.name and ('Rate' in data.name or 'Ratio' in data.name or data.name.startswith('LNS14')):
        # 비율 데이터는 차이 계산
        return data - data.shift(12)
    else:
        # 레벨 데이터는 변화율 계산
        return ((data / data.shift(12)) - 1) * 100

def get_series_data(series_id, start_date='2020-01-01', end_date=None, min_points=12, is_update=False):
    """BLS API를 사용한 개별 시리즈 데이터 가져오기"""
    if not BLS_SESSION:
        print(f"❌ BLS 세션이 초기화되지 않음: {series_id}")
        return None
    
    try:
        start_year = pd.to_datetime(start_date).year
        end_year = pd.to_datetime(end_date).year if end_date else None
        data = get_bls_data(series_id, start_year, end_year)
        
        if data is None or len(data) == 0:
            print(f"❌ 데이터 없음: {series_id}")
            return None
        
        # 날짜 필터링
        if start_date:
            data = data[data.index >= start_date]
        if end_date:
            data = data[data.index <= end_date]
        
        # NaN 제거
        data = data.dropna()
        
        # 업데이트 모드일 때는 최소 데이터 포인트 요구량을 낮춤
        if is_update:
            min_required = 1  # 업데이트 시에는 1개 포인트만 있어도 허용
        else:
            min_required = min_points
        
        if len(data) < min_required:
            print(f"❌ 데이터 부족: {series_id} ({len(data)}개)")
            return None
        
        print(f"✓ 성공: {series_id} ({len(data)}개 포인트)")
        return data
        
    except Exception as e:
        print(f"❌ 오류: {series_id} - {e}")
        return None

# %%
# === CSV 저장/로드 함수들 ===

def save_cps_data_to_csv(file_path='/home/jyp0615/us_eco/cps_data.csv'):
    """
    현재 로드된 CPS 데이터를 CSV 파일로 저장
    
    Args:
        file_path: 저장할 CSV 파일 경로
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 저장할 데이터가 없습니다. 먼저 데이터를 로드하세요.")
        return False
    
    try:
        # raw_data와 메타데이터를 함께 저장
        raw_data = CPS_DATA['raw_data']
        
        # 저장할 데이터 준비
        save_data = {
            'timestamp': raw_data.index.tolist(),
            **{col: raw_data[col].tolist() for col in raw_data.columns}
        }
        
        # DataFrame으로 변환하여 저장
        save_df = pd.DataFrame(save_data)
        save_df.to_csv(file_path, index=False, encoding='utf-8')
        
        # 메타데이터도 별도 저장
        meta_file = file_path.replace('.csv', '_meta.json')
        import json
        with open(meta_file, 'w', encoding='utf-8') as f:
            meta_data = {
                'load_time': CPS_DATA['load_info']['load_time'].isoformat() if CPS_DATA['load_info']['load_time'] else None,
                'start_date': CPS_DATA['load_info']['start_date'],
                'series_count': CPS_DATA['load_info']['series_count'],
                'data_points': CPS_DATA['load_info']['data_points'],
                'latest_values': CPS_DATA['latest_values']
            }
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ CPS 데이터 저장 완료: {file_path}")
        print(f"✅ 메타데이터 저장 완료: {meta_file}")
        return True
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_cps_data_from_csv(file_path='/home/jyp0615/us_eco/cps_data.csv'):
    """
    CSV 파일에서 CPS 데이터 로드
    
    Args:
        file_path: 로드할 CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global CPS_DATA
    
    import os
    if not os.path.exists(file_path):
        print(f"⚠️ CSV 파일이 없습니다: {file_path}")
        return False
    
    try:
        # CSV 데이터 로드
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # timestamp 컬럼을 날짜 인덱스로 변환
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        # 메타데이터 로드
        meta_file = file_path.replace('.csv', '_meta.json')
        latest_values = {}
        if os.path.exists(meta_file):
            import json
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
                latest_values = meta_data.get('latest_values', {})
        
        # 전역 저장소에 저장
        CPS_DATA['raw_data'] = df
        CPS_DATA['mom_data'] = df.apply(calculate_mom_change)
        CPS_DATA['yoy_data'] = df.apply(calculate_yoy_change)
        CPS_DATA['latest_values'] = latest_values
        CPS_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': df.index[0].strftime('%Y-%m-%d') if len(df) > 0 else None,
            'series_count': len(df.columns),
            'data_points': len(df)
        }
        
        print(f"✅ CSV에서 CPS 데이터 로드 완료: {file_path}")
        print_load_info()
        return True
        
    except Exception as e:
        print(f"❌ CSV 로드 실패: {e}")
        return False

# %%
# === 스마트 업데이트 함수들 ===

def check_recent_data_consistency(series_list=None, check_count=3):
    """
    최근 N개 데이터의 일치성을 확인하는 함수 (스마트 업데이트)
    
    Args:
        series_list: 확인할 시리즈 리스트 (None이면 주요 시리즈만)
        check_count: 확인할 최근 데이터 개수
    
    Returns:
        dict: 일치성 확인 결과 (need_update, reason, mismatches)
    """
    if not CPS_DATA['load_info']['loaded']:
        return {'need_update': True, 'reason': '기존 데이터 없음'}
    
    if series_list is None:
        # 주요 시리즈만 체크 (속도 향상)
        series_list = ['LNS14000000', 'LNS11300000', 'LNS12300000', 'LNS13327709']
    
    # BLS API 초기화
    if not initialize_bls_api():
        return {'need_update': True, 'reason': 'API 초기화 실패'}
    
    print(f"📊 최근 {check_count}개 데이터 일치성 확인 중...")
    
    mismatches = []
    new_data_available = False
    
    for series_id in series_list:
        if series_id not in CPS_DATA['raw_data'].columns:
            continue
        
        existing_data = CPS_DATA['raw_data'][series_id]
        
        # 최근 데이터 가져오기
        current_year = datetime.datetime.now().year
        api_data = get_bls_data(series_id, current_year - 1, current_year)
        
        if api_data is None or len(api_data) == 0:
            mismatches.append({
                'series': series_id,
                'reason': 'API 데이터 없음'
            })
            continue
        
        # 먼저 새로운 데이터가 있는지 확인
        latest_existing = existing_data.index[-1]
        latest_api = api_data.index[-1]
        
        if latest_api > latest_existing:
            new_data_available = True
            mismatches.append({
                'series': series_id,
                'reason': '새로운 데이터 존재',
                'existing_latest': latest_existing,
                'api_latest': latest_api
            })
            continue  # 새 데이터가 있으면 다른 체크는 건너뜀
        
        # 최근 N개 데이터 비교 (새 데이터가 없을 때만)
        existing_recent = existing_data.tail(check_count)
        api_recent = api_data.tail(check_count)
        
        # 날짜와 값 모두 비교
        for date in existing_recent.index[-check_count:]:
            if date in api_recent.index:
                existing_val = existing_recent.loc[date]
                api_val = api_recent.loc[date]
                
                # 값이 다르면 불일치
                if abs(existing_val - api_val) > 0.01:  # 0.01 이상 차이
                    mismatches.append({
                        'series': series_id,
                        'date': date,
                        'existing': existing_val,
                        'api': api_val,
                        'diff': abs(existing_val - api_val)
                    })
            else:
                mismatches.append({
                    'series': series_id,
                    'date': date,
                    'existing': existing_recent.loc[date],
                    'api': None,
                    'reason': '날짜 없음'
                })
    
    # 결과 판정 로직 개선
    if new_data_available:
        print(f"🆕 새로운 데이터 감지: 업데이트 필요")
        for mismatch in mismatches[:3]:
            if 'reason' in mismatch and mismatch['reason'] == '새로운 데이터 존재':
                print(f"   - {mismatch['series']}: {mismatch['existing_latest']} → {mismatch['api_latest']}")
        return {'need_update': True, 'reason': '새로운 데이터', 'mismatches': mismatches}
    elif any(m for m in mismatches if m.get('reason') != '새로운 데이터 존재'):
        # 값 불일치가 있는 경우
        value_mismatches = [m for m in mismatches if m.get('reason') != '새로운 데이터 존재']
        print(f"⚠️ 데이터 불일치 발견: {len(value_mismatches)}개")
        for mismatch in value_mismatches[:3]:
            print(f"   - {mismatch}")
        return {'need_update': True, 'reason': '데이터 불일치', 'mismatches': mismatches}
    else:
        print("✅ 최근 데이터가 완전히 일치합니다. 업데이트를 건너뜁니다.")
        return {'need_update': False, 'reason': '데이터 일치'}

def update_cps_data_from_api(start_date=None, series_list=None, smart_update=True):
    """
    API를 사용하여 기존 데이터 업데이트 (스마트 업데이트 지원)
    
    Args:
        start_date: 업데이트 시작 날짜 (None이면 마지막 데이터 이후부터)
        series_list: 업데이트할 시리즈 리스트 (None이면 모든 시리즈)
        smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
        bool: 업데이트 성공 여부
    """
    global CPS_DATA
    
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 기존 데이터가 없습니다. 전체 로드를 수행합니다.")
        return load_all_cps_data()
    
    # 스마트 업데이트 활성화시 최근 데이터 일치성 확인
    if smart_update:
        consistency_check = check_recent_data_consistency()
        
        # 업데이트 필요 없으면 바로 종료
        if not consistency_check['need_update']:
            print("🎯 데이터가 최신 상태입니다. API 호출을 건너뜁니다.")
            return True
        
        # 업데이트가 필요한 경우 처리
        if consistency_check['reason'] == '데이터 불일치':
            print("⚠️ 최근 3개 데이터 불일치로 인한 전체 재로드")
            # 최근 3개월 데이터부터 다시 로드
            last_date = CPS_DATA['raw_data'].index[-1]
            start_date = (last_date - pd.DateOffset(months=3)).strftime('%Y-%m-01')
        elif consistency_check['reason'] == '기존 데이터 없음':
            # 전체 재로드
            return load_all_cps_data(force_reload=True)
        else:
            print(f"🔄 업데이트 진행: {consistency_check['reason']}")
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    # 업데이트 시작 날짜 결정
    if start_date is None:
        last_date = CPS_DATA['raw_data'].index[-1]
        # 마지막 날짜의 다음 달부터 업데이트
        start_date = (last_date + pd.DateOffset(months=1)).strftime('%Y-%m-01')
    
    print(f"🔄 CPS 데이터 업데이트 시작 ({start_date}부터)")
    print("="*50)
    
    if series_list is None:
        series_list = list(CPS_SERIES.keys())
    
    updated_data = {}
    new_count = 0
    
    for series_id in series_list:
        if series_id not in CPS_SERIES:
            continue
        
        # 새로운 데이터 가져오기
        start_year = pd.to_datetime(start_date).year
        new_data = get_bls_data(series_id, start_year)
        
        if new_data is not None and len(new_data) > 0:
            # 기존 데이터와 병합
            if series_id in CPS_DATA['raw_data'].columns:
                existing_data = CPS_DATA['raw_data'][series_id]
                original_count = existing_data.notna().sum()
                
                # 개선된 병합 방식: 인덱스 먼저 확장 후 값 할당
                all_dates = existing_data.index.union(new_data.index).sort_values()
                combined_data = existing_data.reindex(all_dates)
                
                # 새 데이터로 업데이트 (기존 값 덮어쓰기)
                for date, value in new_data.items():
                    combined_data.loc[date] = value
                
                updated_data[series_id] = combined_data
                
                # 실제 추가된 데이터 포인트 수 계산
                final_count = combined_data.notna().sum()
                new_points = final_count - original_count
                new_count += new_points
                
                if new_points > 0:
                    print(f"✅ 업데이트: {series_id} (기존 {original_count}개 + 신규 {len(new_data)}개 → 총 {final_count}개, 실제 추가: {new_points}개)")
                else:
                    print(f"ℹ️  최신 상태: {series_id}")
            else:
                updated_data[series_id] = new_data
                new_count += len(new_data)
                print(f"✅ 신규 추가: {series_id} ({len(new_data)}개 포인트)")
    
    if updated_data:
        # 전역 저장소 업데이트
        updated_df = pd.DataFrame(updated_data)
        CPS_DATA['raw_data'] = updated_df
        CPS_DATA['mom_data'] = updated_df.apply(calculate_mom_change)
        CPS_DATA['yoy_data'] = updated_df.apply(calculate_yoy_change)
        
        # 최신 값 업데이트
        for series_id in updated_data.keys():
            if series_id in CPS_DATA['raw_data'].columns:
                raw_data = CPS_DATA['raw_data'][series_id].dropna()
                if len(raw_data) > 0:
                    CPS_DATA['latest_values'][series_id] = raw_data.iloc[-1]
        
        # 로드 정보 업데이트
        CPS_DATA['load_info']['load_time'] = datetime.datetime.now()
        CPS_DATA['load_info']['series_count'] = len(updated_df.columns)
        CPS_DATA['load_info']['data_points'] = len(updated_df)
        
        print(f"\n✅ 업데이트 완료! 새로 추가된 데이터: {new_count}개 포인트")
        print_load_info()
        
        # 자동으로 CSV에 저장
        save_cps_data_to_csv()
        
        return True
    else:
        print("\n⚠️ 업데이트할 새로운 데이터가 없습니다.")
        return False

# %%
# === 메인 데이터 로드 함수 ===

def load_all_cps_data(start_date='2020-01-01', series_list=None, force_reload=False, csv_file='/home/jyp0615/us_eco/cps_data.csv'):
    """
    CPS 데이터 로드 (CSV 우선, API 백업 방식)
    
    Args:
        start_date: 시작 날짜
        series_list: 로드할 시리즈 리스트 (None이면 모든 시리즈)
        force_reload: 강제 재로드 여부
        csv_file: CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global CPS_DATA
    
    # 이미 로드된 경우 스킵
    if CPS_DATA['load_info']['loaded'] and not force_reload:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    # CSV 파일이 있으면 우선 로드 시도
    import os
    if not force_reload and os.path.exists(csv_file):
        print("📂 기존 CSV 파일에서 데이터 로드 시도...")
        if load_cps_data_from_csv(csv_file):
            print("🔄 CSV 로드 후 스마트 업데이트 시도...")
            # CSV 로드 성공 후 스마트 업데이트
            update_cps_data_from_api(smart_update=True)
            return True
        else:
            print("⚠️ CSV 로드 실패, API에서 전체 로드 시도...")
    
    print("🚀 CPS 데이터 로딩 시작... (BLS API 전용)")
    print("="*50)
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    if series_list is None:
        series_list = list(CPS_SERIES.keys())
    
    # 데이터 수집
    raw_data_dict = {}
    mom_data_dict = {}
    yoy_data_dict = {}
    latest_values = {}
    
    for series_id in series_list:
        if series_id not in CPS_SERIES:
            print(f"⚠️ 알 수 없는 시리즈: {series_id}")
            continue
        
        # 원본 데이터 가져오기
        series_data = get_series_data(series_id, start_date)
        
        if series_data is not None and len(series_data) > 0:
            # 원본 데이터 저장
            raw_data_dict[series_id] = series_data
            
            # MoM 계산
            mom_data = calculate_mom_change(series_data)
            mom_data_dict[series_id] = mom_data
            
            # YoY 계산
            yoy_data = calculate_yoy_change(series_data)
            yoy_data_dict[series_id] = yoy_data
            
            # 최신 값 저장
            if len(series_data.dropna()) > 0:
                latest_value = series_data.dropna().iloc[-1]
                latest_values[series_id] = latest_value
            else:
                print(f"⚠️ 데이터 없음: {series_id}")
        else:
            print(f"❌ 데이터 로드 실패: {series_id}")
    
    # 로드된 데이터가 너무 적으면 오류 발생
    if len(raw_data_dict) < 5:  # 최소 5개 시리즈는 있어야 함
        error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data_dict)}개 (최소 5개 필요)"
        print(error_msg)
        raise ValueError(error_msg)
    
    # 전역 저장소에 저장
    CPS_DATA['raw_data'] = pd.DataFrame(raw_data_dict)
    CPS_DATA['mom_data'] = pd.DataFrame(mom_data_dict)
    CPS_DATA['yoy_data'] = pd.DataFrame(yoy_data_dict)
    CPS_DATA['latest_values'] = latest_values
    CPS_DATA['load_info'] = {
        'loaded': True,
        'load_time': datetime.datetime.now(),
        'start_date': start_date,
        'series_count': len(raw_data_dict),
        'data_points': len(CPS_DATA['raw_data']) if not CPS_DATA['raw_data'].empty else 0
    }
    
    print("\n✅ 데이터 로딩 완료!")
    print_load_info()
    
    # 자동으로 CSV에 저장
    save_cps_data_to_csv(csv_file)
    
    return True

def print_load_info():
    """로드 정보 출력"""
    info = CPS_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not CPS_DATA['raw_data'].empty:
        date_range = f"{CPS_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {CPS_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

def clear_data():
    """데이터 초기화"""
    global CPS_DATA
    CPS_DATA = {
        'raw_data': pd.DataFrame(),
        'mom_data': pd.DataFrame(),
        'yoy_data': pd.DataFrame(),
        'latest_values': {},
        'load_info': {'loaded': False, 'load_time': None, 'start_date': None, 'series_count': 0, 'data_points': 0}
    }
    print("🗑️ 데이터가 초기화되었습니다")

# %%
# === 데이터 접근 함수들 ===

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_cps_data()를 먼저 실행하세요.")
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
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_cps_data()를 먼저 실행하세요.")
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
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_cps_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPS_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in CPS_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPS_DATA['yoy_data'][available_series].copy()

def get_latest_values(series_names=None):
    """최신 값들 반환"""
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_cps_data()를 먼저 실행하세요.")
        return {}
    
    if series_names is None:
        return CPS_DATA['latest_values'].copy()
    
    return {name: CPS_DATA['latest_values'].get(name, 0) for name in series_names if name in CPS_DATA['latest_values']}

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not CPS_DATA['load_info']['loaded']:
        return []
    return list(CPS_DATA['raw_data'].columns)

# %%
# === KPDS 스타일 시각화 함수들 ===

def create_labor_market_dashboard():
    """
    주요 노동시장 지표 대시보드 생성 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    print("US Labor Market Dashboard")
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 데이터 준비
    data_dict = {
        'LNS14000000': '실업률',
        'LNS11300000': '경제활동참가율', 
        'LNS12300000': '고용률',
        'LNS13327709': 'U-6 실업률'
    }
    
    df = get_raw_data(list(data_dict.keys()))
    df = df.rename(columns=data_dict)
    
    # KPDS 다중 라인 차트로 생성
    return df_multi_line_chart(
        df=df,
        ytitle="%"
    )

def create_unemployment_by_demographics(demographic_type='race'):
    """
    인구통계별 실업률 비교 차트 (KPDS 포맷)
    
    Args:
        demographic_type: 'race', 'age', 'education'
    
    Returns:
        plotly figure
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if demographic_type == 'race':
        series_dict = {
            'LNS14000003': '백인', 
            'LNS14000006': '흑인',
            'LNS14032183': '아시아계', 
            'LNS14000009': '히스패닉'
        }
        print_title = "실업률 - 인종별"
    elif demographic_type == 'age':
        series_dict = {
            'LNS14000012': '16-19세',
            'LNS14000025': '20세+ 남성', 
            'LNS14000026': '20세+ 여성'
        }
        print_title = "실업률 - 연령/성별"
    elif demographic_type == 'education':
        series_dict = {
            'LNS14027659': '고졸 미만',
            'LNS14027660': '고졸', 
            'LNS14027689': '대학 중퇴/전문대',
            'LNS14027662': '대졸 이상'
        }
        print_title = "실업률 - 교육수준별"
    else:
        print(f"⚠️ 잘못된 demographic_type: {demographic_type}")
        return None
    
    # 데이터 준비
    df = get_raw_data(list(series_dict.keys()))
    df = df.rename(columns=series_dict)
    
    print(print_title)
    
    # KPDS 다중 라인 차트로 생성
    return df_multi_line_chart(
        df=df,
        ytitle="%"
    )

def create_unemployment_duration_analysis():
    """
    실업 기간 분석 차트 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    print("실업 기간 분석")
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 평균/중간값 실업기간 데이터
    duration_dict = {
        'LNS13008275': '평균 실업기간',
        'LNS13008276': '중간값 실업기간'
    }
    
    df = get_raw_data(list(duration_dict.keys()))
    df = df.rename(columns=duration_dict)
    
    # KPDS 다중 라인 차트로 생성
    return df_multi_line_chart(
        df=df,
        ytitle="주"
    )

def create_employment_type_analysis():
    """
    고용 형태 분석 차트 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    print("고용 형태 분석")
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 고용 형태별 데이터
    employment_dict = {
        'LNS12500000': '풀타임 취업자',
        'LNS12600000': '파트타임 취업자',
        'LNS12032194': '경제적 이유 파트타임'
    }
    
    df = get_raw_data(list(employment_dict.keys()))
    df = df.rename(columns=employment_dict)
    
    # KPDS 다중 라인 차트로 생성
    return df_multi_line_chart(
        df=df,
        ytitle="천명"
    )

def create_labor_force_flows():
    """
    노동시장 참여/이탈 흐름 분석 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 실업 유형별 데이터
    flows_dict = {
        'LNS13023621': '실직자',
        'LNS13023705': '자발적 이직자', 
        'LNS13023557': '재진입자',
        'LNS13023569': '신규진입자'
    }
    
    df = get_raw_data(list(flows_dict.keys()))
    df = df.rename(columns=flows_dict)
    
    # KPDS 다중 라인 차트로 생성
    print("실업자 구성 분석")
    
    return df_multi_line_chart(
        df=df,
        ytitle="천명"
    )

def create_u3_vs_u6_comparison():
    """
    U-3 vs U-6 실업률 비교 차트 (KPDS 포맷)
    
    Returns:
        plotly figure  
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # U-3 vs U-6 데이터
    u_rates_dict = {
        'LNS14000000': 'U-3 (공식 실업률)',
        'LNS13327709': 'U-6 (광의 실업률)'
    }
    
    df = get_raw_data(list(u_rates_dict.keys()))
    df = df.rename(columns=u_rates_dict)
    
    # KPDS 다중 라인 차트로 생성
    print("U-3 vs U-6 실업률 비교")
    
    return df_multi_line_chart(
        df=df,
        ytitle="%"
    )

def create_labor_market_heatmap(start_date=None):
    """
    노동시장 지표 히트맵 (상관관계 분석)
    
    Args:
        start_date: 분석 시작 날짜
    
    Returns:
        plotly figure
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주요 지표 선택
    key_series = [
        'LNS14000000',  # 실업률
        'LNS11300000',  # 경제활동참가율
        'LNS12300000',  # 고용률
        'LNS13327709',  # U-6
        'LNS12032194',  # 경제적 이유 파트타임
        'LNS15026645',  # 구직단념자
        'LNS13008275',  # 평균 실업기간
        'LNS12026620'   # 복수직업 보유율
    ]
    
    # 데이터 가져오기
    data = get_raw_data(key_series)
    
    if start_date:
        data = data[data.index >= start_date]
    
    # 변화율 계산
    data_changes = data.pct_change().dropna()
    
    # 상관관계 계산
    correlation = data_changes.corr()
    
    # 라벨 매핑
    labels = [CPS_KOREAN_NAMES.get(col, col) for col in correlation.columns]
    
    # 히트맵 생성
    fig = go.Figure(data=go.Heatmap(
        z=correlation.values,
        x=labels,
        y=labels,
        colorscale='RdBu',
        zmid=0,
        text=np.round(correlation.values, 2),
        texttemplate='%{text}',
        textfont={"size": 10},
        reversescale=True
    ))
    
    # 레이아웃 설정
    print("노동시장 지표 상관관계 분석")
    
    fig.update_layout(
        width=800,
        height=700,
        paper_bgcolor='white',
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL),
        xaxis=dict(tickangle=-45),
        margin=dict(l=150, r=50, t=100, b=150)
    )
    
    return fig

def create_monthly_change_table(months_back=6):
    """
    최근 N개월 주요 지표 변화 테이블
    
    Args:
        months_back: 표시할 개월 수
    
    Returns:
        plotly figure
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주요 지표 선택
    key_series = [
        'LNS14000000',  # 실업률
        'LNS11300000',  # 경제활동참가율
        'LNS12300000',  # 고용률
        'LNS13327709',  # U-6
        'LNS12000000',  # 취업자수
        'LNS13000000'   # 실업자수
    ]
    
    # 데이터 가져오기
    raw_data = get_raw_data(key_series)
    mom_data = get_mom_data(key_series)
    
    # 최근 N개월 데이터
    recent_raw = raw_data.tail(months_back + 1)
    recent_mom = mom_data.tail(months_back)
    
    # 테이블 데이터 준비
    headers = ['지표'] + [date.strftime('%Y-%m') for date in recent_raw.index[1:]]
    
    table_data = []
    for series_id in key_series:
        if series_id in recent_raw.columns:
            row = [CPS_KOREAN_NAMES.get(series_id, series_id)]
            
            for i in range(1, len(recent_raw)):
                value = recent_raw[series_id].iloc[i]
                change = recent_mom[series_id].iloc[i-1] if i > 0 else 0
                
                # 포맷팅
                if 'Rate' in series_id or 'Ratio' in series_id or series_id.startswith('LNS14'):
                    cell_text = f"{value:.1f}%<br>({change:+.1f}%p)"
                else:
                    cell_text = f"{value:,.0f}<br>({change:+.1f}%)"
                
                row.append(cell_text)
            
            table_data.append(row)
    
    # 테이블 생성
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=headers,
            line_color='darkslategray',
            fill_color='lightgrey',
            align='center',
            font=dict(size=12, family='NanumGothic')
        ),
        cells=dict(
            values=list(zip(*table_data)),
            line_color='darkslategray',
            fill_color='white',
            align=['left'] + ['center'] * months_back,
            font=dict(size=11, family='NanumGothic'),
            height=40
        )
    )])
    
    # 레이아웃 설정
    fig.update_layout(
        title=dict(text=f"주요 노동시장 지표 - 최근 {months_back}개월", 
                  font=dict(family='NanumGothic', size=FONT_SIZE_TITLE)),
        width=900,
        height=400,
        paper_bgcolor='white',
        margin=dict(l=50, r=50, t=80, b=50)
    )
    
    return fig

# %%
# === 새로운 KPDS 스타일 시각화 함수들 ===

def create_age_group_analysis():
    """
    연령대별 노동시장 분석 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 연령대별 실업률 데이터
    age_dict = {
        'LNU04076975': '18세 이상 (비계절조정)',
        'LNU04000048': '25세 이상 (비계절조정)',
        'LNU04000092': '45세 이상 (비계절조정)', 
        'LNU04024230': '55세 이상 (비계절조정)',
        'LNU04000097': '65세 이상 (비계절조정)'
    }
    
    df = get_raw_data(list(age_dict.keys()))
    df = df.rename(columns=age_dict)
    
    print("연령대별 실업률 비교")
    
    return df_multi_line_chart(
        df=df,
        ytitle="%"
    )

def create_race_comparison_chart():
    """
    인종별 노동시장 지표 비교 (KPDS 포맷) 
    
    Returns:
        plotly figure
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 인종별 실업률 데이터
    race_dict = {
        'LNU04000003': '백인 (비계절조정)',
        'LNU04000006': '흑인 (비계절조정)',
        'LNU04032183': '아시아계 (비계절조정)',
        'LNU04035243': '아메리카 원주민 (비계절조정)',
        'LNU04035553': '하와이/태평양 원주민 (비계절조정)'
    }
    
    df = get_raw_data(list(race_dict.keys()))
    df = df.rename(columns=race_dict)
    
    print("인종별 실업률 비교")
    
    return df_multi_line_chart(
        df=df,
        ytitle="%"
    )

def create_seasonal_adjustment_comparison():
    """
    계절조정 vs 비계절조정 비교 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 계절조정 비교 데이터
    seasonal_dict = {
        'LNS14000000': '실업률 (계절조정)',
        'LNU04000000': '실업률 (비계절조정)'
    }
    
    df = get_raw_data(list(seasonal_dict.keys()))
    df = df.rename(columns=seasonal_dict)
    
    print("계절조정 vs 비계절조정 실업률")
    
    return df_multi_line_chart(
        df=df,
        ytitle="%"
    )

def create_participation_rate_by_age():
    """
    연령대별 경제활동참가율 비교 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 연령대별 경제활동참가율
    participation_dict = {
        'LNU01376975': '18세 이상 (비계절조정)',
        'LNU01300048': '25세 이상 (비계절조정)', 
        'LNU01300092': '45세 이상 (비계절조정)',
        'LNU01324230': '55세 이상 (비계절조정)',
        'LNU01300097': '65세 이상 (비계절조정)'
    }
    
    df = get_raw_data(list(participation_dict.keys()))
    df = df.rename(columns=participation_dict)
    
    print("연령대별 경제활동참가율")
    
    return df_multi_line_chart(
        df=df,
        ytitle="%"
    )

def create_employment_level_by_race():
    """
    인종별 취업자수 비교 (KPDS 포맷)
    
    Returns:
        plotly figure  
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 인종별 취업자수
    employment_dict = {
        'LNU02000003': '백인 취업자',
        'LNU02000006': '흑인 취업자', 
        'LNU02032183': '아시아계 취업자'
    }
    
    df = get_raw_data(list(employment_dict.keys()))
    df = df.rename(columns=employment_dict)
    
    print("인종별 취업자수 비교")
    
    return df_multi_line_chart(
        df=df,
        ytitle="천명"
    )

def create_dual_axis_unemployment_employment():
    """
    실업률과 취업자수 이중축 차트 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    df = get_raw_data(['LNS14000000', 'LNS12000000'])
    df = df.rename(columns={
        'LNS14000000': '실업률',
        'LNS12000000': '취업자수'
    })
    
    print("실업률 vs 취업자수")
    
    return df_dual_axis_chart(
        df=df,
        left_cols=['실업률'],
        right_cols=['취업자수'],
        left_title="%", 
        right_title="천명"
    )

# %%
# === 통합 분석 함수들 ===

def run_complete_cps_analysis(start_date='2020-01-01', force_reload=False):
    """
    완전한 CPS 분석 실행 - 데이터 로드 후 모든 차트 생성
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🚀 완전한 CPS 노동시장 분석 시작")
    print("="*50)
    
    # 1. 데이터 로드
    print("\n1️⃣ 데이터 로딩")
    success = load_all_cps_data(start_date=start_date, force_reload=force_reload)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 시각화 생성
    print("\n2️⃣ 시각화 생성")
    
    results = {}
    
    try:
        # 대시보드
        print("   📊 노동시장 대시보드...")
        results['dashboard'] = create_labor_market_dashboard()
        
        # 인구통계별 분석
        print("   📈 인구통계별 실업률...")
        results['unemployment_by_race'] = create_unemployment_by_demographics('race')
        results['unemployment_by_education'] = create_unemployment_by_demographics('education')
        
        # 실업 기간 분석
        print("   ⏱️ 실업 기간 분석...")
        results['unemployment_duration'] = create_unemployment_duration_analysis()
        
        # 고용 형태 분석
        print("   💼 고용 형태 분석...")
        results['employment_type'] = create_employment_type_analysis()
        
        # U-3 vs U-6
        print("   📊 U-3 vs U-6 비교...")
        results['u3_vs_u6'] = create_u3_vs_u6_comparison()
        
        # 노동시장 흐름
        print("   🔄 노동시장 흐름 분석...")
        results['labor_flows'] = create_labor_force_flows()
        
        # 월별 변화 테이블
        print("   📋 월별 변화 테이블...")
        results['monthly_table'] = create_monthly_change_table()
        
    except Exception as e:
        print(f"⚠️ 시각화 생성 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ 분석 완료! 생성된 차트: {len(results)}개")
    return results

def create_economic_briefing(reference_date=None):
    """
    이코노미스트를 위한 경제 브리핑 리포트 생성
    
    Args:
        reference_date: 기준 날짜 (None이면 최신 데이터)
    
    Returns:
        dict: 브리핑 내용
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 최신 데이터 기준
    latest_values = get_latest_values()
    raw_data = get_raw_data()
    
    if reference_date is None:
        reference_date = raw_data.index[-1]
    
    # 주요 지표 추출
    briefing = {
        'reference_date': reference_date.strftime('%Y년 %m월'),
        'headline_indicators': {
            '실업률': f"{latest_values.get('LNS14000000', 0):.1f}%",
            '경제활동참가율': f"{latest_values.get('LNS11300000', 0):.1f}%",
            '고용률': f"{latest_values.get('LNS12300000', 0):.1f}%",
            'U-6 실업률': f"{latest_values.get('LNS13327709', 0):.1f}%"
        }
    }
    
    # 전월 대비 변화
    mom_data = get_mom_data()
    latest_mom = mom_data.iloc[-1]
    
    briefing['monthly_changes'] = {
        '실업률 변화': f"{latest_mom.get('LNS14000000', 0):+.1f}%p",
        '경제활동참가율 변화': f"{latest_mom.get('LNS11300000', 0):+.1f}%p",
        '취업자수 변화': f"{latest_mom.get('LNS12000000', 0):+.1f}%"
    }
    
    # 전년 대비 변화
    yoy_data = get_yoy_data()
    latest_yoy = yoy_data.iloc[-1]
    
    briefing['yearly_changes'] = {
        '실업률 변화': f"{latest_yoy.get('LNS14000000', 0):+.1f}%p",
        '경제활동참가율 변화': f"{latest_yoy.get('LNS11300000', 0):+.1f}%p",
        '취업자수 변화': f"{latest_yoy.get('LNS12000000', 0):+.1f}%"
    }
    
    # 주요 인사이트 생성
    insights = []
    
    # 실업률 트렌드
    unemployment_rate = latest_values.get('LNS14000000', 0)
    if unemployment_rate < 4.0:
        insights.append("실업률이 4% 미만으로 완전고용에 근접한 상태입니다.")
    elif unemployment_rate > 5.0:
        insights.append("실업률이 5%를 초과하여 노동시장 약화 신호가 나타나고 있습니다.")
    
    # U-6와 U-3 격차
    u6_u3_spread = latest_values.get('LNS13327709', 0) - latest_values.get('LNS14000000', 0)
    if u6_u3_spread > 4.0:
        insights.append(f"U-6와 U-3 실업률 격차가 {u6_u3_spread:.1f}%p로 확대되어 불완전고용 문제가 심화되고 있습니다.")
    
    # 평균 실업기간
    avg_duration = latest_values.get('LNS13008275', 0)
    if avg_duration > 20:
        insights.append(f"평균 실업기간이 {avg_duration:.1f}주로 장기실업 문제가 우려됩니다.")
    
    briefing['key_insights'] = insights
    
    # 섹터별 분석
    briefing['demographic_analysis'] = {
        '청년실업률 (16-19세)': f"{latest_values.get('LNS14000012', 0):.1f}%",
        '대졸이상 실업률': f"{latest_values.get('LNS14027662', 0):.1f}%",
        '고졸미만 실업률': f"{latest_values.get('LNS14027659', 0):.1f}%"
    }
    
    return briefing

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 CPS 시리즈 표시"""
    print("=== 사용 가능한 CPS 시리즈 ===")
    
    for series_id, description in CPS_SERIES.items():
        korean_name = CPS_KOREAN_NAMES.get(series_id, description)
        print(f"  '{series_id}': {korean_name} ({description})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in CPS_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_id in series_list:
                korean_name = CPS_KOREAN_NAMES.get(series_id, series_id)
                print(f"    - {series_id}: {korean_name}")

def get_data_status():
    """현재 데이터 상태 반환"""
    return {
        'loaded': CPS_DATA['load_info']['loaded'],
        'series_count': CPS_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': CPS_DATA['load_info']
    }

def create_custom_analysis(series_ids, chart_type='line'):
    """
    사용자 정의 분석 차트 생성
    
    Args:
        series_ids: 분석할 시리즈 ID 리스트
        chart_type: 'line', 'bar', 'scatter'
    
    Returns:
        plotly figure
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 데이터 가져오기
    data = get_raw_data(series_ids)
    
    if data.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 차트 생성
    fig = go.Figure()
    
    if chart_type == 'line':
        for i, col in enumerate(data.columns):
            korean_name = CPS_KOREAN_NAMES.get(col, CPS_SERIES.get(col, col))
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[col],
                name=korean_name,
                mode='lines',
                line=dict(color=get_kpds_color(i), width=2)
            ))
    
    elif chart_type == 'bar':
        # 최신 데이터만 사용
        latest_data = data.iloc[-1]
        labels = [CPS_KOREAN_NAMES.get(col, CPS_SERIES.get(col, col)) for col in data.columns]
        
        fig.add_trace(go.Bar(
            x=labels,
            y=latest_data.values,
            marker_color=[get_kpds_color(i) for i in range(len(labels))]
        ))
    
    elif chart_type == 'scatter':
        if len(data.columns) >= 2:
            x_col, y_col = data.columns[0], data.columns[1]
            x_label = CPS_KOREAN_NAMES.get(x_col, x_col)
            y_label = CPS_KOREAN_NAMES.get(y_col, y_col)
            
            fig.add_trace(go.Scatter(
                x=data[x_col],
                y=data[y_col],
                mode='markers',
                marker=dict(color=deepblue_pds, size=8),
                name=f"{x_label} vs {y_label}"
            ))
    
    # 레이아웃 설정
    print("Custom Labor Market Analysis")
    
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,
        height=500,
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            showgrid=True, gridwidth=1, gridcolor='lightgrey'
        ),
        yaxis=dict(
            showline=False,
            showgrid=True, gridwidth=1, gridcolor='lightgrey'
        ),
        legend=dict(
            orientation="v",
            yanchor="top", y=1,
            xanchor="left", x=1.02,
            font=dict(family='NanumGothic', size=FONT_SIZE_LEGEND)
        ),
        margin=dict(l=80, r=200, t=80, b=60)
    )
    
    return fig

def export_analysis_data(output_format='excel', filename=None):
    """
    분석 데이터를 다양한 형식으로 내보내기
    
    Args:
        output_format: 'excel', 'csv', 'json'
        filename: 저장할 파일명 (None이면 자동 생성)
    
    Returns:
        bool: 저장 성공 여부
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return False
    
    # 파일명 자동 생성
    if filename is None:
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'/home/jyp0615/us_eco/cps_analysis_{timestamp}'
    
    try:
        if output_format == 'excel':
            with pd.ExcelWriter(f'{filename}.xlsx', engine='openpyxl') as writer:
                # 원본 데이터
                CPS_DATA['raw_data'].to_excel(writer, sheet_name='Raw Data')
                
                # MoM 변화
                CPS_DATA['mom_data'].to_excel(writer, sheet_name='MoM Changes')
                
                # YoY 변화
                CPS_DATA['yoy_data'].to_excel(writer, sheet_name='YoY Changes')
                
                # 최신 값 요약
                latest_df = pd.DataFrame({
                    'Series ID': list(CPS_DATA['latest_values'].keys()),
                    'Korean Name': [CPS_KOREAN_NAMES.get(k, k) for k in CPS_DATA['latest_values'].keys()],
                    'Latest Value': list(CPS_DATA['latest_values'].values())
                })
                latest_df.to_excel(writer, sheet_name='Latest Values', index=False)
            
            print(f"✅ Excel 파일 저장 완료: {filename}.xlsx")
        
        elif output_format == 'csv':
            CPS_DATA['raw_data'].to_csv(f'{filename}_raw.csv')
            CPS_DATA['mom_data'].to_csv(f'{filename}_mom.csv')
            CPS_DATA['yoy_data'].to_csv(f'{filename}_yoy.csv')
            print(f"✅ CSV 파일 저장 완료: {filename}_*.csv")
        
        elif output_format == 'json':
            export_data = {
                'metadata': {
                    'load_time': CPS_DATA['load_info']['load_time'].isoformat(),
                    'start_date': CPS_DATA['load_info']['start_date'],
                    'series_count': CPS_DATA['load_info']['series_count']
                },
                'latest_values': CPS_DATA['latest_values'],
                'raw_data': CPS_DATA['raw_data'].to_dict(),
                'mom_data': CPS_DATA['mom_data'].to_dict(),
                'yoy_data': CPS_DATA['yoy_data'].to_dict()
            }
            
            import json
            with open(f'{filename}.json', 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"✅ JSON 파일 저장 완료: {filename}.json")
        
        else:
            print(f"⚠️ 지원하지 않는 형식: {output_format}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ 내보내기 실패: {e}")
        return False

def generate_markdown_report():
    """
    마크다운 형식의 분석 리포트 생성
    
    Returns:
        str: 마크다운 리포트
    """
    if not CPS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    briefing = create_economic_briefing()
    if not briefing:
        return None
    
    report = f"""
# US Labor Market Analysis Report
## {briefing['reference_date']}

### 📊 주요 지표 (Headline Indicators)

| 지표 | 현재값 | 전월 대비 | 전년 대비 |
|------|--------|-----------|-----------|
| 실업률 | {briefing['headline_indicators']['실업률']} | {briefing['monthly_changes']['실업률 변화']} | {briefing['yearly_changes']['실업률 변화']} |
| 경제활동참가율 | {briefing['headline_indicators']['경제활동참가율']} | {briefing['monthly_changes']['경제활동참가율 변화']} | {briefing['yearly_changes']['경제활동참가율 변화']} |
| 고용률 | {briefing['headline_indicators']['고용률']} | - | - |
| U-6 실업률 | {briefing['headline_indicators']['U-6 실업률']} | - | - |

### 📈 주요 인사이트

"""
    
    for insight in briefing['key_insights']:
        report += f"- {insight}\n"
    
    report += f"""

### 🎯 인구통계별 분석

| 그룹 | 실업률 |
|------|--------|
| 청년층 (16-19세) | {briefing['demographic_analysis']['청년실업률 (16-19세)']} |
| 대졸 이상 | {briefing['demographic_analysis']['대졸이상 실업률']} |
| 고졸 미만 | {briefing['demographic_analysis']['고졸미만 실업률']} |

### 📌 데이터 정보

- 데이터 기간: {CPS_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {CPS_DATA['raw_data'].index[-1].strftime('%Y-%m')}
- 시리즈 개수: {CPS_DATA['load_info']['series_count']}개
- 데이터 포인트: {CPS_DATA['load_info']['data_points']}개
- 최종 업데이트: {CPS_DATA['load_info']['load_time'].strftime('%Y-%m-%d %H:%M:%S')}

---
*이 리포트는 BLS CPS 데이터를 기반으로 자동 생성되었습니다.*
"""
    
    return report

# %%
# === 사용 예시 및 메인 실행 ===

print("\n=== CPS 노동시장 분석 도구 사용법 ===")
print("\n🎯 주요 기능:")
print("1. 데이터 로드 (CSV 우선, API 백업):")
print("   load_all_cps_data()  # CSV가 있으면 로드 후 자동 업데이트")
print("   load_cps_data_from_csv()  # CSV에서만 로드")
print("   update_cps_data_from_api()  # 최신 데이터만 API로 업데이트")
print()
print("2. 스마트 업데이트:")
print("   check_recent_data_consistency()  # 최근 3개 데이터 일치성 확인")
print("   update_cps_data_from_api(smart_update=True)  # 스마트 업데이트 (기본값)")
print("   update_cps_data_from_api(smart_update=False)  # 강제 업데이트")
print()
print("3. 시각화 - 대시보드:")
print("   create_labor_market_dashboard()  # 4개 주요 지표 대시보드")
print()
print("4. 시각화 - 인구통계별 분석:")
print("   create_unemployment_by_demographics('race')  # 인종별")
print("   create_unemployment_by_demographics('age')  # 연령별")
print("   create_unemployment_by_demographics('education')  # 교육수준별")
print()
print("5. 시각화 - 심층 분석:")
print("   create_unemployment_duration_analysis()  # 실업 기간 분석")
print("   create_employment_type_analysis()  # 고용 형태 분석")
print("   create_u3_vs_u6_comparison()  # U-3 vs U-6 비교")
print("   create_labor_force_flows()  # 노동시장 흐름")
print("   create_labor_market_heatmap()  # 상관관계 히트맵")
print()
print("6. 테이블 및 리포트:")
print("   create_monthly_change_table()  # 월별 변화 테이블")
print("   create_economic_briefing()  # 경제 브리핑 데이터")
print("   generate_markdown_report()  # 마크다운 리포트")
print()
print("7. 통합 분석:")
print("   run_complete_cps_analysis()  # 모든 분석 실행")
print()
print("8. 데이터 관리:")
print("   save_cps_data_to_csv()  # CSV로 저장")
print("   export_analysis_data('excel')  # Excel로 내보내기")
print("   get_data_status()  # 데이터 상태 확인")
print("   show_available_series()  # 사용 가능한 시리즈")
print("   show_category_options()  # 카테고리 옵션")
print()
print("9. 사용자 정의:")
print("   create_custom_analysis(['LNS14000000', 'LNS11300000'])  # 커스텀 차트")
print()
print("10. 🌎 이민 정책 관련 분석:")
print("   create_immigration_policy_report()  # 종합 이민정책 분석 리포트")
print("   plot_immigration_labor_trends()     # 이민 관련 노동시장 트렌드 차트")
print("   analyze_immigration_labor_impact()  # 히스패닉/라티노 vs 전체 비교분석")
print("   analyze_age_group_participation()   # 연령대별 경제활동참가율 분석")
print()
print("📌 API 키 관리:")
print("   switch_api_key()  # API 키 수동 전환 (일일 한도 초과 시)")

# %%
# === 이민 정책 관련 분석 도구 ===
# 제대로 작동하는지 확인
def analyze_immigration_labor_impact():
    """
    이민 정책 관련 노동시장 영향 분석
    - 히스패닉/라티노 vs 전체 인구 비교
    - 연령대별 노동시장 참여 변화
    - 최근 트렌드 분석
    """
    if CPS_DATA['raw_data'].empty:
        print("❌ 데이터가 로드되지 않았습니다. load_all_cps_data()를 먼저 실행하세요.")
        return None
    
    # 최근 3년 데이터 분석
    recent_data = CPS_DATA['raw_data'].tail(36)  # 최근 36개월
    
    # 히스패닉/라티노 vs 전체 비교 지표
    comparison_metrics = {
        '전체인구_실업률': 'LNS14000000',
        '히스패닉_실업률': 'LNS14000009',
        '전체인구_참가율': 'LNS11300000', 
        '히스패닉_참가율': 'LNS11300009',
        '전체인구_고용률': 'LNS12300000',
        '히스패닉_취업자': 'LNS12000009'
    }
    
    analysis_results = {}
    
    # 최신 값 비교
    latest_values = {}
    for key, series_id in comparison_metrics.items():
        if series_id in recent_data.columns:
            latest_val = recent_data[series_id].dropna().iloc[-1]
            latest_values[key] = latest_val
    
    # 실업률 격차 계산
    if '전체인구_실업률' in latest_values and '히스패닉_실업률' in latest_values:
        unemployment_gap = latest_values['히스패닉_실업률'] - latest_values['전체인구_실업률']
        analysis_results['실업률_격차'] = unemployment_gap
    
    # 참가율 격차 계산  
    if '전체인구_참가율' in latest_values and '히스패닉_참가율' in latest_values:
        participation_gap = latest_values['히스패닉_참가율'] - latest_values['전체인구_참가율']
        analysis_results['참가율_격차'] = participation_gap
    
    # 최근 1년 트렌드
    if len(recent_data) >= 12:
        last_12m = recent_data.tail(12)
        
        # 히스패닉 실업률 변화
        if 'LNS14000009' in last_12m.columns:
            hispanic_unemp_change = (last_12m['LNS14000009'].iloc[-1] - 
                                   last_12m['LNS14000009'].iloc[0])
            analysis_results['히스패닉_실업률_1년변화'] = hispanic_unemp_change
        
        # 전체 실업률 변화
        if 'LNS14000000' in last_12m.columns:
            total_unemp_change = (last_12m['LNS14000000'].iloc[-1] - 
                                last_12m['LNS14000000'].iloc[0])
            analysis_results['전체_실업률_1년변화'] = total_unemp_change
    
    analysis_results['최신값'] = latest_values
    analysis_results['분석일자'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    
    return analysis_results

def plot_immigration_labor_trends():
    """
    이민 관련 노동시장 트렌드 시각화
    """
    if CPS_DATA['raw_data'].empty:
        print("❌ 데이터가 로드되지 않았습니다.")
        return None
    
    # 최근 5년 데이터
    recent_data = CPS_DATA['raw_data'].tail(60)
    
    # 4개 서브플롯 생성
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            '실업률 비교: 전체 vs 히스패닉/라티노',
            '경제활동참가율 비교: 전체 vs 히스패닉/라티노', 
            '연령대별 실업률 트렌드',
            '히스패닉/라티노 노동시장 지표'
        ],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": True}]]
    )
    
    # 1. 실업률 비교
    if 'LNS14000000' in recent_data.columns and 'LNS14000009' in recent_data.columns:
        fig.add_trace(
            go.Scatter(x=recent_data.index, y=recent_data['LNS14000000'],
                      name='전체 실업률', line=dict(color='blue', width=2)),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(x=recent_data.index, y=recent_data['LNS14000009'],
                      name='히스패닉 실업률', line=dict(color='red', width=2)),
            row=1, col=1
        )
    
    # 2. 경제활동참가율 비교
    if 'LNS11300000' in recent_data.columns and 'LNS11300009' in recent_data.columns:
        fig.add_trace(
            go.Scatter(x=recent_data.index, y=recent_data['LNS11300000'],
                      name='전체 참가율', line=dict(color='green', width=2)),
            row=1, col=2
        )
        fig.add_trace(
            go.Scatter(x=recent_data.index, y=recent_data['LNS11300009'],
                      name='히스패닉 참가율', line=dict(color='orange', width=2)),
            row=1, col=2
        )
    
    # 3. 연령대별 실업률
    age_series = {
        '16-19세': 'LNS14000012',
        '20+남성': 'LNS14000025', 
        '20+여성': 'LNS14000026'
    }
    
    colors = ['purple', 'darkblue', 'darkred']
    for i, (name, series_id) in enumerate(age_series.items()):
        if series_id in recent_data.columns:
            fig.add_trace(
                go.Scatter(x=recent_data.index, y=recent_data[series_id],
                          name=f'{name} 실업률', line=dict(color=colors[i], width=2)),
                row=2, col=1
            )
    
    # 4. 히스패닉 노동시장 지표 (이중축)
    if 'LNS12000009' in recent_data.columns:  # 취업자수
        fig.add_trace(
            go.Scatter(x=recent_data.index, y=recent_data['LNS12000009'],
                      name='히스패닉 취업자수', line=dict(color='darkgreen', width=2)),
            row=2, col=2
        )
    
    if 'LNS14000009' in recent_data.columns:  # 실업률
        fig.add_trace(
            go.Scatter(x=recent_data.index, y=recent_data['LNS14000009'],
                      name='히스패닉 실업률', line=dict(color='red', width=2),
                      yaxis='y4'),
            row=2, col=2, secondary_y=True
        )
    
    # 레이아웃 설정
    fig.update_layout(
        title={
            'text': '🌎 이민 정책 관련 노동시장 분석 대시보드',
            'font': {'size': 20, 'family': 'Arial Black'},
            'x': 0.5
        },
        height=800,
        showlegend=True,
        template='plotly_white'
    )
    
    # Y축 라벨 설정
    fig.update_yaxes(title_text="실업률 (%)", row=1, col=1)
    fig.update_yaxes(title_text="참가율 (%)", row=1, col=2)
    fig.update_yaxes(title_text="실업률 (%)", row=2, col=1)
    fig.update_yaxes(title_text="취업자수 (천명)", row=2, col=2)
    fig.update_yaxes(title_text="실업률 (%)", row=2, col=2, secondary_y=True)
    
    return fig

# %%
# === 범용 시각화 함수들 (사용자 지정 시리즈) ===

def plot_cps_series(series_list, chart_type='multi_line', data_type='raw', 
                   periods=36, labels=None, left_ytitle=None, right_ytitle=None, target_date=None):
    """
    범용 CPS 시각화 함수 - 사용자가 원하는 시리즈로 다양한 차트 생성
    
    Args:
        series_list: 시각화할 시리즈 리스트 (예: ['LNS14000000', 'LNS11300000'])
        chart_type: 차트 종류 ('multi_line', 'dual_axis', 'horizontal_bar', 'single_line')
        data_type: 데이터 타입 ('raw', 'mom', 'yoy')
        periods: 표시할 기간 (개월)
        labels: 시리즈 라벨 딕셔너리 (None이면 자동)
        left_ytitle: 왼쪽 Y축 제목
        right_ytitle: 오른쪽 Y축 제목
        target_date: 특정 날짜 기준 (예: '2025-06-01', None이면 최신 데이터)
    
    Returns:
        plotly figure
    """
    if CPS_DATA['raw_data'].empty:
        print("⚠️ 먼저 load_all_cps_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 데이터 타입에 따라 데이터 가져오기
    if data_type == 'raw':
        data = get_raw_data()
        unit = "%" if any('Rate' in CPS_SERIES.get(s, '') or 'Ratio' in CPS_SERIES.get(s, '') for s in series_list) else "천명"
        desc = "수준"
    elif data_type == 'mom':
        data = get_mom_data()
        unit = "%p"
        desc = "전월대비"
    elif data_type == 'yoy':
        data = get_yoy_data()
        unit = "%p"
        desc = "전년동월대비"
    else:
        print("❌ 지원하지 않는 data_type입니다. 'raw', 'mom', 'yoy' 중 선택하세요.")
        return None
    
    if data.empty:
        print("❌ 데이터가 없습니다.")
        return None
    
    # 기간 제한 또는 특정 날짜 기준
    if target_date:
        try:
            target_date_parsed = pd.to_datetime(target_date)
            # 해당 날짜 이전의 데이터만 선택
            filtered_data = data[data.index <= target_date_parsed]
            if filtered_data.empty:
                print(f"⚠️ {target_date} 이전의 데이터가 없습니다.")
                return None
            recent_data = filtered_data.tail(periods)
        except:
            print(f"⚠️ 잘못된 날짜 형식입니다: {target_date}. 'YYYY-MM-DD' 형식을 사용하세요.")
            return None
    else:
        recent_data = data.tail(periods)
    
    # 사용 가능한 컬럼 확인
    available_cols = [col for col in series_list if col in recent_data.columns]
    
    if not available_cols:
        print("❌ 요청한 시리즈가 데이터에 없습니다.")
        print(f"사용 가능한 시리즈: {list(recent_data.columns[:10])}...")
        return None
    
    # 자동 라벨 생성
    if labels is None:
        labels = {col: CPS_KOREAN_NAMES.get(col, CPS_SERIES.get(col, col)) for col in available_cols}
    
    # Y축 제목 자동 설정
    if left_ytitle is None:
        left_ytitle = unit
    
    # 차트 타입별 시각화
    if chart_type == 'multi_line':
        print(f"CPS 시리즈 다중 라인 차트 ({desc})")
        fig = df_multi_line_chart(
            df=recent_data,
            columns=available_cols,
            ytitle=left_ytitle,
            labels=labels
        )
    
    elif chart_type == 'single_line' and len(available_cols) == 1:
        print(f"CPS 시리즈 단일 라인 차트 ({desc})")
        fig = df_line_chart(
            df=recent_data,
            column=available_cols[0],
            ytitle=left_ytitle,
            label=labels[available_cols[0]]
        )
    
    elif chart_type == 'dual_axis' and len(available_cols) >= 2:
        # 반반으로 나누어 왼쪽/오른쪽 축에 배치
        mid = len(available_cols) // 2
        left_cols = available_cols[:mid] if mid > 0 else available_cols[:1]
        right_cols = available_cols[mid:] if mid > 0 else available_cols[1:]
        
        print(f"CPS 시리즈 이중축 차트 ({desc})")
        fig = df_dual_axis_chart(
            df=recent_data,
            left_cols=left_cols,
            right_cols=right_cols,
            left_labels=[labels[col] for col in left_cols],
            right_labels=[labels[col] for col in right_cols],
            left_title=left_ytitle,
            right_title=right_ytitle or left_ytitle
        )
    
    elif chart_type == 'horizontal_bar':
        # 최신값 기준 가로 바 차트
        latest_values = {}
        for col in available_cols:
            latest_val = recent_data[col].dropna().iloc[-1] if not recent_data[col].dropna().empty else 0
            latest_values[labels[col]] = latest_val
        
        # 날짜 정보 표시
        latest_date = recent_data.index[-1].strftime('%Y-%m') if not recent_data.empty else "N/A"
        date_info = f" ({latest_date})" if target_date else ""
        
        print(f"CPS 시리즈 가로 바 차트 ({desc}){date_info}")
        fig = create_horizontal_bar_chart(
            data_dict=latest_values,
            positive_color=deepred_pds,
            negative_color=deepblue_pds,
            unit=unit
        )
    
    else:
        print("❌ 지원하지 않는 chart_type이거나 시리즈 개수가 맞지 않습니다.")
        print("   - single_line: 1개 시리즈")
        print("   - multi_line: 여러 시리즈")
        print("   - dual_axis: 2개 이상 시리즈")
        print("   - horizontal_bar: 여러 시리즈")
        return None
    
    return fig

def analyze_age_group_participation():
    """
    연령대별 경제활동참가율 분석 (이민정책 영향 관점)
    """
    if CPS_DATA['raw_data'].empty:
        print("❌ 데이터가 로드되지 않았습니다.")
        return None
    
    # 연령대별 시리즈
    age_series = {
        '16세이상': 'LNS11300000',
        '20세이상': 'LNS11300024', 
        '25세이상': 'LNS11300048',
        '55세이상': 'LNS11324230'
    }
    
    recent_data = CPS_DATA['raw_data'].tail(36)  # 최근 3년
    
    analysis_results = {}
    
    # 최신 참가율
    latest_participation = {}
    for age_group, series_id in age_series.items():
        if series_id in recent_data.columns:
            latest_val = recent_data[series_id].dropna().iloc[-1]
            latest_participation[age_group] = latest_val
    
    # 1년 변화
    participation_change_1y = {}
    if len(recent_data) >= 12:
        for age_group, series_id in age_series.items():
            if series_id in recent_data.columns:
                current = recent_data[series_id].dropna().iloc[-1]
                year_ago = recent_data[series_id].dropna().iloc[-12]
                change = current - year_ago
                participation_change_1y[age_group] = change
    
    # 3년 변화  
    participation_change_3y = {}
    for age_group, series_id in age_series.items():
        if series_id in recent_data.columns:
            current = recent_data[series_id].dropna().iloc[-1]
            three_years_ago = recent_data[series_id].dropna().iloc[0]
            change = current - three_years_ago
            participation_change_3y[age_group] = change
    
    analysis_results = {
        '최신_참가율': latest_participation,
        '1년_변화': participation_change_1y,
        '3년_변화': participation_change_3y,
        '분석_기간': f"{recent_data.index[0].strftime('%Y-%m')} ~ {recent_data.index[-1].strftime('%Y-%m')}"
    }
    
    return analysis_results

def create_immigration_policy_report():
    """
    이민 정책 관련 종합 리포트 생성
    """
    print("\n" + "="*60)
    print("🌎 이민 정책 관련 노동시장 분석 리포트")
    print("="*60)
    
    # 1. 기본 분석
    immigration_analysis = analyze_immigration_labor_impact()
    if immigration_analysis:
        print("\n📊 히스패닉/라티노 vs 전체 인구 비교:")
        print(f"   실업률 격차: {immigration_analysis.get('실업률_격차', 0):.2f}%p")
        print(f"   참가율 격차: {immigration_analysis.get('참가율_격차', 0):.2f}%p")
        
        if '최신값' in immigration_analysis:
            latest = immigration_analysis['최신값']
            print(f"   최신 히스패닉 실업률: {latest.get('히스패닉_실업률', 0):.1f}%")
            print(f"   최신 전체 실업률: {latest.get('전체인구_실업률', 0):.1f}%")
    
    # 2. 연령대별 분석
    age_analysis = analyze_age_group_participation()
    if age_analysis:
        print(f"\n📈 연령대별 경제활동참가율 (최근 3년 변화):")
        for age_group, change in age_analysis['3년_변화'].items():
            print(f"   {age_group}: {change:+.1f}%p")
    
    # 3. 시각화 생성
    print(f"\n📊 시각화 차트 생성 중...")
    fig = plot_immigration_labor_trends()
    if fig:
        fig.show()
        print("✅ 이민 정책 관련 노동시장 분석 차트가 표시되었습니다.")
    
    print(f"\n분석 완료 시간: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

# %%
# 기본 실행 예제
if __name__ == "__main__":
    print("\n" + "="*60)
    print("🚀 CPS 노동시장 분석 도구 시작")
    print("="*60)
    
    # 데이터 로드 (CSV 우선, 스마트 업데이트 포함)
    success = load_all_cps_data()
    
    if success:
        print("\n✅ 데이터 로드 완료! 분석을 시작할 수 있습니다.")
        print("\n예시: create_labor_market_dashboard() 실행하여 대시보드를 확인하세요.")
    else:
        print("\n❌ 데이터 로드 실패. 문제를 확인하세요.")

# %%
"""
=== 추가 시각화 및 분석 함수들 ===
house_price_and_sales.py와 동일한 구조로 구현된 범용 함수들
"""

def show_series_summary():
    """전체 시리즈 요약 정보 출력"""
    if CPS_DATA is None:
        print("❌ 데이터가 로드되지 않았습니다. load_all_cps_data()를 먼저 실행하세요.")
        return
    
    raw_data = CPS_DATA['raw']
    
    print("📋 CPS Employment Series Summary")
    print("=" * 60)
    
    # 주요 카테고리별 시리즈 카운트
    unemployment_series = [col for col in raw_data.columns if 'LNS14' in col]
    participation_series = [col for col in raw_data.columns if 'LNS11' in col]
    employment_series = [col for col in raw_data.columns if 'LNS12' in col]
    other_series = [col for col in raw_data.columns if not any(x in col for x in ['LNS14', 'LNS11', 'LNS12'])]
    
    print(f"Unemployment Rate Series (LNS14xxx): {len(unemployment_series)}개")
    print(f"Labor Force Participation Series (LNS11xxx): {len(participation_series)}개") 
    print(f"Employment-Population Ratio Series (LNS12xxx): {len(employment_series)}개")
    print(f"Other Series: {len(other_series)}개")
    print(f"Total Series: {len(raw_data.columns)}개")
    print("=" * 60)
    
    # 최근 데이터 상태
    latest_date = raw_data.index.max()
    print(f"Latest Data: {latest_date.strftime('%Y-%m-%d')}")
    
    # 주요 시리즈별 최근값
    print("\n주요 시리즈별 최근값:")
    key_series = ['LNS14000000', 'LNS11300000', 'LNS12300000', 'LNS13327709']
    series_names = {
        'LNS14000000': '실업률',
        'LNS11300000': '경제활동참가율', 
        'LNS12300000': '고용률',
        'LNS13327709': 'U-6 실업률'
    }
    
    for series in key_series:
        if series in raw_data.columns:
            latest_value = raw_data[series].iloc[-1]
            print(f"  {series_names[series]} ({series}): {latest_value:.1f}%")

def plot_multi_line_chart(series_names, data_type='raw', title=None, ytitle=None, labels=None, 
                         width_cm=12, height_cm=8, start_date=None, end_date=None):
    """
    다중 라인 차트 그리기
    
    Parameters:
    - series_names: 시리즈 이름 리스트
    - data_type: 'raw', 'yoy', 'mom' 중 선택
    - title: 차트 제목 (None이면 자동 생성)
    - ytitle: Y축 제목
    - labels: 시리즈 라벨 리스트
    - width_cm, height_cm: 차트 크기
    - start_date, end_date: 날짜 범위 (YYYY-MM-DD 형식)
    """
    if CPS_DATA is None:
        print("❌ 데이터가 로드되지 않았습니다. load_all_cps_data()를 먼저 실행하세요.")
        return
    
    # 데이터 타입에 따라 데이터 선택
    if data_type == 'raw':
        data = get_raw_data(series_names)
        if ytitle is None:
            ytitle = "%"
    elif data_type == 'yoy':
        data = get_yoy_data(series_names)
        if ytitle is None:
            ytitle = "%"
    elif data_type == 'mom':
        data = get_mom_data(series_names)
        if ytitle is None:
            ytitle = "%"
    else:
        print("❌ data_type은 'raw', 'yoy', 'mom' 중 하나여야 합니다.")
        return
    
    if data is None:
        return
    
    # 날짜 범위 필터링
    if start_date:
        data = data[data.index >= start_date]
    if end_date:
        data = data[data.index <= end_date]
    
    # 자동 제목 생성
    if title is None:
        data_type_names = {'raw': 'Labor Market Indicators', 'yoy': 'Labor Market YoY Changes', 'mom': 'Labor Market MoM Changes'}
        title = data_type_names.get(data_type, 'Labor Market Chart')
    
    # 차트 제목 출력 (그래프 자체에는 넣지 않음)
    print(title)
    
    # KPDS 다중 라인 차트 그리기
    df_multi_line_chart(
        df=data,
        columns=series_names,
        title="",  # 제목은 빈 문자열
        xtitle="Date",
        ytitle=ytitle,
        labels=labels,
        width_cm=width_cm,
        height_cm=height_cm
    )

def plot_dual_axis_chart(left_series, right_series, left_data_type='raw', right_data_type='raw',
                        left_labels=None, right_labels=None, left_ytitle=None, right_ytitle=None,
                        title=None, width_cm=12, height_cm=8, start_date=None, end_date=None):
    """
    이중 축 차트 그리기
    
    Parameters:
    - left_series: 왼쪽 Y축 시리즈 (리스트 또는 문자열)
    - right_series: 오른쪽 Y축 시리즈 (리스트 또는 문자열)
    - left_data_type, right_data_type: 'raw', 'yoy', 'mom' 중 선택
    - left_labels, right_labels: 각 축의 시리즈 라벨
    - left_ytitle, right_ytitle: 각 축의 제목
    - title: 차트 제목
    - width_cm, height_cm: 차트 크기
    - start_date, end_date: 날짜 범위
    """
    if CPS_DATA is None:
        print("❌ 데이터가 로드되지 않았습니다. load_all_cps_data()를 먼저 실행하세요.")
        return
    
    # 왼쪽 데이터 가져오기
    if left_data_type == 'raw':
        left_data = get_raw_data(left_series)
    elif left_data_type == 'yoy':
        left_data = get_yoy_data(left_series)
    elif left_data_type == 'mom':
        left_data = get_mom_data(left_series)
    else:
        print("❌ left_data_type은 'raw', 'yoy', 'mom' 중 하나여야 합니다.")
        return
    
    # 오른쪽 데이터 가져오기
    if right_data_type == 'raw':
        right_data = get_raw_data(right_series)
    elif right_data_type == 'yoy':
        right_data = get_yoy_data(right_series)
    elif right_data_type == 'mom':
        right_data = get_mom_data(right_series)
    else:
        print("❌ right_data_type은 'raw', 'yoy', 'mom' 중 하나여야 합니다.")
        return
    
    if left_data is None or right_data is None:
        return
    
    # 데이터 결합
    combined_data = pd.concat([left_data, right_data], axis=1)
    
    # 날짜 범위 필터링
    if start_date:
        combined_data = combined_data[combined_data.index >= start_date]
    if end_date:
        combined_data = combined_data[combined_data.index <= end_date]
    
    # Y축 제목 자동 설정
    if left_ytitle is None:
        left_ytitle = "%"
    
    if right_ytitle is None:
        right_ytitle = "%"
    
    # 자동 제목 생성
    if title is None:
        title = "Labor Market Dual Axis Chart"
    
    # 차트 제목 출력
    print(title)
    
    # 시리즈를 리스트로 변환
    if isinstance(left_series, str):
        left_series = [left_series]
    if isinstance(right_series, str):
        right_series = [right_series]
    
    # KPDS 이중 축 차트 그리기
    df_dual_axis_chart(
        df=combined_data,
        left_cols=left_series,
        right_cols=right_series,
        left_labels=left_labels,
        right_labels=right_labels,
        left_title=left_ytitle,
        right_title=right_ytitle,
        title="",  # 제목은 빈 문자열
        xtitle="Date",
        width_cm=width_cm,
        height_cm=height_cm
    )

def plot_horizontal_bar_chart(series_names, data_type='raw', title=None, periods=12, labels=None):
    """
    수평 막대 차트 그리기 (최근 N개월 데이터)
    
    Parameters:
    - series_names: 시리즈 이름 리스트
    - data_type: 'raw', 'yoy', 'mom' 중 선택
    - title: 차트 제목
    - periods: 표시할 최근 기간 수 (기본값: 12개월)
    - labels: 시리즈 라벨 리스트
    """
    if CPS_DATA is None:
        print("❌ 데이터가 로드되지 않았습니다. load_all_cps_data()를 먼저 실행하세요.")
        return
    
    # 데이터 타입에 따라 데이터 선택
    if data_type == 'raw':
        data = get_raw_data(series_names)
    elif data_type == 'yoy':
        data = get_yoy_data(series_names)
    elif data_type == 'mom':
        data = get_mom_data(series_names)
    else:
        print("❌ data_type은 'raw', 'yoy', 'mom' 중 하나여야 합니다.")
        return
    
    if data is None:
        return
    
    # 최근 N개 기간 데이터만 선택
    recent_data = data.tail(periods)
    
    # 자동 제목 생성
    if title is None:
        data_type_names = {'raw': 'Labor Market Indicators', 'yoy': 'Labor Market YoY Changes', 'mom': 'Labor Market MoM Changes'}
        title = f"{data_type_names.get(data_type, 'Labor Market')} - Recent {periods} Months"
    
    # 차트 제목 출력
    print(title)
    
    # matplotlib을 사용한 수평 막대 차트
    _, ax = plt.subplots(figsize=(10, 6))
    
    # 각 시리즈별로 막대 그리기
    n_series = len(series_names)
    bar_height = 0.8 / n_series
    
    for i, series in enumerate(series_names):
        positions = range(len(recent_data))
        values = recent_data[series].values
        
        # 라벨 설정
        if labels and i < len(labels):
            label = labels[i]
        else:
            label = series
        
        ax.barh([p + i * bar_height for p in positions], values, 
                bar_height, label=label, alpha=0.8)
    
    # 축 설정
    ax.set_yticks([p + bar_height * (n_series - 1) / 2 for p in range(len(recent_data))])
    ax.set_yticklabels([d.strftime('%Y-%m') for d in recent_data.index])
    ax.invert_yaxis()  # 최신 데이터를 위에 표시
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

def plot_series(series_names, chart_type='multi_line', data_type='raw', title=None, 
               labels=None, width_cm=12, height_cm=8, start_date=None, end_date=None,
               periods=12, left_ytitle=None, right_ytitle=None):
    """
    종합적인 시각화 함수 - 원하는 시리즈, 차트 타입, 데이터 변환을 한 번에 선택
    
    Parameters:
    - series_names: 시리즈 이름 (문자열 또는 리스트)
                   dual_axis의 경우: [left_series, right_series] 또는 [[left1, left2], [right1, right2]]
    - chart_type: 'multi_line', 'dual_axis', 'horizontal_bar' 중 선택
    - data_type: 'raw', 'yoy', 'mom' 중 선택
                dual_axis의 경우: 'raw' 또는 ['left_type', 'right_type']
    - title: 차트 제목 (None이면 자동 생성)
    - labels: 시리즈 라벨 (dual_axis의 경우: [left_labels, right_labels])
    - width_cm, height_cm: 차트 크기
    - start_date, end_date: 날짜 범위 (YYYY-MM-DD 형식)
    - periods: horizontal_bar에서 표시할 최근 기간 수
    - left_ytitle, right_ytitle: dual_axis에서 Y축 제목
    
    Examples:
    # 다중 라인 차트
    plot_series(['LNS14000000', 'LNS11300000'], 'multi_line', 'raw')
    
    # 이중 축 차트 (같은 데이터 타입)
    plot_series(['LNS14000000', 'LNS13327709'], 'dual_axis', 'raw')
    
    # 이중 축 차트 (다른 데이터 타입)
    plot_series(['LNS14000000', 'LNS13327709'], 'dual_axis', ['raw', 'yoy'])
    
    # 수평 막대 차트
    plot_series(['LNS14000000'], 'horizontal_bar', 'mom', periods=6)
    """
    if CPS_DATA is None:
        print("❌ 데이터가 로드되지 않았습니다. load_all_cps_data()를 먼저 실행하세요.")
        return
    
    # 차트 타입별 처리
    if chart_type == 'multi_line':
        plot_multi_line_chart(
            series_names=series_names,
            data_type=data_type,
            title=title,
            labels=labels,
            width_cm=width_cm,
            height_cm=height_cm,
            start_date=start_date,
            end_date=end_date
        )
        
    elif chart_type == 'dual_axis':
        # series_names가 2개 요소를 가져야 함
        if not isinstance(series_names, list) or len(series_names) != 2:
            print("❌ dual_axis에서는 series_names가 [left_series, right_series] 형태여야 합니다.")
            return
        
        left_series = series_names[0]
        right_series = series_names[1]
        
        # data_type 처리
        if isinstance(data_type, list) and len(data_type) == 2:
            left_data_type = data_type[0]
            right_data_type = data_type[1]
        else:
            left_data_type = data_type
            right_data_type = data_type
        
        # labels 처리
        if labels and isinstance(labels, list) and len(labels) == 2:
            left_labels = labels[0] if isinstance(labels[0], list) else [labels[0]]
            right_labels = labels[1] if isinstance(labels[1], list) else [labels[1]]
        else:
            left_labels = None
            right_labels = None
        
        plot_dual_axis_chart(
            left_series=left_series,
            right_series=right_series,
            left_data_type=left_data_type,
            right_data_type=right_data_type,
            left_labels=left_labels,
            right_labels=right_labels,
            left_ytitle=left_ytitle,
            right_ytitle=right_ytitle,
            title=title,
            width_cm=width_cm,
            height_cm=height_cm,
            start_date=start_date,
            end_date=end_date
        )
        
    elif chart_type == 'horizontal_bar':
        plot_horizontal_bar_chart(
            series_names=series_names,
            data_type=data_type,
            title=title,
            periods=periods,
            labels=labels
        )
        
    else:
        print("❌ chart_type은 'multi_line', 'dual_axis', 'horizontal_bar' 중 하나여야 합니다.")
        print("사용 가능한 차트 타입:")
        print("  - 'multi_line': 다중 라인 차트")
        print("  - 'dual_axis': 이중 축 차트")
        print("  - 'horizontal_bar': 수평 막대 차트")

def create_labor_market_dashboard_enhanced(data_type='raw'):
    """
    주요 노동시장 지표들의 대시보드 생성 (향상된 버전)
    
    Parameters:
    - data_type: 'raw', 'yoy', 'mom' 중 선택
    """
    key_series = ['LNS14000000', 'LNS11300000', 'LNS12300000', 'LNS13327709']
    labels = ['실업률', '경제활동참가율', '고용률', 'U-6 실업률']
    
    plot_series(
        series_names=key_series,
        chart_type='multi_line',
        data_type=data_type,
        title=f"Major Labor Market Indicators ({data_type.upper()})",
        labels=labels,
        width_cm=14,
        height_cm=9
    )

# %%
run_complete_cps_analysis()
# %%
list_available_series()
# %%
print_load_info()
# %%
labels = {'LNS11000000' : "경제활동인구", 'LNS11300000' : "경제활동참가율(우)"}
plot_cps_series(['LNS11000000', 'LNS11300000'], chart_type='dual_axis', data_type='raw', left_ytitle='천 명', right_ytitle='%', labels=labels)
# %%
plot_cps_series(['LNS13000000'], chart_type='single_line', data_type='raw')
# %%
plot_cps_series(['LNU01076975', 'LNU01000048', 'LNU01000092', 'LNU01024230', 'LNU01000097'], chart_type='multi_line', data_type='raw')

# %%
plot_cps_series(['LNU05000097', 'LNU05024230'], chart_type='multi_line', data_type='raw')

# %%
plot_cps_series(['LNU01000003', 'LNU01000006', 'LNU01032183', 'LNU01035243', 'LNU01035553'], chart_type='multi_line', data_type='raw')

# %%