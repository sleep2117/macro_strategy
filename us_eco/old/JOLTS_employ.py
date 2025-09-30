# %%
"""
BLS API 전용 JOLTS 분석 및 시각화 도구 (KPDS 포맷 적용)
- BLS API만 사용하여 JOLTS 데이터 수집
- CSV 저장/로드 및 스마트 업데이트 (PPI 방식 적용)
- 노동시장 분석을 위한 전문 시각화 (KPDS 포맷)
- IB/이코노미스트 관점의 분석 기능
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
CURRENT_API_KEY = BLS_API_KEY3

# 시각화 라이브러리 불러오기
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

# 한글 폰트 설정
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
plt.rcParams['font.family'] = 'NanumGothic'
plt.rcParams['axes.unicode_minus'] = False

# %%
# === JOLTS 시리즈 ID와 이름 매핑 ===

# JOLTS 시리즈 ID와 영어 이름 매핑
JOLTS_SERIES = {
    # Total nonfarm
    'JTS000000000000000HIL': 'Total nonfarm - Hires',
    'JTS000000000000000JOL': 'Total nonfarm - Job openings',
    'JTS000000000000000LDL': 'Total nonfarm - Layoffs and discharges',
    
    # Total private
    'JTS100000000000000HIL': 'Total private - Hires',
    'JTS100000000000000JOL': 'Total private - Job openings',
    'JTS100000000000000LDL': 'Total private - Layoffs and discharges',
    
    # Mining and logging
    'JTS110099000000000HIL': 'Mining and logging - Hires',
    'JTS110099000000000JOL': 'Mining and logging - Job openings',
    'JTS110099000000000LDL': 'Mining and logging - Layoffs and discharges',
    
    # Construction
    'JTS230000000000000HIL': 'Construction - Hires',
    'JTS230000000000000JOL': 'Construction - Job openings',
    'JTS230000000000000LDL': 'Construction - Layoffs and discharges',
    
    # Manufacturing
    'JTS300000000000000HIL': 'Manufacturing - Hires',
    'JTS300000000000000JOL': 'Manufacturing - Job openings',
    'JTS300000000000000LDL': 'Manufacturing - Layoffs and discharges',
    
    # Trade, transportation, and utilities
    'JTS400000000000000HIL': 'Trade, transportation, and utilities - Hires',
    'JTS400000000000000JOL': 'Trade, transportation, and utilities - Job openings',
    'JTS400000000000000LDL': 'Trade, transportation, and utilities - Layoffs and discharges',
    
    # Information
    'JTS510000000000000HIL': 'Information - Hires',
    'JTS510000000000000JOL': 'Information - Job openings',
    'JTS510000000000000LDL': 'Information - Layoffs and discharges',
    
    # Financial activities
    'JTS510099000000000HIL': 'Financial activities - Hires',
    'JTS510099000000000JOL': 'Financial activities - Job openings',
    'JTS510099000000000LDL': 'Financial activities - Layoffs and discharges',
    
    # Professional and business services
    'JTS540099000000000HIL': 'Professional and business services - Hires',
    'JTS540099000000000JOL': 'Professional and business services - Job openings',
    'JTS540099000000000LDL': 'Professional and business services - Layoffs and discharges',
    
    # Private education and health services
    'JTS600000000000000HIL': 'Private education and health services - Hires',
    'JTS600000000000000JOL': 'Private education and health services - Job openings',
    'JTS600000000000000LDL': 'Private education and health services - Layoffs and discharges',
    
    # Leisure and hospitality
    'JTS700000000000000HIL': 'Leisure and hospitality - Hires',
    'JTS700000000000000JOL': 'Leisure and hospitality - Job openings',
    'JTS700000000000000LDL': 'Leisure and hospitality - Layoffs and discharges',
    
    # Other services
    'JTS810000000000000HIL': 'Other services - Hires',
    'JTS810000000000000JOL': 'Other services - Job openings',
    'JTS810000000000000LDL': 'Other services - Layoffs and discharges',
    
    # Government
    'JTS900000000000000HIL': 'Government - Hires',
    'JTS900000000000000JOL': 'Government - Job openings',
    'JTS900000000000000LDL': 'Government - Layoffs and discharges'
}

# 한국어 이름 매핑
JOLTS_KOREAN_NAMES = {
    # Total nonfarm
    'JTS000000000000000HIL': '전체 비농업 - 채용',
    'JTS000000000000000JOL': '전체 비농업 - 구인',
    'JTS000000000000000LDL': '전체 비농업 - 해고 및 퇴직',
    
    # Total private
    'JTS100000000000000HIL': '전체 민간 - 채용',
    'JTS100000000000000JOL': '전체 민간 - 구인',
    'JTS100000000000000LDL': '전체 민간 - 해고 및 퇴직',
    
    # Sectors
    'JTS110099000000000HIL': '광업 및 벌목업 - 채용',
    'JTS110099000000000JOL': '광업 및 벌목업 - 구인',
    'JTS110099000000000LDL': '광업 및 벌목업 - 해고 및 퇴직',
    
    'JTS230000000000000HIL': '건설업 - 채용',
    'JTS230000000000000JOL': '건설업 - 구인',
    'JTS230000000000000LDL': '건설업 - 해고 및 퇴직',
    
    'JTS300000000000000HIL': '제조업 - 채용',
    'JTS300000000000000JOL': '제조업 - 구인',
    'JTS300000000000000LDL': '제조업 - 해고 및 퇴직',
    
    'JTS400000000000000HIL': '무역/운송/유틸리티 - 채용',
    'JTS400000000000000JOL': '무역/운송/유틸리티 - 구인',
    'JTS400000000000000LDL': '무역/운송/유틸리티 - 해고 및 퇴직',
    
    'JTS510000000000000HIL': '정보산업 - 채용',
    'JTS510000000000000JOL': '정보산업 - 구인',
    'JTS510000000000000LDL': '정보산업 - 해고 및 퇴직',
    
    'JTS510099000000000HIL': '금융업 - 채용',
    'JTS510099000000000JOL': '금융업 - 구인',
    'JTS510099000000000LDL': '금융업 - 해고 및 퇴직',
    
    'JTS540099000000000HIL': '전문/비즈니스 서비스 - 채용',
    'JTS540099000000000JOL': '전문/비즈니스 서비스 - 구인',
    'JTS540099000000000LDL': '전문/비즈니스 서비스 - 해고 및 퇴직',
    
    'JTS600000000000000HIL': '교육/의료 서비스(민간) - 채용',
    'JTS600000000000000JOL': '교육/의료 서비스(민간) - 구인',
    'JTS600000000000000LDL': '교육/의료 서비스(민간) - 해고 및 퇴직',
    
    'JTS700000000000000HIL': '레저/숙박업 - 채용',
    'JTS700000000000000JOL': '레저/숙박업 - 구인',
    'JTS700000000000000LDL': '레저/숙박업 - 해고 및 퇴직',
    
    'JTS810000000000000HIL': '기타 서비스 - 채용',
    'JTS810000000000000JOL': '기타 서비스 - 구인',
    'JTS810000000000000LDL': '기타 서비스 - 해고 및 퇴직',
    
    'JTS900000000000000HIL': '정부 - 채용',
    'JTS900000000000000JOL': '정부 - 구인',
    'JTS900000000000000LDL': '정부 - 해고 및 퇴직'
}

# JOLTS 계층 구조
JOLTS_HIERARCHY = {
    '총계': {
        'Total': ['JTS000000000000000HIL', 'JTS000000000000000JOL', 'JTS000000000000000LDL'],
        'Private': ['JTS100000000000000HIL', 'JTS100000000000000JOL', 'JTS100000000000000LDL'],
        'Government': ['JTS900000000000000HIL', 'JTS900000000000000JOL', 'JTS900000000000000LDL']
    },
    '산업별': {
        'Goods': ['JTS110099000000000JOL', 'JTS230000000000000JOL', 'JTS300000000000000JOL'],
        'Services': ['JTS400000000000000JOL', 'JTS510000000000000JOL', 'JTS510099000000000JOL', 
                    'JTS540099000000000JOL', 'JTS600000000000000JOL', 'JTS700000000000000JOL', 
                    'JTS810000000000000JOL'],
        'Tech & Finance': ['JTS510000000000000JOL', 'JTS510099000000000JOL', 'JTS540099000000000JOL']
    },
    '지표별': {
        'Job Openings': ['JTS000000000000000JOL', 'JTS100000000000000JOL', 'JTS900000000000000JOL'],
        'Hires': ['JTS000000000000000HIL', 'JTS100000000000000HIL', 'JTS900000000000000HIL'],
        'Layoffs': ['JTS000000000000000LDL', 'JTS100000000000000LDL', 'JTS900000000000000LDL']
    }
}

# %%
# === 전역 데이터 저장소 ===

# API 설정
BLS_SESSION = None
API_INITIALIZED = False

# 전역 데이터 저장소
JOLTS_DATA = {
    'raw_data': pd.DataFrame(),      # 원본 레벨 데이터
    'yoy_data': pd.DataFrame(),      # 전년동월대비 변화율 데이터
    'mom_data': pd.DataFrame(),      # 전월대비 변화율 데이터
    'rates_data': pd.DataFrame(),    # 비율 데이터 (구인률, 채용률 등)
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
# === API 초기화 함수 ===

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
# === 데이터 처리 함수들 ===

def calculate_yoy_change(data):
    """전년동월대비 변화율 계산"""
    return ((data / data.shift(12)) - 1) * 100

def calculate_mom_change(data):
    """전월대비 변화율 계산"""
    return ((data / data.shift(1)) - 1) * 100

def calculate_3m_avg(data):
    """3개월 이동평균 계산"""
    return data.rolling(window=3).mean()

def calculate_6m_avg(data):
    """6개월 이동평균 계산"""
    return data.rolling(window=6).mean()

# %%
# === 스마트 업데이트 함수 (PPI 방식 적용) ===

def check_recent_data_consistency(series_list=None, check_count=3):
    """
    최근 N개 데이터의 일치성을 확인하는 함수 (개선된 스마트 업데이트)
    
    Args:
        series_list: 확인할 시리즈 리스트 (None이면 주요 시리즈만)
        check_count: 확인할 최근 데이터 개수
    
    Returns:
        dict: 일치성 확인 결과 (need_update, reason, mismatches)
    """
    if not JOLTS_DATA['load_info']['loaded']:
        return {'need_update': True, 'reason': '기존 데이터 없음'}
    
    if series_list is None:
        # 주요 시리즈만 체크 (속도 향상)
        series_list = ['JTS000000000000000JOL', 'JTS000000000000000HIL', 'JTS000000000000000LDL']
    
    # BLS API 초기화
    if not initialize_bls_api():
        return {'need_update': True, 'reason': 'API 초기화 실패'}
    
    print(f"📊 최근 {check_count}개 데이터 일치성 확인 중...")
    
    mismatches = []
    new_data_available = False
    all_data_identical = True
    
    for series_id in series_list:
        if series_id not in JOLTS_DATA['raw_data'].columns:
            continue
        
        existing_data = JOLTS_DATA['raw_data'][series_id].dropna()
        if len(existing_data) == 0:
            continue
        
        # 최근 데이터 가져오기
        current_year = datetime.datetime.now().year
        api_data = get_bls_data(series_id, current_year - 1, current_year)
        
        if api_data is None or len(api_data) == 0:
            mismatches.append({
                'series': series_id,
                'reason': 'API 데이터 없음'
            })
            all_data_identical = False
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
            all_data_identical = False
            continue  # 새 데이터가 있으면 다른 체크는 건너뜀
        
        # 최근 N개 데이터 비교 (새 데이터가 없을 때만)
        existing_recent = existing_data.tail(check_count)
        api_recent = api_data.tail(check_count)
        
        # 날짜와 값 모두 비교
        for date in existing_recent.index[-check_count:]:
            if date in api_recent.index:
                existing_val = existing_recent.loc[date]
                api_val = api_recent.loc[date]
                
                # 값이 다르면 불일치 (1000.0 이상 차이 - JOLTS 고용 데이터 특성 고려, 천명 단위)
                if abs(existing_val - api_val) > 1000.0:
                    mismatches.append({
                        'series': series_id,
                        'date': date,
                        'existing': existing_val,
                        'api': api_val,
                        'diff': abs(existing_val - api_val)
                    })
                    all_data_identical = False
            else:
                mismatches.append({
                    'series': series_id,
                    'date': date,
                    'existing': existing_recent.loc[date],
                    'api': None,
                    'reason': '날짜 없음'
                })
                all_data_identical = False
    
    # 결과 판정 로직 개선
    if new_data_available:
        print(f"🆕 새로운 데이터 감지: 업데이트 필요")
        for mismatch in mismatches[:3]:
            if 'reason' in mismatch and mismatch['reason'] == '새로운 데이터 존재':
                print(f"   - {mismatch['series']}: {mismatch['existing_latest']} → {mismatch['api_latest']}")
        return {'need_update': True, 'reason': '새로운 데이터', 'mismatches': mismatches}
    elif not all_data_identical:
        # 값 불일치가 있는 경우
        value_mismatches = [m for m in mismatches if m.get('reason') != '새로운 데이터 존재']
        print(f"⚠️ 데이터 불일치 발견: {len(value_mismatches)}개")
        
        # 디버깅: 실제 불일치 내용 출력
        print("🔍 불일치 세부 내용 (최대 3개):")
        for i, mismatch in enumerate(value_mismatches[:3]):
            if 'existing' in mismatch and 'api' in mismatch:
                print(f"   {i+1}. {mismatch.get('series', 'unknown')} ({mismatch.get('date', 'unknown')}): CSV={mismatch['existing']:.1f}, API={mismatch['api']:.1f}, 차이={mismatch['diff']:.1f}")
            else:
                print(f"   {i+1}. {mismatch}")
        
        # JOLTS 특화: 1000.0 이상만 실제 불일치로 간주 (천명 단위)
        significant_mismatches = [m for m in value_mismatches if m.get('diff', 0) > 1000.0]
        if len(significant_mismatches) == 0:
            print("📝 모든 차이가 1000.0 미만입니다. 저장 정밀도 차이로 간주하여 업데이트를 건너뜁니다.")
            return {'need_update': False, 'reason': '미세한 정밀도 차이', 'mismatches': mismatches}
        else:
            print(f"🚨 유의미한 차이 발견: {len(significant_mismatches)}개 (1000.0 이상)")
            return {'need_update': True, 'reason': '데이터 불일치', 'mismatches': mismatches}
    else:
        print("✅ 최근 데이터가 완전히 일치합니다. 업데이트를 건너뜁니다.")
        return {'need_update': False, 'reason': '데이터 일치', 'mismatches': []}

def update_jolts_data_from_api(start_date=None, series_list=None, smart_update=True):
    """
    API를 사용하여 기존 데이터 업데이트 (스마트 업데이트 지원)
    
    Args:
        start_date: 업데이트 시작 날짜 (None이면 마지막 데이터 이후부터)
        series_list: 업데이트할 시리즈 리스트 (None이면 모든 시리즈)
        smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
        bool: 업데이트 성공 여부
    """
    global JOLTS_DATA
    
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 기존 데이터가 없습니다. 전체 로드를 수행합니다.")
        return load_all_jolts_data()
    
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
            last_date = JOLTS_DATA['raw_data'].index[-1]
            start_date = (last_date - pd.DateOffset(months=3)).strftime('%Y-%m-01')
        elif consistency_check['reason'] == '기존 데이터 없음':
            # 전체 재로드
            return load_all_jolts_data(force_reload=True)
        else:
            print(f"🔄 업데이트 진행: {consistency_check['reason']}")
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    # 업데이트 시작 날짜 결정
    if start_date is None:
        last_date = JOLTS_DATA['raw_data'].index[-1]
        # 마지막 날짜의 다음 달부터 업데이트
        start_date = (last_date + pd.DateOffset(months=1)).strftime('%Y-%m-01')
    
    print(f"🔄 JOLTS 데이터 업데이트 시작 ({start_date}부터)")
    print("="*50)
    
    if series_list is None:
        series_list = list(JOLTS_SERIES.keys())
    
    updated_data = {}
    new_count = 0
    
    for series_id in series_list:
        if series_id not in JOLTS_SERIES:
            continue
        
        # 새로운 데이터 가져오기
        start_year = pd.to_datetime(start_date).year
        new_data = get_bls_data(series_id, start_year)
        
        if new_data is not None and len(new_data) > 0:
            # 기존 데이터와 병합
            if series_id in JOLTS_DATA['raw_data'].columns:
                existing_data = JOLTS_DATA['raw_data'][series_id]
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
        JOLTS_DATA['raw_data'] = updated_df
        JOLTS_DATA['yoy_data'] = updated_df.apply(calculate_yoy_change)
        JOLTS_DATA['mom_data'] = updated_df.apply(calculate_mom_change)
        
        # 최신 값 업데이트
        for series_id in updated_data.keys():
            if series_id in JOLTS_DATA['raw_data'].columns:
                raw_data = JOLTS_DATA['raw_data'][series_id].dropna()
                if len(raw_data) > 0:
                    JOLTS_DATA['latest_values'][series_id] = raw_data.iloc[-1]
        
        # 로드 정보 업데이트
        JOLTS_DATA['load_info']['load_time'] = datetime.datetime.now()
        JOLTS_DATA['load_info']['series_count'] = len(updated_df.columns)
        JOLTS_DATA['load_info']['data_points'] = len(updated_df)
        
        print(f"\n✅ 업데이트 완료! 새로 추가된 데이터: {new_count}개 포인트")
        print_load_info()
        
        # 자동으로 CSV에 저장
        save_jolts_data_to_csv()
        
        return True
    else:
        print("\n⚠️ 업데이트할 새로운 데이터가 없습니다.")
        return False

# %%
# === CSV 저장/로드 함수들 ===

def save_jolts_data_to_csv(file_path='/home/jyp0615/us_eco/jolts_data.csv'):
    """
    현재 로드된 JOLTS 데이터를 CSV 파일로 저장
    
    Args:
        file_path: 저장할 CSV 파일 경로
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 저장할 데이터가 없습니다. 먼저 데이터를 로드하세요.")
        return False
    
    try:
        # raw_data와 메타데이터를 함께 저장
        raw_data = JOLTS_DATA['raw_data']
        
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
                'load_time': JOLTS_DATA['load_info']['load_time'].isoformat() if JOLTS_DATA['load_info']['load_time'] else None,
                'start_date': JOLTS_DATA['load_info']['start_date'],
                'series_count': JOLTS_DATA['load_info']['series_count'],
                'data_points': JOLTS_DATA['load_info']['data_points'],
                'latest_values': JOLTS_DATA['latest_values']
            }
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ JOLTS 데이터 저장 완료: {file_path}")
        print(f"✅ 메타데이터 저장 완료: {meta_file}")
        return True
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_jolts_data_from_csv(file_path='/home/jyp0615/us_eco/jolts_data.csv'):
    """
    CSV 파일에서 JOLTS 데이터 로드
    
    Args:
        file_path: 로드할 CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global JOLTS_DATA
    
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
        JOLTS_DATA['raw_data'] = df
        JOLTS_DATA['yoy_data'] = df.apply(calculate_yoy_change)
        JOLTS_DATA['mom_data'] = df.apply(calculate_mom_change)
        JOLTS_DATA['latest_values'] = latest_values
        JOLTS_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': df.index[0].strftime('%Y-%m-%d') if len(df) > 0 else None,
            'series_count': len(df.columns),
            'data_points': len(df)
        }
        
        print(f"✅ CSV에서 JOLTS 데이터 로드 완료: {file_path}")
        print_load_info()
        return True
        
    except Exception as e:
        print(f"❌ CSV 로드 실패: {e}")
        return False

# %%
# === 메인 데이터 로드 함수 ===

def load_all_jolts_data(start_date='2020-01-01', series_list=None, force_reload=False, csv_file='/home/jyp0615/us_eco/jolts_data.csv'):
    """
    JOLTS 데이터 로드 (CSV 우선, API 백업 방식)
    
    Args:
        start_date: 시작 날짜
        series_list: 로드할 시리즈 리스트 (None이면 모든 시리즈)
        force_reload: 강제 재로드 여부
        csv_file: CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global JOLTS_DATA
    
    # 이미 로드된 경우 스킵
    if JOLTS_DATA['load_info']['loaded'] and not force_reload:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    # CSV 파일이 있으면 우선 로드 시도
    import os
    if not force_reload and os.path.exists(csv_file):
        print("📂 기존 CSV 파일에서 데이터 로드 시도...")
        if load_jolts_data_from_csv(csv_file):
            print("🔄 CSV 로드 후 스마트 업데이트 시도...")
            # CSV 로드 성공 후 스마트 업데이트
            update_jolts_data_from_api(smart_update=True)
            return True
        else:
            print("⚠️ CSV 로드 실패, API에서 전체 로드 시도...")
    
    print("🚀 JOLTS 데이터 로딩 시작... (BLS API 전용)")
    print("="*50)
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    if series_list is None:
        series_list = list(JOLTS_SERIES.keys())
    
    # 데이터 수집
    raw_data_dict = {}
    yoy_data_dict = {}
    mom_data_dict = {}
    latest_values = {}
    
    for series_id in series_list:
        if series_id not in JOLTS_SERIES:
            print(f"⚠️ 알 수 없는 시리즈: {series_id}")
            continue
        
        # 원본 데이터 가져오기
        start_year = pd.to_datetime(start_date).year
        series_data = get_bls_data(series_id, start_year)
        
        if series_data is not None and len(series_data) > 0:
            # 날짜 필터링
            if start_date:
                series_data = series_data[series_data.index >= start_date]
            
            # NaN 제거
            series_data = series_data.dropna()
            
            # 전체 로드에서는 최소 12개 필요 (업데이트가 아니므로)
            if len(series_data) < 12:
                print(f"❌ 데이터 부족: {series_id} ({len(series_data)}개)")
                continue
            
            # 원본 데이터 저장
            raw_data_dict[series_id] = series_data
            
            # YoY 계산
            yoy_data = calculate_yoy_change(series_data)
            yoy_data_dict[series_id] = yoy_data
            
            # MoM 계산
            mom_data = calculate_mom_change(series_data)
            mom_data_dict[series_id] = mom_data
            
            # 최신 값 저장
            if len(series_data.dropna()) > 0:
                latest_val = series_data.dropna().iloc[-1]
                latest_values[series_id] = latest_val
            else:
                print(f"⚠️ 데이터 없음: {series_id}")
        else:
            print(f"❌ 데이터 로드 실패: {series_id}")
    
    # 로드된 데이터가 너무 적으면 오류 발생
    if len(raw_data_dict) < 6:  # 최소 6개 시리즈는 있어야 함
        error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data_dict)}개 (최소 6개 필요)"
        print(error_msg)
        raise ValueError(error_msg)
    
    # 전역 저장소에 저장
    JOLTS_DATA['raw_data'] = pd.DataFrame(raw_data_dict)
    JOLTS_DATA['yoy_data'] = pd.DataFrame(yoy_data_dict)
    JOLTS_DATA['mom_data'] = pd.DataFrame(mom_data_dict)
    JOLTS_DATA['latest_values'] = latest_values
    JOLTS_DATA['load_info'] = {
        'loaded': True,
        'load_time': datetime.datetime.now(),
        'start_date': start_date,
        'series_count': len(raw_data_dict),
        'data_points': len(JOLTS_DATA['raw_data']) if not JOLTS_DATA['raw_data'].empty else 0
    }
    
    print("\n✅ 데이터 로딩 완료!")
    print_load_info()
    
    # 자동으로 CSV에 저장
    save_jolts_data_to_csv(csv_file)
    
    return True

def print_load_info():
    """로드 정보 출력"""
    info = JOLTS_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not JOLTS_DATA['raw_data'].empty:
        date_range = f"{JOLTS_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {JOLTS_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

# %%
# === 데이터 접근 함수들 ===

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_jolts_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return JOLTS_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in JOLTS_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return JOLTS_DATA['raw_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """YoY 변화율 데이터 반환"""
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_jolts_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return JOLTS_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in JOLTS_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return JOLTS_DATA['yoy_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화율 데이터 반환"""
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_jolts_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return JOLTS_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in JOLTS_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return JOLTS_DATA['mom_data'][available_series].copy()

def get_latest_values(series_names=None):
    """최신 값들 반환"""
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_jolts_data()를 먼저 실행하세요.")
        return {}
    
    if series_names is None:
        return JOLTS_DATA['latest_values'].copy()
    
    return {name: JOLTS_DATA['latest_values'].get(name, 0) for name in series_names if name in JOLTS_DATA['latest_values']}

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not JOLTS_DATA['load_info']['loaded']:
        return []
    return list(JOLTS_DATA['raw_data'].columns)

# %%
# === 고급 분석 함수들 ===

def calculate_labor_market_tightness():
    """
    노동시장 타이트니스 지표 계산 (V/U ratio proxy)
    구인/실업자 비율의 프록시로 구인건수 사용
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return None
    
    # Total nonfarm job openings
    job_openings = get_raw_data(['JTS000000000000000JOL'])
    
    if job_openings.empty:
        return None
    
    # 노동시장 타이트니스 지표 (정규화)
    tightness = job_openings.copy()
    tightness.columns = ['Labor Market Tightness']
    
    return tightness

def calculate_hiring_efficiency():
    """
    채용 효율성 지표 계산 (Hires/Job Openings)
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return None
    
    hires = get_raw_data(['JTS000000000000000HIL'])
    openings = get_raw_data(['JTS000000000000000JOL'])
    
    if hires.empty or openings.empty:
        return None
    
    # 채용 효율성 = 채용/구인
    efficiency = (hires['JTS000000000000000HIL'] / openings['JTS000000000000000JOL']) * 100
    efficiency_df = pd.DataFrame({'Hiring Efficiency': efficiency})
    
    return efficiency_df

def calculate_layoff_rate_changes():
    """
    해고율 변화 분석 (경기침체 신호)
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return None
    
    layoffs = get_raw_data(['JTS000000000000000LDL'])
    
    if layoffs.empty:
        return None
    
    # 3개월 이동평균
    layoffs_3m = calculate_3m_avg(layoffs)
    # 전년동월대비
    layoffs_yoy = calculate_yoy_change(layoffs)
    
    return {
        'raw': layoffs,
        '3m_avg': layoffs_3m,
        'yoy': layoffs_yoy
    }

def get_sector_rotation_signals():
    """
    섹터 로테이션 신호 분석
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return None
    
    # 주요 섹터별 구인 데이터
    sectors = {
        'Manufacturing': 'JTS300000000000000JOL',
        'Financial': 'JTS510099000000000JOL',
        'Tech/Info': 'JTS510000000000000JOL',
        'Prof Services': 'JTS540099000000000JOL',
        'Leisure': 'JTS700000000000000JOL'
    }
    
    sector_data = {}
    for name, series_id in sectors.items():
        data = get_yoy_data([series_id])
        if not data.empty:
            sector_data[name] = data[series_id]
    
    return pd.DataFrame(sector_data)

# %%
# === KPDS 포맷 시각화 함수들 ===

def create_jolts_overview_dashboard(start_date=None):
    """
    JOLTS 종합 대시보드 (한국어 버전) - KPDS 포맷 적용
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_jolts_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 데이터 준비
    openings = get_raw_data(['JTS000000000000000JOL'])['JTS000000000000000JOL']
    hires = get_raw_data(['JTS000000000000000HIL'])['JTS000000000000000HIL']
    layoffs = get_raw_data(['JTS000000000000000LDL'])['JTS000000000000000LDL']
    
    if start_date:
        openings = openings[openings.index >= start_date]
        hires = hires[hires.index >= start_date]
        layoffs = layoffs[layoffs.index >= start_date]
    
    # 개별 차트 생성 (KPDS 포맷 적용)
    print("1. 구인 vs 채용 차트 생성...")
    data_dict = {
        'openings': openings,
        'hires': hires
    }
    labels_dict = {
        'openings': '구인',
        'hires': '채용'
    }
    
    print("구인 vs 채용")
    fig1 = df_multi_line_chart(
        pd.DataFrame(data_dict),
        ytitle="천 건",
        labels=labels_dict
    )
    
    print("2. 노동시장 타이트니스 차트 생성...")
    tightness_data = {'tightness': openings / hires}
    print("노동시장 타이트니스")
    fig2 = df_line_chart(
        pd.DataFrame(tightness_data),
        ytitle="구인/채용 비율"
    )
    
    print("3. 해고 동향 차트 생성...")
    layoffs_3m = calculate_3m_avg(layoffs)
    layoffs_data = {
        'layoffs': layoffs,
        'layoffs_3m': layoffs_3m
    }
    layoffs_labels = {
        'layoffs': '해고',
        'layoffs_3m': '3개월 평균'
    }
    print("해고 동향")
    fig3 = df_multi_line_chart(
        pd.DataFrame(layoffs_data),
        ytitle="천 건",
        labels=layoffs_labels
    )
    
    print("4. 채용 효율성 차트 생성...")
    efficiency = calculate_hiring_efficiency()
    if efficiency is not None:
        print("채용 효율성")
        fig4 = df_line_chart(
            efficiency,
            ytitle="%"
        )
        return {'fig1': fig1, 'fig2': fig2, 'fig3': fig3, 'fig4': fig4}
    else:
        return {'fig1': fig1, 'fig2': fig2, 'fig3': fig3}

def create_sector_heatmap(metric='job_openings'):
    """
    섹터별 히트맵 (전년동월대비 변화율) - KPDS 포맷 적용
    
    Args:
        metric: 'job_openings', 'hires', 'layoffs'
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_jolts_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 메트릭별 시리즈 선택
    suffix_map = {
        'job_openings': 'JOL',
        'hires': 'HIL',
        'layoffs': 'LDL'
    }
    
    metric_names = {
        'job_openings': '구인',
        'hires': '채용',
        'layoffs': '해고'
    }
    
    suffix = suffix_map.get(metric, 'JOL')
    metric_name = metric_names.get(metric, '구인')
    
    # 섹터 리스트 (한국어)
    sectors = {
        '광업 및 벌목업': f'JTS110099000000000{suffix}',
        '건설업': f'JTS230000000000000{suffix}',
        '제조업': f'JTS300000000000000{suffix}',
        '무역/운송/유틸리티': f'JTS400000000000000{suffix}',
        '정보산업': f'JTS510000000000000{suffix}',
        '금융업': f'JTS510099000000000{suffix}',
        '전문/비즈니스 서비스': f'JTS540099000000000{suffix}',
        '교육/의료 서비스': f'JTS600000000000000{suffix}',
        '레저/숙박업': f'JTS700000000000000{suffix}',
        '기타 서비스': f'JTS810000000000000{suffix}',
        '정부': f'JTS900000000000000{suffix}'
    }
    
    # YoY 데이터 수집
    yoy_data = get_yoy_data(list(sectors.values()))
    
    if yoy_data.empty:
        print("⚠️ 데이터가 없습니다.")
        return None
    
    # 최근 12개월 데이터
    recent_data = yoy_data.tail(12)
    
    # 히트맵 데이터 준비
    heatmap_data = []
    for sector_name, series_id in sectors.items():
        if series_id in recent_data.columns:
            heatmap_data.append(recent_data[series_id].values)
    
    # 월 라벨
    months = [d.strftime('%y년%m월') for d in recent_data.index]
    
    # KPDS 포맷 적용 히트맵 생성
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data,
        x=months,
        y=list(sectors.keys()),
        colorscale='RdBu_r',
        zmid=0,
        text=[[f'{val:.1f}%' for val in row] for row in heatmap_data],
        texttemplate='%{text}',
        textfont={"size": FONT_SIZE_GENERAL-2, "family": "NanumGothic"},
        colorbar=dict(title="전년동월대비 %", titlefont=dict(family='NanumGothic', size=FONT_SIZE_ANNOTATION))
    ))
    
    print(f"섹터별 히트맵 - {metric_name} (전년동월대비 % 변화)")
    
    # KPDS 스타일 레이아웃
    fig.update_layout(
        width=1000,
        height=600,
        paper_bgcolor='white',
        plot_bgcolor='white',
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL),
        xaxis=dict(
            showgrid=False,
            showline=True,
            linewidth=1.3,
            linecolor='lightgrey',
            tickfont=dict(family='NanumGothic', size=FONT_SIZE_GENERAL)
        ),
        yaxis=dict(
            showgrid=False,
            showline=False,
            tickfont=dict(family='NanumGothic', size=FONT_SIZE_GENERAL)
        ),
        margin=dict(l=200, r=80, t=80, b=80)
    )
    
    fig.show()
    return fig

def create_beveridge_curve(start_date='2020-01-01'):
    """
    베버리지 곡선 (Beveridge Curve) - 구인 트렌드 표시 (KPDS 포맷 적용)
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_jolts_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 구인 데이터
    openings = get_raw_data(['JTS000000000000000JOL'])['JTS000000000000000JOL']
    
    if start_date:
        openings = openings[openings.index >= start_date]
    
    # KPDS 포맷으로 라인 차트 생성
    df = pd.DataFrame({'openings': openings})
    
    print("구인 동향 (베버리지 곡선 프록시)")
    fig = df_line_chart(
        df,
        column='openings',
        ytitle="천 건",
        label="구인"
    )
    
    return fig

def create_labor_market_momentum():
    """
    노동시장 모멘텀 지표 차트 (KPDS 포맷 적용)
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_jolts_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 데이터 준비
    openings_mom = get_mom_data(['JTS000000000000000JOL'])['JTS000000000000000JOL']
    hires_mom = get_mom_data(['JTS000000000000000HIL'])['JTS000000000000000HIL']
    layoffs_mom = get_mom_data(['JTS000000000000000LDL'])['JTS000000000000000LDL']
    
    # 3개월 이동평균
    openings_3m = calculate_3m_avg(openings_mom)
    hires_3m = calculate_3m_avg(hires_mom)
    layoffs_3m = calculate_3m_avg(layoffs_mom)
    
    # 데이터프레임 생성
    momentum_data = {
        'openings': openings_3m,
        'hires': hires_3m,
        'layoffs': -layoffs_3m  # 음수로 표시
    }
    
    labels = {
        'openings': '구인 (3개월 평균)',
        'hires': '채용 (3개월 평균)',
        'layoffs': '해고 (3개월 평균, 반전)'
    }
    
    print("노동시장 모멘텀 지표 (전월대비 %, 3개월 이동평균)")
    
    # KPDS 포맷으로 다중 라인 차트 생성
    fig = df_multi_line_chart(
        pd.DataFrame(momentum_data),
        ytitle="%",
        labels=labels
    )
    
    return fig

def create_sector_comparison_chart(metric='job_openings', sectors=None):
    """
    섹터별 비교 차트 - KPDS 포맷 적용
    
    Args:
        metric: 'job_openings', 'hires', 'layoffs'
        sectors: 비교할 섹터 리스트 (None이면 주요 섹터)
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_jolts_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 기본 섹터 설정
    if sectors is None:
        sectors = ['제조업', '금융업', '기술/정보', '전문서비스', '레저']
    
    # 메트릭별 시리즈 매핑
    metric_map = {
        'job_openings': {
            '제조업': 'JTS300000000000000JOL',
            '금융업': 'JTS510099000000000JOL',
            '기술/정보': 'JTS510000000000000JOL',
            '전문서비스': 'JTS540099000000000JOL',
            '레저': 'JTS700000000000000JOL',
            '건설업': 'JTS230000000000000JOL',
            '무역': 'JTS400000000000000JOL',
            '교육/의료': 'JTS600000000000000JOL'
        },
        'hires': {
            '제조업': 'JTS300000000000000HIL',
            '금융업': 'JTS510099000000000HIL',
            '기술/정보': 'JTS510000000000000HIL',
            '전문서비스': 'JTS540099000000000HIL',
            '레저': 'JTS700000000000000HIL',
            '건설업': 'JTS230000000000000HIL',
            '무역': 'JTS400000000000000HIL',
            '교육/의료': 'JTS600000000000000HIL'
        },
        'layoffs': {
            '제조업': 'JTS300000000000000LDL',
            '금융업': 'JTS510099000000000LDL',
            '기술/정보': 'JTS510000000000000LDL',
            '전문서비스': 'JTS540099000000000LDL',
            '레저': 'JTS700000000000000LDL',
            '건설업': 'JTS230000000000000LDL',
            '무역': 'JTS400000000000000LDL',
            '교육/의료': 'JTS600000000000000LDL'
        }
    }
    
    series_map = metric_map.get(metric, metric_map['job_openings'])
    
    # 데이터 수집
    data_dict = {}
    for sector in sectors:
        if sector in series_map:
            series_id = series_map[sector]
            data = get_raw_data([series_id])
            
            if not data.empty:
                # 최근 2년 데이터
                recent_data = data[series_id].tail(24)
                data_dict[sector] = recent_data
    
    if not data_dict:
        print("⚠️ 표시할 데이터가 없습니다.")
        return None
    
    # DataFrame 생성
    df = pd.DataFrame(data_dict)
    
    # 레이아웃
    title_map = {
        'job_openings': '섹터별 구인 현황',
        'hires': '섹터별 채용 현황',
        'layoffs': '섹터별 해고 현황'
    }
    
    ytitle_map = {
        'job_openings': '천 건',
        'hires': '천 건',
        'layoffs': '천 건'
    }
    
    print(title_map.get(metric, '섹터별 노동시장 지표'))
    
    # KPDS 포맷으로 다중 라인 차트 생성
    fig = df_multi_line_chart(
        df,
        ytitle=ytitle_map.get(metric, '천 명'),
        labels=dict(zip(sectors, sectors))
    )
    
    return fig

def create_ib_style_summary_table():
    """
    IB 스타일 요약 테이블 생성 (한국어)
    """
    if not JOLTS_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_jolts_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주요 지표 수집
    indicators = {
        '전체 구인': 'JTS000000000000000JOL',
        '전체 채용': 'JTS000000000000000HIL',
        '전체 해고': 'JTS000000000000000LDL',
        '제조업 구인': 'JTS300000000000000JOL',
        '금융업 구인': 'JTS510099000000000JOL',
        '기술업 구인': 'JTS510000000000000JOL',
        '전문서비스 구인': 'JTS540099000000000JOL'
    }
    
    # 테이블 데이터 준비
    table_data = []
    
    for name, series_id in indicators.items():
        raw_data = get_raw_data([series_id])
        yoy_data = get_yoy_data([series_id])
        mom_data = get_mom_data([series_id])
        
        if not raw_data.empty:
            latest_val = raw_data[series_id].iloc[-1]
            yoy_val = yoy_data[series_id].iloc[-1] if not yoy_data.empty else np.nan
            mom_val = mom_data[series_id].iloc[-1] if not mom_data.empty else np.nan
            
            # 3개월 평균
            mom_3m = calculate_3m_avg(mom_data[series_id])
            mom_3m_val = mom_3m.iloc[-1] if len(mom_3m) > 0 else np.nan
            
            table_data.append({
                '지표': name,
                '최신값': f"{latest_val:,.0f}",
                '전월대비(%)': f"{mom_val:+.1f}%" if not np.isnan(mom_val) else "N/A",
                '전년동월대비(%)': f"{yoy_val:+.1f}%" if not np.isnan(yoy_val) else "N/A",
                '3개월평균(%)': f"{mom_3m_val:+.1f}%" if not np.isnan(mom_3m_val) else "N/A"
            })
    
    # DataFrame 생성
    df = pd.DataFrame(table_data)
    
    # 테이블 시각화
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=list(df.columns),
            fill_color='darkblue',
            font=dict(color='white', size=14, family='NanumGothic'),
            align='left'
        ),
        cells=dict(
            values=[df[col] for col in df.columns],
            fill_color=[['lightgray' if i % 2 == 0 else 'white' for i in range(len(df))]],
            font=dict(color='black', size=12, family='NanumGothic'),
            align='left'
        )
    )])
    
    latest_date = JOLTS_DATA['raw_data'].index[-1].strftime('%B %Y')
    
    print(f"JOLTS 주요 지표 요약 - {latest_date}")
    
    fig.update_layout(
        width=900,
        height=400,
        paper_bgcolor='white',
        font=dict(family='NanumGothic')
    )
    
    fig.show()
    return fig

# %%
# === 통합 분석 함수 ===

def run_complete_jolts_analysis(start_date='2020-01-01', force_reload=False):
    """
    완전한 JOLTS 분석 실행 - IB/이코노미스트 관점 (KPDS 포맷 적용)
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🚀 완전한 JOLTS 노동시장 분석 시작 (KPDS 포맷)")
    print("="*50)
    
    # 1. 데이터 로드
    print("\n1️⃣ 데이터 로딩")
    success = load_all_jolts_data(start_date=start_date, force_reload=force_reload)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 분석 및 시각화 생성
    print("\n2️⃣ 분석 및 시각화 생성")
    
    results = {}
    
    try:
        # IB 스타일 요약 테이블
        print("   📊 요약 테이블 생성...")
        results['summary_table'] = create_ib_style_summary_table()
        
        # 종합 대시보드
        print("   📈 종합 대시보드...")
        results['overview_dashboard'] = create_jolts_overview_dashboard()
        
        # 노동시장 모멘텀
        print("   📈 노동시장 모멘텀 분석...")
        results['momentum'] = create_labor_market_momentum()
        
        # 섹터 히트맵
        print("   🔥 섹터별 히트맵...")
        results['sector_heatmap'] = create_sector_heatmap('job_openings')
        
        # 베버리지 곡선
        print("   📊 베버리지 곡선 프록시...")
        results['beveridge_curve'] = create_beveridge_curve()
        
        # 섹터 비교
        print("   📊 주요 섹터 비교...")
        results['sector_comparison'] = create_sector_comparison_chart('job_openings')
        
    except Exception as e:
        print(f"⚠️ 분석 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ 분석 완료! 생성된 차트: {len(results)}개")
    return results

# %%
# === 유틸리티 함수들 ===

def clear_data():
    """데이터 초기화"""
    global JOLTS_DATA
    JOLTS_DATA = {
        'raw_data': pd.DataFrame(),
        'yoy_data': pd.DataFrame(),
        'mom_data': pd.DataFrame(),
        'rates_data': pd.DataFrame(),
        'latest_values': {},
        'load_info': {'loaded': False, 'load_time': None, 'start_date': None, 'series_count': 0, 'data_points': 0}
    }
    print("🗑️ 데이터가 초기화되었습니다")

def show_available_components():
    """사용 가능한 JOLTS 구성요소 표시"""
    print("=== 사용 가능한 JOLTS 구성요소 ===")
    
    for series_id, description in JOLTS_SERIES.items():
        korean_name = JOLTS_KOREAN_NAMES.get(series_id, description)
        print(f"  '{series_id}': {korean_name} ({description})")

def get_data_status():
    """현재 데이터 상태 반환"""
    return {
        'loaded': JOLTS_DATA['load_info']['loaded'],
        'series_count': JOLTS_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': JOLTS_DATA['load_info']
    }

def show_jolts_hierarchy():
    """JOLTS 계층 구조 표시"""
    print("=== JOLTS 계층 구조 ===\n")
    
    for level, groups in JOLTS_HIERARCHY.items():
        print(f"### {level} ###")
        for group_name, series_list in groups.items():
            print(f"  {group_name}:")
            for series_id in series_list:
                korean_name = JOLTS_KOREAN_NAMES.get(series_id, JOLTS_SERIES.get(series_id, series_id))
                print(f"    - {series_id}: {korean_name}")
        print()

# %%
# === 사용 예시 ===

print("\n=== JOLTS 노동시장 분석 도구 사용법 (KPDS 포맷) ===")
print("1. 데이터 로드 (CSV 우선, API 백업, 스마트 업데이트):")
print("   load_all_jolts_data()  # CSV가 있으면 로드 후 스마트 업데이트")
print("   load_jolts_data_from_csv()  # CSV에서만 로드")
print("   update_jolts_data_from_api()  # 최신 데이터만 API로 업데이트")
print()
print("2. 데이터 저장:")
print("   save_jolts_data_to_csv()  # 현재 데이터를 CSV로 저장")
print()
print("3. KPDS 포맷 분석:")
print("   create_ib_style_summary_table()  # 한국어 요약 테이블")
print("   create_jolts_overview_dashboard()  # KPDS 종합 대시보드")
print("   create_labor_market_momentum()  # KPDS 모멘텀 지표")
print("   create_sector_heatmap()  # KPDS 섹터 히트맵")
print("   create_beveridge_curve()  # KPDS 베버리지 곡선")
print("   create_sector_comparison_chart()  # KPDS 섹터 비교")
print()
print("4. 고급 분석 함수:")
print("   calculate_labor_market_tightness()  # 노동시장 타이트니스")
print("   calculate_hiring_efficiency()  # 채용 효율성")
print("   calculate_layoff_rate_changes()  # 해고율 변화")
print("   get_sector_rotation_signals()  # 섹터 로테이션 신호")
print()
print("5. 통합 분석:")
print("   run_complete_jolts_analysis()  # 전체 KPDS 분석 실행")
print()
print("6. 데이터 상태 확인:")
print("   get_data_status()")
print("   show_available_components()")
print("   show_jolts_hierarchy()")

# %%
# 사용 가능한 구성요소 표시
show_available_components()

# %%
# 예시 실행 (주석 처리)
run_complete_jolts_analysis()
# %%
create_beveridge_curve()
# %%
