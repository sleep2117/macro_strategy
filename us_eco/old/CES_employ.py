# %%
"""
BLS API 전용 미국 고용 보고서 분석 및 시각화 도구
- BLS API를 통한 고용 데이터 수집
- 섹터별 고용 분석
- 임금 및 근로시간 분석
- IB/이코노미스트 스타일 시각화
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
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

# BLS API 키 설정 (CPI/PPI와 동일)
BLS_API_KEY = '56b193612b614cdc9416359fd1c73a74'
BLS_API_KEY2 = '0450ef37363c48b5bedd2ae6fc92dd6e'
BLS_API_KEY3 = 'daf1ca7970b74e81b6a5c7a80a8b8a7f'
CURRENT_API_KEY = BLS_API_KEY

# 시각화 라이브러리 불러오기
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

# %%
# === 고용 보고서 시리즈 ID와 매핑 ===

# 고용 시리즈 ID 매핑 (확장된 버전)
EMPLOYMENT_SERIES = {
    # === ALL EMPLOYEES (천명 단위) ===
    # Core Employment Indicators
    'nonfarm_total': 'CES0000000001',  # Total nonfarm
    'private_total': 'CES0500000001',  # Total private
    'goods_producing': 'CES0600000001',  # Goods-producing
    'service_providing': 'CES0700000001',  # Service-providing
    'private_service': 'CES0800000001',  # Private service-providing
    'government': 'CES9000000001',  # Government
    
    # Goods-Producing Sectors
    'mining_logging': 'CES1000000001',  # Mining and logging
    'construction': 'CES2000000001',  # Construction
    'manufacturing': 'CES3000000001',  # Manufacturing
    'durable_goods': 'CES3100000001',  # Durable goods
    'nondurable_goods': 'CES3200000001',  # Nondurable goods
    
    # Service-Providing Sectors
    'trade_transport_utilities': 'CES4000000001',  # Trade, transportation, and utilities
    'wholesale_trade': 'CES4142000001',  # Wholesale trade
    'retail_trade': 'CES4200000001',  # Retail trade
    'transport_warehouse': 'CES4300000001',  # Transportation and warehousing
    'utilities': 'CES4422000001',  # Utilities
    'information': 'CES5000000001',  # Information
    'financial': 'CES5500000001',  # Financial activities
    'professional_business': 'CES6000000001',  # Professional and business services
    'education_health': 'CES6500000001',  # Private education and health services
    'leisure_hospitality': 'CES7000000001',  # Leisure and hospitality
    'other_services': 'CES8000000001',  # Other services
    
    # === AVERAGE WEEKLY HOURS ===
    'hours_private': 'CES0500000002',  # Total private
    'hours_goods': 'CES0600000002',  # Goods-producing
    'hours_private_service': 'CES0800000002',  # Private service-providing
    'hours_mining': 'CES1000000002',  # Mining and logging
    'hours_construction': 'CES2000000002',  # Construction
    'hours_manufacturing': 'CES3000000002',  # Manufacturing
    'hours_durable': 'CES3100000002',  # Durable goods
    'hours_nondurable': 'CES3200000002',  # Nondurable goods
    'hours_trade': 'CES4000000002',  # Trade, transportation, and utilities
    'hours_wholesale': 'CES4142000002',  # Wholesale trade
    'hours_retail': 'CES4200000002',  # Retail trade
    'hours_transport': 'CES4300000002',  # Transportation and warehousing
    'hours_utilities': 'CES4422000002',  # Utilities
    'hours_information': 'CES5000000002',  # Information
    'hours_financial': 'CES5500000002',  # Financial activities
    'hours_professional': 'CES6000000002',  # Professional and business services
    'hours_education_health': 'CES6500000002',  # Private education and health services
    'hours_leisure': 'CES7000000002',  # Leisure and hospitality
    'hours_other': 'CES8000000002',  # Other services
    
    # === AVERAGE HOURLY EARNINGS ===
    'earnings_private': 'CES0500000003',  # Total private
    'earnings_goods': 'CES0600000003',  # Goods-producing
    'earnings_private_service': 'CES0800000003',  # Private service-providing
    'earnings_mining': 'CES1000000003',  # Mining and logging
    'earnings_construction': 'CES2000000003',  # Construction
    'earnings_manufacturing': 'CES3000000003',  # Manufacturing
    'earnings_durable': 'CES3100000003',  # Durable goods
    'earnings_nondurable': 'CES3200000003',  # Nondurable goods
    'earnings_trade': 'CES4000000003',  # Trade, transportation, and utilities
    'earnings_wholesale': 'CES4142000003',  # Wholesale trade
    'earnings_retail': 'CES4200000003',  # Retail trade
    'earnings_transport': 'CES4300000003',  # Transportation and warehousing
    'earnings_utilities': 'CES4422000003',  # Utilities
    'earnings_information': 'CES5000000003',  # Information
    'earnings_financial': 'CES5500000003',  # Financial activities
    'earnings_professional': 'CES6000000003',  # Professional and business services
    'earnings_education_health': 'CES6500000003',  # Private education and health services
    'earnings_leisure': 'CES7000000003',  # Leisure and hospitality
    'earnings_other': 'CES8000000003',  # Other services
    
    # === AVERAGE WEEKLY EARNINGS ===
    'weekly_earnings_private': 'CES0500000011',  # Total private
    'weekly_earnings_goods': 'CES0600000011',  # Goods-producing
    'weekly_earnings_private_service': 'CES0800000011',  # Private service-providing
    'weekly_earnings_mining': 'CES1000000011',  # Mining and logging
    'weekly_earnings_construction': 'CES2000000011',  # Construction
    'weekly_earnings_manufacturing': 'CES3000000011',  # Manufacturing
    'weekly_earnings_durable': 'CES3100000011',  # Durable goods
    'weekly_earnings_nondurable': 'CES3200000011',  # Nondurable goods
    'weekly_earnings_trade': 'CES4000000011',  # Trade, transportation, and utilities
    'weekly_earnings_wholesale': 'CES4142000011',  # Wholesale trade
    'weekly_earnings_retail': 'CES4200000011',  # Retail trade
    'weekly_earnings_transport': 'CES4300000011',  # Transportation and warehousing
    'weekly_earnings_utilities': 'CES4422000011',  # Utilities
    'weekly_earnings_information': 'CES5000000011',  # Information
    'weekly_earnings_financial': 'CES5500000011',  # Financial activities
    'weekly_earnings_professional': 'CES6000000011',  # Professional and business services
    'weekly_earnings_education_health': 'CES6500000011',  # Private education and health services
    'weekly_earnings_leisure': 'CES7000000011',  # Leisure and hospitality
    'weekly_earnings_other': 'CES8000000011',  # Other services
}

# 한국어 이름 매핑
EMPLOYMENT_KOREAN_NAMES = {
    # 고용 지표
    'nonfarm_total': '전체 비농업 고용',
    'private_total': '민간부문 고용',
    'goods_producing': '재화생산 부문',
    'service_providing': '서비스제공 부문',
    'private_service': '민간 서비스 부문',
    'government': '정부부문',
    
    # 재화생산 섹터
    'mining_logging': '광업·벌목업',
    'construction': '건설업',
    'manufacturing': '제조업',
    'durable_goods': '내구재',
    'nondurable_goods': '비내구재',
    
    # 서비스 섹터
    'trade_transport_utilities': '무역·운송·공공서비스',
    'wholesale_trade': '도매업',
    'retail_trade': '소매업',
    'transport_warehouse': '운송·창고업',
    'utilities': '공공서비스',
    'information': '정보산업',
    'financial': '금융업',
    'professional_business': '전문·비즈니스 서비스',
    'education_health': '교육·의료 서비스',
    'leisure_hospitality': '레저·숙박업',
    'other_services': '기타 서비스',
    
    # 근로시간 지표
    'hours_private': '민간부문 주당 근로시간',
    'hours_goods': '재화생산 주당 근로시간',
    'hours_private_service': '민간서비스 주당 근로시간',
    'hours_mining': '광업 주당 근로시간',
    'hours_construction': '건설업 주당 근로시간',
    'hours_manufacturing': '제조업 주당 근로시간',
    'hours_durable': '내구재 주당 근로시간',
    'hours_nondurable': '비내구재 주당 근로시간',
    'hours_trade': '무역·운송 주당 근로시간',
    'hours_wholesale': '도매업 주당 근로시간',
    'hours_retail': '소매업 주당 근로시간',
    'hours_transport': '운송업 주당 근로시간',
    'hours_utilities': '공공서비스 주당 근로시간',
    'hours_information': '정보산업 주당 근로시간',
    'hours_financial': '금융업 주당 근로시간',
    'hours_professional': '전문서비스 주당 근로시간',
    'hours_education_health': '교육·의료 주당 근로시간',
    'hours_leisure': '레저·숙박 주당 근로시간',
    'hours_other': '기타서비스 주당 근로시간',
    
    # 시간당 임금 지표
    'earnings_private': '민간부문 시급',
    'earnings_goods': '재화생산 시급',
    'earnings_private_service': '민간서비스 시급',
    'earnings_mining': '광업 시급',
    'earnings_construction': '건설업 시급',
    'earnings_manufacturing': '제조업 시급',
    'earnings_durable': '내구재 시급',
    'earnings_nondurable': '비내구재 시급',
    'earnings_trade': '무역·운송 시급',
    'earnings_wholesale': '도매업 시급',
    'earnings_retail': '소매업 시급',
    'earnings_transport': '운송업 시급',
    'earnings_utilities': '공공서비스 시급',
    'earnings_information': '정보산업 시급',
    'earnings_financial': '금융업 시급',
    'earnings_professional': '전문서비스 시급',
    'earnings_education_health': '교육·의료 시급',
    'earnings_leisure': '레저·숙박 시급',
    'earnings_other': '기타서비스 시급',
    
    # 주당 임금 지표
    'weekly_earnings_private': '민간부문 주급',
    'weekly_earnings_goods': '재화생산 주급',
    'weekly_earnings_private_service': '민간서비스 주급',
    'weekly_earnings_mining': '광업 주급',
    'weekly_earnings_construction': '건설업 주급',
    'weekly_earnings_manufacturing': '제조업 주급',
    'weekly_earnings_durable': '내구재 주급',
    'weekly_earnings_nondurable': '비내구재 주급',
    'weekly_earnings_trade': '무역·운송 주급',
    'weekly_earnings_wholesale': '도매업 주급',
    'weekly_earnings_retail': '소매업 주급',
    'weekly_earnings_transport': '운송업 주급',
    'weekly_earnings_utilities': '공공서비스 주급',
    'weekly_earnings_information': '정보산업 주급',
    'weekly_earnings_financial': '금융업 주급',
    'weekly_earnings_professional': '전문서비스 주급',
    'weekly_earnings_education_health': '교육·의료 주급',
    'weekly_earnings_leisure': '레저·숙박 주급',
    'weekly_earnings_other': '기타서비스 주급'
}

# 고용 데이터 계층 구조 (확장된 버전)
EMPLOYMENT_HIERARCHY = {
    'headline': {
        'name': '헤드라인 지표',
        'series': ['nonfarm_total', 'private_total', 'government', 'goods_producing', 'service_providing']
    },
    'goods_sectors': {
        'name': '재화생산 섹터',
        'series': ['mining_logging', 'construction', 'manufacturing', 'durable_goods', 'nondurable_goods']
    },
    'service_sectors': {
        'name': '서비스 섹터',
        'series': ['trade_transport_utilities', 'wholesale_trade', 'retail_trade', 'transport_warehouse', 
                   'utilities', 'information', 'financial', 'professional_business', 
                   'education_health', 'leisure_hospitality', 'other_services']
    },
    'wages_hourly': {
        'name': '시간당 임금',
        'series': ['earnings_private', 'earnings_goods', 'earnings_private_service', 
                   'earnings_manufacturing', 'earnings_construction', 'earnings_financial',
                   'earnings_professional', 'earnings_leisure']
    },
    'wages_weekly': {
        'name': '주당 임금',
        'series': ['weekly_earnings_private', 'weekly_earnings_goods', 'weekly_earnings_private_service',
                   'weekly_earnings_manufacturing', 'weekly_earnings_construction']
    },
    'hours': {
        'name': '근로시간',
        'series': ['hours_private', 'hours_goods', 'hours_private_service', 
                   'hours_manufacturing', 'hours_construction', 'hours_retail',
                   'hours_leisure', 'hours_professional']
    }
}

# %%
# === 전역 데이터 저장소 ===

# API 설정
BLS_SESSION = None
API_INITIALIZED = False

# 전역 데이터 저장소
EMPLOYMENT_DATA = {
    'raw_data': pd.DataFrame(),      # 원본 레벨 데이터
    'yoy_data': pd.DataFrame(),      # 전년동월대비 변화율 데이터
    'mom_data': pd.DataFrame(),      # 전월대비 변화율 데이터
    'yoy_change': pd.DataFrame(),    # 전년동월대비 변화량 (천명)
    'mom_change': pd.DataFrame(),    # 전월대비 변화량 (천명)
    'latest_values': {},             # 최신값들
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0
    }
}

# %%
# === API 초기화 함수 (CPI/PPI와 동일) ===

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
# === 데이터 로드 함수들 ===

def get_series_data(series_key, start_date='2020-01-01', end_date=None, min_points=12, is_update=False):
    """BLS API를 사용한 개별 시리즈 데이터 가져오기"""
    if not BLS_SESSION:
        print(f"❌ BLS 세션이 초기화되지 않음: {series_key}")
        return None
    
    # series_key에서 실제 BLS ID 가져오기
    series_id = EMPLOYMENT_SERIES.get(series_key, series_key)
    
    try:
        start_year = pd.to_datetime(start_date).year
        end_year = pd.to_datetime(end_date).year if end_date else None
        data = get_bls_data(series_id, start_year, end_year)
        
        if data is None or len(data) == 0:
            print(f"❌ 데이터 없음: {series_key} ({series_id})")
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
            print(f"❌ 데이터 부족: {series_key} ({len(data)}개)")
            return None
        
        print(f"✓ 성공: {series_key} ({len(data)}개 포인트)")
        return data
        
    except Exception as e:
        print(f"❌ 오류: {series_key} - {e}")
        return None

def calculate_yoy_change(data):
    """전년동월대비 변화율 계산"""
    return ((data / data.shift(12)) - 1) * 100

def calculate_mom_change(data):
    """전월대비 변화율 계산"""
    return ((data / data.shift(1)) - 1) * 100

def calculate_yoy_diff(data):
    """전년동월대비 변화량 계산 (천명 단위)"""
    # 고용 데이터는 천명 단위이므로 그대로 사용
    return data - data.shift(12)

def calculate_mom_diff(data):
    """전월대비 변화량 계산 (천명 단위)"""
    return data - data.shift(1)

# %%
# === 스마트 업데이트 함수들 ===

def check_recent_employment_data_consistency(series_list=None, check_count=3):
    """
    최근 N개 고용 데이터의 일치성을 확인하는 함수 (개선된 스마트 업데이트)
    
    Args:
        series_list: 확인할 시리즈 리스트 (None이면 주요 시리즈만)
        check_count: 확인할 최근 데이터 개수
    
    Returns:
        dict: 일치성 확인 결과 (need_update, reason, mismatches)
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        return {'need_update': True, 'reason': '기존 데이터 없음'}
    
    if series_list is None:
        # 주요 시리즈만 체크 (속도 향상)
        series_list = ['nonfarm_total', 'private_total', 'manufacturing', 'professional_business']
    
    # BLS API 초기화
    if not initialize_bls_api():
        return {'need_update': True, 'reason': 'API 초기화 실패'}
    
    print(f"📊 최근 {check_count}개 고용 데이터 일치성 확인 중...")
    
    mismatches = []
    new_data_available = False
    all_data_identical = True
    
    for series_name in series_list:
        if series_name not in EMPLOYMENT_SERIES or series_name not in EMPLOYMENT_DATA['raw_data'].columns:
            continue
        
        series_id = EMPLOYMENT_SERIES[series_name]
        existing_data = EMPLOYMENT_DATA['raw_data'][series_name].dropna()
        if len(existing_data) == 0:
            continue
        
        # 최근 데이터 가져오기
        current_year = datetime.datetime.now().year
        api_data = get_bls_data(series_id, current_year - 1, current_year)
        
        if api_data is None or len(api_data) == 0:
            mismatches.append({
                'series': series_name,
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
                'series': series_name,
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
                
                # 값이 다르면 불일치 (고용 데이터는 천명 단위이므로 0.1 이상 차이)
                if abs(existing_val - api_val) > 0.1:
                    mismatches.append({
                        'series': series_name,
                        'date': date,
                        'existing': existing_val,
                        'api': api_val,
                        'diff': abs(existing_val - api_val)
                    })
                    all_data_identical = False
            else:
                mismatches.append({
                    'series': series_name,
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
        for mismatch in value_mismatches[:3]:
            print(f"   - {mismatch}")
        return {'need_update': True, 'reason': '데이터 불일치', 'mismatches': mismatches}
    else:
        print("✅ 최근 데이터가 완전히 일치합니다. 업데이트를 건너뜁니다.")
        return {'need_update': False, 'reason': '데이터 일치', 'mismatches': []}

def update_employment_data_from_api(start_date=None, series_list=None, smart_update=True):
    """
    API를 사용하여 기존 고용 데이터 업데이트 (스마트 업데이트 지원)
    
    Args:
        start_date: 업데이트 시작 날짜 (None이면 마지막 데이터 이후부터)
        series_list: 업데이트할 시리즈 리스트 (None이면 모든 시리즈)
        smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
        bool: 업데이트 성공 여부
    """
    global EMPLOYMENT_DATA
    
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 기존 데이터가 없습니다. 전체 로드를 수행합니다.")
        return load_all_employment_data()
    
    # 스마트 업데이트 활성화시 최근 데이터 일치성 확인
    if smart_update:
        consistency_check = check_recent_employment_data_consistency()
        
        # 업데이트 필요 없으면 바로 종료
        if not consistency_check['need_update']:
            print("🎯 데이터가 최신 상태입니다. API 호출을 건너뜁니다.")
            return True
        
        # 업데이트가 필요한 경우 처리
        if consistency_check['reason'] == '데이터 불일치':
            print("⚠️ 최근 3개 데이터 불일치로 인한 전체 재로드")
            # 최근 3개월 데이터부터 다시 로드
            last_date = EMPLOYMENT_DATA['raw_data'].index[-1]
            start_date = (last_date - pd.DateOffset(months=3)).strftime('%Y-%m-01')
        elif consistency_check['reason'] == '기존 데이터 없음':
            # 전체 재로드
            return load_all_employment_data(force_reload=True)
    
    # 업데이트 대상 시리즈 결정
    if series_list is None:
        series_list = list(EMPLOYMENT_SERIES.keys())
    
    # 시작 날짜 결정
    if start_date is None:
        last_date = EMPLOYMENT_DATA['raw_data'].index[-1]
        # 마지막 데이터 날짜부터 업데이트 (중복 포함하여 안전하게)
        start_date = last_date.strftime('%Y-%m-01')
        print(f"   - 마지막 데이터 날짜: {last_date.strftime('%Y-%m')}")
        print(f"   - 업데이트 시작 날짜: {start_date}")
    
    print(f"📊 고용 데이터 업데이트 시작: {start_date}부터")
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    # 업데이트 성공 카운터
    successful_updates = 0
    failed_updates = 0
    
    # 새로운 데이터 수집
    new_data = {}
    
    for series_key in series_list:
        if series_key not in EMPLOYMENT_SERIES:
            continue
        
        try:
            print(f"\n🔄 업데이트 중: {series_key}")
            
            # 새 데이터 가져오기 (업데이트 모드)
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
        print(f"\n📈 데이터 병합 중... ({len(new_data)}개 시리즈)")
        
        # 기존 데이터와 새 데이터 병합
        existing_df = EMPLOYMENT_DATA['raw_data'].copy()
        
        # 새로운 인덱스 수집
        all_new_dates = set()
        for new_series in new_data.values():
            all_new_dates.update(new_series.index)
        
        # DataFrame 인덱스 확장
        if all_new_dates:
            new_index = existing_df.index.union(pd.Index(all_new_dates)).sort_values()
            existing_df = existing_df.reindex(new_index)
        
        # 각 시리즈별 업데이트
        for series_key, new_series in new_data.items():
            original_non_null = existing_df[series_key].notna().sum() if series_key in existing_df.columns else 0
            
            # 새 데이터로 업데이트 (기존 값 덮어쓰기)
            for date, value in new_series.items():
                existing_df.loc[date, series_key] = value
            
            # 결과 확인
            final_non_null = existing_df[series_key].notna().sum()
            new_points = final_non_null - original_non_null
            print(f"   - {series_key}: 기존 {original_non_null}개 + 신규 {len(new_series)}개 → 총 {final_non_null}개 (실제 추가: {new_points}개)")
        
        # DataFrame 정리
        existing_df = existing_df.sort_index()
        
        # 전체 데이터 재계산
        EMPLOYMENT_DATA['raw_data'] = existing_df
        EMPLOYMENT_DATA['yoy_data'] = existing_df.apply(calculate_yoy_change)
        EMPLOYMENT_DATA['mom_data'] = existing_df.apply(calculate_mom_change)
        EMPLOYMENT_DATA['yoy_change'] = existing_df.apply(calculate_yoy_diff)
        EMPLOYMENT_DATA['mom_change'] = existing_df.apply(calculate_mom_diff)
        
        # 최신값 업데이트
        EMPLOYMENT_DATA['latest_values'] = {}
        for col in existing_df.columns:
            if not existing_df[col].empty:
                latest_val = existing_df[col].dropna().iloc[-1]
                EMPLOYMENT_DATA['latest_values'][col] = latest_val
        
        # 메타 정보 업데이트
        EMPLOYMENT_DATA['load_info'].update({
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'series_count': len(existing_df.columns),
            'data_points': len(existing_df),
            'start_date': existing_df.index[0].strftime('%Y-%m-%d') if len(existing_df) > 0 else None
        })
        
        print(f"✅ 업데이트 완료: {successful_updates}개 성공, {failed_updates}개 실패")
        print(f"📊 총 데이터: {len(existing_df.columns)}개 시리즈, {len(existing_df)}개 포인트")
        
        # 업데이트된 데이터를 CSV에 저장
        save_employment_data_to_csv()
        print("💾 업데이트된 데이터를 CSV에 저장했습니다.")
        
        return True
    else:
        print("❌ 업데이트할 새로운 데이터가 없습니다.")
        return False

# %%
# === CSV 저장/로드 함수들 ===

def save_employment_data_to_csv(file_path='/home/jyp0615/us_eco/employment_data.csv'):
    """
    현재 로드된 고용 데이터를 CSV 파일로 저장
    
    Args:
        file_path: 저장할 CSV 파일 경로
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 저장할 데이터가 없습니다. 먼저 데이터를 로드하세요.")
        return False
    
    try:
        # raw_data와 메타데이터를 함께 저장
        raw_data = EMPLOYMENT_DATA['raw_data']
        
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
                'load_time': EMPLOYMENT_DATA['load_info']['load_time'].isoformat() if EMPLOYMENT_DATA['load_info']['load_time'] else None,
                'start_date': EMPLOYMENT_DATA['load_info']['start_date'],
                'series_count': EMPLOYMENT_DATA['load_info']['series_count'],
                'data_points': EMPLOYMENT_DATA['load_info']['data_points'],
                'latest_values': EMPLOYMENT_DATA['latest_values']
            }
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 고용 데이터 저장 완료: {file_path}")
        print(f"✅ 메타데이터 저장 완료: {meta_file}")
        return True
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_employment_data_from_csv(file_path='/home/jyp0615/us_eco/employment_data.csv'):
    """
    CSV 파일에서 고용 데이터 로드
    
    Args:
        file_path: 로드할 CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global EMPLOYMENT_DATA
    
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
        EMPLOYMENT_DATA['raw_data'] = df
        EMPLOYMENT_DATA['yoy_data'] = df.apply(calculate_yoy_change)
        EMPLOYMENT_DATA['mom_data'] = df.apply(calculate_mom_change)
        EMPLOYMENT_DATA['yoy_change'] = df.apply(calculate_yoy_diff)
        EMPLOYMENT_DATA['mom_change'] = df.apply(calculate_mom_diff)
        EMPLOYMENT_DATA['latest_values'] = latest_values
        EMPLOYMENT_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': df.index[0].strftime('%Y-%m-%d') if len(df) > 0 else None,
            'series_count': len(df.columns),
            'data_points': len(df)
        }
        
        print(f"✅ CSV에서 고용 데이터 로드 완료: {file_path}")
        print_load_info()
        return True
        
    except Exception as e:
        print(f"❌ CSV 로드 실패: {e}")
        return False


# === 메인 데이터 로드 함수 ===

def load_all_employment_data(start_date='2020-01-01', series_list=None, force_reload=False, csv_file='/home/jyp0615/us_eco/employment_data.csv'):
    """
    고용 데이터 로드 (CSV 우선, API 백업 방식)
    
    Args:
        start_date: 시작 날짜
        series_list: 로드할 시리즈 리스트 (None이면 모든 시리즈)
        force_reload: 강제 재로드 여부
        csv_file: CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global EMPLOYMENT_DATA
    
    # 이미 로드된 경우 스킵
    if EMPLOYMENT_DATA['load_info']['loaded'] and not force_reload:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    # CSV 파일이 있으면 우선 로드 시도
    import os
    if not force_reload and os.path.exists(csv_file):
        print("📂 기존 CSV 파일에서 데이터 로드 시도...")
        if load_employment_data_from_csv(csv_file):
            print("🔄 CSV 로드 후 스마트 업데이트 시도...")
            # CSV 로드 성공 후 스마트 업데이트
            update_employment_data_from_api(smart_update=True)
            return True
        else:
            print("⚠️ CSV 로드 실패, API에서 전체 로드 시도...")
    
    print("🚀 고용 데이터 로딩 시작... (BLS API 전용)")
    print("="*50)
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    if series_list is None:
        series_list = list(EMPLOYMENT_SERIES.keys())
    
    # 데이터 수집
    raw_data_dict = {}
    yoy_data_dict = {}
    mom_data_dict = {}
    yoy_change_dict = {}
    mom_change_dict = {}
    latest_values = {}
    
    for series_key in series_list:
        if series_key not in EMPLOYMENT_SERIES:
            print(f"⚠️ 알 수 없는 시리즈: {series_key}")
            continue
        
        # 원본 데이터 가져오기
        series_data = get_series_data(series_key, start_date)
        
        if series_data is not None and len(series_data) > 0:
            # 원본 데이터 저장
            raw_data_dict[series_key] = series_data
            
            # YoY 계산
            yoy_data = calculate_yoy_change(series_data)
            yoy_data_dict[series_key] = yoy_data
            
            # MoM 계산
            mom_data = calculate_mom_change(series_data)
            mom_data_dict[series_key] = mom_data
            
            # YoY 변화량 계산
            yoy_diff = calculate_yoy_diff(series_data)
            yoy_change_dict[series_key] = yoy_diff
            
            # MoM 변화량 계산
            mom_diff = calculate_mom_diff(series_data)
            mom_change_dict[series_key] = mom_diff
            
            # 최신 값 저장
            if len(series_data.dropna()) > 0:
                latest_val = series_data.dropna().iloc[-1]
                yoy_val = yoy_data.dropna().iloc[-1] if len(yoy_data.dropna()) > 0 else None
                mom_val = mom_data.dropna().iloc[-1] if len(mom_data.dropna()) > 0 else None
                yoy_diff_val = yoy_diff.dropna().iloc[-1] if len(yoy_diff.dropna()) > 0 else None
                mom_diff_val = mom_diff.dropna().iloc[-1] if len(mom_diff.dropna()) > 0 else None
                
                latest_values[series_key] = {
                    'level': latest_val,
                    'yoy_pct': yoy_val,
                    'mom_pct': mom_val,
                    'yoy_diff': yoy_diff_val,
                    'mom_diff': mom_diff_val
                }
            else:
                print(f"⚠️ 데이터 없음: {series_key}")
        else:
            print(f"❌ 데이터 로드 실패: {series_key}")
    
    # 로드된 데이터가 너무 적으면 오류 발생
    if len(raw_data_dict) < 5:  # 최소 5개 시리즈는 있어야 함
        error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data_dict)}개 (최소 5개 필요)"
        print(error_msg)
        raise ValueError(error_msg)
    
    # 전역 저장소에 저장
    EMPLOYMENT_DATA['raw_data'] = pd.DataFrame(raw_data_dict)
    EMPLOYMENT_DATA['yoy_data'] = pd.DataFrame(yoy_data_dict)
    EMPLOYMENT_DATA['mom_data'] = pd.DataFrame(mom_data_dict)
    EMPLOYMENT_DATA['yoy_change'] = pd.DataFrame(yoy_change_dict)
    EMPLOYMENT_DATA['mom_change'] = pd.DataFrame(mom_change_dict)
    EMPLOYMENT_DATA['latest_values'] = latest_values
    EMPLOYMENT_DATA['load_info'] = {
        'loaded': True,
        'load_time': datetime.datetime.now(),
        'start_date': start_date,
        'series_count': len(raw_data_dict),
        'data_points': len(EMPLOYMENT_DATA['raw_data']) if not EMPLOYMENT_DATA['raw_data'].empty else 0
    }
    
    print("\n✅ 데이터 로딩 완료!")
    print_load_info()
    
    # 자동으로 CSV에 저장
    save_employment_data_to_csv(csv_file)
    
    return True

def print_load_info():
    """로드 정보 출력"""
    info = EMPLOYMENT_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not EMPLOYMENT_DATA['raw_data'].empty:
        date_range = f"{EMPLOYMENT_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {EMPLOYMENT_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

def clear_data():
    """데이터 초기화"""
    global EMPLOYMENT_DATA
    EMPLOYMENT_DATA = {
        'raw_data': pd.DataFrame(),
        'yoy_data': pd.DataFrame(),
        'mom_data': pd.DataFrame(),
        'yoy_change': pd.DataFrame(),
        'mom_change': pd.DataFrame(),
        'latest_values': {},
        'load_info': {'loaded': False, 'load_time': None, 'start_date': None, 'series_count': 0, 'data_points': 0}
    }
    print("🗑️ 데이터가 초기화되었습니다")

# %%
# === 데이터 접근 함수들 ===

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_employment_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return EMPLOYMENT_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in EMPLOYMENT_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return EMPLOYMENT_DATA['raw_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """YoY 변화율 데이터 반환"""
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_employment_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return EMPLOYMENT_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in EMPLOYMENT_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return EMPLOYMENT_DATA['yoy_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화율 데이터 반환"""
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_employment_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return EMPLOYMENT_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in EMPLOYMENT_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return EMPLOYMENT_DATA['mom_data'][available_series].copy()

def get_yoy_change(series_names=None):
    """YoY 변화량 데이터 반환 (천명 단위)"""
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_employment_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return EMPLOYMENT_DATA['yoy_change'].copy()
    
    available_series = [s for s in series_names if s in EMPLOYMENT_DATA['yoy_change'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return EMPLOYMENT_DATA['yoy_change'][available_series].copy()

def get_mom_change(series_names=None):
    """MoM 변화량 데이터 반환 (천명 단위)"""
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_employment_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return EMPLOYMENT_DATA['mom_change'].copy()
    
    available_series = [s for s in series_names if s in EMPLOYMENT_DATA['mom_change'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return EMPLOYMENT_DATA['mom_change'][available_series].copy()

def get_latest_values(series_names=None):
    """최신값들 반환"""
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_employment_data()를 먼저 실행하세요.")
        return {}
    
    if series_names is None:
        return EMPLOYMENT_DATA['latest_values'].copy()
    
    return {name: EMPLOYMENT_DATA['latest_values'].get(name, {}) for name in series_names if name in EMPLOYMENT_DATA['latest_values']}

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        return []
    return list(EMPLOYMENT_DATA['raw_data'].columns)

# %%
# === IB 스타일 시각화 함수들 ===

def create_nfp_change_chart(months_back=24):
    """
    비농업 고용 월별 변화량 바 차트 (KPDS 스타일)
    
    Args:
        months_back: 표시할 과거 개월 수
        title: 차트 제목
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # NFP 월별 변화량 데이터
    mom_change = get_mom_change(['nonfarm_total'])
    
    if mom_change.empty:
        print("⚠️ NFP 데이터가 없습니다.")
        return None
    
    # 최근 N개월 데이터
    recent_data = mom_change['nonfarm_total'].tail(months_back).dropna()
    
    # 3개월 이동평균 계산
    ma3 = recent_data.rolling(window=3).mean()
    
    # DataFrame으로 준비
    df = pd.DataFrame({
        'nonfarm_change': recent_data,
        'ma3': ma3
    })
    
    # 제목 출력
    latest_date = recent_data.index[-1].strftime('%b-%y')
    title = f"미국 비농업 고용 월별 변화량 (통계: {latest_date})"
    print(title)
    
    # KPDS create_flexible_mixed_chart 함수 사용
    fig = create_flexible_mixed_chart(
        df=df,
        bar_config={
            'columns': ['nonfarm_change'],
            'labels': ['월별 변화'],
            'axis': 'left',
            'color_by_value': True,  # 양수/음수에 따라 색상 변경
            'opacity': 0.8
        },
        line_config={
            'columns': ['ma3'],
            'labels': ['3개월 이동평균'],
            'axis': 'left',
            'colors': [beige_pds],
            'line_width': 2,
            'markers': False
        },
        dual_axis=False,
        left_ytitle="천명"
    )
    
    return fig

def create_sector_employment_heatmap(sector_type='all'):
    """
    섹터별 고용 변화 히트맵 (IB 스타일)
    
    Args:
        title: 차트 제목
        sector_type: 'all', 'goods', 'service' 중 선택
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 섹터 선택
    if sector_type == 'goods':
        sectors = EMPLOYMENT_HIERARCHY['goods_sectors']['series']
    elif sector_type == 'service':
        sectors = EMPLOYMENT_HIERARCHY['service_sectors']['series']
    else:
        sectors = (EMPLOYMENT_HIERARCHY['goods_sectors']['series'] + 
                  EMPLOYMENT_HIERARCHY['service_sectors']['series'][:6])  # 주요 서비스만
    
    # 최근 12개월 MoM 변화율 데이터
    mom_data = get_mom_data(sectors)
    
    if mom_data.empty:
        print("⚠️ 섹터 데이터가 없습니다.")
        return None
    
    # 최근 12개월 데이터
    recent_data = mom_data.tail(12)
    
    # 히트맵용 데이터 준비
    heatmap_data = []
    labels = []
    
    for sector in sectors:
        if sector in recent_data.columns:
            heatmap_data.append(recent_data[sector].values)
            labels.append(EMPLOYMENT_KOREAN_NAMES.get(sector, sector))
    
    # 월 라벨
    month_labels = [d.strftime('%b-%y') for d in recent_data.index]
    
    # 히트맵 생성
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data,
        x=month_labels,
        y=labels,
        colorscale='RdBu_r',  # 빨강(양수) - 파랑(음수)
        zmid=0,
        text=[[f'{val:.1f}%' for val in row] for row in heatmap_data],
        texttemplate='%{text}',
        textfont={"size": 10},
        colorbar=dict(
            title="MoM %",
            titleside="right",
            tickmode="linear",
            tick0=-2,
            dtick=0.5
        )
    ))
    
    # 제목 출력
    title = "미국 고용 섹터별 월별 변화율 (%)"
    print(title)
    
    fig.update_layout(
        paper_bgcolor='white',
        width=686,
        height=400,
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            side='bottom',
            tickangle=0
        ),
        yaxis=dict(
            tickmode='linear',
            autorange='reversed'
        ),
        margin=dict(l=200, r=80, t=80, b=60)
    )
    
    fig.show()
    return fig

def create_goods_vs_services_chart():
    """
    재화생산 vs 서비스 섹터 비교 차트
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 재화생산 vs 서비스 데이터
    series = ['goods_producing', 'service_providing', 'private_service']
    yoy_data = get_yoy_data(series)
    
    if yoy_data.empty:
        print("⚠️ 데이터가 없습니다.")
        return None
    
    # 최근 3년 데이터
    cutoff_date = yoy_data.index[-1] - pd.DateOffset(years=3)
    recent_data = yoy_data[yoy_data.index >= cutoff_date]
    
    fig = go.Figure()
    
    # 재화생산
    if 'goods_producing' in recent_data.columns:
        data = recent_data['goods_producing'].dropna()
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            name='재화생산 부문',
            line=dict(color=get_kpds_color(0), width=2.5),
            mode='lines'
        ))
    
    # 서비스 제공
    if 'service_providing' in recent_data.columns:
        data = recent_data['service_providing'].dropna()
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            name='서비스제공 부문',
            line=dict(color=get_kpds_color(1), width=2.5),
            mode='lines'
        ))
    
    # 민간 서비스
    if 'private_service' in recent_data.columns:
        data = recent_data['private_service'].dropna()
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            name='민간 서비스',
            line=dict(color=get_kpds_color(2), width=2, dash='dot'),
            mode='lines'
        ))
    
    # 제목 출력
    title = "재화생산 vs 서비스제공 부문 고용 (전년동월대비)"
    print(title)
    
    # kpds 스타일 적용
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,
        height=400,
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            title=dict(text=None, font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside'
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white',
            tickformat='.1f',
            showgrid=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="center", x=0.5,
            font=dict(family='NanumGothic', size=FONT_SIZE_LEGEND)
        ),
        margin=dict(l=60, r=60, t=80, b=60),
        hovermode='x unified'
    )
    
    # 날짜 포맷 적용
    fig = format_date_ticks(fig, '%b-%y', "auto", recent_data.index)
    
    # Y축 단위 annotation
    fig.add_annotation(
        text="%",
        xref="paper", yref="paper",
        x=calculate_title_position("%", 'left'), y=1.1,
        showarrow=False,
        font=dict(size=FONT_SIZE_ANNOTATION, family='NanumGothic', color="black"),
        align='left'
    )
    
    # 0선 추가
    fig.add_hline(y=0, line_width=2, line_color="black")
    
    # 경제 이벤트 음영
    fig.add_vrect(x0="2020-03-01", x1="2020-05-01", 
                  fillcolor=lightbeige_pds, opacity=0.3, 
                  line_width=0)
    
    fig.show()
    return fig

def create_sector_wage_comparison():
    """
    섹터별 임금 비교 차트
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주요 섹터별 시급 데이터
    wage_series = ['earnings_manufacturing', 'earnings_construction', 'earnings_financial',
                   'earnings_professional', 'earnings_leisure', 'earnings_retail']
    
    # 원시 데이터 가져오기
    raw_data = get_raw_data(wage_series)
    if raw_data.empty:
        print("⚠️ 임금 데이터가 없습니다.")
        return None
    
    # 최근 24개월 데이터
    recent_data = raw_data.tail(24)
    
    # 사용 가능한 컬럼 확인
    available_cols = [col for col in wage_series if col in recent_data.columns]
    
    if not available_cols:
        print("⚠️ 표시할 임금 데이터가 없습니다.")
        return None
    
    # 한국어 라벨
    wage_labels = {
        'earnings_manufacturing': '제조업 시급',
        'earnings_construction': '건설업 시급',
        'earnings_financial': '금융업 시급',
        'earnings_professional': '전문서비스 시급',
        'earnings_leisure': '여가숙박 시급',
        'earnings_retail': '소매업 시급'
    }
    
    # KPDS 스타일 다중 라인 차트
    print("미국 섹터별 평균 시급 추이 (USD)")
    fig = df_multi_line_chart(
        df=recent_data,
        columns=available_cols,
        ytitle="USD",
        labels=wage_labels
    )
    
    return fig

def create_working_hours_trend():
    """
    근로시간 트렌드 차트
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주요 섹터별 근로시간
    hours_series = ['hours_private', 'hours_manufacturing', 'hours_construction', 
                    'hours_retail', 'hours_leisure']
    
    raw_data = get_raw_data(hours_series)
    
    if raw_data.empty:
        print("⚠️ 근로시간 데이터가 없습니다.")
        return None
    
    # 최근 2년 데이터
    cutoff_date = raw_data.index[-1] - pd.DateOffset(years=2)
    recent_data = raw_data[raw_data.index >= cutoff_date]
    
    # 사용 가능한 컬럼 확인
    available_cols = [col for col in hours_series if col in recent_data.columns]
    
    if not available_cols:
        print("⚠️ 표시할 근로시간 데이터가 없습니다.")
        return None
    
    # 한국어 라벨
    hours_labels = {
        'hours_private': '민간 전체',
        'hours_manufacturing': '제조업',
        'hours_construction': '건설업',
        'hours_retail': '소매업',
        'hours_leisure': '여가숙박업'
    }
    
    # KPDS 스타일 다중 라인 차트
    print("미국 섹터별 주당 평균 근로시간 추이")
    fig = df_multi_line_chart(
        df=recent_data,
        columns=available_cols,
        ytitle="시간",
        labels=hours_labels
    )
    
    return fig

def create_labor_market_tightness_dashboard():
    """
    노동시장 타이트니스 대시보드 (IB 스타일)
    - Quits rate, Job openings, Unemployment rate 등을 종합
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 4개 서브플롯 생성
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Employment-to-Population Ratio', 'Average Weekly Hours Trend',
                       'Wage Growth by Sector', 'Sector Employment Heat Index'),
        specs=[[{"type": "scatter"}, {"type": "scatter"}],
               [{"type": "bar"}, {"type": "scatter"}]],
        vertical_spacing=0.15,
        horizontal_spacing=0.12
    )
    
    # 1. Employment-to-Population Ratio 대용 (NFP 증가율)
    nfp_yoy = get_yoy_data(['nonfarm_total', 'private_total'])
    if not nfp_yoy.empty:
        cutoff_date = nfp_yoy.index[-1] - pd.DateOffset(years=3)
        recent_data = nfp_yoy[nfp_yoy.index >= cutoff_date]
        
        for series in ['nonfarm_total', 'private_total']:
            if series in recent_data.columns:
                data = recent_data[series].dropna()
                name = '전체' if series == 'nonfarm_total' else '민간'
                fig.add_trace(
                    go.Scatter(x=data.index, y=data.values, name=name,
                             line=dict(width=2)),
                    row=1, col=1
                )
    
    # 2. 평균 근로시간 트렌드
    hours_data = get_raw_data(['hours_private', 'hours_manufacturing'])
    if not hours_data.empty:
        cutoff_date = hours_data.index[-1] - pd.DateOffset(years=2)
        recent_data = hours_data[hours_data.index >= cutoff_date]
        
        for series in ['hours_private', 'hours_manufacturing']:
            if series in recent_data.columns:
                data = recent_data[series].dropna()
                name = '민간부문' if series == 'hours_private' else '제조업'
                fig.add_trace(
                    go.Scatter(x=data.index, y=data.values, name=name,
                             line=dict(width=2)),
                    row=1, col=2
                )
    
    # 3. 섹터별 임금 증가율
    wage_sectors = ['earnings_manufacturing', 'earnings_construction', 
                    'earnings_financial', 'earnings_leisure']
    latest_values = get_latest_values(wage_sectors)
    
    if latest_values:
        sectors = []
        yoy_changes = []
        colors = []
        
        for series in wage_sectors:
            if series in latest_values and latest_values[series]['yoy_pct']:
                sectors.append(EMPLOYMENT_KOREAN_NAMES.get(series, series).replace(' 시급', ''))
                yoy_val = latest_values[series]['yoy_pct']
                yoy_changes.append(yoy_val)
                colors.append('#ff4d4d' if yoy_val > 4 else '#4da6ff' if yoy_val < 2 else '#66cc66')
        
        fig.add_trace(
            go.Bar(x=sectors, y=yoy_changes, marker_color=colors,
                  text=[f'{y:.1f}%' for y in yoy_changes],
                  textposition='outside'),
            row=2, col=1
        )
    
    # 4. Employment Heat Index (고용 증가 속도)
    sectors = ['manufacturing', 'construction', 'professional_business', 'leisure_hospitality']
    mom_data = get_mom_data(sectors)
    
    if not mom_data.empty:
        # 최근 6개월 평균 계산
        recent_avg = mom_data.tail(6).mean()
        
        # Heat index 계산 (0-100 scale)
        heat_values = []
        heat_labels = []
        
        for sector in sectors:
            if sector in recent_avg.index:
                value = recent_avg[sector]
                # -1% to +1% 범위를 0-100으로 변환
                heat_index = max(0, min(100, (value + 1) * 50))
                heat_values.append(heat_index)
                heat_labels.append(EMPLOYMENT_KOREAN_NAMES.get(sector, sector))
        
        # Gauge chart 형태로 표현
        theta = np.linspace(0, 180, len(heat_values))
        r = heat_values
        
        fig.add_trace(
            go.Scatterpolar(
                r=r,
                theta=heat_labels,
                fill='toself',
                fillcolor='rgba(255, 0, 0, 0.3)',
                line=dict(color='red', width=2),
                name='Employment Heat'
            ),
            row=2, col=2
        )
    
    # 제목 출력
    title = "Labor Market Tightness Dashboard"
    print(title)
    
    # 레이아웃 업데이트
    fig.update_layout(
        title=dict(
            text=None,
            font=dict(family='Arial', size=18, color='black'),
            x=0.5,
            xanchor='center'
        ),
        showlegend=True,
        height=800,
        width=1200,
        paper_bgcolor='white',
        plot_bgcolor='white'
    )
    
    # 각 서브플롯 축 설정
    fig.update_xaxes(showgrid=False, row=1, col=1)
    fig.update_yaxes(title=None, showgrid=False, row=1, col=1)
    fig.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='black', row=1, col=1)
    
    fig.update_xaxes(showgrid=False, row=1, col=2)
    fig.update_yaxes(title=None, showgrid=False, row=1, col=2)
    
    fig.update_xaxes(tickangle=-45, row=2, col=1)
    fig.update_yaxes(title=None, showgrid=False, row=2, col=1)
    
    # Polar subplot 설정
    fig.update_layout(
        polar2=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                tickmode='linear',
                tick0=0,
                dtick=25
            ),
            angularaxis=dict(
                rotation=90,
                direction='clockwise'
            )
        )
    )
    
    fig.show()
    return fig

def create_employment_diffusion_index():
    """
    고용 확산 지수 - 얼마나 많은 섹터에서 고용이 증가하고 있는지
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 모든 섹터의 MoM 변화량 데이터
    sectors = ['manufacturing', 'construction', 'retail_trade', 'wholesale_trade',
               'transport_warehouse', 'utilities', 'information', 'financial',
               'professional_business', 'education_health', 'leisure_hospitality',
               'other_services', 'mining_logging']
    
    mom_change = get_mom_change(sectors)
    
    if mom_change.empty:
        print("⚠️ 섹터 데이터가 없습니다.")
        return None
    
    # 확산 지수 계산 (양수인 섹터의 비율)
    diffusion_index = []
    dates = []
    
    for date in mom_change.index:
        row = mom_change.loc[date]
        positive_count = (row > 0).sum()
        total_count = row.notna().sum()
        if total_count > 0:
            diffusion = (positive_count / total_count) * 100
            diffusion_index.append(diffusion)
            dates.append(date)
    
    # 3개월 이동평균
    df = pd.DataFrame({'date': dates, 'diffusion': diffusion_index})
    df.set_index('date', inplace=True)
    ma3 = df['diffusion'].rolling(window=3).mean()
    
    fig = go.Figure()
    
    # 확산 지수 막대
    fig.add_trace(go.Bar(
        x=df.index,
        y=df['diffusion'],
        name='Diffusion Index',
        marker_color=['#2ca02c' if x >= 50 else '#d62728' for x in df['diffusion']],
        opacity=0.6
    ))
    
    # 3개월 이동평균선
    fig.add_trace(go.Scatter(
        x=ma3.index,
        y=ma3.values,
        name='3-Month MA',
        line=dict(color='black', width=2.5),
        mode='lines'
    ))
    
    # 제목 출력
    title = "Employment Diffusion Index (Percentage of Industries with Positive Job Growth)"
    print(title)
    
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=500,
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color='black'),
        xaxis=dict(
            showline=True,
            linewidth=1,
            linecolor='black',
            showgrid=False
        ),
        yaxis=dict(
            showline=True,
            linewidth=1,
            linecolor='black',
            title='Diffusion Index (%)',
            showgrid=False,
            range=[0, 100]
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=60, r=30, t=100, b=60),
        hovermode='x unified'
    )
    
    # 50% 기준선
    fig.add_hline(y=50, line_width=2, line_color="red", line_dash="dash",
                  annotation_text="50% (Expansion/Contraction)", annotation_position="right")
    
    # 경제 이벤트 음영
    fig.add_vrect(x0="2020-03-01", x1="2020-05-01",
                  fillcolor="lightgrey", opacity=0.3,
                  line_width=0, annotation_text="COVID-19")
    
    fig.show()
    return fig

def create_wage_pressure_heatmap():
    """
    임금 압력 히트맵 - 섹터별 임금 상승 압력을 시각화
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주요 섹터별 임금 데이터
    wage_sectors = [
        'earnings_manufacturing', 'earnings_construction', 'earnings_retail',
        'earnings_transport', 'earnings_financial', 'earnings_professional',
        'earnings_education_health', 'earnings_leisure', 'earnings_information'
    ]
    
    # YoY 임금 증가율 데이터
    yoy_data = get_yoy_data(wage_sectors)
    
    if yoy_data.empty:
        print("⚠️ 임금 데이터가 없습니다.")
        return None
    
    # 최근 24개월 데이터
    recent_data = yoy_data.tail(24)
    
    # 히트맵 데이터 준비
    heatmap_data = []
    labels = []
    
    for sector in wage_sectors:
        if sector in recent_data.columns:
            heatmap_data.append(recent_data[sector].values)
            labels.append(EMPLOYMENT_KOREAN_NAMES.get(sector, sector).replace(' 시급', ''))
    
    # 월 라벨 (분기별로만 표시)
    month_labels = []
    for i, date in enumerate(recent_data.index):
        if i % 3 == 0:  # 3개월마다
            month_labels.append(date.strftime('%b-%y'))
        else:
            month_labels.append('')
    
    # 컬러스케일 커스텀 (임금 압력 시각화)
    colorscale = [
        [0, '#0571b0'],      # 파란색 (낮은 임금 증가)
        [0.25, '#92c5de'],   # 연한 파란색
        [0.5, '#f7f7f7'],    # 흰색 (중간)
        [0.75, '#f4a582'],   # 연한 빨간색
        [1, '#ca0020']       # 진한 빨간색 (높은 임금 증가)
    ]
    
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data,
        x=month_labels,
        y=labels,
        colorscale=colorscale,
        zmid=3,  # 3% 중심
        zmin=0,
        zmax=6,
        text=[[f'{val:.1f}%' if not np.isnan(val) else '' for val in row] for row in heatmap_data],
        texttemplate='%{text}',
        textfont={"size": 9},
        hovertemplate='%{y}<br>%{x}<br>YoY: %{z:.1f}%<extra></extra>',
        colorbar=dict(
            title="YoY %",
            titleside="right",
            tickmode="linear",
            tick0=0,
            dtick=1,
            thickness=15
        )
    ))
    
    # 제목 출력
    title = "Wage Pressure Heatmap by Sector (Year-over-Year Wage Growth %)"
    print(title)
    
    fig.update_layout(
        paper_bgcolor='white',
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color='black'),
        width=1000,
        height=500,
        xaxis=dict(
            side='bottom',
            tickangle=0,
            tickmode='array',
            tickvals=list(range(len(month_labels))),
            ticktext=month_labels
        ),
        yaxis=dict(
            tickmode='linear',
            autorange='reversed'
        ),
        margin=dict(l=150, r=50, t=100, b=60)
    )
    
    # 3% 및 5% 임계값 표시를 위한 주석
    fig.add_annotation(
        text="← Low Pressure | High Pressure →",
        xref="paper", yref="paper",
        x=0.5, y=-0.15,
        showarrow=False,
        font=dict(size=12, color="grey")
    )
    
    fig.show()
    return fig

def create_employment_leading_indicators():
    """
    고용 선행지표 대시보드
    - 제조업 근로시간, 임시직 고용, 초기 실업수당 청구 등
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 2x2 서브플롯
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Manufacturing Weekly Hours (Leading)',
                       'Temporary Help Services Employment',
                       'Manufacturing vs Services Employment',
                       'Wage Growth Momentum'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": True}, {"secondary_y": False}]],
        vertical_spacing=0.15,
        horizontal_spacing=0.12
    )
    
    # 1. 제조업 주당 근로시간 (선행지표)
    hours_mfg = get_raw_data(['hours_manufacturing'])
    if not hours_mfg.empty:
        cutoff_date = hours_mfg.index[-1] - pd.DateOffset(years=3)
        recent_data = hours_mfg[hours_mfg.index >= cutoff_date]
        
        if 'hours_manufacturing' in recent_data.columns:
            data = recent_data['hours_manufacturing'].dropna()
            ma3 = data.rolling(window=3).mean()
            
            fig.add_trace(
                go.Scatter(x=data.index, y=data.values, name='Actual',
                         line=dict(color='lightgrey', width=1)),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=ma3.index, y=ma3.values, name='3M MA',
                         line=dict(color='blue', width=2)),
                row=1, col=1
            )
    
    # 2. 전문·비즈니스 서비스 고용 (임시직 포함)
    prof_employment = get_yoy_data(['professional_business'])
    if not prof_employment.empty:
        cutoff_date = prof_employment.index[-1] - pd.DateOffset(years=3)
        recent_data = prof_employment[prof_employment.index >= cutoff_date]
        
        if 'professional_business' in recent_data.columns:
            data = recent_data['professional_business'].dropna()
            fig.add_trace(
                go.Scatter(x=data.index, y=data.values, 
                         name='Prof. & Business Services YoY%',
                         line=dict(color='green', width=2)),
                row=1, col=2
            )
            fig.add_hline(y=0, line_width=1, line_color="black", row=1, col=2)
    
    # 3. 제조업 vs 서비스업 고용 (동시 비교)
    comparison_data = get_yoy_data(['manufacturing', 'private_service'])
    if not comparison_data.empty:
        cutoff_date = comparison_data.index[-1] - pd.DateOffset(years=2)
        recent_data = comparison_data[comparison_data.index >= cutoff_date]
        
        if 'manufacturing' in recent_data.columns:
            data = recent_data['manufacturing'].dropna()
            fig.add_trace(
                go.Scatter(x=data.index, y=data.values, name='Manufacturing',
                         line=dict(color='orange', width=2)),
                row=2, col=1, secondary_y=False
            )
        
        if 'private_service' in recent_data.columns:
            data = recent_data['private_service'].dropna()
            fig.add_trace(
                go.Scatter(x=data.index, y=data.values, name='Private Services',
                         line=dict(color='purple', width=2, dash='dot')),
                row=2, col=1, secondary_y=True
            )
    
    # 4. 임금 증가 모멘텀
    wage_data = get_mom_data(['earnings_private'])
    if not wage_data.empty and 'earnings_private' in wage_data.columns:
        recent_data = wage_data['earnings_private'].tail(24).dropna()
        
        # 3개월 및 6개월 이동평균
        ma3 = recent_data.rolling(window=3).mean()
        ma6 = recent_data.rolling(window=6).mean()
        
        fig.add_trace(
            go.Scatter(x=ma3.index, y=ma3.values, name='3M MA',
                     line=dict(color='red', width=2)),
            row=2, col=2
        )
        fig.add_trace(
            go.Scatter(x=ma6.index, y=ma6.values, name='6M MA',
                     line=dict(color='darkred', width=2, dash='dash')),
            row=2, col=2
        )
    
    # 레이아웃 업데이트
    fig.update_layout(
        title=dict(
            text="Employment Leading Indicators Dashboard",
            font=dict(family='Arial', size=18, color='black'),
            x=0.5,
            xanchor='center'
        ),
        showlegend=True,
        height=800,
        width=1200,
        paper_bgcolor='white',
        plot_bgcolor='white'
    )
    
    # 축 레이블
    fig.update_yaxes(title="Hours/Week", row=1, col=1)
    fig.update_yaxes(title="YoY %", row=1, col=2)
    fig.update_yaxes(title="Manufacturing YoY %", row=2, col=1, secondary_y=False)
    fig.update_yaxes(title="Services YoY %", row=2, col=1, secondary_y=True)
    fig.update_yaxes(title="MoM % (MA)", row=2, col=2)
    
    # 격자선
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    
    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", row=2, col=1)
    fig.add_hline(y=0, line_width=1, line_color="black", row=2, col=2)
    
    fig.show()
    return fig

def create_sector_rotation_analysis():
    """
    섹터 로테이션 분석 - 경기 사이클에 따른 섹터별 고용 변화
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 경기 민감 섹터들
    cyclical_sectors = ['construction', 'manufacturing', 'retail_trade', 'leisure_hospitality']
    defensive_sectors = ['education_health', 'utilities', 'government']
    
    # YoY 데이터 가져오기
    all_sectors = cyclical_sectors + defensive_sectors
    yoy_data = get_yoy_data(all_sectors)
    
    if yoy_data.empty:
        print("⚠️ 섹터 데이터가 없습니다.")
        return None
    
    # 최근 5년 데이터
    cutoff_date = yoy_data.index[-1] - pd.DateOffset(years=5)
    recent_data = yoy_data[yoy_data.index >= cutoff_date]
    
    # 2개 서브플롯: 경기민감 vs 방어적
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Cyclical Sectors Employment Growth',
                       'Defensive Sectors Employment Growth'),
        shared_xaxes=True,
        vertical_spacing=0.1
    )
    
    # 경기민감 섹터
    for i, sector in enumerate(cyclical_sectors):
        if sector in recent_data.columns:
            data = recent_data[sector].dropna()
            fig.add_trace(
                go.Scatter(x=data.index, y=data.values,
                         name=EMPLOYMENT_KOREAN_NAMES.get(sector, sector),
                         line=dict(width=2)),
                row=1, col=1
            )
    
    # 방어적 섹터
    for i, sector in enumerate(defensive_sectors):
        if sector in recent_data.columns:
            data = recent_data[sector].dropna()
            fig.add_trace(
                go.Scatter(x=data.index, y=data.values,
                         name=EMPLOYMENT_KOREAN_NAMES.get(sector, sector),
                         line=dict(width=2, dash='dash')),
                row=2, col=1
            )
    
    # 제목 출력
    title = "Sector Rotation Analysis (Cyclical vs Defensive Sectors Employment Growth)"
    print(title)
    
    fig.update_layout(
        paper_bgcolor='white',
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color='black'),
        plot_bgcolor='white',
        width=1000,
        height=700,
        hovermode='x unified',
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=1.02
        ),
        margin=dict(l=60, r=150, t=100, b=60)
    )
    
    # Y축 설정
    fig.update_yaxes(title="YoY %", showgrid=True, gridcolor='lightgrey', row=1, col=1)
    fig.update_yaxes(title="YoY %", showgrid=False, row=2, col=1)
    
    # 0선 추가
    fig.add_hline(y=0, line_width=2, line_color="black", row=1, col=1)
    fig.add_hline(y=0, line_width=2, line_color="black", row=2, col=1)
    
    # 경기 침체기 음영 표시
    fig.add_vrect(x0="2020-03-01", x1="2020-05-01",
                  fillcolor="lightgrey", opacity=0.3,
                  line_width=0, annotation_text="COVID-19")
    
    # 경기 사이클 주석 추가
    fig.add_annotation(
        text="← Early Cycle | Late Cycle →",
        xref="paper", yref="paper",
        x=0.5, y=-0.05,
        showarrow=False,
        font=dict(size=12, color="grey")
    )
    
    fig.show()
    return fig

def create_wage_growth_chart():
    """
    평균 시급 증가율 차트 (인플레이션 지표)
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 평균 시급 YoY 데이터
    yoy_data = get_yoy_data(['avg_hourly_earnings', 'avg_hourly_earnings_prod'])
    
    if yoy_data.empty:
        print("⚠️ 임금 데이터가 없습니다.")
        return None
    
    # 데이터 준비
    fig = go.Figure()
    
    # 전체 평균 시급
    if 'avg_hourly_earnings' in yoy_data.columns:
        data = yoy_data['avg_hourly_earnings'].dropna()
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            name='Average Hourly Earnings (All)',
            line=dict(color='#1f77b4', width=2.5),
            mode='lines'
        ))
    
    # 생산직 평균 시급
    if 'avg_hourly_earnings_prod' in yoy_data.columns:
        data = yoy_data['avg_hourly_earnings_prod'].dropna()
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            name='Average Hourly Earnings (Production)',
            line=dict(color='#ff7f0e', width=2, dash='dot'),
            mode='lines'
        ))
    
    # 제목 출력
    latest_date = yoy_data.index[-1].strftime('%b-%y')
    title = f"US Average Hourly Earnings Growth (Year-over-Year % Change, through {latest_date})"
    print(title)
    
    fig.update_layout(
        paper_bgcolor='white',
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color='black'),
        plot_bgcolor='white',
        width=900,
        height=500,
        xaxis=dict(
            showline=True,
            linewidth=1,
            linecolor='black',
            showgrid=False,
            tickformat='%b-%y'
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white',
            showgrid=False,
            tickformat='.1f',
            zeroline=True,
            zerolinewidth=1,
            zerolinecolor='black'
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=60, r=30, t=100, b=60)
    )
    
    # Y축 단위 annotation (KPDS 스타일)
    fig.add_annotation(
        text="%",
        xref="paper", yref="paper",
        x=calculate_title_position("%", 'left'), y=1.1,
        showarrow=False,
        font=dict(size=FONT_SIZE_ANNOTATION, family='NanumGothic', color="black"),
        align='left'
    )
    
    # 주요 수준선 추가
    fig.add_hline(y=2, line_width=1, line_color="red", line_dash="dash", 
                  annotation_text="2% (Fed Target)", annotation_position="left")
    fig.add_hline(y=4, line_width=1, line_color="orange", line_dash="dash", 
                  annotation_text="4%", annotation_position="left")
    
    # 음영 구간 (경기 침체기 등을 표시할 수 있음)
    # 예시: 2020년 COVID-19 기간
    fig.add_vrect(x0="2020-03-01", x1="2020-05-01", 
                  fillcolor="lightgrey", opacity=0.3, 
                  line_width=0, annotation_text="COVID-19")
    
    fig.show()
    return fig

def create_employment_summary_table():
    """
    고용 지표 요약 테이블 (IB 스타일)
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 최신 데이터 가져오기
    latest_values = get_latest_values()
    
    if not latest_values:
        print("⚠️ 최신 데이터가 없습니다.")
        return None
    
    # 테이블 데이터 준비
    table_data = []
    
    # 헤드라인 지표들
    headline_series = ['nonfarm_total', 'private_total', 'government']
    for series in headline_series:
        if series in latest_values:
            data = latest_values[series]
            row = [
                EMPLOYMENT_KOREAN_NAMES.get(series, series),
                f"{data['level']:,.0f}" if data['level'] else "N/A",
                f"{data['mom_diff']:+.0f}" if data['mom_diff'] else "N/A",
                f"{data['mom_pct']:+.1f}%" if data['mom_pct'] else "N/A",
                f"{data['yoy_diff']:+.0f}" if data['yoy_diff'] else "N/A",
                f"{data['yoy_pct']:+.1f}%" if data['yoy_pct'] else "N/A"
            ]
            table_data.append(row)
    
    # 주요 섹터들
    major_sectors = ['manufacturing', 'construction', 'professional_business', 'leisure_hospitality']
    for series in major_sectors:
        if series in latest_values:
            data = latest_values[series]
            row = [
                EMPLOYMENT_KOREAN_NAMES.get(series, series),
                f"{data['level']:,.0f}" if data['level'] else "N/A",
                f"{data['mom_diff']:+.0f}" if data['mom_diff'] else "N/A",
                f"{data['mom_pct']:+.1f}%" if data['mom_pct'] else "N/A",
                f"{data['yoy_diff']:+.0f}" if data['yoy_diff'] else "N/A",
                f"{data['yoy_pct']:+.1f}%" if data['yoy_pct'] else "N/A"
            ]
            table_data.append(row)
    
    # 임금 지표
    if 'avg_hourly_earnings' in latest_values:
        data = latest_values['avg_hourly_earnings']
        row = [
            "평균 시급 ($)",
            f"${data['level']:.2f}" if data['level'] else "N/A",
            f"${data['mom_diff']:+.2f}" if data['mom_diff'] else "N/A",
            f"{data['mom_pct']:+.1f}%" if data['mom_pct'] else "N/A",
            f"${data['yoy_diff']:+.2f}" if data['yoy_diff'] else "N/A",
            f"{data['yoy_pct']:+.1f}%" if data['yoy_pct'] else "N/A"
        ]
        table_data.append(row)
    
    # 테이블 생성
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=['<b>지표</b>', '<b>수준</b>', '<b>MoM 변화</b>', '<b>MoM %</b>', '<b>YoY 변화</b>', '<b>YoY %</b>'],
            fill_color='#1f77b4',
            font=dict(color='white', size=12),
            align='left',
            height=30
        ),
        cells=dict(
            values=list(zip(*table_data)),
            fill_color=[['lightgrey' if i % 2 == 0 else 'white' for i in range(len(table_data))]],
            align=['left', 'right', 'right', 'right', 'right', 'right'],
            font=dict(size=11),
            height=25
        )
    )])
    
    # 레이아웃
    latest_date = EMPLOYMENT_DATA['raw_data'].index[-1].strftime('%B %Y')
    fig.update_layout(
        title=dict(
            text=f"US Employment Summary - {latest_date}",
            font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color='black')
        ),
        paper_bgcolor='white',
        width=900,
        height=400,
        margin=dict(l=30, r=30, t=60, b=30)
    )
    
    fig.show()
    return fig

def create_employment_momentum_chart():
    """
    고용 모멘텀 차트 (3/6/12개월 평균)
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # NFP 변화량 데이터
    mom_change = get_mom_change(['nonfarm_total'])
    
    if mom_change.empty:
        print("⚠️ NFP 데이터가 없습니다.")
        return None
    
    nfp_change = mom_change['nonfarm_total'].dropna()
    
    # 이동평균 계산
    ma3 = nfp_change.rolling(window=3).mean()
    ma6 = nfp_change.rolling(window=6).mean()
    ma12 = nfp_change.rolling(window=12).mean()
    
    # 최근 3년 데이터만
    cutoff_date = nfp_change.index[-1] - pd.DateOffset(years=3)
    
    fig = go.Figure()
    
    # 12개월 이동평균
    data = ma12[ma12.index >= cutoff_date]
    fig.add_trace(go.Scatter(
        x=data.index,
        y=data.values,
        name='12-Month MA',
        line=dict(color='#2ca02c', width=3),
        mode='lines'
    ))
    
    # 6개월 이동평균
    data = ma6[ma6.index >= cutoff_date]
    fig.add_trace(go.Scatter(
        x=data.index,
        y=data.values,
        name='6-Month MA',
        line=dict(color='#ff7f0e', width=2.5),
        mode='lines'
    ))
    
    # 3개월 이동평균
    data = ma3[ma3.index >= cutoff_date]
    fig.add_trace(go.Scatter(
        x=data.index,
        y=data.values,
        name='3-Month MA',
        line=dict(color='#1f77b4', width=2),
        mode='lines'
    ))
    
    # 제목 출력
    title = "US Employment Momentum (Nonfarm Payrolls Moving Averages - thousands)"
    print(title)
    
    fig.update_layout(
        paper_bgcolor='white',
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color='black'),
        plot_bgcolor='white',
        width=900,
        height=500,
        xaxis=dict(
            showline=True,
            linewidth=1,
            linecolor='black',
            showgrid=False,
            tickformat='%b-%y'
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white',
            showgrid=False,
            zeroline=True,
            zerolinewidth=2,
            zerolinecolor='black'
        ),
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor='rgba(255,255,255,0.8)',
            bordercolor='lightgrey',
            borderwidth=1
        ),
        margin=dict(l=60, r=30, t=80, b=60)
    )
    
    # Y축 단위 annotation (KPDS 스타일)
    fig.add_annotation(
        text="천명",
        xref="paper", yref="paper",
        x=calculate_title_position("천명", 'left'), y=1.1,
        showarrow=False,
        font=dict(size=FONT_SIZE_ANNOTATION, family='NanumGothic', color="black"),
        align='left'
    )
    
    # 주요 수준선
    fig.add_hline(y=0, line_width=2, line_color="black")
    fig.add_hline(y=100, line_width=1, line_color="green", line_dash="dash", 
                  annotation_text="100k", annotation_position="right")
    fig.add_hline(y=200, line_width=1, line_color="blue", line_dash="dash", 
                  annotation_text="200k", annotation_position="right")
    
    fig.show()
    return fig

def create_sector_contribution_chart(months_back=12):
    """
    섹터별 고용 기여도 차트
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주요 섹터들
    sectors = ['manufacturing', 'construction', 'professional_business', 
               'education_health', 'leisure_hospitality', 'government']
    
    # MoM 변화량 데이터
    mom_change = get_mom_change(sectors + ['nonfarm_total'])
    
    if mom_change.empty:
        print("⚠️ 섹터 데이터가 없습니다.")
        return None
    
    # 최근 N개월 데이터
    recent_data = mom_change.tail(months_back)
    
    fig = go.Figure()
    
    # 각 섹터별로 막대 추가
    for i, sector in enumerate(sectors):
        if sector in recent_data.columns:
            values = recent_data[sector].values
            fig.add_trace(go.Bar(
                name=EMPLOYMENT_KOREAN_NAMES.get(sector, sector),
                x=recent_data.index,
                y=values,
                marker_color=get_kpds_color(i)
            ))
    
    # 전체 NFP 변화 라인 추가
    if 'nonfarm_total' in recent_data.columns:
        fig.add_trace(go.Scatter(
            x=recent_data.index,
            y=recent_data['nonfarm_total'].values,
            name='Total NFP',
            line=dict(color='black', width=3),
            mode='lines+markers'
        ))
    
    # 제목 출력
    title = "Sector Contributions to US Employment Change (Monthly change in thousands)"
    print(title)
    
    fig.update_layout(
            font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color='black'),
        barmode='stack',
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=500,
        xaxis=dict(
            showline=True,
            linewidth=1,
            linecolor='black',
            tickformat='%b-%y',
            showgrid=False
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white',
            showgrid=False,
            zeroline=True,
            zerolinewidth=2,
            zerolinecolor='black'
        ),
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=1.02,
            font=dict(size=10)
        ),
        margin=dict(l=60, r=150, t=80, b=60)
    )
    
    # Y축 단위 annotation (KPDS 스타일)
    fig.add_annotation(
        text="천명",
        xref="paper", yref="paper",
        x=calculate_title_position("천명", 'left'), y=1.1,
        showarrow=False,
        font=dict(size=FONT_SIZE_ANNOTATION, family='NanumGothic', color="black"),
        align='left'
    )
    
    fig.show()
    return fig

# %%
# === 통합 분석 함수 ===

def run_complete_employment_analysis(start_date='2020-01-01', force_reload=False):
    """
    완전한 고용 분석 실행 - 데이터 로드 후 모든 차트 생성
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🚀 완전한 고용 보고서 분석 시작 (확장 버전)")
    print("="*50)
    
    # 1. 데이터 로드
    print("\n1️⃣ 데이터 로딩")
    success = load_all_employment_data(start_date=start_date, force_reload=force_reload)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 시각화 생성
    print("\n2️⃣ 시각화 생성")
    
    results = {}
    
    try:
        # NFP 변화량 차트
        print("   📊 NFP 월별 변화량 차트...")
        results['nfp_change'] = create_nfp_change_chart()
        
        # 재화 vs 서비스 비교
        print("   ⚖️ 재화생산 vs 서비스 부문 비교...")
        results['goods_vs_services'] = create_goods_vs_services_chart()
        
        # 섹터별 히트맵 (전체)
        print("   🗺️ 섹터별 고용 히트맵 (전체)...")
        results['sector_heatmap_all'] = create_sector_employment_heatmap(sector_type='all')
        
        # 섹터별 히트맵 (재화생산)
        print("   🏭 재화생산 섹터 히트맵...")
        results['sector_heatmap_goods'] = create_sector_employment_heatmap(
            title="Goods-Producing Sectors Employment Change", 
            sector_type='goods'
        )
        
        # 섹터별 히트맵 (서비스)
        print("   🏢 서비스 섹터 히트맵...")
        results['sector_heatmap_service'] = create_sector_employment_heatmap(
            title="Service Sectors Employment Change",
            sector_type='service'
        )
        
        # 임금 증가율 차트
        print("   💰 평균 시급 증가율 차트...")
        results['wage_growth'] = create_wage_growth_chart()
        
        # 섹터별 임금 비교
        print("   💵 섹터별 임금 비교...")
        results['sector_wages'] = create_sector_wage_comparison()
        
        # 근로시간 트렌드
        print("   ⏰ 근로시간 트렌드...")
        results['hours_trend'] = create_working_hours_trend()
        
        # 고용 요약 테이블
        print("   📋 고용 지표 요약 테이블...")
        results['summary_table'] = create_employment_summary_table()
        
        # 고용 모멘텀 차트
        print("   📈 고용 모멘텀 차트...")
        results['momentum'] = create_employment_momentum_chart()
        
        # 섹터별 기여도
        print("   🏗️ 섹터별 기여도 차트...")
        results['sector_contribution'] = create_sector_contribution_chart()
        
    except Exception as e:
        print(f"⚠️ 시각화 생성 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ 분석 완료! 생성된 차트: {len(results)}개")
    return results

def run_sector_analysis(sector_focus='all'):
    """
    섹터별 심층 분석
    
    Args:
        sector_focus: 'goods', 'service', 'all' 중 선택
    
    Returns:
        dict: 생성된 차트들
    """
    print(f"🔍 섹터별 심층 분석 시작 (포커스: {sector_focus})")
    print("="*50)
    
    # 데이터 로드 확인
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("\n1️⃣ 데이터 로딩")
        success = load_all_employment_data()
        if not success:
            print("❌ 데이터 로딩 실패")
            return None
    else:
        print("✅ 데이터가 이미 로드됨")
    
    results = {}
    
    try:
        if sector_focus in ['goods', 'all']:
            print("\n📊 재화생산 섹터 분석...")
            results['goods_heatmap'] = create_sector_employment_heatmap(
                title="Goods-Producing Sectors - Monthly Changes",
                sector_type='goods'
            )
            
            # 제조업 세부 분석
            if 'durable_goods' in EMPLOYMENT_DATA['raw_data'].columns:
                print("   🔧 내구재 vs 비내구재 분석...")
                durables_data = get_yoy_data(['durable_goods', 'nondurable_goods'])
                if not durables_data.empty:
                    fig = go.Figure()
                    
                    if 'durable_goods' in durables_data.columns:
                        data = durables_data['durable_goods'].dropna()
                        fig.add_trace(go.Scatter(
                            x=data.index,
                            y=data.values,
                            name='내구재',
                            line=dict(color='#1f77b4', width=2.5),
                            mode='lines'
                        ))
                    
                    if 'nondurable_goods' in durables_data.columns:
                        data = durables_data['nondurable_goods'].dropna()
                        fig.add_trace(go.Scatter(
                            x=data.index,
                            y=data.values,
                            name='비내구재',
                            line=dict(color='#ff7f0e', width=2.5),
                            mode='lines'
                        ))
                    
                    fig.update_layout(
                        title="Durable vs Nondurable Goods Employment<br><sub>YoY % Change</sub>",
                        xaxis_title="",
                        yaxis_title="YoY % Change",
                        hovermode='x unified',
                        width=900,
                        height=500
                    )
                    fig.add_hline(y=0, line_width=1, line_color="black")
                    
                    results['durables_analysis'] = fig
        
        if sector_focus in ['service', 'all']:
            print("\n🏢 서비스 섹터 분석...")
            results['service_heatmap'] = create_sector_employment_heatmap(
                title="Service Sectors - Monthly Changes",
                sector_type='service'
            )
        
        # 임금 및 근로시간 통합 분석
        print("\n💼 임금 및 근로시간 통합 분석...")
        results['wage_comparison'] = create_sector_wage_comparison()
        results['hours_analysis'] = create_working_hours_trend()
        
    except Exception as e:
        print(f"⚠️ 분석 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ 섹터 분석 완료! 생성된 차트: {len(results)}개")
    return results

# %%
# === 유틸리티 함수들 ===

def show_available_components():
    """사용 가능한 고용 구성요소 표시"""
    print("=== 사용 가능한 고용 보고서 구성요소 (확장 버전) ===\n")
    
    print("📊 고용 지표 (천명 단위):")
    employment_keys = [k for k in EMPLOYMENT_SERIES.keys() if not any(prefix in k for prefix in ['hours_', 'earnings_', 'weekly_earnings_'])]
    for key in employment_keys:
        bls_id = EMPLOYMENT_SERIES[key]
        korean_name = EMPLOYMENT_KOREAN_NAMES.get(key, key)
        print(f"  '{key}': {korean_name} ({bls_id})")
    
    print("\n⏰ 근로시간 지표:")
    hours_keys = [k for k in EMPLOYMENT_SERIES.keys() if k.startswith('hours_')]
    for key in hours_keys:
        bls_id = EMPLOYMENT_SERIES[key]
        korean_name = EMPLOYMENT_KOREAN_NAMES.get(key, key)
        print(f"  '{key}': {korean_name} ({bls_id})")
    
    print("\n💵 시간당 임금 지표:")
    earnings_keys = [k for k in EMPLOYMENT_SERIES.keys() if k.startswith('earnings_') and not k.startswith('weekly_earnings_')]
    for key in earnings_keys:
        bls_id = EMPLOYMENT_SERIES[key]
        korean_name = EMPLOYMENT_KOREAN_NAMES.get(key, key)
        print(f"  '{key}': {korean_name} ({bls_id})")
    
    print("\n💰 주당 임금 지표:")
    weekly_keys = [k for k in EMPLOYMENT_SERIES.keys() if k.startswith('weekly_earnings_')]
    for key in weekly_keys:
        bls_id = EMPLOYMENT_SERIES[key]
        korean_name = EMPLOYMENT_KOREAN_NAMES.get(key, key)
        print(f"  '{key}': {korean_name} ({bls_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    return {
        'loaded': EMPLOYMENT_DATA['load_info']['loaded'],
        'series_count': EMPLOYMENT_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': EMPLOYMENT_DATA['load_info']
    }

def show_employment_hierarchy():
    """고용 데이터 계층 구조 표시"""
    print("=== 고용 데이터 계층 구조 (확장 버전) ===\n")
    
    for category, info in EMPLOYMENT_HIERARCHY.items():
        print(f"### {info['name']} ###")
        for series_key in info['series']:
            if series_key in EMPLOYMENT_KOREAN_NAMES:
                print(f"  - {series_key}: {EMPLOYMENT_KOREAN_NAMES[series_key]}")
        print()

def print_data_summary():
    """데이터 요약 정보 출력"""
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print("📊 로드된 데이터 요약")
    print("="*50)
    
    # 최신 데이터 날짜
    latest_date = EMPLOYMENT_DATA['raw_data'].index[-1]
    print(f"최신 데이터: {latest_date.strftime('%Y년 %m월')}")
    
    # 주요 지표 최신값
    latest_values = get_latest_values(['nonfarm_total', 'private_total', 'earnings_private', 'hours_private'])
    
    if 'nonfarm_total' in latest_values:
        nfp = latest_values['nonfarm_total']
        print(f"\n🔹 비농업 고용 (NFP)")
        print(f"   수준: {nfp['level']:,.0f}천명")
        print(f"   MoM: {nfp['mom_diff']:+,.0f}천명 ({nfp['mom_pct']:+.1f}%)")
        print(f"   YoY: {nfp['yoy_diff']:+,.0f}천명 ({nfp['yoy_pct']:+.1f}%)")
    
    if 'earnings_private' in latest_values:
        wage = latest_values['earnings_private']
        print(f"\n🔹 민간부문 평균 시급")
        print(f"   수준: ${wage['level']:.2f}")
        print(f"   MoM: ${wage['mom_diff']:+.2f} ({wage['mom_pct']:+.1f}%)")
        print(f"   YoY: ${wage['yoy_diff']:+.2f} ({wage['yoy_pct']:+.1f}%)")
    
    if 'hours_private' in latest_values:
        hours = latest_values['hours_private']
        print(f"\n🔹 민간부문 주당 근로시간")
        print(f"   수준: {hours['level']:.1f}시간")
        print(f"   MoM: {hours['mom_diff']:+.1f}시간 ({hours['mom_pct']:+.1f}%)")
        print(f"   YoY: {hours['yoy_diff']:+.1f}시간 ({hours['yoy_pct']:+.1f}%)")

# %%
# === 사용 예시 ===

print("\n=== 미국 고용 보고서 분석 도구 사용법 (확장 버전) ===")
print("\n1. 데이터 로드 (CSV 우선, API 백업):")
print("   load_all_employment_data()  # CSV가 있으면 로드 후 업데이트")
print("   load_employment_data_from_csv()  # CSV에서만 로드")
print("   update_employment_data_from_api()  # 최신 데이터만 API로 업데이트")
print()
print("2. 데이터 저장:")
print("   save_employment_data_to_csv()  # 현재 데이터를 CSV로 저장")
print()
print("3. IB 스타일 차트 생성:")
print("   create_nfp_change_chart()  # NFP 월별 변화량")
print("   create_goods_vs_services_chart()  # 재화 vs 서비스 비교")
print("   create_sector_employment_heatmap(sector_type='all')  # 섹터별 히트맵")
print("   create_wage_growth_chart()  # 임금 증가율")
print("   create_sector_wage_comparison()  # 섹터별 임금 비교")
print("   create_working_hours_trend()  # 근로시간 트렌드")
print("   create_employment_summary_table()  # 요약 테이블")
print("   create_employment_momentum_chart()  # 모멘텀 차트")
print("   create_sector_contribution_chart()  # 섹터별 기여도")
print()
print("4. 통합 분석:")
print("   run_complete_employment_analysis()  # 전체 분석")
print("   run_sector_analysis('goods')  # 재화생산 섹터 분석")
print("   run_sector_analysis('service')  # 서비스 섹터 분석")
print()
print("5. 데이터 상태 확인:")
print("   get_data_status()  # 로드 상태")
print("   show_available_components()  # 사용 가능한 시리즈")
print("   show_employment_hierarchy()  # 계층 구조")
print("   print_data_summary()  # 데이터 요약")

# %%
# 사용 가능한 구성요소 표시
show_available_components()

# %%
# 고용 데이터 계층 구조 표시
show_employment_hierarchy()

# %%
# === KPDS 스타일 시각화 함수들 ===

def kpds_nfp_change_chart(months_back=24):
    """
    비농업 고용 월별 변화량 차트 (KPDS 스타일)
    
    Args:
        months_back: 표시할 과거 개월 수
        title: 차트 제목
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # NFP 월별 변화량 데이터
    mom_change = get_mom_change(['nonfarm_total'])
    
    if mom_change.empty:
        print("⚠️ NFP 데이터가 없습니다.")
        return None
    
    # 최근 N개월 데이터
    recent_data = mom_change['nonfarm_total'].tail(months_back).dropna()
    
    # 3개월 이동평균 계산
    ma3 = recent_data.rolling(window=3).mean()
    
    # DataFrame으로 준비
    df = pd.DataFrame({
        'nonfarm_change': recent_data,
        'ma3': ma3
    })
    
    # 제목 출력
    latest_date = recent_data.index[-1].strftime('%b-%y')
    title = f"미국 비농업 고용 월별 변화량 ({latest_date})"
    print(title)
    
    # KPDS create_flexible_mixed_chart 함수 사용
    fig = create_flexible_mixed_chart(
        df=df,
        bar_config={
            'columns': ['nonfarm_change'],
            'labels': ['월별 변화'],
            'axis': 'left',
            'color_by_value': True,  # 양수/음수에 따라 색상 변경
            'opacity': 0.8
        },
        line_config={
            'columns': ['ma3'],
            'labels': ['3개월 이동평균'],
            'axis': 'left',
            'colors': [beige_pds],
            'line_width': 2,
            'markers': False
        },
        dual_axis=False,
        left_ytitle="천명"
    )
    
    return fig

def kpds_goods_vs_services_chart():
    """
    재화생산 vs 서비스제공 부문 고용 차트 (KPDS 스타일)
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # YoY 데이터
    yoy_data = get_yoy_data(['goods_producing', 'service_providing', 'private_service'])
    
    if yoy_data.empty:
        print("⚠️ 데이터가 없습니다.")
        return None
    
    # 최근 3년 데이터
    cutoff_date = yoy_data.index[-1] - pd.DateOffset(years=3)
    recent_data = yoy_data[yoy_data.index >= cutoff_date]
    
    # 제목 출력
    title = "재화생산 vs 서비스제공 부문 고용 (전년동월대비)"
    print(title)
    
    # 라벨 설정
    labels = {
        'goods_producing': '재화생산 부문',
        'service_providing': '서비스제공 부문',
        'private_service': '민간 서비스'
    }
    
    # KPDS df_multi_line_chart 함수 사용
    fig = df_multi_line_chart(
        df=recent_data,
        columns=['goods_producing', 'service_providing', 'private_service'],
        ytitle="%",
        labels=labels
    )
    
    return fig

def kpds_wage_growth_chart():
    """
    평균 시급 증가율 차트 (KPDS 스타일)
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 평균 시급 YoY 데이터
    yoy_data = get_yoy_data(['avg_hourly_earnings', 'avg_hourly_earnings_prod'])
    
    if yoy_data.empty:
        print("⚠️ 임금 데이터가 없습니다.")
        return None
    
    # 제목 출력
    latest_date = yoy_data.index[-1].strftime('%b-%y')
    title = f"미국 평균 시급 증가율 ({latest_date})"
    print(title)
    
    # 라벨 설정
    labels = {
        'avg_hourly_earnings': '전체 평균 시급',
        'avg_hourly_earnings_prod': '생산직 평균 시급'
    }
    
    # KPDS df_multi_line_chart 함수 사용
    fig = df_multi_line_chart(
        df=yoy_data,
        columns=['avg_hourly_earnings', 'avg_hourly_earnings_prod'],
        ytitle="%",
        labels=labels
    )
    
    return fig

def kpds_sector_wage_comparison():
    """
    섹터별 임금 비교 차트 (KPDS 스타일)
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주요 섹터별 시급 데이터
    wage_series = ['earnings_manufacturing', 'earnings_construction', 'earnings_financial',
                   'earnings_professional', 'earnings_leisure', 'earnings_retail']
    
    latest_values = get_latest_values(wage_series)
    
    if not latest_values:
        print("⚠️ 임금 데이터가 없습니다.")
        return None
    
    # 데이터 준비
    sectors = []
    hourly_wages = []
    yoy_changes = []
    
    for series in wage_series:
        if series in latest_values and latest_values[series]['level']:
            sectors.append(EMPLOYMENT_KOREAN_NAMES.get(series, series).replace(' 시급', ''))
            hourly_wages.append(latest_values[series]['level'])
            yoy_changes.append(latest_values[series]['yoy_pct'] if latest_values[series]['yoy_pct'] else 0)
    
    # DataFrame 생성
    df = pd.DataFrame({
        'wage': hourly_wages,
        'yoy_change': yoy_changes
    }, index=sectors)
    
    # 제목 출력
    latest_date = EMPLOYMENT_DATA['raw_data'].index[-1].strftime('%b-%y')
    title = f"Sector Wage Comparison - {latest_date}"
    print(title)
    
    # KPDS df_dual_axis_chart 함수 사용
    fig = df_dual_axis_chart(
        df=df,
        left_cols=['wage'],
        right_cols=['yoy_change'],
        left_labels=['시급'],
        right_labels=['YoY 변화'],
        left_title="USD",
        right_title="%"
    )
    
    return fig

def kpds_employment_momentum_chart():
    """
    고용 모멘텀 차트 (KPDS 스타일)
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # NFP 변화량 데이터
    mom_change = get_mom_change(['nonfarm_total'])
    
    if mom_change.empty:
        print("⚠️ NFP 데이터가 없습니다.")
        return None
    
    nfp_data = mom_change['nonfarm_total']
    
    # 이동평균 계산
    ma3 = nfp_data.rolling(window=3).mean()
    ma6 = nfp_data.rolling(window=6).mean()
    ma12 = nfp_data.rolling(window=12).mean()
    
    # 최근 2년 데이터
    cutoff_date = nfp_data.index[-1] - pd.DateOffset(years=2)
    
    df = pd.DataFrame({
        'monthly': nfp_data,
        'ma3': ma3,
        'ma6': ma6,
        'ma12': ma12
    })[cutoff_date:]
    
    # 제목 출력
    title = "미국 고용 모멘텀 (이동평균)"
    print(title)
    
    # 라벨 설정
    labels = {
        'monthly': '월별 변화',
        'ma3': '3개월 평균',
        'ma6': '6개월 평균',
        'ma12': '12개월 평균'
    }
    
    # KPDS df_multi_line_chart 함수 사용
    fig = df_multi_line_chart(
        df=df,
        columns=['monthly', 'ma3', 'ma6', 'ma12'],
        ytitle="천명",
        labels=labels
    )
    
    return fig

# %%
# === KPDS 스타일 주요 시각화 함수들 (새로 추가) ===

def create_kpds_employment_overview():
    """
    KPDS 스타일 고용 개요 차트 (주요 지표) - MoM 기준
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주요 고용 지표 (계절조정이므로 MoM 사용)
    series = ['nonfarm_total', 'private_total', 'manufacturing']
    mom_data = get_mom_data(series)
    
    if mom_data.empty:
        print("⚠️ 고용 데이터가 없습니다.")
        return None
    
    # 최근 3년 데이터
    cutoff_date = mom_data.index[-1] - pd.DateOffset(years=3)
    recent_data = mom_data[mom_data.index >= cutoff_date]
    
    # 사용 가능한 컬럼 확인
    available_cols = [col for col in series if col in recent_data.columns]
    
    if not available_cols:
        print("⚠️ 표시할 고용 지표가 없습니다.")
        return None
    
    # 한국어 라벨
    labels = {
        'nonfarm_total': '비농업 고용',
        'private_total': '민간 고용', 
        'manufacturing': '제조업 고용'
    }
    
    # KPDS 스타일 다중 라인 차트
    print("미국 고용 주요 지표 (전월대비 %)")
    fig = df_multi_line_chart(
        df=recent_data,
        columns=available_cols,
        ytitle="%",
        labels=labels
    )
    
    return fig

def create_kpds_sector_breakdown():
    """
    KPDS 스타일 섹터별 고용 변화 (가로 바 차트)
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주요 섹터들
    sectors = ['manufacturing', 'construction', 'retail_trade', 'leisure_hospitality', 
               'professional_business', 'financial', 'education_health']
    
    mom_data = get_mom_data(sectors)
    
    if mom_data.empty:
        print("⚠️ 섹터별 고용 데이터가 없습니다.")
        return None
    
    # 최신 MoM 변화율
    latest_mom = {}
    for sector in sectors:
        if sector in mom_data.columns:
            latest_value = mom_data[sector].dropna().iloc[-1] if not mom_data[sector].dropna().empty else 0
            latest_mom[sector] = latest_value
    
    # 한국어 라벨
    sector_labels = {
        'manufacturing': '제조업',
        'construction': '건설업',
        'retail_trade': '소매업',
        'leisure_hospitality': '여가·숙박',
        'professional_business': '전문서비스',
        'financial': '금융업',
        'education_health': '교육·보건'
    }
    
    # 라벨 적용
    labeled_data = {}
    for sector, value in latest_mom.items():
        label = sector_labels.get(sector, sector)
        labeled_data[label] = value
    
    # KPDS 스타일 가로 바 차트
    print("섹터별 고용 변화 (전월대비 %)")
    fig = create_horizontal_bar_chart(
        data_dict=labeled_data,
        positive_color=deepred_pds,
        negative_color=deepblue_pds
    )
    
    return fig

def create_kpds_goods_vs_services():
    """
    KPDS 스타일 재화생산 vs 서비스 비교 - MoM 기준
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 재화생산 vs 서비스 데이터 (계절조정이므로 MoM 사용)
    series = ['goods_producing', 'service_providing']
    mom_data = get_mom_data(series)
    
    if mom_data.empty:
        print("⚠️ 데이터가 없습니다.")
        return None
    
    # 최근 3년 데이터
    cutoff_date = mom_data.index[-1] - pd.DateOffset(years=3)
    recent_data = mom_data[mom_data.index >= cutoff_date]
    
    # 사용 가능한 컬럼 확인
    available_cols = [col for col in series if col in recent_data.columns]
    
    if not available_cols:
        print("⚠️ 표시할 데이터가 없습니다.")
        return None
    
    # 한국어 라벨
    labels = {
        'goods_producing': '재화생산 부문',
        'service_providing': '서비스제공 부문'
    }
    
    # KPDS 스타일 다중 라인 차트
    print("재화생산 vs 서비스제공 부문 고용 (전월대비 %)")
    fig = df_multi_line_chart(
        df=recent_data,
        columns=available_cols,
        ytitle="%",
        labels=labels
    )
    
    return fig

def create_kpds_wage_and_hours():
    """
    KPDS 스타일 임금과 근로시간 이중축 차트
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 민간 부문 임금과 근로시간
    wage_series = ['earnings_private']
    hours_series = ['hours_private']
    
    wage_data = get_raw_data(wage_series)
    hours_data = get_raw_data(hours_series)
    
    if wage_data.empty or hours_data.empty:
        print("⚠️ 임금 또는 근로시간 데이터가 없습니다.")
        return None
    
    # 데이터 결합
    combined_data = pd.concat([wage_data, hours_data], axis=1)
    
    # 최근 2년 데이터
    cutoff_date = combined_data.index[-1] - pd.DateOffset(years=2)
    recent_data = combined_data[combined_data.index >= cutoff_date]
    
    # 사용 가능한 컬럼 확인
    if 'earnings_private' not in recent_data.columns or 'hours_private' not in recent_data.columns:
        print("⚠️ 필요한 데이터가 없습니다.")
        return None
    
    # KPDS 스타일 이중축 차트
    print("미국 민간부문 임금과 근로시간")
    fig = df_dual_axis_chart(
        df=recent_data,
        left_cols=['earnings_private'],
        right_cols=['hours_private'],
        left_labels=['평균 시급'],
        right_labels=['주당 근로시간'],
        left_title="USD",
        right_title="시간"
    )
    
    return fig

# %%
# === 범용 시각화 함수들 (사용자 지정 시리즈) ===

def plot_employment_series(series_list, chart_type='multi_line', data_type='mom', 
                          periods=36, labels=None, left_ytitle=None, right_ytitle=None, target_date=None):
    """
    범용 고용 시각화 함수 - 사용자가 원하는 시리즈로 다양한 차트 생성
    
    Args:
        series_list: 시각화할 시리즈 리스트 (예: ['nonfarm_total', 'manufacturing'])
        chart_type: 차트 종류 ('multi_line', 'dual_axis', 'horizontal_bar', 'single_line')
        data_type: 데이터 타입 ('mom', 'raw', 'mom_change')
        periods: 표시할 기간 (개월)
        labels: 시리즈 라벨 딕셔너리 (None이면 자동)
        left_ytitle: 왼쪽 Y축 제목
        right_ytitle: 오른쪽 Y축 제목
        target_date: 특정 날짜 기준 (예: '2025-06-01', None이면 최신 데이터)
    
    Returns:
        plotly figure
    """
    if not EMPLOYMENT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_employment_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 데이터 타입에 따라 데이터 가져오기
    if data_type == 'mom':
        data = get_mom_data(series_list)
        unit = "%"
        desc = "전월대비"
    elif data_type == 'raw':
        data = get_raw_data(series_list) 
        unit = "천명" if any('earnings' not in s for s in series_list) else "USD"
        desc = "수준"
    elif data_type == 'mom_change':
        data = get_mom_change(series_list)
        unit = "천명"
        desc = "월별 변화량"
    else:
        print("❌ 지원하지 않는 data_type입니다. 'mom', 'raw', 'mom_change' 중 선택하세요.")
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
        return None
    
    # 자동 라벨 생성
    if labels is None:
        labels = {col: EMPLOYMENT_KOREAN_NAMES.get(col, col) for col in available_cols}
    
    # Y축 제목 자동 설정
    if left_ytitle is None:
        left_ytitle = unit
    
    # 차트 타입별 시각화
    if chart_type == 'multi_line':
        print(f"고용 시리즈 다중 라인 차트 ({desc})")
        fig = df_multi_line_chart(
            df=recent_data,
            columns=available_cols,
            ytitle=left_ytitle,
            labels=labels
        )
    
    elif chart_type == 'single_line' and len(available_cols) == 1:
        print(f"고용 시리즈 단일 라인 차트 ({desc})")
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
        
        print(f"고용 시리즈 이중축 차트 ({desc})")
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
        
        print(f"고용 시리즈 가로 바 차트 ({desc}){date_info}")
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

def quick_employment_chart(series_list, chart_type='multi_line', periods=24):
    """
    빠른 고용 차트 생성 (MoM 기준)
    
    Args:
        series_list: 시리즈 리스트
        chart_type: 차트 종류
        periods: 기간 (개월)
    """
    return plot_employment_series(
        series_list=series_list,
        chart_type=chart_type,
        data_type='mom',
        periods=periods
    )

def quick_level_chart(series_list, chart_type='multi_line', periods=24):
    """
    빠른 수준 차트 생성 (원시 데이터)
    
    Args:
        series_list: 시리즈 리스트
        chart_type: 차트 종류
        periods: 기간 (개월)
    """
    return plot_employment_series(
        series_list=series_list,
        chart_type=chart_type,
        data_type='raw',
        periods=periods
    )

def compare_sectors(sector_list, chart_type='multi_line', periods=36):
    """
    섹터 비교 차트 (MoM 기준)
    
    Args:
        sector_list: 비교할 섹터 리스트
        chart_type: 차트 종류
        periods: 기간 (개월)
    """
    return plot_employment_series(
        series_list=sector_list,
        chart_type=chart_type,
        data_type='mom',
        periods=periods
    )



print("\n🎨 KPDS 스타일 시각화 함수들이 추가되었습니다:")
print("   create_kpds_employment_overview()  # 고용 주요 지표 (MoM)")
print("   create_kpds_sector_breakdown()  # 섹터별 변화 (바차트)")  
print("   create_kpds_goods_vs_services()  # 재화 vs 서비스 (MoM)")
print("   create_kpds_wage_and_hours()  # 임금과 근로시간 (이중축)")

print("\n🔧 범용 시각화 함수들:")
print("   plot_employment_series(series_list, chart_type, data_type, periods)")
print("     - chart_type: 'multi_line', 'dual_axis', 'horizontal_bar', 'single_line'")
print("     - data_type: 'mom'(전월대비%), 'raw'(수준), 'mom_change'(변화량)")
print("   quick_employment_chart(series_list, chart_type)  # MoM 빠른 차트")
print("   quick_level_chart(series_list, chart_type)  # 수준 빠른 차트") 
print("   compare_sectors(sector_list, chart_type)  # 섹터 비교")

print("\n📋 사용 예시:")
print("   plot_employment_series(['nonfarm_total', 'manufacturing'], 'multi_line', 'mom')")
print("   quick_employment_chart(['retail_trade', 'leisure_hospitality'], 'dual_axis')")
print("   compare_sectors(['manufacturing', 'construction', 'financial'], 'horizontal_bar')")


# %%
# 기본 고용 분석 실행 (주석 처리 - 필요시 실행)
run_complete_employment_analysis()
# %%
create_kpds_sector_breakdown()
# %%
create_kpds_employment_overview()
# %%
compare_sectors(['nonfarm_total', 'private_total', 'goods_producing', 'service_providing'], 'horizontal_bar')
# %%
plot_employment_series(['nonfarm_total', 'private_total', 'government'], 'multi_line', 'mom_change')
# %%
plot_employment_series(['goods_producing', 'service_providing'], 'multi_line', 'mom_change')

# %%
create_kpds_sector_breakdown()
# %%
    'nonfarm_total': 'CES0000000001',  # Total nonfarm
    'private_total': 'CES0500000001',  # Total private
    'goods_producing': 'CES0600000001',  # Goods-producing
    'service_providing': 'CES0700000001',  # Service-providing
    'private_service': 'CES0800000001',  # Private service-providing
    'government': 'CES9000000001',  # Government
    
    # Goods-Producing Sectors
    'mining_logging': 'CES1000000001',  # Mining and logging
    'construction': 'CES2000000001',  # Construction
    'manufacturing': 'CES3000000001',  # Manufacturing
    'durable_goods': 'CES3100000001',  # Durable goods
    'nondurable_goods': 'CES3200000001',  # Nondurable goods
    
    # Service-Providing Sectors
    'trade_transport_utilities': 'CES4000000001',  # Trade, transportation, and utilities
    'wholesale_trade': 'CES4142000001',  # Wholesale trade
    'retail_trade': 'CES4200000001',  # Retail trade
    'transport_warehouse': 'CES4300000001',  # Transportation and warehousing
    'utilities': 'CES4422000001',  # Utilities
    'information': 'CES5000000001',  # Information
    'financial': 'CES5500000001',  # Financial activities
    'professional_business': 'CES6000000001',  # Professional and business services
    'education_health': 'CES6500000001',  # Private education and health services
    'leisure_hospitality': 'CES7000000001',  # Leisure and hospitality
    'other_services': 'CES8000000001',  # Other services