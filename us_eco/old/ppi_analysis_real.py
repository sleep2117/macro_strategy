# %%
"""
BLS API 전용 PPI 분석 및 시각화 도구
- BLS API만 사용하여 PPI 데이터 수집
- 계층구조별 데이터 분류
- YoY/MoM 기준 시각화 지원
- CSV 저장/로드 및 스마트 업데이트
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

# BLS API 키 설정 (CPI와 동일)
BLS_API_KEY = '56b193612b614cdc9416359fd1c73a74'
BLS_API_KEY2 = '0450ef37363c48b5bedd2ae6fc92dd6e'
BLS_API_KEY3 = 'daf1ca7970b74e81b6a5c7a80a8b8a7f'
CURRENT_API_KEY = BLS_API_KEY2

# 시각화 라이브러리 불러오기
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

# %%
# === PPI 시리즈 ID와 한국어 이름 매핑 ===

# PPI 시리즈 ID와 한국어 이름 매핑
PPI_SERIES = {
    # Final Demand (최종 수요)
    'WPSFD4': 'Final demand',
    'WPSFD41': 'Final demand goods',
    'WPSFD411': 'Final demand foods',
    'WPSFD412': 'Final demand energy',
    'WPSFD49104': 'Final demand less foods and energy',
    'WPSFD49116': 'Final demand less foods, energy, & trade services',
    'WPSFD42': 'Final demand services',
    'WPSFD422': 'Final demand transportation and warehousing',
    'WPSFD423': 'Final demand trade services',
    'WPSFD421': 'Final demand services less trade, trans., wrhsg',
    'WPSFD43': 'Final demand construction',

    # Final Demand (최종 수요) - 계절미조정
    'WPUFD4': 'Final demand',
    'WPUFD41': 'Final demand goods',
    'WPUFD411': 'Final demand foods',
    'WPUFD412': 'Final demand energy',
    'WPUFD49104': 'Final demand less foods and energy',
    'WPUFD49116': 'Final demand less foods, energy, & trade services',
    'WPUFD42': 'Final demand services',
    'WPUFD422': 'Final demand transportation and warehousing',
    'WPUFD423': 'Final demand trade services',
    'WPUFD421': 'Final demand services less trade, trans., wrhsg',
    'WPUFD43': 'Final demand construction',
    
    # Intermediate Demand (중간 수요)
    'WPSID61': 'Processed goods for intermediate demand',
    'WPSID62': 'Unprocessed goods for intermediate demand',
    'WPSID63': 'Services for intermediate demand',
    'WPSID54': 'Stage 4 intermediate demand',
    'WPSID53': 'Stage 3 intermediate demand',
    'WPSID52': 'Stage 2 intermediate demand',
    'WPSID51': 'Stage 1 intermediate demand',
    
    # Specific Commodities (주요 품목)
    'WPS1411': 'Motor vehicles',
    'WPS0638': 'Pharmaceutical preparations',
    'WPS0571': 'Gasoline',
    'WPS0221': 'Meats',
    'WPS061': 'Industrial chemicals',
    'WPS081': 'Lumber',
    'WPS1017': 'Steel mill products',
    'WPS057303': 'Diesel fuel',
    'WPS029': 'Prepared animal feeds',
    'WPS0561': 'Crude petroleum',
    'WPS012': 'Grains',
    'WPS101211': 'Carbon steel scrap',
    
    # Services (서비스)
    'WPS5111': 'Outpatient healthcare',
    'WPS5121': 'Inpatient healthcare services',
    'WPS5811': 'Food and alcohol retailing',
    'WPS5831': 'Apparel and jewelry retailing',
    'WPS3022': 'Airline passenger services',
    'WPS4011': 'Securities brokerage, investment, and related',
    'WPS3911': 'Business loans (partial)',
    'WPS4511': 'Legal services',
    'WPS301': 'Truck transportation of freight',
    'WPS057': 'Machinery and equipment wholesaling',
    
    # Finished Goods (완제품)
    'WPSFD49207': 'Finished goods',
    'WPSFD4131': 'Finished core',
    
    # All Commodities (전체 상품)
    'WPSSOP3000': 'All commodities',
    'WPS03THRU15': 'Industrial commodities'
}

# 한국어 이름 매핑
PPI_KOREAN_NAMES = {
    # Final Demand (최종수요) - 계절조정
    'WPSFD4': '최종수요 (계절조정)',
    'WPSFD41': '최종수요 재화 (계절조정)',
    'WPSFD411': '최종수요 식품 (계절조정)',
    'WPSFD412': '최종수요 에너지 (계절조정)',
    'WPSFD49104': '최종수요(식품·에너지 제외) (계절조정)',
    'WPSFD49116': '최종수요(식품·에너지·무역서비스 제외) (계절조정)',
    'WPSFD42': '최종수요 서비스 (계절조정)',
    'WPSFD422': '최종수요 운송·창고업 (계절조정)',
    'WPSFD423': '최종수요 무역서비스 (계절조정)',
    'WPSFD421': '최종수요 서비스(무역·운송·창고 제외) (계절조정)',
    'WPSFD43': '최종수요 건설업 (계절조정)',
    
    # Final Demand (최종수요) - 계절미조정
    'WPUFD4': '최종수요',
    'WPUFD41': '최종수요 재화',
    'WPUFD411': '최종수요 식품',
    'WPUFD412': '최종수요 에너지',
    'WPUFD49104': '최종수요(식품·에너지 제외)',
    'WPUFD49116': '최종수요(식품·에너지·무역서비스 제외)',
    'WPUFD42': '최종수요 서비스',
    'WPUFD422': '최종수요 운송·창고업',
    'WPUFD423': '최종수요 무역서비스',
    'WPUFD421': '최종수요 서비스(무역·운송·창고 제외)',
    'WPUFD43': '최종수요 건설업',
    
    # Intermediate Demand (중간수요) - 계절조정
    'WPSID61': '중간수요 가공재 (계절조정)',
    'WPSID62': '중간수요 미가공재 (계절조정)',
    'WPSID63': '중간수요 서비스 (계절조정)',
    'WPSID54': '4단계 중간수요 (계절조정)',
    'WPSID53': '3단계 중간수요 (계절조정)',
    'WPSID52': '2단계 중간수요 (계절조정)',
    'WPSID51': '1단계 중간수요 (계절조정)',
    
    # Intermediate Demand (중간수요) - 계절미조정 (일반적으로 WPU 접두사 사용)
    'WPUID61': '중간수요 가공재',
    'WPUID62': '중간수요 미가공재',
    'WPUID63': '중간수요 서비스',
    'WPUID54': '4단계 중간수요',
    'WPUID53': '3단계 중간수요',
    'WPUID52': '2단계 중간수요',
    'WPUID51': '1단계 중간수요',
    
    # Specific Commodities (주요 품목) - 계절조정
    'WPS1411': '자동차 (계절조정)',
    'WPS0638': '의약품 (계절조정)',
    'WPS0571': '가솔린 (계절조정)',
    'WPS0221': '육류 (계절조정)',
    'WPS061': '산업화학 (계절조정)',
    'WPS081': '목재 (계절조정)',
    'WPS1017': '제철 제품 (계절조정)',
    'WPS057303': '디젤연료 (계절조정)',
    'WPS029': '사료 (계절조정)',
    'WPS0561': '원유 (계절조정)',
    'WPS012': '곡물 (계절조정)',
    'WPS101211': '탄소강 스크랩 (계절조정)',
    
    # Specific Commodities (주요 품목) - 계절미조정
    'WPU1411': '자동차',
    'WPU0638': '의약품',
    'WPU0571': '가솔린',
    'WPU0221': '육류',
    'WPU061': '산업화학',
    'WPU081': '목재',
    'WPU1017': '제철 제품',
    'WPU057303': '디젤연료',
    'WPU029': '사료',
    'WPU0561': '원유',
    'WPU012': '곡물',
    'WPU101211': '탄소강 스크랩',
    
    # Services (서비스) - 계절조정
    'WPS5111': '외래 의료서비스 (계절조정)',
    'WPS5121': '입원 의료서비스 (계절조정)',
    'WPS5811': '식품·주류 소매 (계절조정)',
    'WPS5831': '의류·보석 소매 (계절조정)',
    'WPS3022': '항공 승객 서비스 (계절조정)',
    'WPS4011': '증권중개·투자 관련 (계절조정)',
    'WPS3911': '기업 대출(부분) (계절조정)',
    'WPS4511': '법률 서비스 (계절조정)',
    'WPS301': '화물 트럭 운송 (계절조정)',
    'WPS057': '기계·장비 도매 (계절조정)',
    
    # Services (서비스) - 계절미조정
    'WPU5111': '외래 의료서비스',
    'WPU5121': '입원 의료서비스',
    'WPU5811': '식품·주류 소매',
    'WPU5831': '의류·보석 소매',
    'WPU3022': '항공 승객 서비스',
    'WPU4011': '증권중개·투자 관련',
    'WPU3911': '기업 대출(부분)',
    'WPU4511': '법률 서비스',
    'WPU3012': '화물 트럭 운송',
    'WPU571': '기계·장비 도매',
    
    # Finished Goods (완제품) - 계절조정
    'WPSFD49207': '완제품 (계절조정)',
    'WPSFD4131': '완제품 코어 (계절조정)',
    
    # Finished Goods (완제품) - 계절미조정
    'WPUFD49207': '완제품',
    'WPUFD4131': '완제품 코어',
    
    # All Commodities (전체 상품) - 계절조정
    'WPS00000000': '전체 상품 (계절조정)',
    'WPS03THRU15': '산업 상품 (계절조정)',
    
    # All Commodities (전체 상품) - 계절미조정
    'WPSSOP3000': '전체 상품',
    'WPU03THRU15': '산업 상품'
}

# 새로운 분류 체계 (계절조정/미조정 구분)
PPI_CATEGORIES = {
    '최종수요_계절조정': {
        '최종수요 전체': ['WPSFD4'],
        '최종수요 재화': ['WPSFD41', 'WPSFD411', 'WPSFD412'],
        '최종수요 서비스': ['WPSFD42', 'WPSFD422', 'WPSFD423', 'WPSFD421'],
        '최종수요 건설': ['WPSFD43'],
        '최종수요 코어': ['WPSFD49104', 'WPSFD49116']
    },
    '최종수요': {
        '최종수요 전체': ['WPUFD4'],
        '최종수요 재화': ['WPUFD41', 'WPUFD411', 'WPUFD412'],
        '최종수요 서비스': ['WPUFD42', 'WPUFD422', 'WPUFD423', 'WPUFD421'],
        '최종수요 건설': ['WPUFD43'],
        '최종수요 코어': ['WPUFD49104', 'WPUFD49116']
    },
    '중간수요_계절조정': {
        '중간수요 가공재': ['WPSID61'],
        '중간수요 미가공재': ['WPSID62'],
        '중간수요 서비스': ['WPSID63'],
        '중간수요 단계별': ['WPSID54', 'WPSID53', 'WPSID52', 'WPSID51']
    },
    '중간수요': {
        '중간수요 가공재': ['WPUID61'],
        '중간수요 미가공재': ['WPUID62'],
        '중간수요 서비스': ['WPUID63'],
        '중간수요 단계별': ['WPUID54', 'WPUID53', 'WPUID52', 'WPUID51']
    },
    '주요품목_계절조정': {
        '에너지 관련': ['WPS0571', 'WPS057303', 'WPS0561'],
        '제조업': ['WPS1411', 'WPS0638', 'WPS061', 'WPS081', 'WPS1017'],
        '식품 농업': ['WPS0221', 'WPS029', 'WPS012', 'WPS101211']
    },
    '주요품목': {
        '에너지 관련': ['WPU0571', 'WPU057303', 'WPU0561'],
        '제조업': ['WPU1411', 'WPU0638', 'WPU061', 'WPU081', 'WPU1017'],
        '식품 농업': ['WPU0221', 'WPU029', 'WPU012', 'WPU101211']
    },
    '서비스_계절조정': {
        '의료서비스': ['WPS5111', 'WPS5121'],
        '비즈니스서비스': ['WPS4011', 'WPS3911', 'WPS4511'],
        '운송서비스': ['WPS3022', 'WPS3012'],
        '소매서비스': ['WPS5811', 'WPS5831', 'WPS571']
    },
    '서비스': {
        '의료서비스': ['WPU5111', 'WPU5121'],
        '비즈니스서비스': ['WPU4011', 'WPU3911', 'WPU4511'],
        '운송서비스': ['WPU3022', 'WPU3012'],
        '소매서비스': ['WPU5811', 'WPU5831', 'WPU571']
    },
    '완제품_계절조정': {
        '완제품 전체': ['WPSFD49207'],
        '완제품 코어': ['WPSFD4131']
    },
    '완제품': {
        '완제품 전체': ['WPUFD49207'],
        '완제품 코어': ['WPUFD4131']
    },
    '전체상품_계절조정': {
        '전체 상품': ['WPS00000000'],
        '산업 상품': ['WPS03THRU15']
    },
    '전체상품': {
        '전체 상품': ['WPU00000000'],
        '산업 상품': ['WPU03THRU15']
    }
}

# 기존 PPI 계층 구조 (하위 호환성을 위해 유지)
PPI_HIERARCHY = {
    '최상위': {
        '전체 상품': ['WPU00000000'],
        '최종수요': ['WPUFD4'],
        '중간수요': ['WPUID61', 'WPUID62', 'WPUID63']
    },
    '상위': {
        '최종수요 재화': ['WPUFD41'],
        '최종수요 서비스': ['WPUFD42'],
        '최종수요 건설': ['WPUFD43'],
        '최종수요 코어': ['WPUFD49104']
    },
    '중위': {
        '식품': ['WPUFD411'],
        '에너지': ['WPUFD412'],
        '자동차': ['WPU1411'],
        '의약품': ['WPU0638'],
        '가솔린': ['WPU0571']
    }
}

# %%
# === 전역 데이터 저장소 ===

# API 설정
BLS_SESSION = None
API_INITIALIZED = False

# 전역 데이터 저장소
PPI_DATA = {
    'raw_data': pd.DataFrame(),      # 원본 레벨 데이터
    'yoy_data': pd.DataFrame(),      # 전년동월대비 변화율 데이터
    'mom_data': pd.DataFrame(),      # 전월대비 변화율 데이터
    'latest_values': {},             # 최신 YoY 값들
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
# === 데이터 로드 함수들 ===

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

def calculate_yoy_change(data):
    """전년동월대비 변화율 계산"""
    return ((data / data.shift(12)) - 1) * 100

def calculate_mom_change(data):
    """전월대비 변화율 계산"""
    return ((data / data.shift(1)) - 1) * 100

# %%
# === CSV 저장/로드 함수들 ===

def save_ppi_data_to_csv(file_path='/home/jyp0615/us_eco/ppi_data.csv'):
    """
    현재 로드된 PPI 데이터를 CSV 파일로 저장
    
    Args:
        file_path: 저장할 CSV 파일 경로
    """
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 저장할 데이터가 없습니다. 먼저 데이터를 로드하세요.")
        return False
    
    try:
        # raw_data와 메타데이터를 함께 저장
        raw_data = PPI_DATA['raw_data']
        
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
                'load_time': PPI_DATA['load_info']['load_time'].isoformat() if PPI_DATA['load_info']['load_time'] else None,
                'start_date': PPI_DATA['load_info']['start_date'],
                'series_count': PPI_DATA['load_info']['series_count'],
                'data_points': PPI_DATA['load_info']['data_points'],
                'latest_values': PPI_DATA['latest_values']
            }
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ PPI 데이터 저장 완료: {file_path}")
        print(f"✅ 메타데이터 저장 완료: {meta_file}")
        return True
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_ppi_data_from_csv(file_path='/home/jyp0615/us_eco/ppi_data.csv'):
    """
    CSV 파일에서 PPI 데이터 로드
    
    Args:
        file_path: 로드할 CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global PPI_DATA
    
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
        PPI_DATA['raw_data'] = df
        PPI_DATA['yoy_data'] = df.apply(calculate_yoy_change)
        PPI_DATA['mom_data'] = df.apply(calculate_mom_change)
        PPI_DATA['latest_values'] = latest_values
        PPI_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': df.index[0].strftime('%Y-%m-%d') if len(df) > 0 else None,
            'series_count': len(df.columns),
            'data_points': len(df)
        }
        
        print(f"✅ CSV에서 PPI 데이터 로드 완료: {file_path}")
        print_load_info()
        return True
        
    except Exception as e:
        print(f"❌ CSV 로드 실패: {e}")
        return False

def check_recent_data_consistency(series_list=None, check_count=3):
    """
    최근 N개 데이터의 일치성을 확인하는 함수 (개선된 스마트 업데이트)
    
    Args:
        series_list: 확인할 시리즈 리스트 (None이면 주요 시리즈만)
        check_count: 확인할 최근 데이터 개수
    
    Returns:
        dict: 일치성 확인 결과 (need_update, reason, mismatches)
    """
    if not PPI_DATA['load_info']['loaded']:
        return {'need_update': True, 'reason': '기존 데이터 없음'}
    
    if series_list is None:
        # 주요 시리즈만 체크 (속도 향상) - 계절미조정 데이터 우선
        series_list = ['WPUFD4', 'WPUFD49104', 'WPU00000000', 'WPUID61']
    
    # BLS API 초기화
    if not initialize_bls_api():
        return {'need_update': True, 'reason': 'API 초기화 실패'}
    
    print(f"📊 최근 {check_count}개 데이터 일치성 확인 중...")
    
    mismatches = []
    new_data_available = False
    
    for series_id in series_list:
        if series_id not in PPI_DATA['raw_data'].columns:
            continue
        
        existing_data = PPI_DATA['raw_data'][series_id]
        
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

def update_ppi_data_from_api(start_date=None, series_list=None, smart_update=True):
    """
    API를 사용하여 기존 데이터 업데이트 (스마트 업데이트 지원)
    
    Args:
        start_date: 업데이트 시작 날짜 (None이면 마지막 데이터 이후부터)
        series_list: 업데이트할 시리즈 리스트 (None이면 모든 시리즈)
        smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
        bool: 업데이트 성공 여부
    """
    global PPI_DATA
    
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 기존 데이터가 없습니다. 전체 로드를 수행합니다.")
        return load_all_ppi_data()
    
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
            last_date = PPI_DATA['raw_data'].index[-1]
            start_date = (last_date - pd.DateOffset(months=3)).strftime('%Y-%m-01')
        elif consistency_check['reason'] == '기존 데이터 없음':
            # 전체 재로드
            return load_all_ppi_data(force_reload=True)
        else:
            print(f"🔄 업데이트 진행: {consistency_check['reason']}")
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    # 업데이트 시작 날짜 결정
    if start_date is None:
        last_date = PPI_DATA['raw_data'].index[-1]
        # 마지막 날짜의 다음 달부터 업데이트
        start_date = (last_date + pd.DateOffset(months=1)).strftime('%Y-%m-01')
    
    print(f"🔄 PPI 데이터 업데이트 시작 ({start_date}부터)")
    print("="*50)
    
    if series_list is None:
        series_list = list(PPI_SERIES.keys())
    
    updated_data = {}
    new_count = 0
    
    for series_id in series_list:
        if series_id not in PPI_SERIES:
            continue
        
        # 새로운 데이터 가져오기
        start_year = pd.to_datetime(start_date).year
        new_data = get_bls_data(series_id, start_year)
        
        if new_data is not None and len(new_data) > 0:
            # 기존 데이터와 병합
            if series_id in PPI_DATA['raw_data'].columns:
                existing_data = PPI_DATA['raw_data'][series_id]
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
        PPI_DATA['raw_data'] = updated_df
        PPI_DATA['yoy_data'] = updated_df.apply(calculate_yoy_change)
        PPI_DATA['mom_data'] = updated_df.apply(calculate_mom_change)
        
        # 최신 값 업데이트
        for series_id in updated_data.keys():
            if series_id in PPI_DATA['yoy_data'].columns:
                yoy_data = PPI_DATA['yoy_data'][series_id].dropna()
                if len(yoy_data) > 0:
                    PPI_DATA['latest_values'][series_id] = yoy_data.iloc[-1]
        
        # 로드 정보 업데이트
        PPI_DATA['load_info']['load_time'] = datetime.datetime.now()
        PPI_DATA['load_info']['series_count'] = len(updated_df.columns)
        PPI_DATA['load_info']['data_points'] = len(updated_df)
        
        print(f"\n✅ 업데이트 완료! 새로 추가된 데이터: {new_count}개 포인트")
        print_load_info()
        
        # 자동으로 CSV에 저장
        save_ppi_data_to_csv()
        
        return True
    else:
        print("\n⚠️ 업데이트할 새로운 데이터가 없습니다.")
        return False

# === 메인 데이터 로드 함수 ===

def load_all_ppi_data(start_date='2020-01-01', series_list=None, force_reload=False, csv_file='/home/jyp0615/us_eco/ppi_data.csv'):
    """
    PPI 데이터 로드 (CSV 우선, API 백업 방식)
    
    Args:
        start_date: 시작 날짜
        series_list: 로드할 시리즈 리스트 (None이면 모든 시리즈)
        force_reload: 강제 재로드 여부
        csv_file: CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global PPI_DATA
    
    # 이미 로드된 경우 스킵
    if PPI_DATA['load_info']['loaded'] and not force_reload:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    # CSV 파일이 있으면 우선 로드 시도
    import os
    if not force_reload and os.path.exists(csv_file):
        print("📂 기존 CSV 파일에서 데이터 로드 시도...")
        if load_ppi_data_from_csv(csv_file):
            print("🔄 CSV 로드 후 스마트 업데이트 시도...")
            # CSV 로드 성공 후 스마트 업데이트
            update_ppi_data_from_api(smart_update=True)
            return True
        else:
            print("⚠️ CSV 로드 실패, API에서 전체 로드 시도...")
    
    print("🚀 PPI 데이터 로딩 시작... (BLS API 전용)")
    print("="*50)
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    if series_list is None:
        series_list = list(PPI_SERIES.keys())
    
    # 데이터 수집
    raw_data_dict = {}
    yoy_data_dict = {}
    mom_data_dict = {}
    latest_values = {}
    
    for series_id in series_list:
        if series_id not in PPI_SERIES:
            print(f"⚠️ 알 수 없는 시리즈: {series_id}")
            continue
        
        # 원본 데이터 가져오기
        series_data = get_series_data(series_id, start_date)
        
        if series_data is not None and len(series_data) > 0:
            # 원본 데이터 저장
            raw_data_dict[series_id] = series_data
            
            # YoY 계산
            yoy_data = calculate_yoy_change(series_data)
            yoy_data_dict[series_id] = yoy_data
            
            # MoM 계산
            mom_data = calculate_mom_change(series_data)
            mom_data_dict[series_id] = mom_data
            
            # 최신 값 저장
            if len(yoy_data.dropna()) > 0:
                latest_yoy = yoy_data.dropna().iloc[-1]
                latest_values[series_id] = latest_yoy
            else:
                print(f"⚠️ YoY 데이터 없음: {series_id}")
        else:
            print(f"❌ 데이터 로드 실패: {series_id}")
    
    # 로드된 데이터가 너무 적으면 오류 발생
    if len(raw_data_dict) < 5:  # 최소 5개 시리즈는 있어야 함
        error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data_dict)}개 (최소 5개 필요)"
        print(error_msg)
        raise ValueError(error_msg)
    
    # 전역 저장소에 저장
    PPI_DATA['raw_data'] = pd.DataFrame(raw_data_dict)
    PPI_DATA['yoy_data'] = pd.DataFrame(yoy_data_dict)
    PPI_DATA['mom_data'] = pd.DataFrame(mom_data_dict)
    PPI_DATA['latest_values'] = latest_values
    PPI_DATA['load_info'] = {
        'loaded': True,
        'load_time': datetime.datetime.now(),
        'start_date': start_date,
        'series_count': len(raw_data_dict),
        'data_points': len(PPI_DATA['raw_data']) if not PPI_DATA['raw_data'].empty else 0
    }
    
    print("\n✅ 데이터 로딩 완료!")
    print_load_info()
    
    # 자동으로 CSV에 저장
    save_ppi_data_to_csv(csv_file)
    
    return True

def print_load_info():
    """로드 정보 출력"""
    info = PPI_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not PPI_DATA['raw_data'].empty:
        date_range = f"{PPI_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {PPI_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

def clear_data():
    """데이터 초기화"""
    global PPI_DATA
    PPI_DATA = {
        'raw_data': pd.DataFrame(),
        'yoy_data': pd.DataFrame(),
        'mom_data': pd.DataFrame(),
        'latest_values': {},
        'load_info': {'loaded': False, 'load_time': None, 'start_date': None, 'series_count': 0, 'data_points': 0}
    }
    print("🗑️ 데이터가 초기화되었습니다")

# %%
# === 데이터 접근 함수들 ===

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_ppi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PPI_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in PPI_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PPI_DATA['raw_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """YoY 변화율 데이터 반환"""
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_ppi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PPI_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in PPI_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PPI_DATA['yoy_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화율 데이터 반환"""
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_ppi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PPI_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in PPI_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PPI_DATA['mom_data'][available_series].copy()

def get_latest_values(series_names=None):
    """최신 YoY 값들 반환"""
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_ppi_data()를 먼저 실행하세요.")
        return {}
    
    if series_names is None:
        return PPI_DATA['latest_values'].copy()
    
    return {name: PPI_DATA['latest_values'].get(name, 0) for name in series_names if name in PPI_DATA['latest_values']}

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not PPI_DATA['load_info']['loaded']:
        return []
    return list(PPI_DATA['raw_data'].columns)

# %%
# === 시각화 함수들 ===

def filter_series_by_selection(selected_series=None, category_filter=None):
    """
    사용자 선택에 따라 시리즈 필터링
    
    Args:
        selected_series: 사용자가 직접 선택한 시리즈 리스트
        category_filter: 카테고리 필터 ('최종수요', '중간수요', '주요품목', '서비스', '완제품', '전체상품')
    
    Returns:
        list: 필터링된 시리즈 리스트
    """
    if selected_series is not None:
        # 사용자 직접 선택 우선
        return selected_series
    
    if category_filter and category_filter in PPI_CATEGORIES:
        # 카테고리 필터 적용
        series_list = []
        for group_series in PPI_CATEGORIES[category_filter].values():
            series_list.extend(group_series)
        return series_list
    
    # 기본값
    return ['WPUFD4', 'WPUFD49104']  # 최종수요, 코어 최종수요 (계절미조정)

def create_filtered_ppi_chart(selected_series=None, category_filter=None, chart_type='yoy'):
    """
    사용자 선택에 따라 필터링된 PPI 차트 생성
    
    Args:
        selected_series: 사용자가 직접 선택한 시리즈 리스트
        category_filter: 카테고리 필터 ('최종수요', '중간수요', '주요품목', '서비스', '완제품', '전체상품')
        chart_type: 차트 타입 ('yoy', 'mom', 'level')
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ppi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 시리즈 필터링
    series_names = filter_series_by_selection(selected_series, category_filter)
    
    # 기존 차트 생성 함수 사용
    return create_ppi_timeseries_chart(series_names, chart_type)

def show_category_options():
    """
    사용 가능한 카테고리 옵션 표시
    """
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in PPI_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_id in series_list:
                korean_name = PPI_KOREAN_NAMES.get(series_id, series_id)
                print(f"    - {series_id}: {korean_name}")

def create_category_comparison_chart(categories=None, chart_type='yoy'):
    """
    여러 카테고리 비교 차트
    
    Args:
        categories: 비교할 카테고리 리스트
        chart_type: 차트 타입 ('yoy', 'mom')
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ppi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if categories is None:
        categories = ['최종수요', '중간수요', '주요품목']
    
    # 각 카테고리에서 대표 시리즈 선택
    representative_series = {
        '최종수요': 'WPUFD4',
        '최종수요_계절조정': 'WPSFD4',
        '중간수요': 'WPUID61',
        '중간수요_계절조정': 'WPSID61',
        '주요품목': 'WPU1411',
        '주요품목_계절조정': 'WPS1411',
        '서비스': 'WPU5111',
        '서비스_계절조정': 'WPS5111',
        '완제품': 'WPUFD49207',
        '완제품_계절조정': 'WPSFD49207',
        '전체상품': 'WPU00000000',
        '전체상품_계절조정': 'WPS00000000'
    }
    
    selected_series = [representative_series.get(cat) for cat in categories if cat in representative_series]
    selected_series = [s for s in selected_series if s is not None]
    
    if not selected_series:
        print("⚠️ 선택된 카테고리에 대한 대표 시리즈를 찾을 수 없습니다.")
        return None
    
    print(f"PPI 카테고리 비교 - {chart_type.upper()}")
    return create_ppi_timeseries_chart(selected_series, chart_type)

def create_ppi_timeseries_chart(series_names=None, chart_type='yoy', category_filter=None):
    """
    저장된 데이터를 사용한 PPI 시계열 차트 생성
    
    Args:
        series_names: 차트에 표시할 시리즈 리스트
        chart_type: 'yoy' (전년동기대비), 'mom' (전월대비), 또는 'level' (수준)
        category_filter: 새로운 분류 체계에서 선택할 카테고리 ('최종수요', '중간수요', '주요품목', '서비스', '완제품', '전체상품')
    """
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ppi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 카테고리 필터 적용
    if category_filter and category_filter in PPI_CATEGORIES:
        series_names = []
        for group_series in PPI_CATEGORIES[category_filter].values():
            series_names.extend(group_series)
    elif series_names is None:
        series_names = ['WPUFD4', 'WPUFD49104']  # 최종수요, 코어 최종수요 (계절미조정)
    
    # 데이터 가져오기
    if chart_type == 'yoy':
        df = get_yoy_data(series_names)
        ytitle = "Percent change from year ago"
        print("Producer Price Index - Year-over-Year Change")
    elif chart_type == 'mom':
        df = get_mom_data(series_names)
        ytitle = "Percent change from month ago"
        print("Producer Price Index - Month-over-Month Change")
    else:
        df = get_raw_data(series_names)
        ytitle = "Index Level"
        print("Producer Price Index - Level")
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 라벨 매핑 (한국어)
    label_mapping = {}
    for series_id in series_names:
        label_mapping[series_id] = PPI_KOREAN_NAMES.get(series_id, PPI_SERIES.get(series_id, series_id))
    
    # 데이터 준비
    chart_data = {}
    for col in df.columns:
        label = label_mapping.get(col, col)
        chart_data[label] = df[col].dropna()
    
    # KPDS 포맷 사용하여 차트 생성
    chart_df = pd.DataFrame(chart_data)
    
    if chart_type in ['yoy', 'mom']:
        fig = df_multi_line_chart(chart_df, ytitle=ytitle)
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    else:
        fig = df_multi_line_chart(chart_df, ytitle=ytitle)
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    
    return fig

def create_ppi_component_comparison(category='상위', chart_type='yoy', use_new_categories=False):
    """
    PPI 구성요소 비교 차트
    
    Args:
        category: PPI_HIERARCHY의 카테고리 ('최상위', '상위', '중위') 또는 PPI_CATEGORIES의 카테고리
        chart_type: 차트 타입 ('yoy', 'mom')
        use_new_categories: 새로운 분류 체계 사용 여부
    """
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ppi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 새로운 분류 체계 사용 여부에 따라 시리즈 수집
    if use_new_categories:
        if category not in PPI_CATEGORIES:
            print(f"⚠️ 잘못된 카테고리: {category}")
            return None
        
        # 모든 시리즈 수집
        all_series = []
        for group in PPI_CATEGORIES[category].values():
            all_series.extend(group)
    else:
        if category not in PPI_HIERARCHY:
            print(f"⚠️ 잘못된 카테고리: {category}")
            return None
        
        # 모든 시리즈 수집
        all_series = []
        for group in PPI_HIERARCHY[category].values():
            all_series.extend(group)
    
    # 데이터 가져오기
    if chart_type == 'yoy':
        df = get_yoy_data(all_series)
    else:
        df = get_mom_data(all_series)
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 라벨 매핑
    chart_data = {}
    for col in df.columns:
        label = PPI_KOREAN_NAMES.get(col, PPI_SERIES.get(col, col))
        chart_data[label] = df[col].dropna()
    
    category_type = "새로운 분류" if use_new_categories else "기존 분류"
    print(f"PPI {category} 카테고리 ({category_type}) - {chart_type.upper()} 변화율")
    
    # KPDS 포맷 사용하여 차트 생성
    chart_df = pd.DataFrame(chart_data)
    fig = df_multi_line_chart(chart_df, 
                             ytitle="Percent change from year ago" if chart_type == 'yoy' else "Percent change from month ago")
    
    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    
    return fig

def create_ppi_bar_chart(selected_components=None):
    """
    PPI 바 차트 생성
    
    Args:
        selected_components: 선택할 구성요소 리스트
    """
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ppi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if selected_components is None:
        selected_components = ['WPUFD4', 'WPUFD411', 'WPUFD412', 'WPUFD49104']
    
    # 최신 데이터 가져오기
    latest_data = get_latest_values(selected_components)
    
    if not latest_data:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 라벨 설정
    labels = {series_id: PPI_KOREAN_NAMES.get(series_id, PPI_SERIES.get(series_id, series_id)) 
              for series_id in selected_components}
    
    # 차트 데이터 준비
    chart_data = {}
    for comp in selected_components:
        if comp in latest_data:
            label = labels.get(comp, comp)
            chart_data[label] = latest_data[comp]
    
    if not PPI_DATA['raw_data'].empty:
        latest_date = PPI_DATA['raw_data'].index[-1].strftime('%B %Y')
        print(f"12-month percentage change, Producer Price Index, selected categories, {latest_date}")
    else:
        print("12-month percentage change, Producer Price Index")
    
    # KPDS 포맷 사용하여 바 차트 생성
    fig = create_kpds_cpi_bar_chart(chart_data)
    
    return fig

def create_ppi_contribution_chart(months_back=6):
    """
    PPI 최종수요 기여도 차트 생성
    
    Args:
        months_back: 표시할 과거 개월 수
    
    Returns:
        plotly figure
    """
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ppi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 최신 MoM 데이터
    mom_data = get_mom_data(['WPUFD41', 'WPUFD411', 'WPUFD412', 'WPUFD42', 'WPUFD43'])
    
    if mom_data.empty:
        print("⚠️ 표시할 데이터가 없습니다.")
        return None
    
    # 최근 N개월 데이터
    recent_data = mom_data.tail(months_back)
    
    # 라벨 매핑
    components_mapping = {
        'WPUFD41': '최종수요 재화',
        'WPUFD411': '최종수요 식품',
        'WPUFD412': '최종수요 에너지',
        'WPUFD42': '최종수요 서비스',
        'WPUFD43': '최종수요 건설업'
    }
    
    # 데이터 준비
    chart_data = {}
    for comp, label in components_mapping.items():
        if comp in recent_data.columns:
            chart_data[label] = recent_data[comp].dropna()
    
    # 월별로 바 차트 생성
    months = recent_data.index[-months_back:]
    month_labels = [month.strftime('%b') for month in months]
    
    fig = go.Figure()
    
    # 각 컴포넌트별로 바 추가
    for i, (label, data) in enumerate(chart_data.items()):
        color = get_kpds_color(i)
        values = [data.loc[month] if month in data.index and not pd.isna(data.loc[month]) else 0 
                 for month in months]
        
        fig.add_trace(go.Bar(
            name=label,
            x=month_labels,
            y=values,
            marker_color=color,
            text=[f'{v:.2f}' for v in values],
            textposition='auto'
        ))
    
    print("US: PPI Final Demand Components % M/M")
    
    # 레이아웃 설정
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,
        height=500,
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside'
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white',
            tickformat='.2f',
            title=dict(text='변화율 (%)', font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE))
        ),
        legend=dict(
            orientation="v",
            yanchor="top", y=1,
            xanchor="left", x=1.02,
            font=dict(family='NanumGothic', size=FONT_SIZE_LEGEND)
        ),
        margin=dict(l=80, r=150, t=80, b=60)
    )
    
    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.7)
    
    fig.show()
    return fig

def create_ppi_horizontal_bar_chart(data_series=None, series_ids=None, num_categories=20, 
                                    korean_names_dict=None, special_highlight=None):
    """
    PPI 가로 바 차트 생성 (시리즈 ID 직접 입력 지원)
    
    Args:
        data_series: 외부에서 제공하는 데이터 (pandas Series 또는 dict). 
                    None이면 내부 MoM 데이터 사용
        series_ids: 시리즈 ID 리스트 (이 경우 내부 데이터에서 해당 시리즈만 추출)
        num_categories: 표시할 카테고리 수
        korean_names_dict: 한국어 이름 매핑 딕셔너리 (선택사항)
        special_highlight: 특별 강조할 시리즈 ID 리스트 (주황색 표시)
    
    Returns:
        plotly figure
    """
    # 데이터 준비
    if data_series is not None:
        # 외부 데이터 사용
        if isinstance(data_series, dict):
            latest_data = pd.Series(data_series)
        elif isinstance(data_series, list):
            # 리스트가 들어온 경우 series_ids로 처리
            series_ids = data_series
            data_series = None
        else:
            latest_data = data_series.copy()
        
        # 한국어 이름 매핑
        if korean_names_dict is None:
            korean_names_dict = {}
    
    # series_ids가 제공된 경우 내부 데이터에서 해당 시리즈만 추출
    if series_ids is not None or data_series is None:
        if not PPI_DATA['load_info']['loaded']:
            print("⚠️ 먼저 load_all_ppi_data()를 실행하여 데이터를 로드하세요.")
            return None
        
        # 최신 MoM 데이터
        mom_data = get_mom_data()
        
        if series_ids is not None:
            # 특정 시리즈만 선택
            available_series = [s for s in series_ids if s in mom_data.columns]
            if not available_series:
                print(f"⚠️ 요청한 시리즈가 데이터에 없습니다: {series_ids}")
                return None
            latest_data = mom_data[available_series].iloc[-1].dropna()
        else:
            # 모든 시리즈 사용
            latest_data = mom_data.iloc[-1].dropna()
        
        korean_names_dict = PPI_KOREAN_NAMES
    
    # NaN 값 제거
    latest_data = latest_data.dropna()
    
    if len(latest_data) == 0:
        print("⚠️ 표시할 데이터가 없습니다.")
        return None
    
    # 라벨 매핑된 데이터 딕셔너리 생성
    labeled_data = {}
    series_to_label_map = {}  # 시리즈 ID와 라벨 매핑
    
    for series_id, value in latest_data.items():
        if korean_names_dict:
            label = korean_names_dict.get(series_id, PPI_SERIES.get(series_id, series_id))
        else:
            label = str(series_id)
        labeled_data[label] = value
        series_to_label_map[series_id] = label
    
    # 특별 강조 색상 설정
    if special_highlight is None:
        special_highlight = ['WPSFD4', 'WPS00000000']  # 기본 강조 시리즈
    
    print("PPI Categories (m/m % change)")
    
    # 일반화된 함수 사용하여 차트 생성
    fig = create_horizontal_bar_chart(
        data_dict=labeled_data,
        num_categories=num_categories,
        sort_data=True,
        ascending=True  # 가로 바차트에서는 작은 값이 아래쪽
    )
    
    # 특별 강조 색상 적용 (기존 차트에 색상 재설정)
    if fig and special_highlight:
        colors = []
        
        for i, trace_label in enumerate(fig.data[0].y):
            # 라벨을 통해 원래 시리즈 ID 찾기
            original_series = None
            for series_id, label in series_to_label_map.items():
                if label == trace_label:
                    original_series = series_id
                    break
            
            value = fig.data[0].x[i]
            
            # 특별 강조 시리즈인지 확인
            if original_series and original_series in special_highlight:
                colors.append('#FFA500')  # 주황색
            elif value >= 0:
                colors.append(deepred_pds)  # 상승: deepred_pds
            else:
                colors.append(deepblue_pds)  # 하락: deepblue_pds
        
        # 색상 업데이트
        fig.data[0].marker.color = colors
    
    return fig

# %%
# === 통합 분석 함수 ===

def run_complete_ppi_analysis(start_date='2020-01-01', force_reload=False):
    """
    완전한 PPI 분석 실행 - 데이터 로드 후 모든 차트 생성
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🚀 완전한 PPI 분석 시작")
    print("="*50)
    
    # 1. 데이터 로드 (한 번만!)
    print("\n1️⃣ 데이터 로딩")
    success = load_all_ppi_data(start_date=start_date, force_reload=force_reload)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 시각화 생성 (저장된 데이터 사용)
    print("\n2️⃣ 시각화 생성")
    
    results = {}
    
    try:
        # 최신 바 차트
        print("   📊 최신 PPI 바 차트...")
        results['bar_chart'] = create_ppi_bar_chart(['WPUFD4', 'WPUFD411', 'WPUFD412', 'WPUFD49104'])
        
        # 주요 지표 시계열
        print("   📈 주요 PPI 지표 시계열...")
        results['main_timeseries'] = create_ppi_timeseries_chart(['WPUFD4', 'WPUFD49104'], 'yoy')
        
        # 구성요소 비교
        print("   🔍 PPI 구성요소 비교...")
        results['components_comparison'] = create_ppi_component_comparison('상위', 'yoy')
        
        # 기여도 차트
        print("   📊 PPI 기여도 차트...")
        results['contribution_chart'] = create_ppi_contribution_chart()
        
        # 가로 바 차트
        print("   📈 카테고리별 가로 바 차트...")
        results['horizontal_bar_chart'] = create_ppi_horizontal_bar_chart()
        
    except Exception as e:
        print(f"⚠️ 시각화 생성 중 오류: {e}")
    
    print(f"\n✅ 분석 완료! 생성된 차트: {len(results)}개")
    return results

def run_detailed_ppi_analysis():
    """
    세부 PPI 분석 실행 - 계층별 차트
    
    Returns:
        dict: 생성된 차트들
    """
    print("🚀 세부 PPI 분석 시작")
    print("="*50)
    
    # 1. 데이터 로드 확인
    if not PPI_DATA['load_info']['loaded']:
        print("\n1️⃣ 데이터 로딩")
        success = load_all_ppi_data()
        if not success:
            print("❌ 데이터 로딩 실패")
            return None
    else:
        print("✅ 데이터가 이미 로드됨")
    
    results = {}
    
    try:
        # 2. 계층별 차트
        print("\n2️⃣ 계층별 분석...")
        
        # 최상위 계층
        print("   📊 최상위 계층...")
        results['top_level'] = create_ppi_component_comparison('최상위', 'yoy')
        
        # 상위 계층
        print("   📈 상위 계층...")
        results['upper_level'] = create_ppi_component_comparison('상위', 'mom')
        
        # 중위 계층
        print("   🔍 중위 계층...")
        results['mid_level'] = create_ppi_component_comparison('중위', 'yoy')
        
    except Exception as e:
        print(f"⚠️ 분석 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ 세부 분석 완료! 생성된 항목: {len(results)}개")
    return results

# %%
# === 유틸리티 함수들 ===

def show_available_components():
    """사용 가능한 PPI 구성요소 표시"""
    print("=== 사용 가능한 PPI 구성요소 ===")
    
    for series_id, description in PPI_SERIES.items():
        korean_name = PPI_KOREAN_NAMES.get(series_id, description)
        print(f"  '{series_id}': {korean_name} ({description})")

def get_data_status():
    """현재 데이터 상태 반환"""
    return {
        'loaded': PPI_DATA['load_info']['loaded'],
        'series_count': PPI_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': PPI_DATA['load_info']
    }

def show_ppi_hierarchy():
    """PPI 계층 구조 표시"""
    print("=== PPI 계층 구조 ===\n")
    
    for level, groups in PPI_HIERARCHY.items():
        print(f"### {level} ###")
        for group_name, series_list in groups.items():
            print(f"  {group_name}:")
            for series_id in series_list:
                korean_name = PPI_KOREAN_NAMES.get(series_id, PPI_SERIES.get(series_id, series_id))
                print(f"    - {series_id}: {korean_name}")
        print()

# %%
# === 사용 예시 ===

def check_mom_all_commodities_issue():
    """
    MoM 전체 상품 상승률 문제 확인
    """
    if not PPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ppi_data()를 실행하여 데이터를 로드하세요.")
        return False
    
    # 최근 MoM 데이터 확인
    mom_data = get_mom_data()
    if mom_data.empty:
        print("⚠️ MoM 데이터가 없습니다.")
        return False
    
    latest_mom = mom_data.iloc[-1].dropna()
    latest_date = mom_data.index[-1]
    
    print(f"=== {latest_date.strftime('%Y-%m')} MoM 데이터 분석 ===")
    print(f"\n전체 상품 (WPU00000000): {latest_mom.get('WPU00000000', 'N/A'):.2f}%")
    
    # 최종수요 관련 지표들
    final_demand_series = {
        'WPUFD4': '최종수요',
        'WPUFD41': '최종수요 재화',
        'WPUFD42': '최종수요 서비스',
        'WPUFD412': '최종수요 에너지',
        'WPUFD49104': '최종수요 코어'
    }
    
    print("\n최종수요 관련 지표:")
    for series_id, name in final_demand_series.items():
        value = latest_mom.get(series_id, None)
        if value is not None:
            print(f"  {name} ({series_id}): {value:.2f}%")
    
    # 상승률 정렬
    sorted_mom = latest_mom.sort_values(ascending=False)
    
    print("\n상위 10개 MoM 상승률:")
    for i, (series, value) in enumerate(sorted_mom.head(10).items(), 1):
        korean_name = PPI_KOREAN_NAMES.get(series, series)
        print(f"  {i:2d}. {korean_name} ({series}): {value:.2f}%")
    
    print("\n하위 10개 MoM 상승률:")
    for i, (series, value) in enumerate(sorted_mom.tail(10).items(), 1):
        korean_name = PPI_KOREAN_NAMES.get(series, series)
        print(f"  {i:2d}. {korean_name} ({series}): {value:.2f}%")
    
    # 전체 상품의 순위 확인
    all_commodities_value = latest_mom.get('WPU00000000', None)
    if all_commodities_value is not None:
        rank = (sorted_mom > all_commodities_value).sum() + 1
        print(f"\n전체 상품 순위: {len(sorted_mom)}개 중 {rank}위")
        print(f"전체 상품보다 높은 상승률: {(sorted_mom > all_commodities_value).sum()}개")
        print(f"전체 상품보다 낮은 상승률: {(sorted_mom < all_commodities_value).sum()}개")
    
    # 전체 상품이 가장 낮은 상승률인지 확인
    if all_commodities_value is not None:
        is_lowest = all_commodities_value == sorted_mom.iloc[-1]
        print(f"\n전체 상품이 가장 낮은 상승률인가? {is_lowest}")
        
        if is_lowest:
            print("⚠️ 이상합니다! 전체 상품이 가장 낮은 상승률을 기록했습니다.")
            print("전체 상품은 일반적으로 각 구성요소의 가중평균이므로 가장 낮을 수 없습니다.")
            
            # 원인 분석
            print("\n가능한 원인:")
            print("1. 데이터 오류 또는 계산 오류")
            print("2. 일부 구성요소의 가중치가 음수일 가능성")
            print("3. 전체 상품 지수의 계산 방식 문제")
            
            return True
        else:
            print("✅ 정상적인 범위 내에 있습니다.")
            return False
    
    return False

print("\n=== PPI 분석 도구 사용법 ===")
print("\n✨ 새로운 기능:")
print("   show_category_options()  # 사용 가능한 카테고리 옵션 표시")
print("   create_filtered_ppi_chart(selected_series=['WPUFD4', 'WPUFD41'])  # 사용자 선택 시리즈")
print("   create_filtered_ppi_chart(category_filter='최종수요')  # 카테고리 필터 (계절미조정)")
print("   create_filtered_ppi_chart(category_filter='최종수요_계절조정')  # 계절조정 데이터")
print("   create_category_comparison_chart(['최종수요', '중간수요'])  # 카테고리 비교")
print("   create_ppi_horizontal_bar_chart(series_ids=['WPUFD4', 'WPUFD41'])  # 시리즈 ID로 바 차트")
print("   check_mom_all_commodities_issue()  # MoM 전체 상품 문제 확인")
print("1. 데이터 로드 (CSV 우선, API 백업):")
print("   load_all_ppi_data()  # CSV가 있으면 로드 후 업데이트")
print("   load_ppi_data_from_csv()  # CSV에서만 로드")
print("   update_ppi_data_from_api()  # 최신 데이터만 API로 업데이트")
print()
print("2. 데이터 저장:")
print("   save_ppi_data_to_csv()  # 현재 데이터를 CSV로 저장")
print()
print("3. API 키 관리:")
print("   switch_api_key()  # API 키 수동 전환")
print()
print("4. 스마트 업데이트:")
print("   check_recent_data_consistency()  # 최근 3개 데이터 일치성 확인")
print("   update_ppi_data_from_api(smart_update=True)  # 스마트 업데이트")
print("   update_ppi_data_from_api(smart_update=False)  # 강제 업데이트")
print()
print("5. 차트 생성:")
print("   create_ppi_bar_chart()")
print("   create_ppi_timeseries_chart(['WPUFD4', 'WPUFD49104'])  # 계절미조정")
print("   create_ppi_timeseries_chart(['WPSFD4', 'WPSFD49104'])  # 계절조정")
print("   create_ppi_component_comparison('상위', 'yoy')")
print("   create_ppi_component_comparison('최종수요', 'yoy', use_new_categories=True)  # 새로운 분류 사용")
print("   create_ppi_contribution_chart()")
print("   create_ppi_horizontal_bar_chart()  # 전체 MoM 데이터")
print("   create_ppi_horizontal_bar_chart(series_ids=['WPUFD4', 'WPUFD41'])  # 특정 시리즈만")
print()
print("6. 통합 분석:")
print("   run_complete_ppi_analysis()")
print("   run_detailed_ppi_analysis()")
print()
print("7. 데이터 상태 확인:")
print("   get_data_status()")
print("   show_available_components()")
print("   show_ppi_hierarchy()")
print("   show_category_options()  # 새로운 분류 체계 옵션")

# %%
# 사용 가능한 구성요소 표시
# show_available_components()

# %%
# PPI 계층 구조 표시
# show_ppi_hierarchy()

# %%
# 기본 PPI 분석 실행
run_complete_ppi_analysis()

# %%
# 새로운 분류 체계를 사용한 구성요소 비교
# print("\n=== 새로운 분류 체계 사용 예시 ===")
# create_ppi_component_comparison('최종수요', 'yoy', use_new_categories=True)

# %%
# create_ppi_component_comparison('최상위', 'yoy')

# %%
# create_ppi_contribution_chart()

# %%
# MoM 전체 상품 상승률 문제 확인
# check_mom_all_commodities_issue()

# %%
# 새로운 분류 체계 옵션 표시
# show_category_options()

# %%
# 새로운 분류 체계를 사용한 차트 예시
# print("\n=== 새로운 분류 체계 사용 예시 ===")
# print("1. 최종수요 카테고리 시각화 (계절미조정):")
create_filtered_ppi_chart(category_filter='최종수요', chart_type='yoy')

# print("\n2. 최종수요 카테고리 시각화 (계절조정):")
create_filtered_ppi_chart(category_filter='최종수요_계절조정', chart_type='mom')

# print("\n3. 사용자 선택 시리즈 시각화:")
# create_filtered_ppi_chart(selected_series=['WPUFD4', 'WPU00000000', 'WPUID61'], chart_type='mom')

# print("\n4. 카테고리 비교 차트:")
create_category_comparison_chart(['최종수요', '중간수요', '전체상품'], 'yoy')

# %%
create_ppi_horizontal_bar_chart(num_categories=100)
# %%
# create_ppi_bar_chart()

# %%
# create_ppi_bar_chart(['WPU1411', 'WPU0638', 'WPU0571', 'WPU0221', 'WPU061',
#                        'WPU081', 'WPU1017', 'WPU057303', 'WPU029', 'WPU0561', 'WPU012', 'WPU101211'])
# %%
# 시리즈 ID로 직접 가로 바 차트 생성 (새로운 기능)
create_ppi_horizontal_bar_chart(series_ids=['WPU1411', 'WPU0638', 'WPU0571', 'WPU0221', 'WPU061',
                       'WPU081', 'WPU1017', 'WPU057303', 'WPU029', 'WPU0561', 'WPU012', 'WPU101211'],
                       num_categories=12)
# %%
