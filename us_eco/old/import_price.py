# %%
"""
BLS API 전용 Import Prices 분석 및 시각화 도구
- BLS API만 사용하여 Import Prices 데이터 수집
- CSV 저장/로드 및 스마트 업데이트
- YoY/MoM 기준 시각화 지원
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

# BLS API 키 설정 (PPI와 동일)
BLS_API_KEY = '56b193612b614cdc9416359fd1c73a74'
BLS_API_KEY2 = '0450ef37363c48b5bedd2ae6fc92dd6e'
BLS_API_KEY3 = 'daf1ca7970b74e81b6a5c7a80a8b8a7f'
CURRENT_API_KEY = BLS_API_KEY

# 시각화 라이브러리 불러오기
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

# %%
# === Import Prices 시리즈 ID와 이름 매핑 ===

# Import Prices 시리즈 ID와 영어 이름 매핑
IMPORT_SERIES = {
    'EIUIR': 'Imports - All Commodities',
    'EIUIR10': 'Imports - Fuels and Lubricants',
    'EIUIREXFUELS': 'All Imports Excluding Fuels',
    'EIUIR0': 'Imports - Foods, Feeds, and Beverages',
    'EIUIR1': 'Imports - Industrial Supplies and Materials',
    'EIUIR2': 'Imports - Capital Goods',
    'EIUIR3': 'Imports - Automotive Vehicles',
    'EIUIR4': 'Imports - Consumer Goods',
    'EIUIV131': 'Import Air Freight'
}

# 한국어 이름 매핑
IMPORT_KOREAN_NAMES = {
    'EIUIR': '수입품 - 전체',
    'EIUIR10': '수입품 - 연료 및 윤활유',
    'EIUIREXFUELS': '수입품 - 연료 제외 전체',
    'EIUIR0': '수입품 - 식품, 사료 및 음료',
    'EIUIR1': '수입품 - 산업용 원자재',
    'EIUIR2': '수입품 - 자본재',
    'EIUIR3': '수입품 - 자동차',
    'EIUIR4': '수입품 - 소비재',
    'EIUIV131': '수입 항공 화물'
}

# Import Prices 계층 구조
IMPORT_HIERARCHY = {
    '주요 지표': {
        'Total': ['EIUIR', 'EIUIREXFUELS'],
        'Energy': ['EIUIR10']
    },
    '세부 품목': {
        'Consumer': ['EIUIR0', 'EIUIR4'],
        'Industrial': ['EIUIR1', 'EIUIR2'],
        'Transportation': ['EIUIR3', 'EIUIV131']
    }
}

# %%
# === 전역 데이터 저장소 ===

# API 설정
BLS_SESSION = None
API_INITIALIZED = False

# 전역 데이터 저장소
IMPORT_DATA = {
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

def save_import_data_to_csv(file_path='/home/jyp0615/us_eco/import_prices_data.csv'):
    """
    현재 로드된 Import Prices 데이터를 CSV 파일로 저장
    
    Args:
        file_path: 저장할 CSV 파일 경로
    """
    if not IMPORT_DATA['load_info']['loaded']:
        print("⚠️ 저장할 데이터가 없습니다. 먼저 데이터를 로드하세요.")
        return False
    
    try:
        # raw_data와 메타데이터를 함께 저장
        raw_data = IMPORT_DATA['raw_data']
        
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
                'load_time': IMPORT_DATA['load_info']['load_time'].isoformat() if IMPORT_DATA['load_info']['load_time'] else None,
                'start_date': IMPORT_DATA['load_info']['start_date'],
                'series_count': IMPORT_DATA['load_info']['series_count'],
                'data_points': IMPORT_DATA['load_info']['data_points'],
                'latest_values': IMPORT_DATA['latest_values']
            }
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Import Prices 데이터 저장 완료: {file_path}")
        print(f"✅ 메타데이터 저장 완료: {meta_file}")
        return True
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_import_data_from_csv(file_path='/home/jyp0615/us_eco/import_prices_data.csv'):
    """
    CSV 파일에서 Import Prices 데이터 로드
    
    Args:
        file_path: 로드할 CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global IMPORT_DATA
    
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
        IMPORT_DATA['raw_data'] = df
        IMPORT_DATA['yoy_data'] = df.apply(calculate_yoy_change)
        IMPORT_DATA['mom_data'] = df.apply(calculate_mom_change)
        IMPORT_DATA['latest_values'] = latest_values
        IMPORT_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': df.index[0].strftime('%Y-%m-%d') if len(df) > 0 else None,
            'series_count': len(df.columns),
            'data_points': len(df)
        }
        
        print(f"✅ CSV에서 Import Prices 데이터 로드 완료: {file_path}")
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
    if not IMPORT_DATA['load_info']['loaded']:
        return {'need_update': True, 'reason': '기존 데이터 없음'}
    
    if series_list is None:
        # 주요 시리즈만 체크 (속도 향상)
        series_list = ['EIUIR', 'EIUIREXFUELS', 'EIUIR10']
    
    # BLS API 초기화
    if not initialize_bls_api():
        return {'need_update': True, 'reason': 'API 초기화 실패'}
    
    print(f"📊 최근 {check_count}개 데이터 일치성 확인 중...")
    
    mismatches = []
    new_data_available = False
    all_data_identical = True
    
    for series_id in series_list:
        if series_id not in IMPORT_DATA['raw_data'].columns:
            continue
        
        existing_data = IMPORT_DATA['raw_data'][series_id].dropna()
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
                
                # 값이 다르면 불일치
                if abs(existing_val - api_val) > 0.01:  # 0.01 이상 차이
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
        for mismatch in value_mismatches[:3]:
            print(f"   - {mismatch}")
        return {'need_update': True, 'reason': '데이터 불일치', 'mismatches': mismatches}
    else:
        print("✅ 최근 데이터가 완전히 일치합니다. 업데이트를 건너뜁니다.")
        return {'need_update': False, 'reason': '데이터 일치', 'mismatches': []}

def update_import_data_from_api(start_date=None, series_list=None, smart_update=True):
    """
    API를 사용하여 기존 데이터 업데이트 (스마트 업데이트 지원)
    
    Args:
        start_date: 업데이트 시작 날짜 (None이면 마지막 데이터 이후부터)
        series_list: 업데이트할 시리즈 리스트 (None이면 모든 시리즈)
        smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
        bool: 업데이트 성공 여부
    """
    global IMPORT_DATA
    
    if not IMPORT_DATA['load_info']['loaded']:
        print("⚠️ 기존 데이터가 없습니다. 전체 로드를 수행합니다.")
        return load_all_import_data()
    
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
            last_date = IMPORT_DATA['raw_data'].index[-1]
            start_date = (last_date - pd.DateOffset(months=3)).strftime('%Y-%m-01')
        elif consistency_check['reason'] == '기존 데이터 없음':
            # 전체 재로드
            return load_all_import_data(force_reload=True)
        else:
            print(f"🔄 업데이트 진행: {consistency_check['reason']}")
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    # 업데이트 시작 날짜 결정
    if start_date is None:
        last_date = IMPORT_DATA['raw_data'].index[-1]
        # 마지막 날짜의 다음 달부터 업데이트
        start_date = (last_date + pd.DateOffset(months=1)).strftime('%Y-%m-01')
    
    print(f"🔄 Import Prices 데이터 업데이트 시작 ({start_date}부터)")
    print("="*50)
    
    if series_list is None:
        series_list = list(IMPORT_SERIES.keys())
    
    updated_data = {}
    new_count = 0
    
    for series_id in series_list:
        if series_id not in IMPORT_SERIES:
            continue
        
        # 새로운 데이터 가져오기
        start_year = pd.to_datetime(start_date).year
        new_data = get_bls_data(series_id, start_year)
        
        if new_data is not None and len(new_data) > 0:
            # 기존 데이터와 병합
            if series_id in IMPORT_DATA['raw_data'].columns:
                existing_data = IMPORT_DATA['raw_data'][series_id]
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
        IMPORT_DATA['raw_data'] = updated_df
        IMPORT_DATA['yoy_data'] = updated_df.apply(calculate_yoy_change)
        IMPORT_DATA['mom_data'] = updated_df.apply(calculate_mom_change)
        
        # 최신 값 업데이트
        for series_id in updated_data.keys():
            if series_id in IMPORT_DATA['yoy_data'].columns:
                yoy_data = IMPORT_DATA['yoy_data'][series_id].dropna()
                if len(yoy_data) > 0:
                    IMPORT_DATA['latest_values'][series_id] = yoy_data.iloc[-1]
        
        # 로드 정보 업데이트
        IMPORT_DATA['load_info']['load_time'] = datetime.datetime.now()
        IMPORT_DATA['load_info']['series_count'] = len(updated_df.columns)
        IMPORT_DATA['load_info']['data_points'] = len(updated_df)
        
        print(f"\n✅ 업데이트 완료! 새로 추가된 데이터: {new_count}개 포인트")
        print_load_info()
        
        # 자동으로 CSV에 저장
        save_import_data_to_csv()
        
        return True
    else:
        print("\n⚠️ 업데이트할 새로운 데이터가 없습니다.")
        return False

# === 메인 데이터 로드 함수 ===

def load_all_import_data(start_date='2020-01-01', series_list=None, force_reload=False, csv_file='/home/jyp0615/us_eco/import_prices_data.csv'):
    """
    Import Prices 데이터 로드 (CSV 우선, API 백업 방식)
    
    Args:
        start_date: 시작 날짜
        series_list: 로드할 시리즈 리스트 (None이면 모든 시리즈)
        force_reload: 강제 재로드 여부
        csv_file: CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global IMPORT_DATA
    
    # 이미 로드된 경우 스킵
    if IMPORT_DATA['load_info']['loaded'] and not force_reload:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    # CSV 파일이 있으면 우선 로드 시도
    import os
    if not force_reload and os.path.exists(csv_file):
        print("📂 기존 CSV 파일에서 데이터 로드 시도...")
        if load_import_data_from_csv(csv_file):
            print("🔄 CSV 로드 후 스마트 업데이트 시도...")
            # CSV 로드 성공 후 스마트 업데이트
            update_import_data_from_api(smart_update=True)
            return True
        else:
            print("⚠️ CSV 로드 실패, API에서 전체 로드 시도...")
    
    print("🚀 Import Prices 데이터 로딩 시작... (BLS API 전용)")
    print("="*50)
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    if series_list is None:
        series_list = list(IMPORT_SERIES.keys())
    
    # 데이터 수집
    raw_data_dict = {}
    yoy_data_dict = {}
    mom_data_dict = {}
    latest_values = {}
    
    for series_id in series_list:
        if series_id not in IMPORT_SERIES:
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
    if len(raw_data_dict) < 3:  # 최소 3개 시리즈는 있어야 함
        error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data_dict)}개 (최소 3개 필요)"
        print(error_msg)
        raise ValueError(error_msg)
    
    # 전역 저장소에 저장
    IMPORT_DATA['raw_data'] = pd.DataFrame(raw_data_dict)
    IMPORT_DATA['yoy_data'] = pd.DataFrame(yoy_data_dict)
    IMPORT_DATA['mom_data'] = pd.DataFrame(mom_data_dict)
    IMPORT_DATA['latest_values'] = latest_values
    IMPORT_DATA['load_info'] = {
        'loaded': True,
        'load_time': datetime.datetime.now(),
        'start_date': start_date,
        'series_count': len(raw_data_dict),
        'data_points': len(IMPORT_DATA['raw_data']) if not IMPORT_DATA['raw_data'].empty else 0
    }
    
    print("\n✅ 데이터 로딩 완료!")
    print_load_info()
    
    # 자동으로 CSV에 저장
    save_import_data_to_csv(csv_file)
    
    return True

def print_load_info():
    """로드 정보 출력"""
    info = IMPORT_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not IMPORT_DATA['raw_data'].empty:
        date_range = f"{IMPORT_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {IMPORT_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

def clear_data():
    """데이터 초기화"""
    global IMPORT_DATA
    IMPORT_DATA = {
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
    if not IMPORT_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_import_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return IMPORT_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in IMPORT_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return IMPORT_DATA['raw_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """YoY 변화율 데이터 반환"""
    if not IMPORT_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_import_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return IMPORT_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in IMPORT_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return IMPORT_DATA['yoy_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화율 데이터 반환"""
    if not IMPORT_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_import_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return IMPORT_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in IMPORT_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return IMPORT_DATA['mom_data'][available_series].copy()

def get_latest_values(series_names=None):
    """최신 YoY 값들 반환"""
    if not IMPORT_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_import_data()를 먼저 실행하세요.")
        return {}
    
    if series_names is None:
        return IMPORT_DATA['latest_values'].copy()
    
    return {name: IMPORT_DATA['latest_values'].get(name, 0) for name in series_names if name in IMPORT_DATA['latest_values']}

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not IMPORT_DATA['load_info']['loaded']:
        return []
    return list(IMPORT_DATA['raw_data'].columns)

# %%
# === 시각화 함수들 ===

def create_import_timeseries_chart(series_names=None, chart_type='yoy', start_date=None):
    """
    저장된 데이터를 사용한 Import Prices 시계열 차트 생성
    
    Args:
        series_names: 차트에 표시할 시리즈 리스트 (원하는 만큼 추가 가능)
        chart_type: 'yoy' (전년동기대비), 'mom' (전월대비), 또는 'raw' (원본 수준)
        title: 차트 제목
        start_date: 시작 날짜 (예: '2021-01-01')
    """
    if not IMPORT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_import_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if series_names is None:
        series_names = ['EIUIR', 'EIUIREXFUELS']  # 전체, 연료 제외
    
    # 데이터 가져오기
    if chart_type == 'yoy':
        df = get_yoy_data(series_names)
        ytitle = "Percent change from year ago"
    elif chart_type == 'mom':
        df = get_mom_data(series_names)
        ytitle = "Percent change from month ago"
    else:  # raw
        df = get_raw_data(series_names)
        ytitle = "Index Level"
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 시작 날짜 필터링
    if start_date:
        df = df[df.index >= start_date]
    
    # 라벨 매핑 (한국어)
    label_mapping = {}
    for series_id in series_names:
        label_mapping[series_id] = IMPORT_KOREAN_NAMES.get(series_id, IMPORT_SERIES.get(series_id, series_id))
    
    # 데이터 준비
    chart_data = {}
    for col in df.columns:
        label = label_mapping.get(col, col)
        chart_data[label] = df[col].dropna()
    
    # 타이틀 출력
    if chart_type == 'yoy':
        print("Import Price Index - Year-over-Year Change")
    elif chart_type == 'mom':
        print("Import Price Index - Month-over-Month Change")
    else:
        print("Import Price Index - Level")
    
    # KPDS 포맷 사용하여 차트 생성
    chart_df = pd.DataFrame(chart_data)
    
    if chart_type in ['yoy', 'mom']:
        fig = df_multi_line_chart(chart_df, ytitle=ytitle)
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    else:
        fig = df_multi_line_chart(chart_df, ytitle=ytitle)
    
    return fig

def create_import_horizontal_bar_chart(chart_type='yoy'):
    """
    항목별 변화율을 보여주는 가로 바 차트 생성
    
    Args:
        chart_type: 'yoy' 또는 'mom'
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if not IMPORT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_import_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 최신 데이터 가져오기
    if chart_type == 'yoy':
        data = get_yoy_data()
    else:  # mom
        data = get_mom_data()
    
    latest_data = data.iloc[-1].dropna()
    
    # 데이터 정렬 (내림차순)
    sorted_data = latest_data.sort_values(ascending=True)  # 가로 바차트이므로 ascending=True
    
    # 라벨 적용
    categories = []
    values = []
    colors = []
    
    for series_id, value in sorted_data.items():
        label = IMPORT_KOREAN_NAMES.get(series_id, IMPORT_SERIES.get(series_id, series_id))
        categories.append(label)
        values.append(value)
        
        # 주요 지표는 특별 색상
        if series_id in ['EIUIR', 'EIUIREXFUELS']:
            colors.append('#FFA500')  # 주황색
        elif value >= 0:
            colors.append(deepred_pds)  # 상승: deepred_pds
        else:
            colors.append(deepblue_pds)  # 하락: deepblue_pds
    
    # 가로 바 차트 생성
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=categories,
        x=values,
        orientation='h',
        marker_color=colors,
        text=[f'{v:.1f}%' for v in values],
        textposition='outside' if all(v >= 0 for v in values) else 'auto',
        showlegend=False
    ))
    
    # 타이틀 출력
    if chart_type == 'yoy':
        print("Import Prices - Year-over-Year % Change (Latest)")
    else:
        print("Import Prices - Month-over-Month % Change (Latest)")
    
    # 최신 날짜 가져오기
    latest_date = IMPORT_DATA['raw_data'].index[-1].strftime('%B %Y')
    
    # 레이아웃 설정
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=800,
        height=max(400, len(categories) * 40),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.1f',
            range=[min(values) * 1.2 if min(values) < 0 else -1, max(values) * 1.2]
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white'
        ),
        margin=dict(l=250, r=80, t=80, b=60)
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    fig.show()
    return fig

def create_import_component_comparison(category='주요 지표', chart_type='yoy'):
    """
    Import Prices 구성요소 비교 차트
    
    Args:
        category: IMPORT_HIERARCHY의 카테고리 ('주요 지표', '세부 품목')
        chart_type: 차트 타입 ('yoy', 'mom', 'raw')
        title: 차트 제목
    """
    if not IMPORT_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_import_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if category not in IMPORT_HIERARCHY:
        print(f"⚠️ 잘못된 카테고리: {category}")
        return None
    
    # 모든 시리즈 수집
    all_series = []
    for group in IMPORT_HIERARCHY[category].values():
        all_series.extend(group)
    
    # 타이틀 출력
    print(f"Import Prices {category} - {chart_type.upper()}")
    
    # 시계열 차트 생성
    return create_import_timeseries_chart(all_series, chart_type)

# %%
# === 통합 분석 함수 ===

def run_complete_import_analysis(start_date='2020-01-01', force_reload=False):
    """
    완전한 Import Prices 분석 실행 - 데이터 로드 후 모든 차트 생성
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🚀 완전한 Import Prices 분석 시작")
    print("="*50)
    
    # 1. 데이터 로드 (한 번만!)
    print("\n1️⃣ 데이터 로딩")
    success = load_all_import_data(start_date=start_date, force_reload=force_reload)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 시각화 생성 (저장된 데이터 사용)
    print("\n2️⃣ 시각화 생성")
    
    results = {}
    
    try:
        # 주요 지표 시계열 (YoY)
        print("   📈 주요 Import Price 지표 시계열 (YoY)...")
        results['main_timeseries_yoy'] = create_import_timeseries_chart(['EIUIR', 'EIUIREXFUELS', 'EIUIR10'], 'yoy')
        
        # 주요 지표 시계열 (MoM)
        print("   📈 주요 Import Price 지표 시계열 (MoM)...")
        results['main_timeseries_mom'] = create_import_timeseries_chart(['EIUIR', 'EIUIREXFUELS', 'EIUIR10'], 'mom')
        
        # 세부 품목 비교
        print("   🔍 세부 품목 비교...")
        results['detail_comparison'] = create_import_component_comparison('세부 품목', 'yoy')
        
        # YoY 가로 바 차트
        print("   📊 YoY 변화율 가로 바 차트...")
        results['horizontal_bar_yoy'] = create_import_horizontal_bar_chart('yoy')
        
        # MoM 가로 바 차트
        print("   📊 MoM 변화율 가로 바 차트...")
        results['horizontal_bar_mom'] = create_import_horizontal_bar_chart('mom')
        
    except Exception as e:
        print(f"⚠️ 시각화 생성 중 오류: {e}")
    
    print(f"\n✅ 분석 완료! 생성된 차트: {len(results)}개")
    return results

# %%
# === 유틸리티 함수들 ===

def show_available_components():
    """사용 가능한 Import Prices 구성요소 표시"""
    print("=== 사용 가능한 Import Prices 구성요소 ===")
    
    for series_id, description in IMPORT_SERIES.items():
        korean_name = IMPORT_KOREAN_NAMES.get(series_id, description)
        print(f"  '{series_id}': {korean_name} ({description})")

def get_data_status():
    """현재 데이터 상태 반환"""
    return {
        'loaded': IMPORT_DATA['load_info']['loaded'],
        'series_count': IMPORT_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': IMPORT_DATA['load_info']
    }

def show_import_hierarchy():
    """Import Prices 계층 구조 표시"""
    print("=== Import Prices 계층 구조 ===\n")
    
    for level, groups in IMPORT_HIERARCHY.items():
        print(f"### {level} ###")
        for group_name, series_list in groups.items():
            print(f"  {group_name}:")
            for series_id in series_list:
                korean_name = IMPORT_KOREAN_NAMES.get(series_id, IMPORT_SERIES.get(series_id, series_id))
                print(f"    - {series_id}: {korean_name}")
        print()

# %%
# === 사용 예시 ===

print("\n=== Import Prices 분석 도구 사용법 ===")
print("1. 데이터 로드 (CSV 우선, API 백업):")
print("   load_all_import_data()  # CSV가 있으면 로드 후 업데이트")
print("   load_import_data_from_csv()  # CSV에서만 로드")
print("   update_import_data_from_api()  # 최신 데이터만 API로 업데이트")
print()
print("2. 데이터 저장:")
print("   save_import_data_to_csv()  # 현재 데이터를 CSV로 저장")
print()
print("3. API 키 관리:")
print("   switch_api_key()  # API 키 수동 전환")
print()
print("4. 스마트 업데이트:")
print("   check_recent_data_consistency()  # 최근 3개 데이터 일치성 확인")
print("   update_import_data_from_api(smart_update=True)  # 스마트 업데이트")
print("   update_import_data_from_api(smart_update=False)  # 강제 업데이트")
print()
print("5. 차트 생성:")
print("   # 시계열 차트 (원하는 시리즈를 리스트로 추가 가능)")
print("   create_import_timeseries_chart(['EIUIR', 'EIUIR10'], 'yoy')  # YoY")
print("   create_import_timeseries_chart(['EIUIR', 'EIUIR10'], 'mom')  # MoM")
print("   create_import_timeseries_chart(['EIUIR', 'EIUIR10'], 'raw')  # 원본 레벨")
print()
print("   # 가로 바 차트")
print("   create_import_horizontal_bar_chart('yoy')  # YoY 변화율")
print("   create_import_horizontal_bar_chart('mom')  # MoM 변화율")
print()
print("   # 구성요소 비교")
print("   create_import_component_comparison('주요 지표', 'yoy')")
print("   create_import_component_comparison('세부 품목', 'mom')")
print()
print("6. 통합 분석:")
print("   run_complete_import_analysis()")
print()
print("7. 데이터 상태 확인:")
print("   get_data_status()")
print("   show_available_components()")
print("   show_import_hierarchy()")

# %%
# 사용 가능한 구성요소 표시
show_available_components()

# %%
# Import Prices 계층 구조 표시
show_import_hierarchy()

# %%
# 기본 Import Prices 분석 실행 (주석 처리됨 - 필요시 활성화)
run_complete_import_analysis()

# %%
# 예시: 특정 시리즈들의 시계열 차트 생성
# create_import_timeseries_chart(['EIUIR', 'EIUIR10', 'EIUIREXFUELS'], 'yoy', start_date='2022-01-01')

# %%
# 예시: MoM 가로 바 차트
create_import_horizontal_bar_chart('mom')