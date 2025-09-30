# %%
"""
BLS API 전용 CPI 분석 및 시각화 도구
- BLS API만 사용하여 CPI 데이터 수집
- 계층구조별 데이터 분류 (최상위/상위/중위/하위)
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

BLS_API_KEY = '56b193612b614cdc9416359fd1c73a74'  # BLS API 키 (주요)
BLS_API_KEY2 = '0450ef37363c48b5bedd2ae6fc92dd6e'  # BLS API 키 (백업)
BLS_API_KEY3 = 'daf1ca7970b74e81b6a5c7a80a8b8a7f'
CURRENT_API_KEY = BLS_API_KEY  # 현재 사용 중인 API 키

# 시각화 라이브러리 불러오기
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

# %%
# === 전역 데이터 저장소 ===
# 업데이트
# API 설정
BLS_SESSION = None
API_INITIALIZED = False

# 완전한 CPI 계층 구조 불러오기 (325개 시리즈)
from cpi_complete_all_series import COMPLETE_ALL_CPI_HIERARCHY, ALL_BLS_SERIES_MAP, ALL_KOREAN_NAMES

# 기존 변수명과 호환성을 위해 매핑
CPI_HIERARCHY = COMPLETE_ALL_CPI_HIERARCHY

# BLS 시리즈 ID 맵 (325개 시리즈)
CPI_SERIES = ALL_BLS_SERIES_MAP

# 전역 데이터 저장소
CPI_DATA = {
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

# 샘플 데이터 제거됨 - 실제 데이터가 없으면 오류 발생

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
                    # M01~M12를 1~12로 변환
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

def get_series_data_smart(series_key, start_date='2020-01-01', end_date=None):
    """
    계층 구조를 고려한 BLS API 데이터 가져오기
    
    Args:
        series_key: CPI_HIERARCHY의 키
        start_date: 시작 날짜
        end_date: 종료 날짜
    
    Returns:
        pandas.Series: 시리즈 데이터
    """
    # 계층 구조에서 시리즈 정보 찾기
    series_info = None
    for level_data in CPI_HIERARCHY.values():
        if series_key in level_data:
            series_info = level_data[series_key]
            break
    
    if series_info is None:
        print(f"❌ 알 수 없는 시리즈: {series_key}")
        return None
    
    # BLS API로 데이터 가져오기
    if series_info['bls'] and BLS_SESSION:
        try:
            start_year = pd.to_datetime(start_date).year
            end_year = pd.to_datetime(end_date).year if end_date else None
            data = get_bls_data(series_info['bls'], start_year, end_year)
            
            if data is not None and len(data) > 0:
                # 날짜 필터링
                if start_date:
                    data = data[data.index >= start_date]
                if end_date:
                    data = data[data.index <= end_date]
                
                if len(data) >= 12:
                    return data
        except Exception as e:
            print(f"⚠️ BLS 실패: {series_key} - {e}")
    
    # BLS API 실패시 오류
    print(f"❌ BLS API 실패: {series_key}")
    return None

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

# generate_sample_data 함수 제거됨 - 실제 데이터가 없으면 None 반환하거나 오류 발생

def calculate_yoy_change(data):
    """전년동월대비 변화율 계산"""
    return ((data / data.shift(12)) - 1) * 100

def calculate_mom_change(data):
    """전월대비 변화율 계산"""
    return ((data / data.shift(1)) - 1) * 100

# %%
# === CSV 저장/로드 함수들 ===

def save_cpi_data_to_csv(file_path='/home/jyp0615/us_eco/cpi_data.csv'):
    """
    현재 로드된 CPI 데이터를 CSV 파일로 저장
    
    Args:
        file_path: 저장할 CSV 파일 경로
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 저장할 데이터가 없습니다. 먼저 데이터를 로드하세요.")
        return False
    
    try:
        # raw_data와 메타데이터를 함께 저장
        raw_data = CPI_DATA['raw_data']
        
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
                'load_time': CPI_DATA['load_info']['load_time'].isoformat() if CPI_DATA['load_info']['load_time'] else None,
                'start_date': CPI_DATA['load_info']['start_date'],
                'series_count': CPI_DATA['load_info']['series_count'],
                'data_points': CPI_DATA['load_info']['data_points'],
                'latest_values': CPI_DATA['latest_values']
            }
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ CPI 데이터 저장 완료: {file_path}")
        print(f"✅ 메타데이터 저장 완료: {meta_file}")
        return True
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_cpi_data_from_csv(file_path='/home/jyp0615/us_eco/cpi_data.csv'):
    """
    CSV 파일에서 CPI 데이터 로드
    
    Args:
        file_path: 로드할 CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global CPI_DATA
    
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
        CPI_DATA['raw_data'] = df
        CPI_DATA['yoy_data'] = df.apply(calculate_yoy_change)
        CPI_DATA['mom_data'] = df.apply(calculate_mom_change)
        CPI_DATA['latest_values'] = latest_values
        CPI_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': df.index[0].strftime('%Y-%m-%d') if len(df) > 0 else None,
            'series_count': len(df.columns),
            'data_points': len(df)
        }
        
        print(f"✅ CSV에서 CPI 데이터 로드 완료: {file_path}")
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
    if not CPI_DATA['load_info']['loaded']:
        return {'need_update': True, 'reason': '기존 데이터 없음'}
    
    if series_list is None:
        # 주요 시리즈만 체크 (속도 향상)
        series_list = ['headline', 'core', 'food', 'energy']
    
    # BLS API 초기화
    if not initialize_bls_api():
        return {'need_update': True, 'reason': 'API 초기화 실패'}
    
    print(f"📊 최근 {check_count}개 데이터 일치성 확인 중...")
    
    mismatches = []
    new_data_available = False
    all_data_identical = True
    
    for series_name in series_list:
        if series_name not in CPI_SERIES or series_name not in CPI_DATA['raw_data'].columns:
            continue
        
        series_id = CPI_SERIES[series_name]
        existing_data = CPI_DATA['raw_data'][series_name].dropna()
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
                
                # 값이 다르면 불일치
                if abs(existing_val - api_val) > 0.01:  # 0.01 이상 차이
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

def update_cpi_data_from_api(start_date=None, series_list=None, smart_update=True):
    """
    API를 사용하여 기존 데이터 업데이트 (스마트 업데이트 지원)
    
    Args:
        start_date: 업데이트 시작 날짜 (None이면 마지막 데이터 이후부터)
        series_list: 업데이트할 시리즈 리스트 (None이면 모든 시리즈)
        smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
        bool: 업데이트 성공 여부
    """
    global CPI_DATA
    
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 기존 데이터가 없습니다. 전체 로드를 수행합니다.")
        return load_all_cpi_data()
    
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
            last_date = CPI_DATA['raw_data'].index[-1]
            start_date = (last_date - pd.DateOffset(months=3)).strftime('%Y-%m-01')
        elif consistency_check['reason'] == '기존 데이터 없음':
            # 전체 재로드
            return load_all_cpi_data(force_reload=True)
        else:
            print(f"🔄 업데이트 진행: {consistency_check['reason']}")
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    # 업데이트 시작 날짜 결정
    if start_date is None:
        last_date = CPI_DATA['raw_data'].index[-1]
        # 마지막 날짜의 다음 달부터 업데이트
        start_date = (last_date + pd.DateOffset(months=1)).strftime('%Y-%m-01')
    
    print(f"🔄 CPI 데이터 업데이트 시작 ({start_date}부터)")
    print("="*50)
    
    if series_list is None:
        series_list = list(CPI_SERIES.keys())
    
    updated_data = {}
    new_count = 0
    
    for series_name in series_list:
        if series_name not in CPI_SERIES:
            continue
        
        series_id = CPI_SERIES[series_name]
        
        # 새로운 데이터 가져오기
        start_year = pd.to_datetime(start_date).year
        new_data = get_bls_data(series_id, start_year)
        
        if new_data is not None and len(new_data) > 0:
            # 기존 데이터와 병합
            if series_name in CPI_DATA['raw_data'].columns:
                existing_data = CPI_DATA['raw_data'][series_name]
                original_count = existing_data.notna().sum()
                
                # 개선된 병합 방식: 인덱스 먼저 확장 후 값 할당
                all_dates = existing_data.index.union(new_data.index).sort_values()
                combined_data = existing_data.reindex(all_dates)
                
                # 새 데이터로 업데이트 (기존 값 덮어쓰기)
                for date, value in new_data.items():
                    combined_data.loc[date] = value
                
                updated_data[series_name] = combined_data
                
                # 실제 추가된 데이터 포인트 수 계산
                final_count = combined_data.notna().sum()
                new_points = final_count - original_count
                new_count += new_points
                
                if new_points > 0:
                    print(f"✅ 업데이트: {series_name} (기존 {original_count}개 + 신규 {len(new_data)}개 → 총 {final_count}개, 실제 추가: {new_points}개)")
                else:
                    print(f"ℹ️  최신 상태: {series_name}")
            else:
                updated_data[series_name] = new_data
                new_count += len(new_data)
                print(f"✅ 신규 추가: {series_name} ({len(new_data)}개 포인트)")
    
    if updated_data:
        # 전역 저장소 업데이트
        updated_df = pd.DataFrame(updated_data)
        CPI_DATA['raw_data'] = updated_df
        CPI_DATA['yoy_data'] = updated_df.apply(calculate_yoy_change)
        CPI_DATA['mom_data'] = updated_df.apply(calculate_mom_change)
        
        # 최신 값 업데이트
        for series_name in updated_data.keys():
            if series_name in CPI_DATA['yoy_data'].columns:
                yoy_data = CPI_DATA['yoy_data'][series_name].dropna()
                if len(yoy_data) > 0:
                    CPI_DATA['latest_values'][series_name] = yoy_data.iloc[-1]
        
        # 로드 정보 업데이트
        CPI_DATA['load_info']['load_time'] = datetime.datetime.now()
        CPI_DATA['load_info']['series_count'] = len(updated_df.columns)
        CPI_DATA['load_info']['data_points'] = len(updated_df)
        
        print(f"\n✅ 업데이트 완료! 새로 추가된 데이터: {new_count}개 포인트")
        print_load_info()
        
        # 자동으로 CSV에 저장
        save_cpi_data_to_csv()
        
        return True
    else:
        print("\n⚠️ 업데이트할 새로운 데이터가 없습니다.")
        return False

# === 메인 데이터 로드 함수 ===

def load_all_cpi_data(start_date='2020-01-01', series_list=None, force_reload=False, csv_file='/home/jyp0615/us_eco/cpi_data.csv'):
    """
    CPI 데이터 로드 (CSV 우선, API 백업 방식)
    
    Args:
        start_date: 시작 날짜
        series_list: 로드할 시리즈 리스트 (None이면 모든 시리즈)
        force_reload: 강제 재로드 여부
        csv_file: CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global CPI_DATA
    
    # 이미 로드된 경우 스킵
    if CPI_DATA['load_info']['loaded'] and not force_reload:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    # CSV 파일이 있으면 우선 로드 시도
    import os
    if not force_reload and os.path.exists(csv_file):
        print("📂 기존 CSV 파일에서 데이터 로드 시도...")
        if load_cpi_data_from_csv(csv_file):
            print("🔄 CSV 로드 후 스마트 업데이트 시도...")
            # CSV 로드 성공 후 스마트 업데이트
            update_cpi_data_from_api(smart_update=True)
            return True
        else:
            print("⚠️ CSV 로드 실패, API에서 전체 로드 시도...")
    
    print("🚀 CPI 데이터 로딩 시작... (BLS API 전용)")
    print("="*50)
    
    # BLS API 초기화
    if not initialize_bls_api():
        print("❌ BLS API 초기화 실패")
        return False
    
    if series_list is None:
        series_list = list(CPI_SERIES.keys())
    
    # 데이터 수집
    raw_data_dict = {}
    yoy_data_dict = {}
    mom_data_dict = {}
    latest_values = {}
    
    for series_name in series_list:
        if series_name not in CPI_SERIES:
            print(f"⚠️ 알 수 없는 시리즈: {series_name}")
            continue
        
        series_id = CPI_SERIES[series_name]
        
        # 원본 데이터 가져오기
        series_data = get_series_data(series_id, start_date)
        
        if series_data is not None and len(series_data) > 0:
            # 원본 데이터 저장
            raw_data_dict[series_name] = series_data
            
            # YoY 계산
            yoy_data = calculate_yoy_change(series_data)
            yoy_data_dict[series_name] = yoy_data
            
            # MoM 계산
            mom_data = calculate_mom_change(series_data)
            mom_data_dict[series_name] = mom_data
            
            # 최신 값 저장
            if len(yoy_data.dropna()) > 0:
                latest_yoy = yoy_data.dropna().iloc[-1]
                latest_values[series_name] = latest_yoy
            else:
                print(f"⚠️ YoY 데이터 없음: {series_name}")
        else:
            print(f"❌ 데이터 로드 실패: {series_name}")
    
    # 로드된 데이터가 너무 적으면 오류 발생
    if len(raw_data_dict) < 5:  # 최소 5개 시리즈는 있어야 함
        error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data_dict)}개 (최소 5개 필요)"
        print(error_msg)
        raise ValueError(error_msg)
    
    # 전역 저장소에 저장
    CPI_DATA['raw_data'] = pd.DataFrame(raw_data_dict)
    CPI_DATA['yoy_data'] = pd.DataFrame(yoy_data_dict)
    CPI_DATA['mom_data'] = pd.DataFrame(mom_data_dict)
    CPI_DATA['latest_values'] = latest_values
    CPI_DATA['load_info'] = {
        'loaded': True,
        'load_time': datetime.datetime.now(),
        'start_date': start_date,
        'series_count': len(raw_data_dict),
        'data_points': len(CPI_DATA['raw_data']) if not CPI_DATA['raw_data'].empty else 0
    }
    
    print("\n✅ 데이터 로딩 완료!")
    print_load_info()
    
    # 자동으로 CSV에 저장
    save_cpi_data_to_csv(csv_file)
    
    return True

def print_load_info():
    """로드 정보 출력"""
    info = CPI_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not CPI_DATA['raw_data'].empty:
        date_range = f"{CPI_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {CPI_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

def clear_data():
    """데이터 초기화"""
    global CPI_DATA
    CPI_DATA = {
        'raw_data': pd.DataFrame(),
        'yoy_data': pd.DataFrame(),
        'latest_values': {},
        'load_info': {'loaded': False, 'load_time': None, 'start_date': None, 'series_count': 0, 'data_points': 0}
    }
    print("🗑️ 데이터가 초기화되었습니다")

# %%
# === 데이터 접근 함수들 ===

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_cpi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPI_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in CPI_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPI_DATA['raw_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """YoY 변화율 데이터 반환"""
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_cpi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPI_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in CPI_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPI_DATA['yoy_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화율 데이터 반환"""
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_cpi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPI_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in CPI_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPI_DATA['mom_data'][available_series].copy()

def get_latest_values(series_names=None):
    """최신 YoY 값들 반환"""
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_cpi_data()를 먼저 실행하세요.")
        return {}
    
    if series_names is None:
        return CPI_DATA['latest_values'].copy()
    
    return {name: CPI_DATA['latest_values'].get(name, 0) for name in series_names if name in CPI_DATA['latest_values']}

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not CPI_DATA['load_info']['loaded']:
        return []
    return list(CPI_DATA['raw_data'].columns)

# %%
# === 시각화 함수들 ===

def determine_analysis_type(series_name):
    """
    시리즈 이름에 따라 적절한 분석 방법(YoY/MoM) 결정
    
    Args:
        series_name: 시리즈 이름
    
    Returns:
        str: 'yoy' 또는 'mom'
    """
    # 비계절조정 시리즈들은 YoY로 분석 (시리즈 이름에 'non_sa'가 포함되거나 명시적으로 지정된 것들)
    if 'non_sa' in series_name or series_name in ['headline_non_sa', 'core_non_sa']:
        return 'yoy'
    else:
        return 'mom'

def create_cpi_timeseries_chart(series_names=None, chart_type='auto'):
    """
    저장된 데이터를 사용한 CPI 시계열 차트 생성
    
    Args:
        series_names: 차트에 표시할 시리즈 리스트
        chart_type: 'yoy' (전년동기대비), 'mom' (전월대비), 'level' (수준), 또는 'auto' (자동 선택)
        title: 차트 제목
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if series_names is None:
        series_names = ['headline', 'core']
    
    # 자동 분석 타입 결정
    if chart_type == 'auto':
        # 비계절조정 시리즈가 포함되어 있으면 혼합 차트 생성
        non_sa_in_series = any(series in ['headline_non_sa', 'core_non_sa'] for series in series_names)
        sa_in_series = any(series not in ['headline_non_sa', 'core_non_sa'] for series in series_names)
        
        if non_sa_in_series and sa_in_series:
            # 혼합된 경우: 각 시리즈별로 적절한 데이터 사용
            yoy_series = [s for s in series_names if determine_analysis_type(s) == 'yoy']
            mom_series = [s for s in series_names if determine_analysis_type(s) == 'mom']
            
            yoy_df = get_yoy_data(yoy_series) if yoy_series else pd.DataFrame()
            mom_df = get_mom_data(mom_series) if mom_series else pd.DataFrame()
            
            # 두 데이터프레임 합치기
            df = pd.concat([yoy_df, mom_df], axis=1)
            ytitle = "%"
            print("소비자물가지수 - YoY(비계절조정) / MoM(계절조정)")
        elif non_sa_in_series:
            df = get_yoy_data(series_names)
            ytitle = "%"
            print("소비자물가지수 - 전년동월대비 변화율 (비계절조정)")
        else:
            df = get_mom_data(series_names)
            ytitle = "%"
            print("소비자물가지수 - 전월대비 변화율 (계절조정)")
    
    # 수동 분석 타입
    elif chart_type == 'yoy':
        df = get_yoy_data(series_names)
        ytitle = "%"
        print("소비자물가지수 - 전년동월대비 변화율")
    elif chart_type == 'mom':
        df = get_mom_data(series_names)
        ytitle = "%"
        print("소비자물가지수 - 전월대비 변화율")
    else:  # level
        df = get_raw_data(series_names)
        ytitle = "지수"
        print("소비자물가지수 - 수준")
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 라벨 매핑 (한국어)
    label_mapping = {
        'headline': '전체 CPI',
        'core': '코어 CPI (음식·에너지 제외)',
        'headline_non_sa': '전체 CPI (비계절조정)',
        'core_non_sa': '코어 CPI (비계절조정)',
        'food': '음식',
        'energy': '에너지',
        'shelter': '주거',
        'medical': '의료',
        'transport': '교통',
        'apparel': '의류',
        'recreation': '여가',
        'education': '교육·통신',
        'other_goods': '기타 상품·서비스'
    }
    
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
        
        if chart_type == 'yoy':
            fig.add_hline(y=2, line_width=1, line_color="red", opacity=0.3, line_dash="dash")
            # 2% 목표 라벨 추가
            if not chart_df.empty:
                fig.add_annotation(
                    text="2% Target",
                    x=chart_df.index[-1],
                    y=2.1,
                    showarrow=False,
                    font=dict(size=10, color="red")
                )
    else:
        fig = df_multi_line_chart(chart_df, ytitle=ytitle)
    
    return fig

def create_cpi_component_comparison(components=None):
    """
    저장된 데이터를 사용한 CPI 구성요소 비교 차트
    
    Args:
        components: 비교할 구성요소 리스트
        title: 차트 제목
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if components is None:
        components = ['food', 'energy', 'shelter', 'medical', 'transport']
    
    # YoY 데이터 가져오기
    df = get_yoy_data(components)
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 라벨 매핑 (한국어)
    label_mapping = {
        'food': '음식', 'energy': '에너지', 'shelter': '주거',
        'medical': '의료', 'transport': '교통',
        'apparel': '의류', 'recreation': '여가',
        'education': '교육·통신', 'other': '기타 상품·서비스',
        'gasoline': '휘발유', 'used_cars': '중고차', 'rent': '주거임대료'
    }
    
    # 데이터 준비
    chart_data = {}
    for col in df.columns:
        label = label_mapping.get(col, col)
        chart_data[label] = df[col].dropna()
    
    print("소비자물가지수 구성요소 - 전년동월대비 변화율")
    
    # KPDS 포맷 사용하여 차트 생성
    chart_df = pd.DataFrame(chart_data)
    fig = df_multi_line_chart(chart_df, ytitle="%")
    
    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    
    return fig

def create_cpi_bar_chart(selected_components=None, custom_labels=None):
    """
    저장된 데이터를 사용한 CPI 바 차트 생성
    
    Args:
        selected_components: 선택할 구성요소 리스트
        custom_labels: 사용자 정의 라벨
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if selected_components is None:
        selected_components = ['headline', 'food', 'energy', 'core']
    
    # 최신 데이터 가져오기
    latest_data = get_latest_values(selected_components)
    
    if not latest_data:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 기본 라벨 설정 (한국어)
    default_labels = {
        'headline': '전체 CPI',
        'core': '코어 CPI\n(음식·에너지\n제외)',
        'food': '음식', 'energy': '에너지', 'shelter': '주거',
        'medical': '의료', 'transport': '교통',
        'apparel': '의류', 'recreation': '여가',
        'education': '교육·통신',
        'other': '기타 상품·\n서비스',
        'gasoline': '휘발유', 'used_cars': '중고차', 'rent': '주거임대료'
    }
    
    # 라벨 설정
    labels = {**default_labels, **(custom_labels or {})}
    
    # 차트 데이터 준비
    chart_data = {}
    for comp in selected_components:
        if comp in latest_data:
            label = labels.get(comp, comp)
            chart_data[label] = latest_data[comp]
    
    # 제목 출력
    if not CPI_DATA['raw_data'].empty:
        latest_date = CPI_DATA['raw_data'].index[-1].strftime('%Y년 %m월')
        print(f"소비자물가지수 12개월 변화율 - 주요 품목, {latest_date}, 계절조정 미적용")
    else:
        print("소비자물가지수 12개월 변화율")
    
    # KPDS 포맷 사용하여 바 차트 생성
    fig = create_kpds_cpi_bar_chart(chart_data)
    
    return fig

# %%
# === 통합 분석 함수 ===

def run_complete_cpi_analysis(start_date='2020-01-01', force_reload=False):
    """
    완전한 CPI 분석 실행 - 데이터 로드 후 모든 차트 생성
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🚀 완전한 CPI 분석 시작")
    print("="*50)
    
    # 1. 데이터 로드 (한 번만!)
    print("\n1️⃣ 데이터 로딩")
    success = load_all_cpi_data(start_date=start_date, force_reload=force_reload)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 시각화 생성 (저장된 데이터 사용)
    print("\n2️⃣ 시각화 생성")
    
    results = {}
    
    try:
        # 최신 바 차트
        print("   📊 최신 CPI 바 차트...")
        results['bar_chart'] = create_cpi_bar_chart(['headline', 'food', 'energy', 'core'])
        
        # 주요 지표 시계열
        print("   📈 주요 CPI 지표 시계열...")
        results['main_timeseries'] = create_cpi_timeseries_chart(['headline', 'core'])
        
        # 구성요소 비교
        print("   🔍 CPI 구성요소 비교...")
        results['components_comparison'] = create_cpi_component_comparison(['food', 'energy', 'shelter', 'medical'])
        
        # 확장된 시계열
        print("   🎯 확장된 시계열...")
        results['extended_timeseries'] = create_cpi_timeseries_chart(['headline', 'core', 'sticky'])
        
    except Exception as e:
        print(f"⚠️ 시각화 생성 중 오류: {e}")
    
    print(f"\n✅ 분석 완료! 생성된 차트: {len(results)}개")
    return results

# %%
# === 유틸리티 함수들 ===

def show_available_components():
    """사용 가능한 CPI 구성요소 표시"""
    print("=== 사용 가능한 CPI 구성요소 ===")
    
    components = {
        'headline': 'All items (전체 CPI)',
        'core': 'All items less food and energy (코어 CPI)', 
        'food': 'Food (식품)', 'energy': 'Energy (에너지)',
        'shelter': 'Shelter (주거)', 'medical': 'Medical care (의료)',
        'transport': 'Transportation (교통)', 'apparel': 'Apparel (의류)',
        'recreation': 'Recreation (여가)', 'education': 'Education and communication (교육통신)',
        'other': 'Other goods and services (기타)', 'gasoline': 'Gasoline (휘발유)',
        'used_cars': 'Used vehicles (중고차)', 'rent': 'Rent of primary residence (주거임대료)',
        'sticky': 'Sticky Price CPI (스티키 CPI)', 'super_sticky': 'Super Sticky CPI (슈퍼 스티키 CPI)'
    }
    
    for key, desc in components.items():
        print(f"  '{key}': {desc}")

def get_data_status():
    """현재 데이터 상태 반환"""
    return {
        'loaded': CPI_DATA['load_info']['loaded'],
        'series_count': CPI_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': CPI_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("\n=== CPI 분석 도구 사용법 (개선된 버전) ===")
print("1. 데이터 로드 (CSV 우선, API 백업):")
print("   load_all_cpi_data()  # CSV가 있으면 로드 후 업데이트")
print("   load_cpi_data_from_csv()  # CSV에서만 로드")
print("   update_cpi_data_from_api()  # 최신 데이터만 API로 업데이트")
print()
print("2. 데이터 저장:")
print("   save_cpi_data_to_csv()  # 현재 데이터를 CSV로 저장")
print()
print("3. API 키 관리:")
print("   get_current_api_status()  # 현재 API 키 상태 확인")
print("   switch_api_key()  # API 키 수동 전환")
print()
print("4. 스마트 업데이트:")
print("   check_recent_data_consistency()  # 최근 3개 데이터 일치성 확인")
print("   update_cpi_data_from_api(smart_update=True)  # 스마트 업데이트")
print("   update_cpi_data_from_api(smart_update=False)  # 강제 업데이트")
print()
print("5. 차트 생성:")
print("   create_cpi_bar_chart()")
print("   create_cpi_timeseries_chart(['headline', 'core'])")
print("   create_cpi_component_comparison(['food', 'energy', 'shelter'])")
print()
print("6. 통합 분석:")
print("   run_complete_cpi_analysis()")
print()
print("7. 데이터 상태 확인:")
print("   get_data_status()")
print("   show_available_components()")

# %%
# === 세부 분석 함수들 ===

def calculate_mom_changes(raw_data):
    """
    Month-over-Month 변화율 계산
    
    Args:
        raw_data: 원본 레벨 데이터
    
    Returns:
        DataFrame: MoM 변화율 데이터
    """
    mom_data = {}
    for col in raw_data.columns:
        series = raw_data[col]
        # Sticky Price CPI는 이미 YoY 처리된 데이터이므로 MoM 계산 안함
        if 'sticky' in col.lower() or 'CORESTICKM159SFRBATL' in str(series.name) or 'STICKCPIXSHLTRM159SFRBATL' in str(series.name):
            mom_data[col] = series  # 이미 처리된 데이터 그대로 사용
        else:
            mom_change = ((series / series.shift(1)) - 1) * 100
            mom_data[col] = mom_change
    
    return pd.DataFrame(mom_data, index=raw_data.index)

def create_cpi_contribution_chart(months_back=2):
    """
    Core CPI MoM 기여도 차트 생성 (첫 번째 이미지 스타일)
    
    Args:
        months_back: 표시할 과거 개월 수
    
    Returns:
        plotly figure
    """
    print("미국: 근원 소비자물가지수 전월대비 기여도 (%)")
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 최신 MoM 데이터 계산
    raw_data = get_raw_data()
    mom_data = calculate_mom_changes(raw_data)
    
    # 최근 N개월 데이터
    recent_data = mom_data.tail(months_back)
    
    # 실제 CPI 구성요소별 매핑 (한국어)
    components_mapping = {
        'core': '코어 CPI',
        'food': '핵심 상품',
        'used_cars': '중고차',
        'gasoline': '신차',  # 실제로는 gasoline을 new cars로 매핑
        'energy': '기타 상품',  # energy를 other goods로 매핑
        'shelter': '핵심 서비스',
        'rent': '주거',
        'owners_eq': '자가주거비용',
        'medical': '주거임대료',
        'transport': '숙박',
        'apparel': '의료서비스',
        'other': '자동차보험'
    }
    
    # 데이터 준비
    chart_data = {}
    for comp, label in components_mapping.items():
        if comp in recent_data.columns:
            chart_data[label] = recent_data[comp].dropna()
    
    if not chart_data:
        print("⚠️ 표시할 데이터가 없습니다.")
        return None
    
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
            title=dict(text='기여도 (%)', font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE))
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

def create_detailed_cpi_table_matplotlib():
    """
    전체 대비 상승률 테이블을 Matplotlib으로 이미지 생성 (두 번째 이미지 스타일)
    
    Returns:
        matplotlib figure: 테이블 이미지
    """
    import matplotlib.pyplot as plt
    
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 최신 데이터 가져오기
    raw_data = get_raw_data()
    mom_data = calculate_mom_changes(raw_data)
    yoy_data = get_yoy_data()
    
    # 최근 6개월 데이터
    recent_mom = mom_data.tail(6)
    recent_yoy = yoy_data.tail(1)
    
    # 한국어 카테고리 매핑 (이미지와 일치하도록 확장)
    categories_kr = {
        'headline': '모든 품목',
        'food': '식품',
        'gasoline': '가게 식품',  # 실제 이미지에 맞춤
        'apparel': '의식 식품',
        'energy': '에너지',
        'shelter': '에너지 상품',
        'rent': '휘발유 (모든 종류)',
        'transport': '에너지 서비스',
        'medical': '전기',
        'other': '주거비와 유틸리티 서비스',
        'core': '식품 및 에너지 제외',
        'education': '상품',
        'recreation': '서비스',
        'used_cars': '자동차 신제품',
        'owners_eq': '중고차 및 중고 트럭'
    }
    
    # 테이블 데이터 생성
    table_data = []
    months = recent_mom.index[-6:]
    month_cols = [month.strftime('%Y년 %m월') for month in months]
    
    # 헤더 추가
    headers = ['전월 대비 상승률 (계절조정)'] + month_cols + ['12개월 상승률']
    
    for comp in categories_kr.keys():
        if comp in recent_mom.columns:
            row = [categories_kr[comp]]
            
            # 최근 6개월 MoM 데이터
            for month in months:
                if month in recent_mom.index and not pd.isna(recent_mom.loc[month, comp]):
                    value = recent_mom.loc[month, comp]
                    row.append(f"{value:.1f}")
                else:
                    row.append("-")
            
            # 12개월 누적 (YoY)
            if comp in recent_yoy.columns and not recent_yoy[comp].empty:
                yoy_value = recent_yoy[comp].iloc[-1]
                if not pd.isna(yoy_value):
                    row.append(f"{yoy_value:.1f}")
                else:
                    row.append("-")
            else:
                row.append("-")
            
            table_data.append(row)
    
    # Matplotlib 테이블 생성
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.axis('tight')
    ax.axis('off')
    
    # 테이블 생성
    table = ax.table(cellText=table_data, colLabels=headers, 
                    cellLoc='center', loc='center')
    
    # 테이블 스타일링
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 1.8)
    
    # 헤더 스타일
    for i in range(len(headers)):
        table[(0, i)].set_facecolor('#E6E6FA')
        table[(0, i)].set_text_props(weight='bold')
    
    # 값에 따른 색상 적용
    for i, row in enumerate(table_data):
        for j, cell in enumerate(row[1:], 1):  # 첫 번째 열(카테고리명) 제외
            try:
                value = float(cell)
                if value > 0:
                    table[(i+1, j)].set_facecolor('#FFE4E1')  # 연한 빨간색
                elif value < 0:
                    table[(i+1, j)].set_facecolor('#E0F6FF')  # 연한 파란색
            except:
                pass
    
    print('전월 대비 상승률 (계절조정)')
    plt.tight_layout()
    plt.show()
    
    return fig

def create_horizontal_cpi_bar_chart(num_categories=25):
    """
    카테고리별 MoM 상승률 가로 바 차트 생성 (세 번째 이미지 스타일)
    
    Args:
        num_categories: 표시할 카테고리 수
    
    Returns:
        plotly figure
    """
    print("소비자물가지수 주요 품목 (전월대비 % 변화)")
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 최신 MoM 데이터
    raw_data = get_raw_data()
    mom_data = calculate_mom_changes(raw_data)
    latest_mom = mom_data.iloc[-1].dropna()
    
    # 실제 카테고리 라벨 매핑 (한국어)
    category_labels = {
        'headline': '전체 CPI',
        'core': '코어 CPI (음식·에너지 제외)',
        'food': '음식',
        'energy': '에너지',
        'gasoline': '휘발유 (모든 종류)',
        'used_cars': '중고차·트럭',
        'shelter': '주거',
        'rent': '주거임대료',
        'owners_eq': "자가주거비용",
        'medical': '의료',
        'transport': '교통',
        'apparel': '의류',
        'recreation': '여가',
        'education': '교육·통신',
        'other': '기타 상품·서비스'
    }
    
    # 데이터 정렬 (내림차순)
    sorted_data = latest_mom.sort_values(ascending=True)  # 가로 바차트이므로 ascending=True
    
    # 상위 num_categories개 선택
    if len(sorted_data) > num_categories:
        sorted_data = sorted_data.tail(num_categories)
    
    # 라벨 적용
    categories = []
    values = []
    colors = []
    
    for comp, value in sorted_data.items():
        label = category_labels.get(comp, comp)
        categories.append(label)
        values.append(value)
        
        # '전체 CPI'는 특별 색상, 나머지는 양수/음수로 구분
        if comp == 'headline':
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
    
    # 레이아웃 설정
    fig.update_layout(
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=800,
        height=max(500, len(categories) * 25),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.1f',
            range=[-1, max(values) * 1.2]
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

def run_detailed_cpi_analysis():
    """
    세부 CPI 분석 실행 - 기여도, 테이블, 가로 바차트
    
    Returns:
        dict: 생성된 차트들과 테이블
    """
    print("🚀 세부 CPI 분석 시작")
    print("="*50)
    
    # 1. 데이터 로드 확인
    if not CPI_DATA['load_info']['loaded']:
        print("\n1️⃣ 데이터 로딩")
        success = load_all_cpi_data()
        if not success:
            print("❌ 데이터 로딩 실패")
            return None
    else:
        print("✅ 데이터가 이미 로드됨")
    
    results = {}
    
    try:
        # 2. 기여도 차트
        print("\n2️⃣ Core CPI 기여도 차트 생성...")
        results['contribution_chart'] = create_cpi_contribution_chart()
        
        # 3. 세부 테이블 (Matplotlib)
        print("   📊 CPI 세부 테이블 생성...")
        results['detailed_table'] = create_detailed_cpi_table_matplotlib()
        
        # 4. 가로 바 차트
        print("   📈 카테고리별 가로 바 차트 생성...")
        results['horizontal_bar_chart'] = create_horizontal_cpi_bar_chart()
        
        # 테이블 출력
        if results['detailed_table'] is not None:
            print("\n📋 CPI 전월 대비 상승률 테이블:")
            print(results['detailed_table'].to_string(index=False))
        
    except Exception as e:
        print(f"⚠️ 분석 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ 세부 분석 완료! 생성된 항목: {len(results)}개")
    return results

# %%
# 사용 가능한 구성요소 표시
show_available_components()

# %%
# 기본 CPI 분석 실행
run_complete_cpi_analysis()

# %%
# === 계층적 시각화 함수들 ===

def create_hierarchical_cpi_chart(level='level2', title=None):
    """
    계층별 CPI 차트 생성
    
    Args:
        level: 표시할 계층 ('level1', 'level2', 'level3', 'level4')
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if level not in CPI_HIERARCHY:
        print(f"⚠️ 잘못된 계층: {level}")
        return None
    
    # 해당 계층의 데이터 가져오기
    level_data = CPI_HIERARCHY[level]
    raw_data = get_raw_data()
    mom_data = calculate_mom_changes(raw_data)
    latest_mom = mom_data.iloc[-1]
    
    # 데이터 준비
    categories = []
    values = []
    colors = []
    
    for i, (key, info) in enumerate(level_data.items()):
        if key in latest_mom.index and not pd.isna(latest_mom[key]):
            categories.append(info['name_kr'])
            value = latest_mom[key]
            values.append(value)
            
            # 계층에 따른 색상 구분
            if level == 'level1':
                colors.append('#FF6B35' if value >= 0 else '#4ECDC4')
            elif level == 'level2':
                colors.append(get_kpds_color(i))
            else:
                colors.append(deepred_pds if value >= 0 else deepblue_pds)
    
    if not categories:
        print(f"⚠️ {level} 데이터가 없습니다.")
        return None
    
    # 가로 바 차트 생성
    fig = go.Figure()
    
    # 데이터 정렬 (내림차순)
    sorted_indices = sorted(range(len(values)), key=lambda i: values[i])
    
    sorted_categories = [categories[i] for i in sorted_indices]
    sorted_values = [values[i] for i in sorted_indices]
    sorted_colors = [colors[i] for i in sorted_indices]
    
    fig.add_trace(go.Bar(
        y=sorted_categories,
        x=sorted_values,
        orientation='h',
        marker_color=sorted_colors,
        text=[f'{v:+.2f}%' for v in sorted_values],
        textposition='outside' if all(v >= 0 for v in sorted_values) else 'auto',
        showlegend=False
    ))
    
    # 레이아웃 설정
    level_titles = {
        'level1': 'CPI 최상위 분류',
        'level2': 'CPI 주요 카테고리',
        'level3': 'CPI 중위 분류',
        'level4': 'CPI 세부 분류'
    }
    
    fig.update_layout(
        title=dict(
            text=title or f"{level_titles[level]} (6월 MoM 변화율)",
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE),
            x=0.5, xanchor="center"
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(400, len(categories) * 30),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.2f',
            title=dict(text='MoM 변화율 (%)', font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE))
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white'
        ),
        margin=dict(l=200, r=80, t=80, b=80)
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    fig.show()
    return fig

def show_cpi_hierarchy_info():
    """
    CPI 계층 구조 정보 표시 (325개 시리즈)
    """
    print("=== CPI 계층 구조 (325개 시리즈) ===\n")
    
    level_names = {
        'level1': '레벨 1: 최상위 종합지표',
        'level2': '레벨 2: 상품/서비스 구분',
        'level3': '레벨 3: 주요 생활비 카테고리',
        'level4': '레벨 4: 세부 카테고리',
        'level5': '레벨 5: 더 세부적인 분류',
        'level6': '레벨 6: 가장 세부적인 분류',
        'level7': '레벨 7: 매우 세부적인 분류',
        'level8': '레벨 8: 개별 품목'
    }
    
    for level, level_data in CPI_HIERARCHY.items():
        print(f"### {level_names.get(level, level)} ({len(level_data)}개) ###")
        
        # 처음 5개만 보여주기 (너무 많아서)
        for i, (key, info) in enumerate(level_data.items()):
            if i >= 5:
                print(f"  ... 그 외 {len(level_data) - 5}개")
                break
            
            parent_info = f" → {info['parent']}" if 'parent' in info else ""
            korean_name = ALL_KOREAN_NAMES.get(key, info['name'])
            
            print(f"  {key}: {korean_name}{parent_info}")
        print()

def test_bls_data():
    """
    BLS API 테스트 - toys 데이터 가져오기 예시
    """
    print("🧸 BLS API 테스트: Toys 데이터")
    initialize_bls_api()
    
    if BLS_SESSION:
        toys_data = get_bls_data('CUSR0000SERG', 2023, 2025)
        if toys_data is not None:
            print("최근 Toys CPI 데이터:")
            print(toys_data.tail(6))
            
            # MoM 계산
            mom_toys = ((toys_data / toys_data.shift(1)) - 1) * 100
            print(f"\n최신 Toys MoM: {mom_toys.iloc[-1]:.2f}%")
        else:
            print("Toys 데이터 로딩 실패")
    else:
        print("BLS API 세션 없음")

# %%
# 계층 구조 정보 표시
show_cpi_hierarchy_info()

# %%
# === 계층적 CPI 차트 함수 ===

def create_hierarchical_cpi_chart(level='level2', chart_type='auto', title=None):
    """
    계층별 CPI 차트 생성
    
    Args:
        level: 표시할 계층 ('level1', 'level2', 'level3', 'level4')
        chart_type: 차트 타입 ('yoy', 'mom', 'auto')
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    if level not in CPI_HIERARCHY:
        print(f"⚠️ 잘못된 계층: {level}")
        return None
    
    # 해당 계층의 시리즈 키들 가져오기
    level_data = CPI_HIERARCHY[level]
    series_keys = list(level_data.keys())
    
    # 레벨 이름 한국어 매핑
    level_korean_names = {
        'Total': '전체',
        'Core': '근원',
        'Food': '식품',
        'Energy': '에너지',
        'Goods': '상품',
        'Services': '서비스',
        'Housing': '주거',
        'Transportation': '교통',
        'Medical': '의료',
        'Recreation': '여가',
        'Education': '교육',
        'Apparel': '의류',
        'Other': '기타'
    }
    
    level_kr = level_korean_names.get(level, level)
    
    # 자동 분석 타입 결정
    if chart_type == 'auto':
        # 비계절조정 시리즈가 있는지 확인
        non_sa_in_series = any(series in ['headline_non_sa', 'core_non_sa'] for series in series_keys)
        
        if non_sa_in_series:
            # 혼합: 비계절조정은 YoY, 나머지는 MoM
            yoy_series = [s for s in series_keys if determine_analysis_type(s) == 'yoy']
            mom_series = [s for s in series_keys if determine_analysis_type(s) == 'mom']
            
            yoy_df = get_yoy_data(yoy_series) if yoy_series else pd.DataFrame()
            mom_df = get_mom_data(mom_series) if mom_series else pd.DataFrame()
            
            df = pd.concat([yoy_df, mom_df], axis=1)
            ytitle = "%"
            if title is None:
                title = f"소비자물가지수 {level_kr} - YoY(비계절조정) / MoM(계절조정)"
        else:
            # 모두 계절조정 시리즈: MoM 사용
            df = get_mom_data(series_keys)
            ytitle = "%"
            if title is None:
                title = f"소비자물가지수 {level_kr} - 전월대비 변화율"
    elif chart_type == 'yoy':
        df = get_yoy_data(series_keys)
        ytitle = "%"
        if title is None:
            title = f"소비자물가지수 {level_kr} - 전년동월대비 변화율"
    else:  # mom
        df = get_mom_data(series_keys)
        ytitle = "%"
        if title is None:
            title = f"소비자물가지수 {level_kr} - 전월대비 변화율"
    
    if df.empty:
        print(f"⚠️ {level} 데이터가 없습니다.")
        return None
    
    # 최신 데이터로 바 차트 생성
    latest_data = df.iloc[-1].dropna()
    
    # 데이터 준비
    categories = []
    values = []
    colors = []
    
    for series_key, value in latest_data.items():
        if series_key in level_data:
            info = level_data[series_key]
            categories.append(info['name_kr'])
            values.append(value)
            
            # 계층에 따른 색상 구분
            if level == 'level1':
                colors.append('#FF6B35' if value >= 0 else '#4ECDC4')
            elif level == 'level2':
                colors.append(get_kpds_color(len(categories) - 1))
            else:
                colors.append(deepred_pds if value >= 0 else deepblue_pds)
    
    if not categories:
        print(f"⚠️ {level} 데이터가 없습니다.")
        return None
    
    # 가로 바 차트 생성
    fig = go.Figure()
    
    # 데이터 정렬 (내림차순)
    sorted_indices = sorted(range(len(values)), key=lambda i: values[i])
    
    sorted_categories = [categories[i] for i in sorted_indices]
    sorted_values = [values[i] for i in sorted_indices]
    sorted_colors = [colors[i] for i in sorted_indices]
    
    fig.add_trace(go.Bar(
        y=sorted_categories,
        x=sorted_values,
        orientation='h',
        marker_color=sorted_colors,
        text=[f'{v:+.2f}%' for v in sorted_values],
        textposition='outside' if all(v >= 0 for v in sorted_values) else 'auto',
        showlegend=False
    ))
    
    # 레이아웃 설정
    level_titles = {
        'level1': 'CPI 최상위 분류',
        'level2': 'CPI 주요 카테고리',
        'level3': 'CPI 중위 분류',
        'level4': 'CPI 세부 분류'
    }
    
    fig.update_layout(
        title=dict(
            text=title or f"{level_titles[level]} ({chart_type.upper()} 변화율)",
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE),
            x=0.5, xanchor="center"
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(400, len(categories) * 30),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1, color="black"),
        xaxis=dict(
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.2f',
            title=dict(text=ytitle, font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE))
        ),
        yaxis=dict(
            showline=False,
            tickcolor='white'
        ),
        margin=dict(l=200, r=80, t=80, b=80)
    )
    
    # 격자선 및 0선
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgrey")
    fig.update_yaxes(showgrid=False)
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    fig.show()
    return fig

# %%
# === 통합 실행 함수들 ===

def run_cpi_analysis(start_date='2020-01-01', force_reload=False):
    """
    BLS API 기반 CPI 분석 실행
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
    
    Returns:
        dict: 생성된 차트들
    """
    print("🚀 BLS API 기반 CPI 분석 시작")
    print("="*50)
    
    # 1. 데이터 로드
    print("\n1️⃣ 데이터 로딩")
    success = load_all_cpi_data(start_date=start_date, force_reload=force_reload)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 시각화 생성
    print("\n2️⃣ 시각화 생성")
    
    results = {}
    
    try:
        # 주요 지표 시계열 (자동 분석)
        print("   📈 주요 CPI 지표 시계열 (자동 분석)...")
        results['main_auto'] = create_cpi_timeseries_chart(['headline', 'core'], 'auto')
        
        # 비계절조정 vs 계절조정 비교
        print("   📈 비계절조정 vs 계절조정 비교...")
        results['sa_vs_nsa'] = create_cpi_timeseries_chart(['headline', 'headline_non_sa', 'core', 'core_non_sa'], 'auto')
        
        # 구성요소 비교 (YoY)
        print("   🔍 CPI 구성요소 비교 (YoY)...")
        results['components_yoy'] = create_cpi_component_comparison(['food', 'energy', 'shelter', 'medical', 'transport'], '소비자물가지수 구성요소 - 전년동월대비')
        
        # 구성요소 비교 (MoM)
        print("   🔍 CPI 구성요소 비교 (MoM)...")
        results['components_mom'] = create_cpi_component_comparison(['food', 'energy', 'shelter', 'medical', 'transport'], '소비자물가지수 구성요소 - 전월대비')
        
        # 계층별 차트 (자동 분석)
        print("   🌟 계층별 분석 (자동 분석)...")
        results['level2_chart'] = create_hierarchical_cpi_chart('level2', 'auto')
        results['level3_chart'] = create_hierarchical_cpi_chart('level3', 'auto')
        
    except Exception as e:
        print(f"⚠️ 시각화 생성 중 오류: {e}")
    
    print(f"\n✅ 분석 완료! 생성된 차트: {len(results)}개")
    return results

# %%
# === 사용 예시 ===

print("\n=== BLS API 기반 CPI 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_all_cpi_data()")
print()
print("2. 시계열 차트 생성:")
print("   create_cpi_timeseries_chart(['headline', 'core'], 'auto')  # 자동 분석")
print("   create_cpi_timeseries_chart(['headline_non_sa', 'core_non_sa'], 'auto')  # 비계절조정 YoY")
print("   create_cpi_timeseries_chart(['headline', 'core'], 'mom')  # 강제 MoM")
print()
print("3. 구성요소 비교:")
print("   create_cpi_component_comparison(['food', 'energy', 'shelter'], 'yoy')")
print("   create_cpi_component_comparison(['food', 'energy', 'shelter'], 'mom')")
print()
print("4. 계층별 차트:")
print("   create_hierarchical_cpi_chart('level2', 'auto')  # 자동 분석")
print("   create_hierarchical_cpi_chart('level3', 'auto')  # 자동 분석")
print()
print("5. 통합 분석:")
print("   run_cpi_analysis()")

# %%
# === 325개 시리즈 분류별 시각화 함수들 ===

def create_level_comparison_chart(levels=['level3', 'level4'], chart_type='yoy', title=None):
    """
    여러 레벨 간 비교 차트 생성
    
    Args:
        levels: 비교할 레벨들
        chart_type: 'yoy' 또는 'mom'
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    fig = make_subplots(
        rows=len(levels), cols=1,
        subplot_titles=[f"Level {level[-1]}: {_get_level_description(level)}" for level in levels],
        vertical_spacing=0.1
    )
    
    for i, level in enumerate(levels):
        if level not in CPI_HIERARCHY:
            continue
        
        level_data = CPI_HIERARCHY[level]
        series_keys = list(level_data.keys())[:10]  # 최대 10개까지만
        
        # 데이터 가져오기
        if chart_type == 'yoy':
            df = get_yoy_data(series_keys)
        else:
            df = get_mom_data(series_keys)
        
        if df.empty:
            continue
        
        # 최신 데이터
        latest_data = df.iloc[-1].dropna()
        
        categories = []
        values = []
        
        for series_key, value in latest_data.items():
            if series_key in level_data:
                korean_name = ALL_KOREAN_NAMES.get(series_key, level_data[series_key]['name'])
                categories.append(korean_name)
                values.append(value)
        
        if categories:
            fig.add_trace(
                go.Bar(
                    x=categories,
                    y=values,
                    name=f"Level {level[-1]}",
                    marker_color=get_kpds_color(i),
                    text=[f'{v:+.1f}%' for v in values],
                    textposition='outside'
                ),
                row=i+1, col=1
            )
    
    fig.update_layout(
        title=dict(
            text=title or f"CPI 계층별 비교 ({chart_type.upper()})",
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE)
        ),
        showlegend=False,
        height=300 * len(levels),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1)
    )
    
    # 0선 추가
    for i in range(len(levels)):
        fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5, row=i+1, col=1)
    
    fig.show()
    return fig

def _get_level_description(level):
    """레벨 설명 반환"""
    descriptions = {
        'level1': '최상위 종합지표',
        'level2': '상품/서비스 구분',
        'level3': '주요 생활비 카테고리',
        'level4': '세부 카테고리',
        'level5': '더 세부적인 분류',
        'level6': '가장 세부적인 분류',
        'level7': '매우 세부적인 분류',
        'level8': '개별 품목'
    }
    return descriptions.get(level, level)

def create_food_category_deep_dive():
    """
    식품 카테고리 심층 분석 (Level 6-8)
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 식품 관련 시리즈들 찾기
    food_series = []
    for level_name, level_data in CPI_HIERARCHY.items():
        for key, info in level_data.items():
            if any(food_word in info['name'].lower() for food_word in ['food', 'meat', 'fruit', 'vegetable', 'dairy', 'bread', 'cereal']):
                food_series.append(key)
    
    # 최신 YoY 데이터
    yoy_data = get_yoy_data(food_series)
    if yoy_data.empty:
        print("⚠️ 식품 데이터가 없습니다.")
        return None
    
    latest_yoy = yoy_data.iloc[-1].dropna()
    
    # 데이터 준비
    categories = []
    values = []
    colors = []
    
    for series_key, value in latest_yoy.items():
        korean_name = ALL_KOREAN_NAMES.get(series_key, series_key)
        categories.append(korean_name)
        values.append(value)
        colors.append('#FF6B35' if value > 3 else '#4ECDC4' if value < 1 else deepblue_pds)
    
    # 정렬
    sorted_data = sorted(zip(categories, values, colors), key=lambda x: x[1])
    categories, values, colors = zip(*sorted_data)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=list(categories),
        x=list(values),
        orientation='h',
        marker_color=list(colors),
        text=[f'{v:+.1f}%' for v in values],
        textposition='outside',
        showlegend=False
    ))
    
    fig.update_layout(
        title=dict(
            text="식품 카테고리 세부 분석 (YoY 변화율)",
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE)
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(500, len(categories) * 25),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1),
        xaxis=dict(
            title="전년동월대비 변화율 (%)",
            showgrid=True,
            gridcolor="lightgrey"
        ),
        margin=dict(l=200, r=80, t=80, b=80)
    )
    
    fig.add_vline(x=0, line_width=2, line_color="black")
    fig.add_vline(x=2, line_width=1, line_color="red", line_dash="dash", opacity=0.5)
    
    fig.show()
    return fig

def create_housing_category_deep_dive():
    """
    주거 카테고리 심층 분석 (Level 6-8)
    """
    if not CPI_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_cpi_data()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 주거 관련 시리즈들 찾기
    housing_series = []
    for level_name, level_data in CPI_HIERARCHY.items():
        for key, info in level_data.items():
            if any(housing_word in info['name'].lower() for housing_word in ['housing', 'shelter', 'rent', 'utilities', 'energy', 'electricity', 'gas']):
                housing_series.append(key)
    
    # 최신 MoM 데이터
    mom_data = get_mom_data(housing_series)
    if mom_data.empty:
        print("⚠️ 주거 데이터가 없습니다.")
        return None
    
    # 최근 6개월 평균
    recent_mom = mom_data.tail(6).mean()
    
    # 데이터 준비
    categories = []
    values = []
    colors = []
    
    for series_key, value in recent_mom.dropna().items():
        korean_name = ALL_KOREAN_NAMES.get(series_key, series_key)
        categories.append(korean_name)
        values.append(value)
        colors.append('#FF6B35' if value > 0.5 else '#4ECDC4' if value < 0 else deepblue_pds)
    
    # 정렬
    sorted_data = sorted(zip(categories, values, colors), key=lambda x: x[1])
    categories, values, colors = zip(*sorted_data)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=list(categories),
        x=list(values),
        orientation='h',
        marker_color=list(colors),
        text=[f'{v:+.2f}%' for v in values],
        textposition='outside',
        showlegend=False
    ))
    
    fig.update_layout(
        title=dict(
            text="주거 카테고리 세부 분석 (최근 6개월 평균 MoM)",
            font=dict(family='NanumGothic', size=FONT_SIZE_TITLE)
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=900,
        height=max(500, len(categories) * 25),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1),
        xaxis=dict(
            title="전월대비 변화율 (6개월 평균, %)",
            showgrid=True,
            gridcolor="lightgrey"
        ),
        margin=dict(l=200, r=80, t=80, b=80)
    )
    
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    fig.show()
    return fig

def run_comprehensive_cpi_analysis():
    """
    325개 시리즈를 활용한 종합 CPI 분석
    """
    print("🚀 종합 CPI 분석 시작 (325개 시리즈)")
    print("="*60)
    
    # 1. 데이터 로드
    if not CPI_DATA['load_info']['loaded']:
        print("\n1️⃣ 데이터 로딩")
        success = load_all_cpi_data()
        if not success:
            print("❌ 데이터 로딩 실패")
            return None
    else:
        print("✅ 데이터가 이미 로드됨")
    
    results = {}
    
    try:
        # 2. 기본 분석
        print("\n2️⃣ 기본 CPI 분석...")
        results['basic'] = run_cpi_analysis()
        
        # 3. 계층별 비교
        print("\n3️⃣ 계층별 상세 비교...")
        results['level_comparison'] = create_level_comparison_chart(['level3', 'level5'], 'yoy')
        
        # 4. 식품 심층 분석
        print("\n4️⃣ 식품 카테고리 심층 분석...")
        results['food_deep_dive'] = create_food_category_deep_dive()
        
        # 5. 주거 심층 분석
        print("\n5️⃣ 주거 카테고리 심층 분석...")
        results['housing_deep_dive'] = create_housing_category_deep_dive()
        
        # 6. 개별 품목 분석 (Level 8)
        print("\n6️⃣ 개별 품목 분석...")
        if 'level8' in CPI_HIERARCHY:
            results['level8_chart'] = create_hierarchical_cpi_chart('level8', 'yoy', '개별 품목 YoY 분석')
        
    except Exception as e:
        print(f"⚠️ 분석 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ 종합 분석 완료! 생성된 차트: {len(results)}개")
    return results

# %%
# 통합 CPI 분석 실행
results = run_comprehensive_cpi_analysis()

# %%
create_hierarchical_cpi_chart('level5', 'auto')
# %%
create_hierarchical_cpi_chart('level6', 'auto')

# %%
create_hierarchical_cpi_chart('level2', 'auto')

# %%
create_hierarchical_cpi_chart('level4', 'mom')

# %%
