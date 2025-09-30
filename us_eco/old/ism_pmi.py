# %%
import requests
import pandas as pd
from urllib.parse import urlencode
import dbnomics
import sys
import warnings
import os
warnings.filterwarnings('ignore')

# KPDS 시각화 라이브러리 불러오기 (필수)
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

print("✓ KPDS 시각화 포맷 로드됨")

# %%

def fetch_all_ism_data():
    """
    모든 ISM PMI 구성요소 데이터를 가져와서 통합 데이터프레임으로 반환
    
    각 카테고리별로 시리즈 유형이 다름:
    - pmi: pm (제조업 종합지수)
    - 일반: hi, in, lo, ne, sa (% Higher, Index, % Lower, Net, % Same)
    - supdel: fa, in, sl, ne, sa (% Faster, Index, % Slower, Net, % Same)  
    - cusinv: abri, in, tohi, tolo, ne, re (% About Right, Index, % Too High, % Too Low, Net, % Reporting)
    - buypol: 21개의 특별한 시리즈들
    """
    
    # 카테고리별 시리즈 유형 정의
    category_series = {
        'pmi': ['pm'],  # 제조업 PMI 종합지수
        'neword': ['hi', 'in', 'lo', 'ne', 'sa'],
        'production': ['hi', 'in', 'lo', 'ne', 'sa'],
        'employment': ['hi', 'in', 'lo', 'ne', 'sa'],
        'supdel': ['fa', 'in', 'sl', 'ne', 'sa'],  # Faster/Slower 대신 Higher/Lower
        'inventories': ['hi', 'in', 'lo', 'ne', 'sa'],
        'cusinv': ['abri', 'in', 'tohi', 'tolo', 'ne', 're'],  # About Right, Too High, Too Low, Reporting
        'prices': ['hi', 'in', 'lo', 'ne', 'sa'],
        'bacord': ['hi', 'in', 'lo', 'ne', 'sa'],
        'newexpord': ['hi', 'in', 'lo', 'ne', 'sa'],
        'imports': ['hi', 'in', 'lo', 'ne', 'sa'],
        'buypol': [  # Buying Policy - 웹사이트에서 확인된 실제 시리즈들
            'caex-1ye', 'caex-30da', 'caex-60da', 'caex-6mo', 'caex-90da', 'caex-avda', 'caex-hamo',
            'mrsu-1ye', 'mrsu-30da', 'mrsu-60da'
        ]
    }
    
    # 모든 데이터를 저장할 데이터프레임
    all_data = pd.DataFrame()
    
    print("ISM PMI 데이터 수집 시작...")
    
    for category, series_types in category_series.items():
        print(f"\n{category} 데이터 수집 중...")
        
        for series_type in series_types:
            try:
                # 데이터 가져오기
                data = dbnomics.fetch_series('ISM', category, series_type)[['period', 'value']]
                
                # 컬럼명 설정
                column_name = f"{category}_{series_type}"
                data.columns = ['date', column_name]
                
                # 날짜 변환
                data['date'] = pd.to_datetime(data['date'])
                data = data.set_index('date')
                
                # 기존 데이터프레임과 병합
                if all_data.empty:
                    all_data = data
                else:
                    all_data = all_data.join(data, how='outer')
                
                print(f"  {column_name}: {len(data)} 행 수집완료")
                
            except Exception as e:
                print(f"  {category}_{series_type} 데이터 수집 실패: {str(e)}")
                continue
    
    print(f"\n총 {len(all_data.columns)}개 시리즈, {len(all_data)} 행 수집완료")
    print(f"데이터 기간: {all_data.index.min()} ~ {all_data.index.max()}")
    
    return all_data

# %%
def fetch_all_nm_ism_data():
    """
    모든 ISM 비제조업(Non-Manufacturing) PMI 구성요소 데이터를 가져와서 통합 데이터프레임으로 반환
    
    각 카테고리별로 시리즈 유형이 다름:
    - nm-pmi: pm (PMI 종합지수)
    - 일반(8개): hi, in, lo, sa (% Higher, Index, % Lower, % Same)
    - nm-supdel: fa, in, sa, sl (% Faster, Index, % Same, % Slower)  
    - nm-invsen: abri, in, tohi, tolo (% About Right, Index, % Too High, % Too Low)
    """
    
    # 비제조업 카테고리별 시리즈 유형 정의
    nm_category_series = {
        'nm-pmi': ['pm'],  # PMI 종합지수
        'nm-busact': ['hi', 'in', 'lo', 'sa'],  # Business Activity
        'nm-neword': ['hi', 'in', 'lo', 'sa'],  # New Orders
        'nm-employment': ['hi', 'in', 'lo', 'sa'],  # Employment
        'nm-supdel': ['fa', 'in', 'sa', 'sl'],  # Supplier Deliveries - Faster/Slower
        'nm-inventories': ['hi', 'in', 'lo', 'sa'],  # Inventories
        'nm-prices': ['hi', 'in', 'lo', 'sa'],  # Prices
        'nm-invsen': ['abri', 'in', 'tohi', 'tolo'],  # Inventory Sentiment
        'nm-bacord': ['hi', 'in', 'lo', 'sa'],  # Backlog of Orders
        'nm-newexpord': ['hi', 'in', 'lo', 'sa'],  # New Export Orders
        'nm-imports': ['hi', 'in', 'lo', 'sa'],  # Imports
    }
    
    # 모든 데이터를 저장할 데이터프레임
    all_nm_data = pd.DataFrame()
    
    print("ISM 비제조업 PMI 데이터 수집 시작...")
    
    for category, series_types in nm_category_series.items():
        print(f"\n{category} 데이터 수집 중...")
        
        for series_type in series_types:
            try:
                # 데이터 가져오기
                data = dbnomics.fetch_series('ISM', category, series_type)[['period', 'value']]
                
                # 컬럼명 설정
                column_name = f"{category}_{series_type}"
                data.columns = ['date', column_name]
                
                # 날짜 변환
                data['date'] = pd.to_datetime(data['date'])
                data = data.set_index('date')
                
                # 기존 데이터프레임과 병합
                if all_nm_data.empty:
                    all_nm_data = data
                else:
                    all_nm_data = all_nm_data.join(data, how='outer')
                
                print(f"  {column_name}: {len(data)} 행 수집완료")
                
            except Exception as e:
                print(f"  {category}_{series_type} 데이터 수집 실패: {str(e)}")
                continue
    
    print(f"\n총 {len(all_nm_data.columns)}개 비제조업 시리즈, {len(all_nm_data)} 행 수집완료")
    print(f"데이터 기간: {all_nm_data.index.min()} ~ {all_nm_data.index.max()}")
    
    return all_nm_data


# %%
# === Enhanced ISM PMI Analysis System ===
"""
스마트 업데이트 및 투자은행 스타일 분석 기능 추가
- CSV 저장/로드 및 자동 중복 체크
- KPDS 포맷 시각화
- 경제학자/투자은행 스타일 분석 리포트
"""
# 추가 라이브러리 import는 필요시에만
# import numpy as np
# import plotly.graph_objects as go  
# from plotly.subplots import make_subplots
from datetime import datetime
import sys
import warnings
import os
import json
warnings.filterwarnings('ignore')

# 시각화 라이브러리 불러오기
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

# %%
# === ISM PMI 시리즈 ID와 이름 매핑 ===

# 제조업 PMI 한국어 이름 매핑 (명확한 구분)
ISM_KOREAN_NAMES = {
    'pmi_pm': 'PMI 종합지수 (제조업)',
    'neword_in': '신규주문 (제조업)',
    'production_in': '생산 (제조업)',
    'employment_in': '고용 (제조업)',
    'supdel_in': '공급업체 배송 (제조업)',
    'inventories_in': '재고 (제조업)',
    'prices_in': '가격 (제조업)',
    'bacord_in': '수주잔고 (제조업)',
    'newexpord_in': '신규 수출주문 (제조업)',
    'imports_in': '수입 (제조업)',
    'cusinv_in': '고객 재고 (제조업)'
}

# 비제조업 PMI 한국어 이름 매핑 (명확한 구분)
NM_ISM_KOREAN_NAMES = {
    'nm-pmi_pm': 'PMI 종합지수 (비제조업)',
    'nm-busact_in': '사업활동 (비제조업)',
    'nm-neword_in': '신규주문 (비제조업)',
    'nm-employment_in': '고용 (비제조업)',
    'nm-supdel_in': '공급업체 배송 (비제조업)',
    'nm-inventories_in': '재고 (비제조업)',
    'nm-prices_in': '가격 (비제조업)',
    'nm-bacord_in': '수주잔고 (비제조업)',
    'nm-newexpord_in': '신규 수출주문 (비제조업)',
    'nm-imports_in': '수입 (비제조업)',
    'nm-invsen_in': '재고 감정 (비제조업)'
}

# PMI 계층 구조
ISM_HIERARCHY = {
    '주요 지표': {
        'Manufacturing': ['pmi_pm', 'neword_in', 'production_in'],
        'Non-Manufacturing': ['nm-pmi_pm', 'nm-busact_in', 'nm-neword_in']
    },
    '세부 구성': {
        'Labor': ['employment_in', 'nm-employment_in'],
        'Supply Chain': ['supdel_in', 'nm-supdel_in', 'inventories_in', 'nm-inventories_in'],
        'Pricing': ['prices_in', 'nm-prices_in'],
        'Trade': ['newexpord_in', 'nm-newexpord_in', 'imports_in', 'nm-imports_in']
    }
}

# %%
# === 전역 데이터 저장소 ===

# 전역 데이터 저장소
ISM_DATA = {
    'raw_data': pd.DataFrame(),          # 원본 레벨 데이터
    'yoy_data': pd.DataFrame(),          # 전년동월대비 변화율 데이터
    'mom_data': pd.DataFrame(),          # 전월대비 변화율 데이터
    'diffusion_data': pd.DataFrame(),    # 확산지수 (50 기준선 대비)
    'latest_values': {},                 # 최신 값들
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0
    }
}

# %%
# === 스마트 업데이트 및 CSV 관리 함수들 ===

def save_ism_data_to_csv(file_path='/home/jyp0615/us_eco/ism_pmi_data.csv'):
    """
    현재 로드된 ISM PMI 데이터를 CSV 파일로 저장
    
    Args:
        file_path: 저장할 CSV 파일 경로
    """
    if not ISM_DATA['load_info']['loaded']:
        print("⚠️ 저장할 데이터가 없습니다. 먼저 데이터를 로드하세요.")
        return False
    
    try:
        # raw_data와 메타데이터를 함께 저장
        raw_data = ISM_DATA['raw_data']
        
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
        with open(meta_file, 'w', encoding='utf-8') as f:
            meta_data = {
                'load_time': ISM_DATA['load_info']['load_time'].isoformat() if ISM_DATA['load_info']['load_time'] else None,
                'start_date': ISM_DATA['load_info']['start_date'],
                'series_count': ISM_DATA['load_info']['series_count'],
                'data_points': ISM_DATA['load_info']['data_points'],
                'latest_values': ISM_DATA['latest_values']
            }
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ ISM PMI 데이터 저장 완료: {file_path}")
        print(f"✅ 메타데이터 저장 완료: {meta_file}")
        return True
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_ism_data_from_csv(file_path='/home/jyp0615/us_eco/ism_pmi_data.csv'):
    """
    CSV 파일에서 ISM PMI 데이터 로드
    
    Args:
        file_path: 로드할 CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global ISM_DATA
    
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
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
                latest_values = meta_data.get('latest_values', {})
        
        # 전역 저장소에 저장
        ISM_DATA['raw_data'] = df
        ISM_DATA['yoy_data'] = df.apply(calculate_yoy_change)
        ISM_DATA['mom_data'] = df.apply(calculate_mom_change)
        ISM_DATA['diffusion_data'] = df.apply(calculate_diffusion_index)
        ISM_DATA['latest_values'] = latest_values
        ISM_DATA['load_info'] = {
            'loaded': True,
            'load_time': datetime.datetime.now(),
            'start_date': df.index[0].strftime('%Y-%m-%d') if len(df) > 0 else None,
            'series_count': len(df.columns),
            'data_points': len(df)
        }
        
        print(f"✅ CSV에서 ISM PMI 데이터 로드 완료: {file_path}")
        print_load_info()
        return True
        
    except Exception as e:
        print(f"❌ CSV 로드 실패: {e}")
        return False

def check_recent_data_consistency_lightweight():
    """
    경량화된 일치성 확인 함수 - 주요 지표만 최소 API 호출
    
    Returns:
        dict: 일치성 확인 결과 (need_update, reason)
    """
    if not ISM_DATA['load_info']['loaded']:
        return {'need_update': True, 'reason': '기존 데이터 없음'}
    
    print("🔍 경량화된 데이터 일치성 확인 중...")
    
    try:
        existing_data = ISM_DATA['raw_data']
        
        # 기존 데이터의 최신 날짜 확인
        if 'pmi_pm' not in existing_data.columns:
            return {'need_update': True, 'reason': '주요 지표 없음'}
        
        existing_latest_date = existing_data['pmi_pm'].dropna().index[-1]
        print(f"   기존 데이터 최신 날짜: {existing_latest_date.strftime('%Y-%m')}")
        
        # 주요 지표(PMI 종합지수)만 가져와서 확인 - 경량화!
        recent_api_data = dbnomics.fetch_series('ISM', 'pmi', 'pm')[['period', 'value']]
        recent_api_data['period'] = pd.to_datetime(recent_api_data['period'])
        recent_api_data = recent_api_data.set_index('period').sort_index()
        
        # 최신 API 날짜 확인
        api_latest_date = recent_api_data.index[-1]
        print(f"   API 데이터 최신 날짜: {api_latest_date.strftime('%Y-%m')}")
        
        # 날짜 비교
        if api_latest_date > existing_latest_date:
            print(f"🆕 새로운 데이터 발견: {existing_latest_date.strftime('%Y-%m')} → {api_latest_date.strftime('%Y-%m')}")
            return {'need_update': True, 'reason': f'새로운 데이터 ({api_latest_date.strftime("%Y-%m")})'}
        else:
            print("✅ 최신 데이터가 일치함")
            return {'need_update': False, 'reason': '데이터 일치'}
    
    except Exception as e:
        print(f"⚠️ 일치성 확인 중 오류: {e}")
        return {'need_update': True, 'reason': f'확인 오류: {str(e)}'}

# %%
# === 분석 계산 함수들 ===

def calculate_yoy_change(data):
    """전년동월대비 변화율 계산"""
    return ((data / data.shift(12)) - 1) * 100

def calculate_mom_change(data):
    """전월대비 변화율 계산"""
    return ((data / data.shift(1)) - 1) * 100

def calculate_diffusion_index(data):
    """확산지수 계산 (50 기준선 대비)"""
    return data - 50

# %%
# === Enhanced 데이터 로드 함수 ===

def load_all_ism_data_enhanced(start_date='2020-01-01', force_reload=False, smart_update=True, csv_file='/home/jyp0615/us_eco/ism_pmi_data.csv'):
    """
    모든 ISM PMI 데이터 로드 (스마트 업데이트 지원)
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
        csv_file: CSV 파일 경로
    
    Returns:
        bool: 로드 성공 여부
    """
    global ISM_DATA
    
    # 이미 로드된 경우 스킵 (강제 재로드가 아닌 경우)
    if ISM_DATA['load_info']['loaded'] and not force_reload and not smart_update:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    # 스마트 업데이트 로직
    needs_api_call = True
    consistency_result = None
    
    if smart_update and not force_reload:
        print("🤖 스마트 업데이트 모드 활성화")
        
        # 먼저 CSV에서 데이터 로드 시도
        csv_loaded = load_ism_data_from_csv(csv_file)
        
        if csv_loaded and not ISM_DATA['raw_data'].empty:
            # CSV 데이터가 이미 전역 저장소에 로드됨 (load_ism_data_from_csv 함수에서)
            
            # 경량화된 일치성 확인
            consistency_result = check_recent_data_consistency_lightweight()
            
            # 일치성 결과 출력
            print(f"🔍 일치성 확인 결과: {consistency_result.get('reason', '알 수 없음')}")
            
            needs_api_call = consistency_result.get('need_update', True)
            
            if not needs_api_call:
                print("✅ 최근 데이터가 일치함 - API 호출 건너뛰기")
                # CSV 데이터를 그대로 사용 
                # 메타데이터 업데이트
                latest_values = {}
                for col in ISM_DATA['raw_data'].columns:
                    if not ISM_DATA['raw_data'][col].isna().all():
                        latest_values[col] = ISM_DATA['raw_data'][col].dropna().iloc[-1]
                
                ISM_DATA['latest_values'] = latest_values
                ISM_DATA['load_info'] = {
                    'loaded': True,
                    'load_time': datetime.datetime.now(),
                    'start_date': start_date,
                    'series_count': len(ISM_DATA['raw_data'].columns),
                    'data_points': len(ISM_DATA['raw_data']),
                    'source': 'CSV (스마트 업데이트)',
                    'consistency_check': consistency_result
                }
                
                print("💾 CSV 데이터 사용 (일치성 확인됨)")
                print_load_info()
                return True
            else:
                print("📡 데이터 불일치 감지 - 전체 API 호출 진행")
    
    # API를 통한 전체 데이터 로드
    if needs_api_call:
        print("🚀 ISM PMI 데이터 로딩 시작... (DBnomics API)")
        print("="*50)
        
        try:
            # 제조업 데이터 수집
            print("1️⃣ 제조업 PMI 데이터 수집...")
            manu_data = fetch_all_ism_data()
            
            # 비제조업 데이터 수집
            print("\n2️⃣ 비제조업 PMI 데이터 수집...")
            nm_data = fetch_all_nm_ism_data()
            
            # 데이터 통합
            print("\n3️⃣ 데이터 통합...")
            if not manu_data.empty and not nm_data.empty:
                combined_data = manu_data.join(nm_data, how='outer')
            elif not manu_data.empty:
                combined_data = manu_data
            elif not nm_data.empty:
                combined_data = nm_data
            else:
                print("❌ 로드된 데이터가 없습니다.")
                return False
            
            if len(combined_data.columns) < 5:  # 최소 5개 시리즈는 있어야 함
                error_msg = f"❌ 로드된 시리즈가 너무 적습니다: {len(combined_data.columns)}개"
                print(error_msg)
                return False
            
            # 날짜 필터링
            if start_date:
                combined_data = combined_data[combined_data.index >= start_date]
            
            # 전역 저장소에 저장
            ISM_DATA['raw_data'] = combined_data
            ISM_DATA['yoy_data'] = combined_data.apply(calculate_yoy_change)
            ISM_DATA['mom_data'] = combined_data.apply(calculate_mom_change)
            ISM_DATA['diffusion_data'] = combined_data.apply(calculate_diffusion_index)
            
            # 최신 값 저장
            latest_values = {}
            for col in combined_data.columns:
                if not combined_data[col].isna().all():
                    latest_values[col] = combined_data[col].dropna().iloc[-1]
            ISM_DATA['latest_values'] = latest_values
            
            ISM_DATA['load_info'] = {
                'loaded': True,
                'load_time': datetime.datetime.now(),
                'start_date': start_date,
                'series_count': len(combined_data.columns),
                'data_points': len(combined_data),
                'source': 'API (전체 로드)'
            }
            
            if consistency_result:
                ISM_DATA['load_info']['consistency_check'] = consistency_result
            
            # CSV에 저장
            save_ism_data_to_csv(csv_file)
            
            print("\n✅ 데이터 로딩 완료!")
            print_load_info()
            
            return True
            
        except Exception as e:
            print(f"❌ 데이터 로딩 실패: {e}")
            return False
    
    return False

def update_ism_data_from_api_enhanced(smart_update=True):
    """
    API를 사용하여 기존 데이터 업데이트 (스마트 업데이트 지원)
    
    Args:
        smart_update: 스마트 업데이트 사용 여부 (최근 3개 데이터 비교)
    
    Returns:
        bool: 업데이트 성공 여부
    """
    global ISM_DATA
    
    if not ISM_DATA['load_info']['loaded']:
        print("⚠️ 기존 데이터가 없습니다. 전체 로드를 수행합니다.")
        return load_all_ism_data_enhanced()
    
    # 스마트 업데이트 활성화시 최근 데이터 일치성 확인
    if smart_update:
        consistency_check = check_recent_data_consistency_lightweight()
        
        # 업데이트 필요 없으면 바로 종료
        if not consistency_check.get('need_update', True):
            print("🎯 데이터가 최신 상태입니다. API 호출을 건너뜁니다.")
            return True
        
        print(f"🔄 업데이트 진행: {consistency_check['reason']}")
    
    print(f"🔄 ISM PMI 데이터 업데이트 시작")
    print("="*50)
    
    try:
        # 새로운 데이터 가져오기
        print("📊 최신 제조업 PMI 데이터 가져오기...")
        new_manu_data = fetch_all_ism_data()
        
        print("📊 최신 비제조업 PMI 데이터 가져오기...")
        new_nm_data = fetch_all_nm_ism_data()
        
        # 데이터 통합
        if not new_manu_data.empty and not new_nm_data.empty:
            new_combined_data = new_manu_data.join(new_nm_data, how='outer')
        elif not new_manu_data.empty:
            new_combined_data = new_manu_data
        elif not new_nm_data.empty:
            new_combined_data = new_nm_data
        else:
            print("⚠️ 새로운 데이터가 없습니다.")
            return False
        
        # 기존 데이터와 병합
        existing_data = ISM_DATA['raw_data']
        all_dates = existing_data.index.union(new_combined_data.index).sort_values()
        
        # 각 시리즈별 업데이트
        updated_data = {}
        for col in existing_data.columns:
            if col in new_combined_data.columns:
                combined_series = existing_data[col].reindex(all_dates)
                # 새 데이터로 업데이트 (기존 값 덮어쓰기)
                for date, value in new_combined_data[col].items():
                    combined_series.loc[date] = value
                updated_data[col] = combined_series
            else:
                updated_data[col] = existing_data[col].reindex(all_dates)
        
        # 새로운 시리즈 추가
        for col in new_combined_data.columns:
            if col not in existing_data.columns:
                updated_data[col] = new_combined_data[col].reindex(all_dates)
        
        # 전역 저장소 업데이트
        updated_df = pd.DataFrame(updated_data)
        ISM_DATA['raw_data'] = updated_df
        ISM_DATA['yoy_data'] = updated_df.apply(calculate_yoy_change)
        ISM_DATA['mom_data'] = updated_df.apply(calculate_mom_change)
        ISM_DATA['diffusion_data'] = updated_df.apply(calculate_diffusion_index)
        
        # 최신 값 업데이트
        latest_values = {}
        for col in updated_df.columns:
            if not updated_df[col].isna().all():
                latest_values[col] = updated_df[col].dropna().iloc[-1]
        ISM_DATA['latest_values'] = latest_values
        
        # 로드 정보 업데이트
        ISM_DATA['load_info']['load_time'] = datetime.datetime.now()
        ISM_DATA['load_info']['series_count'] = len(updated_df.columns)
        ISM_DATA['load_info']['data_points'] = len(updated_df)
        
        print(f"\n✅ 업데이트 완료!")
        print_load_info()
        
        # 자동으로 CSV에 저장
        save_ism_data_to_csv()
        
        return True
        
    except Exception as e:
        print(f"❌ 업데이트 실패: {e}")
        return False

def print_load_info():
    """로드 정보 출력"""
    info = ISM_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not ISM_DATA['raw_data'].empty:
        date_range = f"{ISM_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {ISM_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

# %%
# === 데이터 접근 함수들 ===

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not ISM_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_ism_data_enhanced()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return ISM_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in ISM_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return ISM_DATA['raw_data'][available_series].copy()

def get_diffusion_data(series_names=None):
    """확산지수 데이터 반환 (50 기준선 대비)"""
    if not ISM_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_ism_data_enhanced()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return ISM_DATA['diffusion_data'].copy()
    
    available_series = [s for s in series_names if s in ISM_DATA['diffusion_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return ISM_DATA['diffusion_data'][available_series].copy()

# %%
# === 투자은행/이코노미스트 스타일 시각화 함수들 ===

def create_ism_timeseries_chart(series_names=None, chart_type='diffusion', start_date=None):
    """
    저장된 데이터를 사용한 ISM PMI 시계열 차트 생성
    
    Args:
        series_names: 차트에 표시할 시리즈 리스트
        chart_type: 'diffusion' (확산지수), 'raw' (원본 수준)
        title: 차트 제목
        start_date: 시작 날짜 (예: '2021-01-01')
    """
    if not ISM_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ism_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    if series_names is None:
        series_names = ['pmi_pm', 'nm-pmi_pm', 'neword_in', 'nm-neword_in']  # 주요 지표들
    
    # 데이터 가져오기
    if chart_type == 'diffusion':
        df = get_diffusion_data(series_names)
        ytitle = "포인트 (50 기준선 대비)"
        print("ISM PMI 확산지수 - 50 기준선 대비")
    else:  # raw
        df = get_raw_data(series_names)
        ytitle = "지수"
        print("ISM PMI 지수")
    
    if df.empty:
        print("⚠️ 요청한 데이터가 없습니다.")
        return None
    
    # 시작 날짜 필터링
    if start_date:
        df = df[df.index >= start_date]
    
    # 라벨 매핑
    label_mapping = {**ISM_KOREAN_NAMES, **NM_ISM_KOREAN_NAMES}
    chart_data = {}
    for col in df.columns:
        label = label_mapping.get(col, col)
        chart_data[label] = df[col].dropna()
    
    # KPDS 포맷 사용하여 차트 생성
    chart_df = pd.DataFrame(chart_data)
    
    # 라벨 매핑 적용
    labels = {col: col for col in chart_df.columns}  # 이미 한국어로 변환된 컬럼명
    
    fig = df_multi_line_chart(chart_df, chart_df.columns.tolist(), ytitle=ytitle, labels=labels)
    
    if chart_type == 'diffusion':
        fig.add_hline(y=0, line_width=2, line_color="black", opacity=0.8)
    else:
        # PMI 50 기준선 추가
        fig.add_hline(y=50, line_width=2, line_color="red", opacity=0.7, 
                      annotation_text="50 기준선 (확장/수축)")
    
    return fig

def create_ism_horizontal_bar_chart(chart_type='diffusion'):
    """
    ISM PMI 구성요소별 현재 상태를 보여주는 가로 바 차트 생성
    
    Args:
        chart_type: 'diffusion', 'raw'
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if not ISM_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ism_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 최신 데이터 가져오기
    if chart_type == 'diffusion':
        data = get_diffusion_data()
        default_title = "ISM PMI 확산지수 - 50 기준선 대비 (최신)"
        unit = "포인트"
        print(default_title)
    else:  # raw
        data = get_raw_data()
        default_title = "ISM PMI 지수 (최신)"
        unit = "지수"
        print(default_title)
    
    # 주요 Index 시리즈만 선택
    index_columns = [col for col in data.columns if col.endswith('_in') or col.endswith('_pm')]
    latest_data = data[index_columns].iloc[-1].dropna()
    
    # 데이터 정렬
    sorted_data = latest_data.sort_values(ascending=True)
    
    # 라벨 적용
    label_mapping = {**ISM_KOREAN_NAMES, **NM_ISM_KOREAN_NAMES}
    categories = []
    values = []
    colors = []
    
    for series_id, value in sorted_data.items():
        label = label_mapping.get(series_id, series_id)
        categories.append(label)
        values.append(value)
        
        # 주요 지표는 특별 색상
        if series_id in ['pmi_pm', 'nm-pmi_pm']:
            colors.append('#FFA500')  # 주황색
        elif value >= 0:
            colors.append(deepred_pds)  # 상승: deepred_pds
        else:
            colors.append(deepblue_pds)  # 하락: deepblue_pds
    
    # 가로 바 차트 생성 (KPDS 색상 사용)
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=categories,
        x=values,
        orientation='h',
        marker_color=colors,
        text=[f'{v:.1f}' for v in values],
        textposition='outside' if all(v >= 0 for v in values) else 'auto',
        showlegend=False,
        textfont=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1)
    ))
    
    # 최신 날짜 가져오기
    latest_date = ISM_DATA['raw_data'].index[-1].strftime('%B %Y')
    
    # 레이아웃 설정 (KPDS 포맷 준수)
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,  # KPDS 표준 너비
        height=max(400, len(categories) * 30),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            title=dict(text="", font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='.1f',
            range=[min(values) * 1.2 if min(values) < 0 else -abs(max(values)) * 0.1, max(values) * 1.2],
            showgrid=True, gridwidth=1, gridcolor="lightgrey"
        ),
        yaxis=dict(
            title=dict(text="", font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=False,
            tickcolor='white',
            showgrid=False
        ),
        margin=dict(l=200, r=80, t=80, b=60),
        legend=dict(
            font=dict(family='NanumGothic', size=FONT_SIZE_LEGEND),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # 0선 추가
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    # Y축 단위 annotation
    fig.add_annotation(
        text=unit,
        xref="paper", yref="paper",
        x=-0.02, y=1.1,  # 왼쪽 위치로 고정
        showarrow=False,
        font=dict(size=FONT_SIZE_ANNOTATION, family='NanumGothic', color="black"),
        align='left'
    )
    
    return fig

def create_ism_mom_bar_chart(sector='both'):
    """
    ISM PMI 구성요소별 MoM 변화를 보여주는 가로 바 차트 생성
    
    Args:
        sector: 'manufacturing', 'services', 'both' 
        title: 차트 제목
    
    Returns:
        plotly figure
    """
    if not ISM_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ism_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 전월대비 변화율 계산이 필요하면 추가
    if 'mom_data' not in ISM_DATA or ISM_DATA['mom_data'].empty:
        ISM_DATA['mom_data'] = ISM_DATA['raw_data'].apply(calculate_mom_change)
    
    # 최신 MoM 데이터 가져오기
    latest_mom = ISM_DATA['mom_data'].iloc[-1]
    
    # 섹터별 시리즈 선택
    if sector == 'manufacturing':
        index_columns = [col for col in latest_mom.index if col.endswith('_in') or col.endswith('_pm')]
        index_columns = [col for col in index_columns if not col.startswith('nm-')]
        default_title = "제조업 PMI 구성요소 MoM 변화율"
        print(default_title)
    elif sector == 'services':
        index_columns = [col for col in latest_mom.index if col.startswith('nm-') and (col.endswith('_in') or col.endswith('_pm'))]
        default_title = "비제조업 PMI 구성요소 MoM 변화율"
        print(default_title)
    else:  # both
        index_columns = [col for col in latest_mom.index if col.endswith('_in') or col.endswith('_pm')]
        default_title = "ISM PMI 구성요소 MoM 변화율 (제조업 vs 비제조업)"
        print(default_title)
    
    # 데이터 필터링 및 정렬
    valid_data = latest_mom[index_columns].dropna()
    sorted_data = valid_data.sort_values(ascending=True)
    
    # 라벨 적용
    label_mapping = {**ISM_KOREAN_NAMES, **NM_ISM_KOREAN_NAMES}
    categories = []
    values = []
    colors = []
    
    for series_id, value in sorted_data.items():
        label = label_mapping.get(series_id, series_id)
        categories.append(label)
        values.append(value)
        
        # 색상 구분: 제조업 vs 비제조업
        if series_id.startswith('nm-'):
            if value >= 0:
                colors.append(deepblue_pds)  # 비제조업 상승
            else:
                colors.append(blue_pds)  # 비제조업 하락
        else:
            if value >= 0:
                colors.append(deepred_pds)  # 제조업 상승
            else:
                colors.append(red_pds)  # 제조업 하락
    
    # 가로 바 차트 생성 (KPDS 포맷)
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=categories,
        x=values,
        orientation='h',
        marker_color=colors,
        text=[f'{v:+.2f}%' for v in values],
        textposition='outside' if all(v >= 0 for v in values) else 'auto',
        showlegend=False,
        textfont=dict(family='NanumGothic', size=FONT_SIZE_GENERAL-1)
    ))
    
    # 최신 날짜 가져오기
    latest_date = ISM_DATA['raw_data'].index[-1].strftime('%Y년 %m월')
    
    # 레이아웃 설정 (KPDS 포맷 준수)
    fig.update_layout(
        paper_bgcolor='white',
        plot_bgcolor='white',
        width=686,  # KPDS 표준 너비
        height=max(400, len(categories) * 30),
        font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL, color="black"),
        xaxis=dict(
            title=dict(text="MoM 변화율 (%)", font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=True, linewidth=1.3, linecolor='lightgrey',
            tickwidth=1.3, tickcolor='lightgrey',
            ticks='outside',
            tickformat='+.1f',
            range=[min(values) * 1.2 if min(values) < 0 else -abs(max(values)) * 0.1, max(values) * 1.2],
            showgrid=True, gridwidth=1, gridcolor="lightgrey"
        ),
        yaxis=dict(
            title=dict(text="", font=dict(family='NanumGothic', size=FONT_SIZE_AXIS_TITLE)),
            showline=False,
            tickcolor='white',
            showgrid=False
        ),
        margin=dict(l=250, r=100, t=80, b=60),  # 긴 라벨을 위해 왼쪽 여백 증가
    )
    
    # 0선 추가
    fig.add_vline(x=0, line_width=2, line_color="black")
    
    return fig

def create_manufacturing_vs_services_chart(chart_type='raw', start_date=None):
    """
    제조업 vs 비제조업 PMI 비교 차트
    
    Args:
        chart_type: 'diffusion', 'raw'
        title: 차트 제목
        start_date: 시작 날짜
    """
    if not ISM_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ism_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    # 제조업 vs 비제조업 주요 지표 선택
    series_names = ['pmi_pm', 'nm-pmi_pm', 'neword_in', 'nm-neword_in', 'production_in', 'nm-busact_in']
    
    print("제조업 vs 비제조업 PMI 비교")
    
    return create_ism_timeseries_chart(series_names, chart_type, start_date)

def create_ism_economic_dashboard(start_date='2022-01-01'):
    """
    ISM PMI 경제 대시보드 - 투자은행 스타일
    여러 차트를 한 번에 생성하여 경제 상황을 종합적으로 분석
    
    Args:
        start_date: 분석 시작 날짜
    
    Returns:
        dict: 생성된 차트들
    """
    if not ISM_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ism_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("🏦 ISM PMI 경제 대시보드 생성 중...")
    print("="*50)
    
    results = {}
    
    try:
        # 1. 제조업 vs 비제조업 PMI 종합지수
        print("📊 1. 제조업 vs 비제조업 PMI 종합지수...")
        results['main_pmi_comparison'] = create_manufacturing_vs_services_chart('raw', 
                                                                               "제조업 vs 비제조업 PMI 종합지수", start_date)
        
        # 2. 확산지수 차트 (50 기준선 대비)
        print("📊 2. PMI 확산지수 (50 기준선 대비)...")
        results['diffusion_index'] = create_ism_timeseries_chart(['pmi_pm', 'nm-pmi_pm'], 'diffusion', 
                                                                "ISM PMI 확산지수 - 50 기준선 대비", start_date)
        
        # 3. 현재 상태 가로 바 차트
        print("📊 3. 현재 PMI 상태...")
        results['current_status'] = create_ism_horizontal_bar_chart('diffusion')
        
        # 4. MoM 변화율 가로 바 차트 (제조업 vs 비제조업)
        print("📊 4. MoM 변화율 비교...")
        results['mom_comparison'] = create_ism_mom_bar_chart('both')
        
        # 5. 제조업 세부 MoM 분석
        print("📊 5. 제조업 세부 MoM 분석...")
        results['manufacturing_mom'] = create_ism_mom_bar_chart('manufacturing')
        
        # 6. 비제조업 세부 MoM 분석
        print("📊 6. 비제조업 세부 MoM 분석...")
        results['services_mom'] = create_ism_mom_bar_chart('services')
        
        # 7. 공급망 관련 지표들
        print("📊 7. 공급망 관련 지표...")
        supply_series = ['supdel_in', 'nm-supdel_in', 'inventories_in', 'nm-inventories_in', 'prices_in', 'nm-prices_in']
        results['supply_chain'] = create_ism_timeseries_chart(supply_series, 'raw', "공급망 관련 PMI 지표", start_date)
        
        # 8. 고용 관련 지표
        print("📊 8. 고용 관련 지표...")
        employment_series = ['employment_in', 'nm-employment_in']
        results['employment'] = create_ism_timeseries_chart(employment_series, 'raw', "고용 관련 PMI 지표", start_date)
        
    except Exception as e:
        print(f"⚠️ 차트 생성 중 오류: {e}")
    
    print(f"\n✅ 경제 대시보드 완료! 생성된 차트: {len(results)}개")
    return results

# %%
# === 경제 분석 리포트 함수 ===

def generate_ism_economic_report():
    """
    투자은행 스타일의 ISM PMI 경제 분석 리포트 생성
    """
    if not ISM_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_ism_data_enhanced()를 실행하여 데이터를 로드하세요.")
        return None
    
    print("📝 ISM PMI 경제 분석 리포트 생성")
    print("="*50)
    
    # 최신 데이터 가져오기
    latest_date = ISM_DATA['raw_data'].index[-1]
    latest_raw = ISM_DATA['raw_data'].iloc[-1]
    latest_diffusion = ISM_DATA['diffusion_data'].iloc[-1]
    
    print(f"📅 분석 기준일: {latest_date.strftime('%Y년 %m월')}")
    print()
    
    # 1. 주요 지표 요약
    print("📊 주요 지표 현황:")
    key_indicators = ['pmi_pm', 'nm-pmi_pm', 'neword_in', 'nm-neword_in', 'employment_in', 'nm-employment_in']
    
    for indicator in key_indicators:
        if indicator in latest_raw.index:
            value = latest_raw[indicator]
            diffusion = latest_diffusion[indicator] if indicator in latest_diffusion.index else None
            korean_name = {**ISM_KOREAN_NAMES, **NM_ISM_KOREAN_NAMES}.get(indicator, indicator)
            
            # 상태 판정
            if indicator.endswith('_pm'):  # PMI 종합지수
                status = "확장" if value > 50 else "수축"
                status_emoji = "🟢" if value > 50 else "🔴"
            else:
                status = "개선" if diffusion > 0 else "악화"
                status_emoji = "🟢" if diffusion > 0 else "🔴"
            
            print(f"  {status_emoji} {korean_name}: {value:.1f} ({status})")
    
    print()
    
    # 2. 제조업 vs 비제조업 비교
    print("🏭 제조업 vs 비제조업 비교:")
    manu_pmi = latest_raw.get('pmi_pm', 0)
    services_pmi = latest_raw.get('nm-pmi_pm', 0)
    
    print(f"  제조업 PMI: {manu_pmi:.1f}")
    print(f"  비제조업 PMI: {services_pmi:.1f}")
    
    if manu_pmi > 0 and services_pmi > 0:
        gap = manu_pmi - services_pmi
        if abs(gap) < 1:
            print(f"  → 제조업과 비제조업이 균형잡힌 상태 (격차: {gap:+.1f}p)")
        elif gap > 0:
            print(f"  → 제조업이 비제조업보다 강세 (+{gap:.1f}p)")
        else:
            print(f"  → 비제조업이 제조업보다 강세 ({gap:.1f}p)")
    
    print()
    
    # 3. 공급망 및 인플레이션 압력
    print("🚛 공급망 및 가격 동향:")
    supply_indicators = {
        'supdel_in': '제조업 공급업체 배송',
        'nm-supdel_in': '비제조업 공급업체 배송', 
        'prices_in': '제조업 가격',
        'nm-prices_in': '비제조업 가격'
    }
    
    for indicator, name in supply_indicators.items():
        if indicator in latest_raw.index:
            value = latest_raw[indicator]
            if 'supdel' in indicator:
                # 공급업체 배송: 높을수록 배송 지연 (인플레이션 압력)
                pressure = "높음" if value > 50 else "낮음"
                emoji = "🔴" if value > 55 else "🟡" if value > 45 else "🟢"
            else:
                # 가격: 높을수록 가격 상승 압력
                pressure = "높음" if value > 60 else "보통" if value > 40 else "낮음"
                emoji = "🔴" if value > 60 else "🟡" if value > 40 else "🟢"
            
            print(f"  {emoji} {name}: {value:.1f} (압력 {pressure})")
    
    print()
    
    # 4. 경제 전망 종합
    print("🔮 경제 전망 종합:")
    
    # PMI 기반 경기 판단
    if manu_pmi > 50 and services_pmi > 50:
        outlook = "경기 확장 지속"
        outlook_emoji = "🟢"
    elif manu_pmi > 50 or services_pmi > 50:
        outlook = "경기 혼조, 주의 필요"
        outlook_emoji = "🟡"
    else:
        outlook = "경기 수축, 경계 필요"
        outlook_emoji = "🔴"
    
    print(f"  {outlook_emoji} 기본 시나리오: {outlook}")
    
    # 리스크 요인
    risks = []
    if latest_raw.get('prices_in', 0) > 60 or latest_raw.get('nm-prices_in', 0) > 60:
        risks.append("인플레이션 압력")
    if latest_raw.get('supdel_in', 0) > 55 or latest_raw.get('nm-supdel_in', 0) > 55:
        risks.append("공급망 병목")
    if latest_raw.get('employment_in', 0) < 45 or latest_raw.get('nm-employment_in', 0) < 45:
        risks.append("고용 둔화")
    
    if risks:
        print(f"  ⚠️ 주요 리스크: {', '.join(risks)}")
    else:
        print(f"  ✅ 주요 리스크 요인 제한적")
    
    print()
    print("="*50)
    print("💡 본 분석은 ISM PMI 데이터를 기반으로 한 기계적 해석입니다.")
    print("   투자 결정시에는 추가적인 경제지표와 전문가 분석을 참고하시기 바랍니다.")

# %%
# === 통합 분석 함수 ===

def run_complete_ism_analysis_enhanced(start_date='2020-01-01', force_reload=False, smart_update=True):
    """
    완전한 ISM PMI 분석 실행 - 데이터 로드 후 모든 차트 생성 (스마트 업데이트 지원)
    
    Args:
        start_date: 데이터 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
    
    Returns:
        dict: 생성된 차트들
    """
    print("🚀 완전한 ISM PMI 분석 시작 (스마트 업데이트 지원)")
    print("="*50)
    
    # 1. 데이터 로드 (스마트 업데이트!)
    print("\n1️⃣ 데이터 로딩 (스마트 업데이트)")
    success = load_all_ism_data_enhanced(start_date=start_date, force_reload=force_reload, smart_update=smart_update)
    
    if not success:
        print("❌ 데이터 로딩 실패")
        return None
    
    # 2. 경제 대시보드 생성
    print("\n2️⃣ 경제 대시보드 생성")
    results = create_ism_economic_dashboard(start_date='2022-01-01')
    
    # 3. 경제 분석 리포트
    print("\n3️⃣ 경제 분석 리포트 생성")
    generate_ism_economic_report()
    
    return results

# %%
# === 사용 예시 ===

print("\n" + "="*80)
print("🎯 권장 사용법:")
print("   🏆 run_complete_ism_analysis_enhanced(smart_update=True)  # 이것만 실행하면 됨!")
print()
print("📊 개별 함수 사용법:")
print("1. 데이터 로드 (스마트 업데이트):")
print("   load_all_ism_data_enhanced(smart_update=True)  # 기본값, 일치 시 API 건너뛰기")
print("   load_all_ism_data_enhanced(force_reload=True)  # 강제 전체 재로드")
print("   load_ism_data_from_csv()  # CSV에서만 로드")
print("   update_ism_data_from_api_enhanced(smart_update=True)  # 스마트 업데이트")
print()
print("2. 💾 데이터 저장:")
print("   save_ism_data_to_csv()  # 현재 데이터를 CSV로 저장 (자동 호출됨)")
print()
print("3. 🤖 스마트 업데이트 기능:")
print("   check_recent_data_consistency()  # 최근 3개월 데이터 일치성 확인")
print("   print_consistency_results()  # 일치성 결과 상세 출력")
print("   - 허용 오차: 5.0점 (PMI 지수 기준)")
print("   - CSV 데이터와 API 데이터 비교")
print("   - 불일치 시에만 전체 API 호출")
print()
print("4. 📈 차트 생성:")
print("   create_ism_timeseries_chart(['pmi_pm', 'nm-pmi_pm'], 'diffusion')  # 확산지수")
print("   create_ism_timeseries_chart(['pmi_pm', 'nm-pmi_pm'], 'raw')  # 원본 레벨")
print("   create_ism_horizontal_bar_chart('diffusion')  # 확산지수")
print("   create_ism_mom_bar_chart('both')  # MoM 변화율 (제조업 vs 비제조업)")
print("   create_ism_mom_bar_chart('manufacturing')  # 제조업 MoM 분석")
print("   create_ism_mom_bar_chart('services')  # 비제조업 MoM 분석")
print("   create_manufacturing_vs_services_chart('raw')  # 섹터 비교")
print()
print("5. 🎯 통합 분석:")
print("   run_complete_ism_analysis_enhanced(smart_update=True)  # 전체 분석 (기본값)")
print("   create_ism_economic_dashboard()  # 경제 대시보드")
print("   generate_ism_economic_report()  # 경제 분석 리포트")
print("="*80)

# %%
run_complete_ism_analysis_enhanced(smart_update=True)

# %%
create_ism_mom_bar_chart('services')
# %%
create_ism_mom_bar_chart('manufacturing')
# %%
create_ism_timeseries_chart(['pmi_pm', 'nm-pmi_pm'], 'raw')  # 원본 레벨")
# %%
