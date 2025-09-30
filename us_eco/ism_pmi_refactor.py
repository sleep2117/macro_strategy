# %%
"""
ISM PMI 데이터 분석 (리팩토링 버전)
- dbnomics API를 사용한 ISM PMI 데이터 수집
- 시리즈 정의와 분석 로직만 포함
- 리팩토링된 통합 구조 적용
"""

import pandas as pd
import numpy as np
import datetime as dt
import sys
import warnings
# dbnomics is optional; handle gracefully when unavailable
try:
    import dbnomics
    HAS_DBNOMICS = True
except ImportError:
    dbnomics = None
    HAS_DBNOMICS = False
    print("⚠️ dbnomics 패키지를 찾을 수 없습니다. 기존 CSV 데이터만 사용합니다.")
import os
import json
warnings.filterwarnings('ignore')

# KPDS 시각화 라이브러리 불러오기 (필수)
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

# 통합 유틸리티 함수 불러오기 (시각화 함수용)
try:
    from us_eco_utils import plot_economic_series, export_economic_data
    print("✓ us_eco_utils 시각화 함수 로드됨")
except ImportError:
    print("⚠️ us_eco_utils 일부 함수 사용 불가 (dbnomics 전용 함수 사용)")

print("✓ KPDS 시각화 포맷 로드됨")

# %%
# === ISM PMI 시리즈 정의 (리팩토링 버전) ===

# 제조업 PMI 시리즈 딕셔너리 (새로운 구조: 시리즈_이름: dbnomics_path)
ISM_MANUFACTURING_SERIES = {
    'pmi_pm': ('ISM', 'pmi', 'pm'),                    # PMI 종합지수
    'neword_in': ('ISM', 'neword', 'in'),             # 신규주문 Index
    'production_in': ('ISM', 'production', 'in'),      # 생산 Index
    'employment_in': ('ISM', 'employment', 'in'),      # 고용 Index
    'supdel_in': ('ISM', 'supdel', 'in'),             # 공급업체 배송 Index
    'inventories_in': ('ISM', 'inventories', 'in'),    # 재고 Index
    'prices_in': ('ISM', 'prices', 'in'),             # 가격 Index
    'bacord_in': ('ISM', 'bacord', 'in'),             # 수주잔고 Index
    'newexpord_in': ('ISM', 'newexpord', 'in'),       # 신규 수출주문 Index
    'imports_in': ('ISM', 'imports', 'in'),            # 수입 Index
    'cusinv_in': ('ISM', 'cusinv', 'in')              # 고객 재고 Index
}

# 비제조업 PMI 시리즈 딕셔너리
ISM_NONMANUFACTURING_SERIES = {
    'nm_pmi_pm': ('ISM', 'nm-pmi', 'pm'),             # PMI 종합지수
    'nm_busact_in': ('ISM', 'nm-busact', 'in'),       # 사업활동 Index
    'nm_neword_in': ('ISM', 'nm-neword', 'in'),       # 신규주문 Index
    'nm_employment_in': ('ISM', 'nm-employment', 'in'), # 고용 Index
    'nm_supdel_in': ('ISM', 'nm-supdel', 'in'),       # 공급업체 배송 Index
    'nm_inventories_in': ('ISM', 'nm-inventories', 'in'), # 재고 Index
    'nm_prices_in': ('ISM', 'nm-prices', 'in'),       # 가격 Index
    'nm_bacord_in': ('ISM', 'nm-bacord', 'in'),       # 수주잔고 Index
    'nm_newexpord_in': ('ISM', 'nm-newexpord', 'in'), # 신규 수출주문 Index
    'nm_imports_in': ('ISM', 'nm-imports', 'in'),     # 수입 Index
    'nm_invsen_in': ('ISM', 'nm-invsen', 'in')        # 재고 감정 Index
}

# 통합 시리즈 딕셔너리
ISM_SERIES = {**ISM_MANUFACTURING_SERIES, **ISM_NONMANUFACTURING_SERIES}

# 한국어 이름 매핑 (기존 것 그대로 유지)
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
    'cusinv_in': '고객 재고 (제조업)',
    'nm_pmi_pm': 'PMI 종합지수 (비제조업)',
    'nm_busact_in': '사업활동 (비제조업)',
    'nm_neword_in': '신규주문 (비제조업)',
    'nm_employment_in': '고용 (비제조업)',
    'nm_supdel_in': '공급업체 배송 (비제조업)',
    'nm_inventories_in': '재고 (비제조업)',
    'nm_prices_in': '가격 (비제조업)',
    'nm_bacord_in': '수주잔고 (비제조업)',
    'nm_newexpord_in': '신규 수출주문 (비제조업)',
    'nm_imports_in': '수입 (비제조업)',
    'nm_invsen_in': '재고 감정 (비제조업)'
}

# 카테고리 분류 (기존 것 그대로 유지)
ISM_CATEGORIES = {
    '주요 지표': {
        'Manufacturing': ['pmi_pm', 'neword_in', 'production_in'],
        'Non-Manufacturing': ['nm_pmi_pm', 'nm_busact_in', 'nm_neword_in']
    },
    '세부 구성': {
        'Labor': ['employment_in', 'nm_employment_in'],
        'Supply Chain': ['supdel_in', 'nm_supdel_in', 'inventories_in', 'nm_inventories_in'],
        'Pricing': ['prices_in', 'nm_prices_in'],
        'Trade': ['newexpord_in', 'nm_newexpord_in', 'imports_in', 'nm_imports_in']
    }
}

# %%
# === 전역 변수 ===

CSV_FILE_PATH = '/home/jyp0615/us_eco/data/ism_pmi_data_refactored.csv'
ISM_DATA = {
    'raw_data': pd.DataFrame(),
    'mom_data': pd.DataFrame(),
    'yoy_data': pd.DataFrame(),
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0,
        'source': None,
    },
}

# %%
# === DBnomics 전용 데이터 수집 함수 ===

def fetch_manufacturing_data():
    """제조업 ISM PMI 데이터 수집 (DBnomics API 사용)"""
    manufacturing_data = pd.DataFrame()

    if not HAS_DBNOMICS:
        print("⚠️ dbnomics 미설치: 제조업 데이터를 API로 가져올 수 없습니다.")
        return manufacturing_data
    
    for series_name, (provider, category, series_type) in ISM_MANUFACTURING_SERIES.items():
        try:
            data = dbnomics.fetch_series(provider, category, series_type)[['period', 'value']]
            data.columns = ['date', series_name]
            data['date'] = pd.to_datetime(data['date'])
            data = data.set_index('date')
            
            if manufacturing_data.empty:
                manufacturing_data = data
            else:
                manufacturing_data = manufacturing_data.join(data, how='outer')
                
        except Exception as e:
            print(f"⚠️ {series_name} 수집 실패: {str(e)}")
            continue
    
    return manufacturing_data

def fetch_nonmanufacturing_data():
    """비제조업 ISM PMI 데이터 수집 (DBnomics API 사용)"""
    nonmanufacturing_data = pd.DataFrame()

    if not HAS_DBNOMICS:
        print("⚠️ dbnomics 미설치: 비제조업 데이터를 API로 가져올 수 없습니다.")
        return nonmanufacturing_data
    
    for series_name, (provider, category, series_type) in ISM_NONMANUFACTURING_SERIES.items():
        try:
            data = dbnomics.fetch_series(provider, category, series_type)[['period', 'value']]
            data.columns = ['date', series_name]
            data['date'] = pd.to_datetime(data['date'])
            data = data.set_index('date')
            
            if nonmanufacturing_data.empty:
                nonmanufacturing_data = data
            else:
                nonmanufacturing_data = nonmanufacturing_data.join(data, how='outer')
                
        except Exception as e:
            print(f"⚠️ {series_name} 수집 실패: {str(e)}")
            continue
    
    return nonmanufacturing_data

# %%
# === 데이터 로드 함수 (핵심) ===

def load_ism_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 ISM PMI 데이터 로드"""
    global ISM_DATA

    print("🚀 ISM PMI 데이터 로딩 시작... (DBnomics API)")
    print("="*50)

    if not HAS_DBNOMICS:
        print("⚠️ dbnomics 패키지가 없어 CSV 데이터로 대체합니다.")
        if not os.path.exists(CSV_FILE_PATH):
            print(f"❌ CSV 파일을 찾을 수 없습니다: {CSV_FILE_PATH}")
            return False

        try:
            cached = pd.read_csv(CSV_FILE_PATH, index_col=0)
            cached.index = pd.to_datetime(cached.index)
            if start_date:
                cached = cached[cached.index >= start_date]
            mom = cached.pct_change() * 100
            yoy = cached.pct_change(periods=12) * 100
            ISM_DATA = {
                'raw_data': cached,
                'mom_data': mom,
                'yoy_data': yoy,
                'load_info': {
                    'loaded': True,
                    'load_time': dt.datetime.now(),
                    'start_date': start_date,
                    'series_count': cached.shape[1],
                    'data_points': cached.shape[0],
                    'source': 'CSV (offline)',
                },
            }
            print("✅ CSV 데이터 로드 완료 (오프라인 모드)")
            print_load_info()
            return True
        except Exception as exc:
            print(f"❌ CSV 로드 실패: {exc}")
            return False

    try:
        # 1. 제조업 데이터 수집
        print("1️⃣ 제조업 PMI 데이터 수집...")
        manu_data = fetch_manufacturing_data()
        
        # 2. 비제조업 데이터 수집  
        print("2️⃣ 비제조업 PMI 데이터 수집...")
        nm_data = fetch_nonmanufacturing_data()
        
        # 3. 데이터 통합
        print("3️⃣ 데이터 통합...")
        if not manu_data.empty and not nm_data.empty:
            combined_data = manu_data.join(nm_data, how='outer')
        elif not manu_data.empty:
            combined_data = manu_data
        elif not nm_data.empty:
            combined_data = nm_data
        else:
            print("❌ 로드된 데이터가 없습니다.")
            return False
        
        # 날짜 필터링
        if start_date:
            combined_data = combined_data[combined_data.index >= start_date]
        
        # MoM, YoY 계산
        combined_data_mom = combined_data.pct_change() * 100
        combined_data_yoy = combined_data.pct_change(periods=12) * 100
        
        # 전역 저장소에 저장
        ISM_DATA = {
            'raw_data': combined_data,
            'mom_data': combined_data_mom,
            'yoy_data': combined_data_yoy,
            'load_info': {
                'loaded': True,
                'load_time': dt.datetime.now(),
                'start_date': start_date,
                'series_count': len(combined_data.columns),
                'data_points': len(combined_data),
                'source': 'DBnomics'
            }
        }
        
        print("✅ 데이터 로딩 완료!")
        print_load_info()
        
        # CSV 저장
        try:
            os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
            combined_data.to_csv(CSV_FILE_PATH)
            print(f"✅ CSV 저장 완료: {CSV_FILE_PATH}")
        except Exception as e:
            print(f"⚠️ CSV 저장 실패: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ 데이터 로딩 실패: {e}")
        return False

def print_load_info():
    """ISM PMI 데이터 로드 정보 출력"""
    if not ISM_DATA or 'load_info' not in ISM_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = ISM_DATA['load_info']
    print(f"\n📊 ISM PMI 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in ISM_DATA and not ISM_DATA['raw_data'].empty:
        latest_date = ISM_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 (필수) ===

def plot_ism_series_advanced(series_list, chart_type='multi_line', 
                             data_type='raw', periods=None, target_date=None,
                             left_ytitle=None, right_ytitle=None):
    """범용 ISM PMI 시각화 함수 - KPDS 포맷 사용"""
    if not ISM_DATA:
        print("⚠️ 먼저 load_ism_data()를 실행하세요.")
        return None

    # 데이터 타입 선택
    if data_type == 'mom':
        data = ISM_DATA['mom_data']
        default_ytitle = "%"
    elif data_type == 'yoy':
        data = ISM_DATA['yoy_data'] 
        default_ytitle = "%"
    else:  # raw
        data = ISM_DATA['raw_data']
        default_ytitle = "pt"
    
    # 시리즈 필터링
    available_series = [s for s in series_list if s in data.columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_list}")
        return None
    
    plot_data = data[available_series].copy()
    
    # 기간 제한
    if periods:
        plot_data = plot_data.iloc[-periods:]
    
    # 특정 날짜까지
    if target_date:
        plot_data = plot_data[plot_data.index <= target_date]
    
    # 한국어 라벨 적용
    labels = {col: ISM_KOREAN_NAMES.get(col, col) for col in plot_data.columns}
    
    # Y축 제목 설정
    if not left_ytitle:
        left_ytitle = default_ytitle
    if not right_ytitle:
        right_ytitle = default_ytitle
    
    # 차트 타입별 시각화
    if chart_type == 'multi_line':
        print("ISM PMI 멀티라인 차트")
        return df_multi_line_chart(plot_data, plot_data.columns.tolist(), 
                                 ytitle=left_ytitle, labels=labels)
    
    elif chart_type == 'single_line':
        print(f"ISM PMI 단일 시리즈 차트: {labels[available_series[0]]}")
        return df_single_line_chart(plot_data[available_series[0]], 
                                   ytitle=left_ytitle, 
                                   label=labels[available_series[0]])
    
    elif chart_type == 'horizontal_bar':
        print("ISM PMI 가로 바 차트 (최신 값)")
        latest_data = plot_data.iloc[-1].sort_values(ascending=True)
        categories = [labels.get(idx, idx) for idx in latest_data.index]
        values = latest_data.values
        
        # 간단한 가로 바 차트 (KPDS 포맷)
        import plotly.graph_objects as go
        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=categories,
            x=values,
            orientation='h',
            marker_color=deepblue_pds,
            text=[f'{v:.1f}' for v in values],
            textposition='outside'
        ))
        
        fig.update_layout(
            paper_bgcolor='white',
            plot_bgcolor='white', 
            width=686,
            height=max(400, len(categories) * 30),
            font=dict(family='NanumGothic', size=FONT_SIZE_GENERAL),
            xaxis_title=left_ytitle,
            margin=dict(l=200, r=80, t=60, b=60)
        )
        return fig
    
    elif chart_type == 'dual_axis' and len(available_series) >= 2:
        print("ISM PMI 듀얼 축 차트")
        return df_dual_axis_chart(plot_data[available_series[0]], 
                                plot_data[available_series[1]],
                                left_ytitle=left_ytitle,
                                right_ytitle=right_ytitle,
                                left_label=labels[available_series[0]],
                                right_label=labels[available_series[1]])
    
    else:
        print(f"⚠️ 지원하지 않는 차트 타입: {chart_type}")
        return None

# %%
# === 데이터 Export 함수 (필수) ===

def export_ism_data(series_list, data_type='raw', periods=None, 
                    target_date=None, export_path=None, file_format='excel'):
    """ISM PMI 데이터 export 함수"""
    if not ISM_DATA:
        print("⚠️ 먼저 load_ism_data()를 실행하세요.")
        return None

    # 데이터 타입 선택
    if data_type == 'mom':
        data = ISM_DATA['mom_data']
    elif data_type == 'yoy':
        data = ISM_DATA['yoy_data']
    else:  # raw
        data = ISM_DATA['raw_data']
    
    # 시리즈 필터링
    available_series = [s for s in series_list if s in data.columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_list}")
        return None
    
    export_data = data[available_series].copy()
    
    # 기간 제한
    if periods:
        export_data = export_data.iloc[-periods:]
    
    # 특정 날짜까지
    if target_date:
        export_data = export_data[export_data.index <= target_date]
    
    # 한국어 컬럼명으로 변경
    export_data.columns = [ISM_KOREAN_NAMES.get(col, col) for col in export_data.columns]
    
    # 파일 경로 설정
    if not export_path:
        timestamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        if file_format == 'excel':
            export_path = f'/home/jyp0615/us_eco/exports/ism_pmi_{data_type}_{timestamp}.xlsx'
        else:
            export_path = f'/home/jyp0615/us_eco/exports/ism_pmi_{data_type}_{timestamp}.csv'
    
    try:
        os.makedirs(os.path.dirname(export_path), exist_ok=True)
        
        if file_format == 'excel':
            export_data.to_excel(export_path)
        else:
            export_data.to_csv(export_path, encoding='utf-8')
        
        print(f"✅ 데이터 Export 완료: {export_path}")
        print(f"📊 Export된 데이터: {len(export_data)} 행 x {len(export_data.columns)} 컬럼")
        
        return export_path
        
    except Exception as e:
        print(f"❌ Export 실패: {e}")
        return None

# %%
# === 데이터 접근 함수들 (필수) ===

def clear_ism_data():
    """ISM PMI 데이터 초기화"""
    global ISM_DATA
    ISM_DATA = {}
    print("🗑️ ISM PMI 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not ISM_DATA or 'raw_data' not in ISM_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_ism_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return ISM_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in ISM_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return ISM_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not ISM_DATA or 'mom_data' not in ISM_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_ism_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return ISM_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in ISM_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return ISM_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not ISM_DATA or 'yoy_data' not in ISM_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_ism_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return ISM_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in ISM_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return ISM_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not ISM_DATA or 'raw_data' not in ISM_DATA:
        return []
    return list(ISM_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 (필수) ===

def show_available_series():
    """사용 가능한 ISM PMI 시리즈 표시"""
    print("=== 사용 가능한 ISM PMI 시리즈 ===")
    
    for series_name, (provider, category, series_type) in ISM_SERIES.items():
        korean_name = ISM_KOREAN_NAMES.get(series_name, series_name)
        dbnomics_id = f"{provider}/{category}/{series_type}"
        print(f"  '{series_name}': {korean_name} ({dbnomics_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in ISM_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_name in series_list:
                korean_name = ISM_KOREAN_NAMES.get(series_name, series_name)
                dbnomics_path = ISM_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({dbnomics_path})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not ISM_DATA or 'load_info' not in ISM_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': ISM_DATA['load_info']['loaded'],
        'series_count': ISM_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': ISM_DATA['load_info']
    }

# %%
# === 사용 예시 (필수) ===

if __name__ == "__main__":
    print("=== 리팩토링된 ISM PMI 분석 도구 사용법 ===")
    print("1. 데이터 로드:")
    print("   load_ism_data()  # 스마트 업데이트")
    print("   load_ism_data(force_reload=True)  # 강제 재로드")
    print()
    print("2. 🔥 범용 시각화 (가장 강력!):")
    print("   plot_ism_series_advanced(['pmi_pm', 'nm_pmi_pm'], 'multi_line', 'raw')")
    print("   plot_ism_series_advanced(['pmi_pm'], 'horizontal_bar', 'raw', left_ytitle='지수')")
    print("   plot_ism_series_advanced(['pmi_pm'], 'single_line', 'mom', periods=24, left_ytitle='%')")
    print("   plot_ism_series_advanced(['pmi_pm', 'nm_pmi_pm'], 'dual_axis', 'raw', left_ytitle='제조업', right_ytitle='비제조업')")
    print()
    print("3. 🔥 데이터 Export:")
    print("   export_ism_data(['pmi_pm', 'nm_pmi_pm'], 'raw')")
    print("   export_ism_data(['pmi_pm'], 'mom', periods=24, file_format='csv')")
    print("   export_ism_data(['pmi_pm'], 'yoy', target_date='2024-06-01')")
    print()
    print("✅ plot_ism_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
    print("✅ export_ism_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
    print("✅ DBnomics API를 사용한 ISM PMI 전용 리팩토링!")

    load_ism_data()
    plot_ism_series_advanced(['pmi_pm', 'nm_pmi_pm'], 'multi_line', 'raw')
