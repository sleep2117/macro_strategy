🔧 US Economic Data 리팩토링 프롬프트 
(Export 기능 포함 - 완전 업데이트 버전)

이 파일을 us_eco_utils.py를 활용해서
리팩토링해줘. 다음 규칙을 정확히 따라줘:

### 🚫 절대 건드리면 안되는 것들:
1. **시리즈 ID와 매핑**: 기존 시리즈
딕셔너리와 ID는 그대로 유지
2. **시리즈 개수**: 기존에 있던 시리즈
개수 그대로 유지
3. **한국어 이름 매핑**: 기존 KOREAN_NAMES
 딕셔너리 그대로 유지
4. **API 키**: 기존 API 키들 그대로 유지
5. **카테고리 분류**: 기존 CATEGORIES 딕셔너리 유지

### ✅ 리팩토링 구조:

#### 1. **헤더 섹션 (필수)**:
```python
# %%
"""
[기존 파일명] 데이터 분석 (리팩토링 버전)
- us_eco_utils를 사용한 통합 구조
- 시리즈 정의와 분석 로직만 포함
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import warnings
warnings.filterwarnings('ignore')

# 통합 유틸리티 함수 불러오기
from us_eco_utils import *
```

#### 2. **API 키 설정 (필수)**:

```python
# %%
# === BLS API 키 설정 ===
api_config.BLS_API_KEY = '...'
api_config.BLS_API_KEY2 = '...'
api_config.BLS_API_KEY3 = '...'

# 또는 FRED 데이터면:
# api_config.FRED_API_KEY = '...'
```

#### 3. **시리즈 정의 (기존 것 그대로 유지)**:

```python
# %%
# === [데이터명] 시리즈 정의 ===
[기존 시리즈 딕셔너리들 그대로 복사]
[기존 한국어 매핑 딕셔너리 그대로 복사]
[기존 카테고리 분류 딕셔너리 그대로 복사]
```

#### 4. **전역 변수 설정**:

```python
# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/[파일명]_data_refactored.csv'
[데이터명]_DATA = {}
```

#### 5. **데이터 로드 함수 (핵심)**:

```python
# %%
# === 데이터 로드 함수 ===
def load_[데이터명]_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 [데이터명] 데이터 로드"""
    global [데이터명]_DATA

    # 시리즈 딕셔너리를 직접 전달 (2024.08.24 업데이트)
    # SERIES는 이미 {'시리즈_이름': 'API_ID'} 형태여야 함
    result = load_economic_data(
        series_dict=[시리즈_딕셔너리],
        data_source='BLS',  # 또는 'FRED'
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=1000.0  # 고용:1000.0, 일반:10.0, CPI:10.0
    )

    if result:
        [데이터명]_DATA = result
        print_load_info()
        return True
    else:
        print("❌ [데이터명] 데이터 로드 실패")
        return False

def print_load_info():
    """[데이터명] 데이터 로드 정보 출력"""
    if not [데이터명]_DATA or 'load_info' not in [데이터명]_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = [데이터명]_DATA['load_info']
    print(f"\n📊 [데이터명] 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in [데이터명]_DATA and not [데이터명]_DATA['raw_data'].empty:
        latest_date = [데이터명]_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")
```

#### 6. **🔥 범용 시각화 함수 (필수)**:

```python
# %%
# === 범용 시각화 함수 ===
def plot_[데이터명]_series_advanced(series_list, chart_type='multi_line', 
                                    data_type='mom', periods=None, target_date=None,
                                    left_ytitle=None, right_ytitle=None):
    """범용 [데이터명] 시각화 함수 - plot_economic_series 활용"""
    if not [데이터명]_DATA:
        print("⚠️ 먼저 load_[데이터명]_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=[데이터명]_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle,
        right_ytitle=right_ytitle,
        korean_names=[한국어_매핑_딕셔너리]
    )
```

#### 7. **🔥 데이터 Export 함수 (필수)**:

```python
# %%
# === 데이터 Export 함수 ===
def export_[데이터명]_data(series_list, data_type='mom', periods=None, 
                           target_date=None, export_path=None, file_format='excel'):
    """[데이터명] 데이터 export 함수 - export_economic_data 활용"""
    if not [데이터명]_DATA:
        print("⚠️ 먼저 load_[데이터명]_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=[데이터명]_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=[한국어_매핑_딕셔너리],
        export_path=export_path,
        file_format=file_format
    )
```

#### 8. **데이터 접근 함수들 (필수)**:

```python
# %%
# === 데이터 접근 함수들 ===

def clear_[데이터명]_data():
    """[데이터명] 데이터 초기화"""
    global [데이터명]_DATA
    [데이터명]_DATA = {}
    print("🗑️ [데이터명] 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not [데이터명]_DATA or 'raw_data' not in [데이터명]_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_[데이터명]_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return [데이터명]_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in [데이터명]_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return [데이터명]_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not [데이터명]_DATA or 'mom_data' not in [데이터명]_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_[데이터명]_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return [데이터명]_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in [데이터명]_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return [데이터명]_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not [데이터명]_DATA or 'yoy_data' not in [데이터명]_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_[데이터명]_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return [데이터명]_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in [데이터명]_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return [데이터명]_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not [데이터명]_DATA or 'raw_data' not in [데이터명]_DATA:
        return []
    return list([데이터명]_DATA['raw_data'].columns)
```

#### 9. **유틸리티 함수들 (필수)**:

```python
# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 [데이터명] 시리즈 표시"""
    print("=== 사용 가능한 [데이터명] 시리즈 ===")
    
    # 2024.08.24 업데이트: 시리즈 이름이 key, API_ID가 value
    for series_name, series_id in [시리즈_딕셔너리].items():
        korean_name = [한국어_매핑_딕셔너리].get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in [카테고리_딕셔너리].items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            # 2024.08.24 업데이트: 카테고리도 시리즈 이름 기준으로 수정
            for series_name in series_list:
                korean_name = [한국어_매핑_딕셔너리].get(series_name, series_name)
                api_id = [시리즈_딕셔너리].get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not [데이터명]_DATA or 'load_info' not in [데이터명]_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': [데이터명]_DATA['load_info']['loaded'],
        'series_count': [데이터명]_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': [데이터명]_DATA['load_info']
    }
```

#### 10. **기존 분석 함수들 보존 (선택사항)**:

```python
# %%
# === [데이터명] 전용 시각화/분석 함수들 (보존) ===

# 기존 create_*, plot_*, analyze_*, run_* 함수들은 모두 그대로 유지
# 단, 아래 중복 함수들은 제거:
# - initialize_*, switch_api_key
# - save_*_to_csv, load_*_from_csv  
# - calculate_*, get_*_data
# - get_bls_data, get_series_data
# - update_*_from_api
# - check_recent_data_consistency
```

#### 11. **사용 예시 (필수)**:

```python
# %%
# === 사용 예시 ===

print("=== 리팩토링된 [데이터명] 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_[데이터명]_data()  # 스마트 업데이트")
print("   load_[데이터명]_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_[데이터명]_series_advanced(['series1', 'series2'], 'multi_line', 'mom')")
print("   plot_[데이터명]_series_advanced(['series1'], 'horizontal_bar', 'yoy', left_ytitle='%')")
print("   plot_[데이터명]_series_advanced(['series1'], 'single_line', 'mom', periods=24, left_ytitle='천 명')")
print("   plot_[데이터명]_series_advanced(['series1', 'series2'], 'dual_axis', 'raw', left_ytitle='%', right_ytitle='천 명')")
print()
print("3. 🔥 데이터 Export:")
print("   export_[데이터명]_data(['series1', 'series2'], 'mom')")
print("   export_[데이터명]_data(['series1'], 'raw', periods=24, file_format='csv')")
print("   export_[데이터명]_data(['series1'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_[데이터명]_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_[데이터명]_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
```

### ⚠️ 중요 사항:

1. **🔑 시리즈 딕셔너리 구조 변경 (2024.08.24 업데이트)**: 
   - **OLD (잘못된 구조)**: `SERIES = {'API_ID': '시리즈 설명'}` ❌
   - **NEW (올바른 구조)**: `SERIES = {'시리즈_이름': 'API_ID'}` ✅
   - **예시**:
     ```python
     # 잘못된 구조 (수정 필요)
     SERIES = {
         'WPSFD4': 'Final demand',  # API_ID가 key
         'JTS000000000000000JOL': 'Total nonfarm - Job openings'
     }
     
     # 올바른 구조 (올바른 형태)
     SERIES = {
         'final_demand_sa': 'WPSFD4',  # 시리즈 이름이 key
         'total_nonfarm_openings': 'JTS000000000000000JOL'
     }
     ```
   - **데이터 로드시**: `result = load_economic_data(series_dict=SERIES, ...)` 직접 전달
   - **한국어 매핑도 수정**: key를 새로운 시리즈 이름으로 변경 필수
2. **⭐ Y축 제목 파라미터 필수**: `left_ytitle=None, right_ytitle=None` 파라미터를 `plot_[데이터명]_series_advanced` 함수에 반드시 추가하고 `plot_economic_series`에 전달
3. **ytitle은 단위만**: "%" or "천 명" 등 단위만 사용
4. **periods=None**: 기본값을 None으로 해서 전체 데이터 표시
5. **🗑️ 기존 중복 함수들 모두 제거**: 
   - API 초기화 함수들 (initialize_*, switch_api_key)
   - CSV 저장/로드 함수들 (save_*_to_csv, load_*_from_csv)
   - 데이터 계산 함수들 (calculate_*, get_*_data)
   - API 호출 함수들 (get_bls_data, get_series_data)
   - 데이터 업데이트 함수들 (update_*_from_api)
   - 검증 함수들 (check_recent_data_consistency)
   - 모든 중복된 유틸리티 함수들
6. **데이터 소스 구분**: BLS면 'BLS', FRED면 'FRED'
7. **허용 오차 설정**: 고용 데이터면 1000.0, 일반 지표면 10.0, CPI 데이터면 10.0
8. **Export 기능 추가**: plot_xxx_series_advanced와 동일한 로직으로 export_xxx_data 함수 생성
9. **🔧 시각화 함수 보존**: 데이터별 전용 시각화 함수들은 그대로 유지
10. **분석 함수 보존**: run_*_analysis, analyze_* 등 분석 함수들은 그대로 유지
11. **데이터 접근 함수**: get_raw_data, get_mom_data, get_yoy_data 등 필수 추가
12. **안전성 체크**: 모든 함수에서 데이터 존재 여부 확인

### 📝 작업 순서:

1. **기존 파일 분석**: 시리즈 정의, 시각화 함수, 분석 함수 파악
2. **헤더 업데이트**: us_eco_utils import 및 API 설정
3. **시리즈 정의 복사**: 기존 딕셔너리들 그대로 유지
4. **핵심 함수 추가**: load_[데이터명]_data, plot_[데이터명]_series_advanced, export_[데이터명]_data
5. **데이터 접근 함수 추가**: get_raw_data, get_mom_data, get_yoy_data 등
6. **유틸리티 함수 추가**: show_available_series, show_category_options 등
7. **🗑️ 불필요한 함수 제거**: 위에 나열된 모든 중복 함수들 삭제
8. **시각화/분석 함수 보존**: 데이터별 전용 함수들은 그대로 유지
9. **사용 예시 업데이트**: 새로운 함수들 포함
10. **구문 검사**: python3 -m py_compile로 오류 확인

### 🎯 최종 목표:
- **통합된 구조**: us_eco_utils 기반
- **깔끔한 코드**: 중복 함수 완전 제거
- **기능 보존**: 기존 시각화/분석 기능 모두 유지
- **새로운 기능**: 범용 시각화 및 Export 기능 추가
- **데이터 접근**: 안전한 데이터 접근 함수들
- **완벽한 사용성**: 직관적이고 강력한 API

### 💡 참고 파일:
완벽한 리팩토링 예시는 다음 파일들을 참조:
- **완전 수정 완료**: `/home/jyp0615/Macro-analysis/us_eco/JOLTS_employ_refactor.py`
- **완전 수정 완료**: `/home/jyp0615/Macro-analysis/us_eco/PPI_analysis_refactor.py`
- **완전 수정 완료**: `/home/jyp0615/Macro-analysis/us_eco/import_price_refactor.py`
- **참고 (기본 구조)**: `/home/jyp0615/Macro-analysis/us_eco/ADP_employ_refactored.py`

### 🚨 시리즈 딕셔너리 구조 변경 필수!
**기존 CSV 파일 삭제 후 재생성 필요**: 구 형태로 저장된 CSV는 새 구조와 호환되지 않음

이렇게 리팩토링해줘!