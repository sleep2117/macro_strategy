# Portfolio Performance Analysis Dashboard

매달 누적되는 트랙 레코드를 기반으로 포트폴리오 성과를 분석하는 Streamlit 대시보드입니다.

## 🎯 주요 기능

### 📊 Overview (개요)
- 전체 포트폴리오 가치 추이
- 누적 수익률 차트
- 주요 성과 지표 (샤프 지수, 최대 낙폭, 승률 등)

### 📈 Performance Analysis (성과 분석)
- 일별 수익률 분석
- 월별 성과 요약
- 리스크 지표 및 낙폭 분석

### 💼 Holdings (보유 자산)
- 현재 보유 종목 현황
- 자산 유형별 배분
- 계좌별 성과 비교

### 💳 Transactions (거래 내역)
- 전체 거래 내역
- 거래 유형별 통계
- 거래 수수료 분석

### 🎯 Momentum Allocation (모멘텀 기반 자산 배분)
- 3/6/12개월 모멘텀 수익률 자동 계산
- 모멘텀 계수 기반 추천 비중 산출
- 국가별/자산별 배분 시각화
- 커스텀 티커 지원

### 🚦 Absolute Momentum (절대 모멘텀 신호)
- 자산별 매수/중단 신호 (🟢 Buy / 🔴 Stop)
- 6개월 모멘텀 기반 트렌드 판단
- 시장 상태 요약 (강세 자산 비율)
- 신호별 색상 구분 테이블

### ⚖️ Asset Allocation (자산 배분 시뮬레이터)
- 주식:채권 비율 조정 (50:50 ~ 80:20)
- 코어:새틀라이트 비율 조정 (50:50 ~ 100:0)
- 현재 포트폴리오 분석
- 리밸런싱 워크플로우 가이드

---

## 🗂️ 데이터 구조

```
Portfolio/
├── data/                           # 현재 월 데이터
│   ├── account_master.csv
│   ├── daily_performance.csv
│   ├── holdings_snapshot.csv
│   └── transaction_log.csv
│
├── database/                       # 누적 데이터베이스
│   ├── historical_performance.csv
│   ├── historical_holdings.csv
│   ├── historical_transactions.csv
│   ├── account_master.csv
│   ├── metadata.json
│   └── backups/
│
├── app.py                          # Streamlit 대시보드
├── data_loader.py                  # 데이터 로더
├── portfolio_analyzer.py            # 성과 분석 로직
└── update_database.py              # 데이터 통합 스크립트
```

---

## 🚀 시작하기

### 1. 의존성 설치
```bash
pip install pandas numpy streamlit plotly openpyxl yfinance
```

### 2. 데이터베이스 초기화
```bash
python update_database.py
```

### 3. Streamlit 앱 실행
```bash
streamlit run app.py
```

### 4. 브라우저 접속
- 자동으로 http://localhost:8501 이 열립니다.

---

## 📊 현재 포트폴리오 성과

**기간**: 2025-10-01 ~ 2025-10-30 (17일)

| 지표 | 값 |
|------|-----|
| 시작 금액 | ₩24,628,488 |
| 기말 금액 | ₩36,749,069 |
| 총 수익률 | **49.21%** |
| 투자 손익 | ₩195,749 |
| 샤프 지수 | 3.71 |
| 최대 낙폭 | -0.30% |
| 승률 | 70.59% |
| 변동성 | 2.22% |

---

## 📅 매달 데이터 업데이트 방법

### 1. 새 월 데이터 준비
증권사에서 다음 파일들을 `data/` 폴더에 저장:
- account_master.csv
- daily_performance.csv
- holdings_snapshot.csv
- transaction_log.csv

### 2. 데이터베이스 업데이트 실행
```bash
python update_database.py
```

이 스크립트는 자동으로:
- 기존 데이터 백업
- 새 데이터와 기존 데이터 통합
- 중복 제거
- 메타데이터 업데이트

### 3. Streamlit 앱에서 확인
업데이트된 데이터는 즉시 대시보드에 반영됩니다.

---

## 📈 성과 지표 설명

### Sharpe Ratio (샤프 지수)
- 위험 대비 수익률
- 값이 클수록 좋음 (보통 1 이상이 양호)
- **현재**: 3.71 (매우 우수)

### Maximum Drawdown (최대 낙폭)
- 고점 대비 최대 하락률
- 값이 작을수록 좋음
- **현재**: -0.30% (매우 안정적)

### Win Rate (승률)
- 수익을 낸 날의 비율
- 값이 높을수록 좋음
- **현재**: 70.59% (우수)

### Volatility (변동성)
- 연환산 변동성
- 값이 작을수록 안정적
- **현재**: 2.22% (매우 안정적)

---

## 🔧 커스터마이징

### 성과 지표 추가
`portfolio_analyzer.py`의 `calculate_performance_metrics()` 함수에 추가

### 차트 스타일 변경
`app.py`의 Plotly 설정 수정

### 데이터 필드 추가
`data_loader.py`에서 로딩 로직 수정

---

## 📚 추가 문서

- [DEVELOPMENT_LOG.md](DEVELOPMENT_LOG.md) - 개발 진행 상황 및 완료 기능 목록
- [TAA_GUIDE.md](TAA_GUIDE.md) - TAA 도구 사용 가이드 (모멘텀 전략 상세 설명)
- [DATABASE_DESIGN.md](DATABASE_DESIGN.md) - 데이터베이스 설계 문서
- [investment_method.md](investment_method.md) - 투자 철학 및 전략

---

## 🐛 문제 해결

### 인코딩 오류
- CSV 파일이 UTF-8 BOM으로 저장되어 있는지 확인

### 데이터 업데이트 실패
- `data/` 폴더에 4개 파일이 모두 있는지 확인
- CSV 파일 형식이 올바른지 확인

### Streamlit 실행 오류
```bash
pip install --upgrade streamlit plotly
```

---

## 📝 최근 업데이트 (2025-11-01)

### ✅ 완료된 기능
- [x] 모멘텀 기반 TAA (전술적 자산 배분) 도구
- [x] 절대 모멘텀 신호 대시보드
- [x] 자산 배분 시뮬레이터 & 리밸런싱 도구
- [x] yfinance 실시간 시장 데이터 연동

### 📋 향후 개선 계획
- [ ] 백테스팅 & 시나리오 분석
- [ ] 새틀라이트 종목 스크리너
- [ ] 벤치마크 성과 비교 (S&P500 vs 포트폴리오)
- [ ] 섹터별 성과 분석
- [ ] 월별 리포트 PDF 자동 생성
- [ ] 알림 기능 (목표 수익률 도달 시)

---

**마지막 업데이트**: 2025-11-01
