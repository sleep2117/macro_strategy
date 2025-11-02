# 포트폴리오 통합 데이터베이스 설계

## 개요
매달 새로운 데이터를 누적하여 장기 트랙 레코드를 구축하는 데이터베이스 시스템

## 데이터 흐름

```
data/ (현재 월 데이터)
  ├── account_master.csv
  ├── daily_performance.csv
  ├── holdings_snapshot.csv
  └── transaction_log.csv
          ↓
    [데이터 통합 스크립트]
          ↓
database/ (누적 데이터)
  ├── historical_performance.csv
  ├── historical_holdings.csv
  ├── historical_transactions.csv
  ├── account_master.csv
  └── metadata.json
```

---

## 1. 원본 데이터 (data/)

### 1.1 account_master.csv
**목적**: 계좌 마스터 정보

| 컬럼명 | 설명 | 예시 |
|--------|------|------|
| 계좌번호 | 계좌 번호 | 7096******-01 |
| 계좌명 | 계좌 이름 | 종합(평생혜택 비대면) |
| 고객명 | 고객 이름 | 박진영 |
| 관계 | 관계 | 본인 |

**특징**: 계좌 정보는 거의 변하지 않음, 신규 계좌 추가만 발생

---

### 1.2 daily_performance.csv
**목적**: 일일 포트폴리오 성과

| 컬럼명 | 설명 | 예시 |
|--------|------|------|
| 일자 | 날짜 | 2025-10-30 |
| 기초평가금액 | 시작 금액 | 36748054 |
| 입금액 | 당일 입금 | 0 |
| 출금액 | 당일 출금 | 0 |
| 기말평가금액 | 종료 금액 | 36749069 |
| 투자손익 | 당일 손익 | 1015 |

**특징**:
- 매일 1개 row 추가
- 일자가 Primary Key

---

### 1.3 holdings_snapshot.csv
**목적**: 특정 시점의 보유 종목 현황

| 컬럼명 | 설명 | 예시 |
|--------|------|------|
| 기준일자 | 기준 날짜 | 2025-10-31 |
| 계좌번호 | 계좌 번호 | 7096******-01 |
| 상품유형 | 상품 종류 | 해외주식 |
| 종목명 | 종목 이름 | 앤테로 리소스 |
| 종목코드/통화 | 코드 또는 통화 | USD |
| 잔고수량 | 보유 수량 | 17 |
| 현재가 | 현재 가격 | 30.4 |
| 매수금액 | 매수 원가 | 810233 |
| 평가금액 | 현재 평가액 | 735509 |
| 평가손익 | 평가 손익 | -74724 |
| 수익률(%) | 수익률 | -9.22 |

**특징**:
- 월말 또는 특정 시점 스냅샷
- (기준일자, 계좌번호, 종목명)이 Composite Key

---

### 1.4 transaction_log.csv
**목적**: 모든 거래 내역

| 컬럼명 | 설명 | 예시 |
|--------|------|------|
| 거래일자 | 거래 날짜 | 2025-10-31 |
| 계좌번호 | 계좌 번호 | 7096****-01 |
| 거래번호 | 거래 순번 | 1 |
| 거래유형 | 거래 타입 | 매수 |
| 종목명 | 종목 이름 | 약정식RP |
| 거래수량 | 수량 | 1000 |
| 거래단가 | 단가 | 1000 |
| 거래금액 | 거래 금액 | 1000 |
| 정산금액 | 정산 금액 | 1000 |
| 수수료/세금 | 수수료 | 0 |
| 통화코드 | 통화 | USD |
| 외화거래금액 | 외화 거래 금액 | 0.00 |
| 외화정산금액 | 외화 정산 금액 | 0.00 |
| 외화예수금액 | 외화 예수 금액 | 0.00 |

**특징**:
- 모든 거래 기록
- (거래일자, 계좌번호, 거래번호)가 Composite Key

---

## 2. 통합 데이터베이스 (database/)

### 2.1 historical_performance.csv
**목적**: 모든 일일 성과 데이터 누적

**구조**: daily_performance.csv와 동일

**업데이트 로직**:
```python
# 1. 기존 데이터 로드
historical = pd.read_csv('database/historical_performance.csv')

# 2. 새 데이터 로드
new_data = pd.read_csv('data/daily_performance.csv')

# 3. 중복 제거 (일자 기준)
combined = pd.concat([historical, new_data])
combined = combined.drop_duplicates(subset=['일자'], keep='last')

# 4. 정렬 및 저장
combined = combined.sort_values('일자')
combined.to_csv('database/historical_performance.csv', index=False)
```

---

### 2.2 historical_holdings.csv
**목적**: 모든 보유 종목 스냅샷 누적

**구조**: holdings_snapshot.csv와 동일

**업데이트 로직**:
```python
# 1. 기존 데이터 로드
historical = pd.read_csv('database/historical_holdings.csv')

# 2. 새 데이터 로드
new_data = pd.read_csv('data/holdings_snapshot.csv')

# 3. 중복 제거 (기준일자, 계좌번호, 종목명 기준)
combined = pd.concat([historical, new_data])
combined = combined.drop_duplicates(
    subset=['기준일자', '계좌번호', '종목명'],
    keep='last'
)

# 4. 정렬 및 저장
combined = combined.sort_values(['기준일자', '계좌번호'])
combined.to_csv('database/historical_holdings.csv', index=False)
```

---

### 2.3 historical_transactions.csv
**목적**: 모든 거래 내역 누적

**구조**: transaction_log.csv와 동일

**업데이트 로직**:
```python
# 1. 기존 데이터 로드
historical = pd.read_csv('database/historical_transactions.csv')

# 2. 새 데이터 로드
new_data = pd.read_csv('data/transaction_log.csv')

# 3. 중복 제거 (거래일자, 계좌번호, 거래번호 기준)
combined = pd.concat([historical, new_data])
combined = combined.drop_duplicates(
    subset=['거래일자', '계좌번호', '거래번호'],
    keep='last'
)

# 4. 정렬 및 저장
combined = combined.sort_values(['거래일자', '거래번호'])
combined.to_csv('database/historical_transactions.csv', index=False)
```

---

### 2.4 account_master.csv
**목적**: 최신 계좌 마스터 정보

**업데이트 로직**:
```python
# 단순히 최신 데이터로 덮어쓰기
import shutil
shutil.copy('data/account_master.csv', 'database/account_master.csv')
```

---

### 2.5 metadata.json
**목적**: 데이터베이스 메타데이터

**구조**:
```json
{
  "last_update": "2025-10-31T21:34:00",
  "data_period": {
    "start": "2025-10-01",
    "end": "2025-10-31"
  },
  "record_counts": {
    "performance": 17,
    "holdings": 6,
    "transactions": 15,
    "accounts": 2
  },
  "version": "1.0.0"
}
```

---

## 3. 데이터 통합 워크플로우

### 3.1 월별 데이터 업데이트 프로세스

```
1. 새 월의 데이터를 data/ 폴더에 저장
   ├── account_master.csv
   ├── daily_performance.csv
   ├── holdings_snapshot.csv
   └── transaction_log.csv

2. 데이터 통합 스크립트 실행
   python scripts/update_database.py

3. 스크립트 실행 순서:
   a) 데이터 유효성 검사
   b) 중복 제거
   c) database/ 폴더에 통합
   d) metadata.json 업데이트
   e) 백업 생성

4. Streamlit 앱에서 database/ 데이터 사용
```

---

## 4. 데이터 품질 관리

### 4.1 유효성 검사

- **일자 형식**: YYYY-MM-DD
- **금액 필드**: 숫자형, NULL 불가
- **계좌번호**: 기존 마스터와 일치
- **중복 체크**: Primary Key 기준

### 4.2 에러 처리

- 데이터 형식 오류 → 로그 기록 후 스킵
- 중복 데이터 → 최신 데이터로 덮어쓰기
- 누락 필드 → 기본값 또는 NULL

---

## 5. 백업 전략

### 5.1 자동 백업

```
database/
  ├── backups/
  │   ├── 2025-10-31/
  │   │   ├── historical_performance.csv
  │   │   ├── historical_holdings.csv
  │   │   └── historical_transactions.csv
  │   └── 2025-09-30/
  └── [현재 파일들]
```

### 5.2 백업 주기
- 매번 업데이트 시 이전 버전 백업
- 월별 폴더로 관리

---

## 6. 향후 확장

- SQLite 또는 PostgreSQL로 마이그레이션
- 실시간 데이터 연동
- API 개발 (FastAPI)
- 데이터 시각화 대시보드 고도화

---

**마지막 업데이트**: 2025-10-31
