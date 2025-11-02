"""
데이터 통합 스크립트 - data/ 폴더의 데이터를 database/로 누적
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import shutil

# 경로 설정
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATABASE_DIR = BASE_DIR / "database"
BACKUP_DIR = DATABASE_DIR / "backups"

# 현재 날짜
TODAY = datetime.now().strftime("%Y-%m-%d")


def read_csv_with_bom(file_path):
    """BOM이 있는 CSV 파일 읽기"""
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        content = f.read()

    # 따옴표로 감싸진 행을 처리
    lines = []
    for line in content.strip().split('\n'):
        # 따옴표 제거
        cleaned_line = line.strip().strip('"')
        lines.append(cleaned_line)

    # StringIO를 사용하여 DataFrame으로 변환
    from io import StringIO
    df = pd.read_csv(StringIO('\n'.join(lines)))
    return df


def backup_existing_database():
    """기존 데이터베이스 백업"""
    if not DATABASE_DIR.exists():
        return

    backup_folder = BACKUP_DIR / TODAY
    backup_folder.mkdir(parents=True, exist_ok=True)

    files_to_backup = [
        'historical_performance.csv',
        'historical_holdings.csv',
        'historical_transactions.csv',
        'account_master.csv',
        'metadata.json'
    ]

    for file in files_to_backup:
        src = DATABASE_DIR / file
        if src.exists():
            dst = backup_folder / file
            shutil.copy(src, dst)
            print(f"  [OK] Backed up: {file}")


def update_performance():
    """일일 성과 데이터 통합"""
    print("\n[1] Updating performance data...")

    # 새 데이터 로드
    new_data = read_csv_with_bom(DATA_DIR / 'daily_performance.csv')

    # 기존 데이터 로드 (없으면 빈 DataFrame)
    historical_file = DATABASE_DIR / 'historical_performance.csv'
    if historical_file.exists():
        historical = pd.read_csv(historical_file)
    else:
        historical = pd.DataFrame()

    # 통합
    if historical.empty:
        combined = new_data
    else:
        combined = pd.concat([historical, new_data], ignore_index=True)
        # 중복 제거 (일자 기준, 최신 데이터 유지)
        combined = combined.drop_duplicates(subset=['일자'], keep='last')

    # 정렬
    combined = combined.sort_values('일자').reset_index(drop=True)

    # 저장
    combined.to_csv(historical_file, index=False, encoding='utf-8-sig')
    print(f"  [OK] Performance data updated: {len(combined)} records")

    return len(combined)


def update_holdings():
    """보유 종목 스냅샷 통합"""
    print("\n[2] Updating holdings data...")

    # 새 데이터 로드
    new_data = read_csv_with_bom(DATA_DIR / 'holdings_snapshot.csv')

    # 기존 데이터 로드
    historical_file = DATABASE_DIR / 'historical_holdings.csv'
    if historical_file.exists():
        historical = pd.read_csv(historical_file)
    else:
        historical = pd.DataFrame()

    # 통합
    if historical.empty:
        combined = new_data
    else:
        combined = pd.concat([historical, new_data], ignore_index=True)
        # 중복 제거 (기준일자, 계좌번호, 종목명 기준)
        combined = combined.drop_duplicates(
            subset=['기준일자', '계좌번호', '종목명'],
            keep='last'
        )

    # 정렬
    combined = combined.sort_values(['기준일자', '계좌번호']).reset_index(drop=True)

    # 저장
    combined.to_csv(historical_file, index=False, encoding='utf-8-sig')
    print(f"  [OK] Holdings data updated: {len(combined)} records")

    return len(combined)


def update_transactions():
    """거래 내역 통합"""
    print("\n[3] Updating transaction data...")

    # 새 데이터 로드
    new_data = read_csv_with_bom(DATA_DIR / 'transaction_log.csv')

    # 기존 데이터 로드
    historical_file = DATABASE_DIR / 'historical_transactions.csv'
    if historical_file.exists():
        historical = pd.read_csv(historical_file)
    else:
        historical = pd.DataFrame()

    # 통합
    if historical.empty:
        combined = new_data
    else:
        combined = pd.concat([historical, new_data], ignore_index=True)
        # 중복 제거 (거래일자, 계좌번호, 거래번호 기준)
        combined = combined.drop_duplicates(
            subset=['거래일자', '계좌번호', '거래번호'],
            keep='last'
        )

    # 정렬
    combined = combined.sort_values(['거래일자', '거래번호']).reset_index(drop=True)

    # 저장
    combined.to_csv(historical_file, index=False, encoding='utf-8-sig')
    print(f"  [OK] Transaction data updated: {len(combined)} records")

    return len(combined)


def update_account_master():
    """계좌 마스터 정보 업데이트"""
    print("\n[4] Updating account master...")

    # 단순 복사
    src = DATA_DIR / 'account_master.csv'
    dst = DATABASE_DIR / 'account_master.csv'

    # 새 데이터 로드하여 레코드 수 확인
    df = read_csv_with_bom(src)

    shutil.copy(src, dst)
    print(f"  [OK] Account master updated: {len(df)} accounts")

    return len(df)


def update_metadata(counts):
    """메타데이터 업데이트"""
    print("\n[5] Updating metadata...")

    # 기간 계산
    perf_file = DATABASE_DIR / 'historical_performance.csv'
    if perf_file.exists():
        df_perf = pd.read_csv(perf_file)
        start_date = df_perf['일자'].min()
        end_date = df_perf['일자'].max()
    else:
        start_date = end_date = TODAY

    metadata = {
        "last_update": datetime.now().isoformat(),
        "data_period": {
            "start": start_date,
            "end": end_date
        },
        "record_counts": {
            "performance": counts['performance'],
            "holdings": counts['holdings'],
            "transactions": counts['transactions'],
            "accounts": counts['accounts']
        },
        "version": "1.0.0"
    }

    metadata_file = DATABASE_DIR / 'metadata.json'
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(f"  [OK] Metadata updated")
    print(f"    Period: {start_date} ~ {end_date}")
    print(f"    Records: {counts}")


def main():
    """메인 실행 함수"""
    print("="*60)
    print("포트폴리오 데이터베이스 업데이트")
    print("="*60)

    # 폴더 생성
    DATABASE_DIR.mkdir(exist_ok=True)
    BACKUP_DIR.mkdir(exist_ok=True)

    # 백업
    print("\n[Backup] Creating backup...")
    backup_existing_database()

    # 데이터 통합
    counts = {}
    counts['performance'] = update_performance()
    counts['holdings'] = update_holdings()
    counts['transactions'] = update_transactions()
    counts['accounts'] = update_account_master()

    # 메타데이터 업데이트
    update_metadata(counts)

    print("\n" + "="*60)
    print("[SUCCESS] Database update completed!")
    print("="*60)


if __name__ == "__main__":
    main()
