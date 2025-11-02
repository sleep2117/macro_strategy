"""
Data loader for portfolio database
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# 경로 설정
BASE_DIR = Path(__file__).parent
DATABASE_DIR = BASE_DIR / "database"


def load_performance_history() -> pd.DataFrame:
    """
    Load historical performance data

    Returns:
        DataFrame with columns: 일자, 기초평가금액, 입금액, 출금액, 기말평가금액, 투자손익
    """
    file_path = DATABASE_DIR / "historical_performance.csv"

    if not file_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(file_path)

    # 날짜 변환
    df['일자'] = pd.to_datetime(df['일자'])

    # 숫자 변환
    numeric_cols = ['기초평가금액', '입금액', '출금액', '기말평가금액', '투자손익']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def load_holdings_history() -> pd.DataFrame:
    """
    Load historical holdings snapshots

    Returns:
        DataFrame with columns: 기준일자, 계좌번호, 상품유형, 종목명, etc.
    """
    file_path = DATABASE_DIR / "historical_holdings.csv"

    if not file_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(file_path)

    # 날짜 변환
    df['기준일자'] = pd.to_datetime(df['기준일자'])

    # 숫자 변환
    numeric_cols = ['잔고수량', '현재가', '매수금액', '평가금액', '평가손익', '수익률(%)']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def load_transaction_history() -> pd.DataFrame:
    """
    Load historical transaction log

    Returns:
        DataFrame with columns: 거래일자, 계좌번호, 거래유형, 종목명, etc.
    """
    file_path = DATABASE_DIR / "historical_transactions.csv"

    if not file_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(file_path)

    # 날짜 변환
    df['거래일자'] = pd.to_datetime(df['거래일자'])

    # 숫자 변환
    numeric_cols = ['거래수량', '거래단가', '거래금액', '정산금액', '수수료/세금',
                    '외화거래금액', '외화정산금액', '외화예수금액']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def load_account_master() -> pd.DataFrame:
    """
    Load account master data

    Returns:
        DataFrame with columns: 계좌번호, 계좌명, 고객명, 관계
    """
    file_path = DATABASE_DIR / "account_master.csv"

    if not file_path.exists():
        return pd.DataFrame()

    # Read with custom parsing for quoted CSV
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        content = f.read()

    # Remove quotes and parse
    lines = []
    for line in content.strip().split('\n'):
        cleaned_line = line.strip().strip('"')
        lines.append(cleaned_line)

    from io import StringIO
    df = pd.read_csv(StringIO('\n'.join(lines)))

    return df


def load_metadata() -> dict:
    """
    Load database metadata

    Returns:
        dict with metadata information
    """
    file_path = DATABASE_DIR / "metadata.json"

    if not file_path.exists():
        return {}

    with open(file_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    return metadata


def get_date_range() -> tuple:
    """
    Get available date range from performance data

    Returns:
        (start_date, end_date) tuple
    """
    df = load_performance_history()

    if df.empty:
        return None, None

    return df['일자'].min(), df['일자'].max()


def filter_by_date_range(df: pd.DataFrame, date_col: str,
                          start_date=None, end_date=None) -> pd.DataFrame:
    """
    Filter DataFrame by date range

    Args:
        df: DataFrame to filter
        date_col: Name of date column
        start_date: Start date (inclusive)
        end_date: End date (inclusive)

    Returns:
        Filtered DataFrame
    """
    if df.empty:
        return df

    result = df.copy()

    if start_date is not None:
        result = result[result[date_col] >= pd.to_datetime(start_date)]

    if end_date is not None:
        result = result[result[date_col] <= pd.to_datetime(end_date)]

    return result


def get_latest_holdings() -> pd.DataFrame:
    """
    Get latest holdings snapshot

    Returns:
        DataFrame with most recent holdings
    """
    df = load_holdings_history()

    if df.empty:
        return df

    # Get latest date
    latest_date = df['기준일자'].max()

    # Filter for latest date
    return df[df['기준일자'] == latest_date].copy()


def get_performance_summary(start_date=None, end_date=None) -> dict:
    """
    Get performance summary for date range

    Args:
        start_date: Start date (default: earliest)
        end_date: End date (default: latest)

    Returns:
        dict with performance summary
    """
    df = load_performance_history()

    if df.empty:
        return {}

    # Filter by date range
    df = filter_by_date_range(df, '일자', start_date, end_date)

    if df.empty:
        return {}

    # Calculate metrics
    start_amount = df.iloc[0]['기초평가금액']
    end_amount = df.iloc[-1]['기말평가금액']
    total_deposits = df['입금액'].sum()
    total_withdrawals = df['출금액'].sum()
    total_investment_profit = df['투자손익'].sum()

    # Calculate returns (입출금 제외, 순수 투자수익만)
    # 복리 수익률 계산
    daily_returns = (df['투자손익'] / df['기초평가금액']) * 100
    total_return_pct = ((1 + daily_returns / 100).prod() - 1) * 100

    summary = {
        'period': {
            'start': df['일자'].min(),
            'end': df['일자'].max()
        },
        'amount': {
            'start': start_amount,
            'end': end_amount,
            'change': end_amount - start_amount
        },
        'cash_flow': {
            'deposits': total_deposits,
            'withdrawals': total_withdrawals,
            'net': total_deposits - total_withdrawals
        },
        'profit': {
            'investment': total_investment_profit,
            'total': end_amount - start_amount - (total_deposits - total_withdrawals)
        },
        'return': {
            'percentage': total_return_pct
        },
        'days': len(df)
    }

    return summary


def get_holdings_by_account() -> pd.DataFrame:
    """
    Get latest holdings grouped by account

    Returns:
        DataFrame with holdings summary by account
    """
    df = get_latest_holdings()

    if df.empty:
        return df

    summary = df.groupby('계좌번호').agg({
        '평가금액': 'sum',
        '평가손익': 'sum',
        '종목명': 'count'
    }).rename(columns={'종목명': '보유종목수'})

    # Calculate return percentage
    summary['수익률(%)'] = (summary['평가손익'] / (summary['평가금액'] - summary['평가손익'])) * 100

    return summary


def get_holdings_by_asset_type() -> pd.DataFrame:
    """
    Get latest holdings grouped by asset type

    Returns:
        DataFrame with holdings summary by asset type
    """
    df = get_latest_holdings()

    if df.empty:
        return df

    summary = df.groupby('상품유형').agg({
        '평가금액': 'sum',
        '평가손익': 'sum',
        '종목명': 'count'
    }).rename(columns={'종목명': '종목수'})

    # Calculate return percentage
    summary['수익률(%)'] = (summary['평가손익'] / (summary['평가금액'] - summary['평가손익'])) * 100

    return summary


def get_transaction_summary(start_date=None, end_date=None) -> dict:
    """
    Get transaction summary for date range

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        dict with transaction summary
    """
    df = load_transaction_history()

    if df.empty:
        return {}

    # Filter by date range
    df = filter_by_date_range(df, '거래일자', start_date, end_date)

    if df.empty:
        return {}

    # Calculate daily average
    unique_days = df['거래일자'].nunique()
    daily_average = len(df) / unique_days if unique_days > 0 else 0

    summary = {
        'total_transactions': len(df),
        'period': {
            'start': df['거래일자'].min(),
            'end': df['거래일자'].max()
        },
        'by_type': df['거래유형'].value_counts().to_dict(),
        'total_amount': df['거래금액'].sum(),
        'total_fees': df['수수료/세금'].sum(),
        'daily_average': daily_average
    }

    return summary


def calculate_daily_returns() -> pd.Series:
    """
    Calculate daily returns (%)

    Returns:
        Series with daily returns
    """
    df = load_performance_history()

    if df.empty:
        return pd.Series()

    # Calculate return based on 기초평가금액 and 투자손익
    returns = (df['투자손익'] / df['기초평가금액']) * 100
    returns.index = df['일자']

    return returns


def calculate_cumulative_returns() -> pd.Series:
    """
    Calculate cumulative returns from the start

    Returns:
        Series with cumulative returns (%)
    """
    df = load_performance_history()

    if df.empty:
        return pd.Series()

    start_amount = df.iloc[0]['기초평가금액']
    cumulative_returns = ((df['기말평가금액'] - start_amount) / start_amount) * 100
    cumulative_returns.index = df['일자']

    return cumulative_returns
