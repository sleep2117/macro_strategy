"""
Portfolio analysis tools
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import data_loader


class PortfolioAnalyzer:
    """
    Portfolio performance analyzer
    """

    def __init__(self):
        """Initialize analyzer"""
        self.performance = data_loader.load_performance_history()
        self.holdings = data_loader.load_holdings_history()
        self.transactions = data_loader.load_transaction_history()
        self.accounts = data_loader.load_account_master()
        self.metadata = data_loader.load_metadata()

    def get_overall_summary(self) -> dict:
        """
        Get overall portfolio summary

        Returns:
            dict with overall metrics
        """
        summary = data_loader.get_performance_summary()

        # Add latest holdings info
        latest_holdings = data_loader.get_latest_holdings()
        if not latest_holdings.empty:
            summary['holdings'] = {
                'count': len(latest_holdings),
                'total_value': latest_holdings['평가금액'].sum(),
                'total_profit': latest_holdings['평가손익'].sum()
            }

        # Add transaction info
        trans_summary = data_loader.get_transaction_summary()
        if trans_summary:
            summary['transactions'] = trans_summary

        return summary

    def calculate_performance_metrics(self, start_date=None, end_date=None) -> dict:
        """
        Calculate key performance metrics

        Args:
            start_date: Start date for calculation
            end_date: End date for calculation

        Returns:
            dict with performance metrics
        """
        df = self.performance.copy()

        # Filter by date
        if start_date:
            df = df[df['일자'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['일자'] <= pd.to_datetime(end_date)]

        if df.empty:
            return {}

        # Calculate metrics
        start_amount = df.iloc[0]['기초평가금액']
        end_amount = df.iloc[-1]['기말평가금액']

        # Daily returns (순수 투자손익만 반영)
        daily_returns = (df['투자손익'] / df['기초평가금액']) * 100

        # Total return (복리 계산, 입출금 영향 제거)
        # 각 일의 수익률을 복리로 누적
        total_return = ((1 + daily_returns / 100).prod() - 1) * 100

        # 입출금 정보 (참고용)
        total_deposits = df['입금액'].sum()
        total_withdrawals = df['출금액'].sum()
        net_cash_flow = total_deposits - total_withdrawals

        # Volatility (annualized)
        volatility = daily_returns.std() * np.sqrt(252)

        # Sharpe ratio (assuming risk-free rate = 0 for simplicity)
        sharpe = (daily_returns.mean() * 252) / volatility if volatility > 0 else 0

        # Maximum drawdown
        cumulative = (1 + daily_returns / 100).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min() * 100

        # Win rate
        winning_days = len(daily_returns[daily_returns > 0])
        total_days = len(daily_returns)
        win_rate = (winning_days / total_days) * 100 if total_days > 0 else 0

        # Average win/loss
        avg_win = daily_returns[daily_returns > 0].mean() if len(daily_returns[daily_returns > 0]) > 0 else 0
        avg_loss = daily_returns[daily_returns < 0].mean() if len(daily_returns[daily_returns < 0]) > 0 else 0
        win_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

        metrics = {
            'total_return': total_return,
            'volatility': volatility,
            'sharpe_ratio': sharpe,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'win_loss_ratio': win_loss_ratio,
            'best_day': daily_returns.max(),
            'worst_day': daily_returns.min(),
            'days': len(df),
            'total_deposits': total_deposits,
            'total_withdrawals': total_withdrawals,
            'net_cash_flow': net_cash_flow
        }

        return metrics

    def get_daily_performance_chart_data(self) -> pd.DataFrame:
        """
        Get data for daily performance chart

        Returns:
            DataFrame with date, amount, daily_return, cumulative_return
        """
        df = self.performance.copy()

        if df.empty:
            return df

        # Calculate returns
        df['daily_return'] = (df['투자손익'] / df['기초평가금액']) * 100

        start_amount = df.iloc[0]['기초평가금액']
        df['cumulative_return'] = ((df['기말평가금액'] - start_amount) / start_amount) * 100

        return df[['일자', '기말평가금액', 'daily_return', 'cumulative_return']]

    def get_holdings_breakdown(self) -> dict:
        """
        Get detailed holdings breakdown

        Returns:
            dict with holdings analysis
        """
        latest = data_loader.get_latest_holdings()

        if latest.empty:
            return {}

        # By asset type
        by_asset = data_loader.get_holdings_by_asset_type()

        # By account
        by_account = data_loader.get_holdings_by_account()

        # Top holdings
        top_holdings = latest.nlargest(10, '평가금액')[
            ['종목명', '평가금액', '평가손익', '수익률(%)']
        ]

        breakdown = {
            'by_asset_type': by_asset.to_dict(),
            'by_account': by_account.to_dict(),
            'top_holdings': top_holdings.to_dict('records'),
            'total_value': latest['평가금액'].sum(),
            'total_profit': latest['평가손익'].sum()
        }

        return breakdown

    def get_transaction_analysis(self, start_date=None, end_date=None) -> dict:
        """
        Get transaction analysis

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            dict with transaction analysis
        """
        df = self.transactions.copy()

        # Filter by date
        if start_date:
            df = df[df['거래일자'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['거래일자'] <= pd.to_datetime(end_date)]

        if df.empty:
            return {}

        # By transaction type
        by_type = df.groupby('거래유형').agg({
            '거래금액': 'sum',
            '정산금액': 'sum',
            '수수료/세금': 'sum',
            '거래번호': 'count'
        }).rename(columns={'거래번호': '건수'})

        # Daily transaction count
        daily_count = df.groupby(df['거래일자'].dt.date).size()

        # Total fees
        total_fees = df['수수료/세금'].sum()

        analysis = {
            'total_transactions': len(df),
            'by_type': by_type.to_dict(),
            'daily_average': len(df) / df['거래일자'].nunique() if df['거래일자'].nunique() > 0 else 0,
            'total_fees': total_fees,
            'period': {
                'start': df['거래일자'].min(),
                'end': df['거래일자'].max()
            }
        }

        return analysis

    def get_monthly_performance(self) -> pd.DataFrame:
        """
        Get monthly performance summary

        Returns:
            DataFrame with monthly metrics
        """
        df = self.performance.copy()

        if df.empty:
            return df

        # Add year-month column
        df['year_month'] = df['일자'].dt.to_period('M')

        # Group by month
        monthly = df.groupby('year_month').agg({
            '기초평가금액': 'first',
            '기말평가금액': 'last',
            '입금액': 'sum',
            '출금액': 'sum',
            '투자손익': 'sum'
        })

        # Calculate monthly return
        monthly['월간수익률'] = ((monthly['기말평가금액'] - monthly['기초평가금액']) / monthly['기초평가금액']) * 100

        return monthly

    def get_drawdown_series(self) -> pd.Series:
        """
        Calculate drawdown series over time

        Returns:
            Series with drawdown percentages
        """
        df = self.performance.copy()

        if df.empty:
            return pd.Series()

        # Calculate cumulative returns
        start_amount = df.iloc[0]['기초평가금액']
        values = df['기말평가금액']

        # Calculate running maximum
        running_max = values.expanding().max()

        # Calculate drawdown
        drawdown = ((values - running_max) / running_max) * 100

        drawdown.index = df['일자']

        return drawdown

    def get_account_comparison(self) -> pd.DataFrame:
        """
        Compare performance across accounts

        Returns:
            DataFrame with account-level comparison
        """
        latest = data_loader.get_latest_holdings()

        if latest.empty:
            return pd.DataFrame()

        # Group by account
        comparison = latest.groupby('계좌번호').agg({
            '평가금액': 'sum',
            '평가손익': 'sum',
            '매수금액': 'sum',
            '종목명': 'count'
        }).rename(columns={'종목명': '보유종목수'})

        # Calculate return
        comparison['수익률(%)'] = (comparison['평가손익'] / comparison['매수금액']) * 100

        # Add account names from master
        if not self.accounts.empty:
            account_names = self.accounts.set_index('계좌번호')['계좌명'].to_dict()
            comparison['계좌명'] = comparison.index.map(account_names)

        return comparison

    def export_summary_report(self) -> dict:
        """
        Export comprehensive summary report

        Returns:
            dict with all key metrics and summaries
        """
        report = {
            'metadata': self.metadata,
            'overall_summary': self.get_overall_summary(),
            'performance_metrics': self.calculate_performance_metrics(),
            'holdings_breakdown': self.get_holdings_breakdown(),
            'transaction_analysis': self.get_transaction_analysis(),
            'monthly_performance': self.get_monthly_performance().to_dict() if not self.get_monthly_performance().empty else {},
            'account_comparison': self.get_account_comparison().to_dict() if not self.get_account_comparison().empty else {}
        }

        return report


def get_analyzer() -> PortfolioAnalyzer:
    """
    Factory function to create analyzer instance

    Returns:
        PortfolioAnalyzer instance
    """
    return PortfolioAnalyzer()
