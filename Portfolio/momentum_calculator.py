"""
Momentum-based Asset Allocation Calculator
Tactical Asset Allocation (TAA) using momentum strategies
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path

# Default index tickers
DEFAULT_INDICES = {
    'S&P500': 'SPY',
    'KOSPI': '069500.KS',
    'CSI300': 'ASHR',
    'Europe': 'VGK',
    'US Treasury': 'TLT',
    'KR Treasury': '148070.KS'
}


def fetch_index_data(tickers: dict, period: str = '2y') -> pd.DataFrame:
    """
    Fetch historical price data for indices

    Args:
        tickers: dict of {name: ticker}
        period: yfinance period (1y, 2y, 5y, etc.)

    Returns:
        DataFrame with adjusted close prices
    """
    data = {}

    for name, ticker in tickers.items():
        try:
            df = yf.Ticker(ticker).history(period=period)
            if not df.empty:
                data[name] = df['Close']
        except Exception as e:
            print(f"Error fetching {name} ({ticker}): {e}")

    if not data:
        return pd.DataFrame()

    result = pd.DataFrame(data)
    result = result.fillna(method='ffill')  # Forward fill missing values

    return result


def calculate_momentum(prices: pd.DataFrame, months: int) -> pd.Series:
    """
    Calculate N-month momentum (total return)

    Args:
        prices: DataFrame with price data
        months: Number of months for momentum calculation

    Returns:
        Series with momentum returns (%)
    """
    if prices.empty:
        return pd.Series()

    # Get price N months ago
    days_back = months * 30  # Approximate

    if len(prices) < days_back:
        # Not enough data, use all available
        start_price = prices.iloc[0]
    else:
        start_price = prices.iloc[-days_back]

    current_price = prices.iloc[-1]

    # Calculate return
    momentum = ((current_price - start_price) / start_price) * 100

    return momentum


def calculate_dual_momentum(prices: pd.DataFrame, short_months: int = 3,
                           long_months: int = 12) -> pd.Series:
    """
    Calculate dual momentum (average of two periods)

    Args:
        prices: DataFrame with price data
        short_months: Short-term momentum period
        long_months: Long-term momentum period

    Returns:
        Series with average momentum
    """
    short_mom = calculate_momentum(prices, short_months)
    long_mom = calculate_momentum(prices, long_months)

    # Average of both
    dual_mom = (short_mom + long_mom) / 2

    return dual_mom


def calculate_allocation_weights(momentum: pd.Series, power: float = 2.0,
                                min_momentum: float = 0.0) -> pd.Series:
    """
    Calculate allocation weights based on momentum scores

    Args:
        momentum: Series with momentum returns (%)
        power: Momentum coefficient (1, 2, or 3)
        min_momentum: Minimum momentum to include (default 0 = no filter)

    Returns:
        Series with allocation weights (%)
    """
    # Filter by minimum momentum
    filtered_mom = momentum[momentum >= min_momentum].copy()

    if filtered_mom.empty:
        # No assets pass filter, equal weight
        return pd.Series(100.0 / len(momentum), index=momentum.index)

    # Apply power to momentum scores
    # Use (1 + return/100)^power to handle both positive and negative returns
    scores = ((1 + filtered_mom / 100) ** power)

    # Normalize to 100%
    weights = (scores / scores.sum()) * 100

    # Fill non-included assets with 0
    result = pd.Series(0.0, index=momentum.index)
    result.update(weights)

    return result


def get_absolute_momentum_signals(prices: pd.DataFrame, threshold_months: int = 6) -> pd.DataFrame:
    """
    Get absolute momentum signals (Buy/Hold/Sell)

    Args:
        prices: DataFrame with price data
        threshold_months: Momentum period for threshold (default 6)

    Returns:
        DataFrame with signals and returns
    """
    momentum = calculate_momentum(prices, threshold_months)

    signals = pd.DataFrame({
        'Return (%)': momentum,
        'Signal': ['🟢 Buy' if r > 0 else '🔴 Stop' for r in momentum]
    })

    return signals


def get_momentum_allocation_table(tickers: dict = None, momentum_months: list = None,
                                 power: float = 2.0, use_dual: bool = False) -> pd.DataFrame:
    """
    Get complete momentum allocation analysis

    Args:
        tickers: dict of {name: ticker}. If None, use defaults
        momentum_months: list of momentum periods. If None, use [3, 6, 12]
        power: Momentum coefficient
        use_dual: Use dual momentum (3+12 average)

    Returns:
        DataFrame with momentum scores and allocation weights
    """
    if tickers is None:
        tickers = DEFAULT_INDICES

    if momentum_months is None:
        momentum_months = [3, 6, 12]

    # Fetch price data
    prices = fetch_index_data(tickers, period='2y')

    if prices.empty:
        return pd.DataFrame()

    # Calculate momentum for different periods
    result = pd.DataFrame(index=prices.columns)

    for months in momentum_months:
        mom = calculate_momentum(prices, months)
        result[f'{months}M Return (%)'] = mom.round(2)

    # Calculate recommended allocation
    if use_dual:
        primary_momentum = calculate_dual_momentum(prices, 3, 12)
        result['Dual Momentum (%)'] = primary_momentum.round(2)
    else:
        # Use 6-month as default
        primary_momentum = calculate_momentum(prices, 6)

    weights = calculate_allocation_weights(primary_momentum, power=power)
    result['Recommended Weight (%)'] = weights.round(2)

    return result


def compare_with_current_holdings(recommended_weights: pd.Series,
                                  current_holdings: pd.DataFrame) -> pd.DataFrame:
    """
    Compare recommended weights with current holdings

    Args:
        recommended_weights: Series with recommended weights (%)
        current_holdings: DataFrame with current holdings

    Returns:
        DataFrame comparing current vs recommended
    """
    # Calculate current weights
    total_value = current_holdings['평가금액'].sum()

    current_weights = {}
    for idx, row in current_holdings.iterrows():
        name = row['종목명']
        weight = (row['평가금액'] / total_value) * 100
        current_weights[name] = weight

    # Create comparison table
    comparison = pd.DataFrame({
        'Current Weight (%)': pd.Series(current_weights),
        'Recommended Weight (%)': recommended_weights
    }).fillna(0)

    comparison['Difference (%)'] = comparison['Recommended Weight (%)'] - comparison['Current Weight (%)']

    return comparison.round(2)


def calculate_rebalancing_trades(comparison: pd.DataFrame, total_portfolio_value: float) -> pd.DataFrame:
    """
    Calculate required trades for rebalancing

    Args:
        comparison: DataFrame from compare_with_current_holdings
        total_portfolio_value: Total portfolio value

    Returns:
        DataFrame with required trades
    """
    trades = comparison.copy()

    # Calculate dollar amounts
    trades['Current Value'] = (trades['Current Weight (%)'] / 100) * total_portfolio_value
    trades['Target Value'] = (trades['Recommended Weight (%)'] / 100) * total_portfolio_value
    trades['Trade Amount'] = trades['Target Value'] - trades['Current Value']
    trades['Action'] = trades['Trade Amount'].apply(
        lambda x: f'Buy ₩{abs(x):,.0f}' if x > 0 else f'Sell ₩{abs(x):,.0f}' if x < 0 else 'Hold'
    )

    return trades


class MomentumAllocator:
    """
    Main class for momentum-based allocation
    """

    def __init__(self, tickers: dict = None):
        """
        Initialize allocator

        Args:
            tickers: dict of {name: ticker}
        """
        self.tickers = tickers if tickers else DEFAULT_INDICES
        self.prices = None
        self.last_update = None

    def update_prices(self, period: str = '2y'):
        """Update price data"""
        self.prices = fetch_index_data(self.tickers, period)
        self.last_update = datetime.now()

    def get_momentum_scores(self, months: int = 6) -> pd.Series:
        """Get momentum scores for all assets"""
        if self.prices is None or self.prices.empty:
            self.update_prices()

        return calculate_momentum(self.prices, months)

    def get_recommended_allocation(self, months: int = 6, power: float = 2.0) -> pd.Series:
        """Get recommended allocation weights"""
        momentum = self.get_momentum_scores(months)
        return calculate_allocation_weights(momentum, power)

    def get_absolute_signals(self, threshold_months: int = 6) -> pd.DataFrame:
        """Get absolute momentum signals"""
        if self.prices is None or self.prices.empty:
            self.update_prices()

        return get_absolute_momentum_signals(self.prices, threshold_months)

    def get_full_analysis(self, momentum_months: list = None, power: float = 2.0) -> pd.DataFrame:
        """Get complete analysis table"""
        if self.prices is None or self.prices.empty:
            self.update_prices()

        return get_momentum_allocation_table(self.tickers, momentum_months, power)
