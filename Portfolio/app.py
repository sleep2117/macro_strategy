"""
Portfolio Performance Analysis Dashboard
Streamlit App
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import data_loader
import portfolio_analyzer
import yfinance as yf
from pathlib import Path

def format_won(value) -> str:
    """Format currency as KRW."""
    if value is None or pd.isna(value):
        return "-"
    return f"₩{value:,.0f}"


def format_pct(value, digits: int = 2) -> str:
    """Format percentage with fallback."""
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.{digits}f}%"


def classify_asset_type(asset_text: str) -> str:
    """Roughly bucket holdings into Stock/Bond/Cash/Other using 상품종류 text."""
    text = str(asset_text).lower()
    if any(keyword in text for keyword in ["주식", "stock", "etf", "equity", "지수"]):
        return "Stock"
    if any(keyword in text for keyword in ["채", "bond"]):
        return "Bond"
    if any(keyword in text for keyword in ["rp", "현금", "cash", "머니", "mmf"]):
        return "Cash"
    return "Other"


def get_value_column(df: pd.DataFrame) -> str:
    """Best-effort detection of 평가금액 column name."""
    for candidate in ["평가금액", "평가 금액", "평가가액", "평가금액(원)"]:
        if candidate in df.columns:
            return candidate
    # Fallback: first numeric column
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    return numeric_cols[0] if numeric_cols else df.columns[0]


def get_asset_type_column(df: pd.DataFrame) -> str:
    """Pick the column that represents asset type."""
    for candidate in ['상품유형', '상품종류', '상품구분', '자산군']:
        if candidate in df.columns:
            return candidate
    return None


def map_holdings_to_targets(latest_holdings: pd.DataFrame,
                            targets: list,
                            ticker_map: dict) -> pd.Series:
    """
    Map holdings rows to target buckets using 종목명 and ticker hints.

    Returns:
        Series with index = bucket name and values = summed 평가금액
    """
    if latest_holdings.empty:
        return pd.Series(dtype=float)

    value_col = get_value_column(latest_holdings)
    name_col = '종목명' if '종목명' in latest_holdings.columns else latest_holdings.columns[0]

    def match_bucket(name: str) -> str:
        cleaned = str(name).upper().replace(" ", "")
        for target in targets:
            target_key = str(target).upper().replace(" ", "")
            ticker_value = str(ticker_map.get(target, "")).upper().replace(".", "")
            if target_key and target_key in cleaned:
                return target
            if ticker_value and ticker_value in cleaned:
                return target
        return "Unmapped"

    working = latest_holdings.copy()
    working['allocation_bucket'] = working[name_col].apply(match_bucket)

    grouped = working.groupby('allocation_bucket')[value_col].sum()
    return grouped

# -------------------------
# Core/Satellite Allocation
# -------------------------

# yfinance ticker mapping
CORE_TICKERS = {
    "SPY": "SPY",           # S&P500
    "K51": "069500.KS",     # KOSPI200 ETF
    "416090": "416090.KS",  # ACE 중국과창판STAR
}

SATELLITE_TICKERS = {
    "FRO": "FRO",           # Frontline
    "AR": "AR",             # Antero Resources
    "REMX": "REMX",         # 희토류 ETF
    "HG1": "HG=F",          # 구리 선물
    "URANIUM": "URA",       # 우라늄 ETF
    "GC1": "GC=F",          # 금 선물
}

BOND_TICKERS = {
    "TLT": "TLT",           # 미국채 20년
}

MOMENTUM_LOOKBACKS = (63, 126, 252)  # 약 3/6/12개월 거래일
DEFAULT_POWER = 2.0  # 3개월 수익률 기준 제곱

BASE_DIR = Path(__file__).resolve().parent
GLOBAL_UNIVERSE_DIR = BASE_DIR.parent / "global_universe"
FX_DIR = GLOBAL_UNIVERSE_DIR / "data" / "fx"


@st.cache_data(show_spinner=False)
def fetch_prices(ticker_map: dict, period: str = "400d") -> pd.DataFrame:
    """Download adjusted closes from yfinance."""
    tickers = list(ticker_map.values())
    if not tickers:
        return pd.DataFrame()

    data = yf.download(
        tickers,
        period=period,
        progress=False,
        group_by="ticker",
        auto_adjust=False,
    )

    closes = {}
    for logical, yft in ticker_map.items():
        try:
            if isinstance(data.columns, pd.MultiIndex):
                closes[logical] = data[yft]["Adj Close"].dropna()
            else:
                closes[logical] = data["Adj Close"].dropna()
        except KeyError:
            closes[logical] = pd.Series(dtype=float)

    return pd.DataFrame(closes)


def momentum_metrics(price_series: pd.Series, lookbacks=MOMENTUM_LOOKBACKS) -> dict:
    """Compute 3/6/12M returns."""
    returns = {}
    for days, label in zip(lookbacks, ["ret_3m", "ret_6m", "ret_12m"]):
        if len(price_series) < days + 1:
            returns[label] = float("nan")
            continue
        start_price = price_series.iloc[-days - 1]
        end_price = price_series.iloc[-1]
        returns[label] = (end_price / start_price - 1) * 100
    return returns


def momentum_score(ret_3m: float, power: float = DEFAULT_POWER) -> float:
    """Score based on 3M return."""
    if pd.isna(ret_3m):
        return 0.0
    return max((1 + ret_3m / 100) ** power, 0.0)


def weights_from_scores(scores: dict) -> dict:
    total = sum(scores.values())
    if total <= 0:
        n = len(scores)
        return {k: (100 / n) for k in scores} if n else {}
    return {k: v / total * 100 for k, v in scores.items()}


def compute_core_satellite_allocation(
    stock_weight: float = 70.0,
    bond_weight: float = 30.0,
    core_ratio: float = 0.7,
    satellite_ratio: float = 0.3,
    power: float = DEFAULT_POWER,
) -> pd.DataFrame:
    """
    Calculate allocation across core/satellite/bond buckets using momentum scores.
    """
    price_map = {**CORE_TICKERS, **SATELLITE_TICKERS, **BOND_TICKERS}
    prices = fetch_prices(price_map)

    rows = []

    def process_bucket(name: str, tickers: dict, bucket_weight: float):
        series_scores = {}
        metrics_cache = {}
        for logical in tickers:
            s = prices[logical].dropna() if logical in prices else pd.Series(dtype=float)
            metrics = momentum_metrics(s)
            metrics_cache[logical] = metrics
            series_scores[logical] = momentum_score(metrics["ret_3m"], power)

        weight_pct = weights_from_scores(series_scores)

        for logical in tickers:
            m = metrics_cache.get(logical, {})
            rows.append({
                "group": name,
                "ticker": logical,
                "yfinance": tickers[logical],
                "ret_3m": m.get("ret_3m"),
                "ret_6m": m.get("ret_6m"),
                "ret_12m": m.get("ret_12m"),
                "score": series_scores.get(logical, 0.0),
                "group_weight_pct": bucket_weight,
                "final_weight_pct": bucket_weight * (weight_pct.get(logical, 0.0) / 100),
            })

    core_weight = stock_weight * core_ratio
    satellite_weight = stock_weight * satellite_ratio

    process_bucket("Core", CORE_TICKERS, core_weight)
    process_bucket("Satellite", SATELLITE_TICKERS, satellite_weight)
    process_bucket("Bond", BOND_TICKERS, bond_weight)

    df = pd.DataFrame(rows)
    return df.sort_values(["group", "final_weight_pct"], ascending=[True, False])


@st.cache_data(show_spinner=False)
def load_fx_to_krw(currency_code: str) -> pd.Series:
    """Load FX rates to KRW for a given currency."""
    if not currency_code or currency_code == "KRW":
        return pd.Series(dtype=float)

    fx_path = FX_DIR / f"{currency_code}.csv"
    if not fx_path.exists():
        return pd.Series(dtype=float)

    df = pd.read_csv(fx_path)
    df['Date'] = pd.to_datetime(df['Date'])
    series = df.set_index('Date')['toKRW'].dropna()
    return series


# Page config
st.set_page_config(
    page_title="Portfolio Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
.metric-card {
    background-color: #f0f2f6;
    padding: 20px;
    border-radius: 10px;
    margin: 10px 0;
}
.big-font {
    font-size:20px !important;
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def load_analyzer():
    """Load portfolio analyzer (cached)"""
    return portfolio_analyzer.get_analyzer()


def render_sidebar():
    """Render sidebar"""
    with st.sidebar:
        st.title("📊 Portfolio Dashboard")
        st.markdown("---")

        # Metadata
        metadata = data_loader.load_metadata()
        if metadata:
            st.subheader("Database Info")
            st.caption(f"Last update: {metadata['last_update'][:10]}")
            st.caption(f"Period: {metadata['data_period']['start']} ~ {metadata['data_period']['end']}")
            st.caption(f"Records: {metadata['record_counts']['performance']} days")

        st.markdown("---")

        # Date range selection
        st.subheader("Period Selection")

        # Get available date range
        perf_data = data_loader.load_performance_history()
        if not perf_data.empty:
            min_date = perf_data['일자'].min().date()
            max_date = perf_data['일자'].max().date()

            # Period preset
            period_preset = st.selectbox(
                "Preset",
                ["All Time", "YTD", "Last 1 Year", "Last 6 Months", "Last 3 Months", "Monthly", "Custom"]
            )

            from datetime import datetime, timedelta

            if period_preset == "All Time":
                start_date = min_date
                end_date = max_date
            elif period_preset == "YTD":
                start_date = datetime(max_date.year, 1, 1).date()
                end_date = max_date
            elif period_preset == "Last 1 Year":
                start_date = max_date - timedelta(days=365)
                end_date = max_date
            elif period_preset == "Last 6 Months":
                start_date = max_date - timedelta(days=180)
                end_date = max_date
            elif period_preset == "Last 3 Months":
                start_date = max_date - timedelta(days=90)
                end_date = max_date
            elif period_preset == "Monthly":
                # Month selector
                available_months = perf_data['일자'].dt.to_period('M').unique()
                month_options = sorted([str(m) for m in available_months])

                selected_month = st.selectbox("Select Month", month_options, index=len(month_options)-1)

                # Convert to start and end dates
                import pandas as pd
                period = pd.Period(selected_month)
                start_date = period.start_time.date()
                end_date = period.end_time.date()
            else:  # Custom
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input("Start", min_date, min_value=min_date, max_value=max_date)
                with col2:
                    end_date = st.date_input("End", max_date, min_value=min_date, max_value=max_date)

            st.caption(f"Selected: {start_date} ~ {end_date}")
        else:
            start_date = None
            end_date = None

        st.markdown("---")

        # Navigation
        page = st.radio(
            "Navigation",
            ["Overview", "Performance Analysis", "Holdings", "Transactions", "Dividends", "Cash Flow", "Risk", "Core/Satellite Allocation"],
            label_visibility="collapsed"
        )

    return page, start_date, end_date


def render_overview(start_date=None, end_date=None):
    """Render overview page"""
    st.header("Portfolio Overview")

    analyzer = load_analyzer()

    # Get summary with date filter
    summary = data_loader.get_performance_summary(start_date, end_date)
    metrics = analyzer.calculate_performance_metrics(start_date, end_date)

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Value",
            f"₩{summary['amount']['end']:,.0f}",
            delta=f"₩{summary['amount']['change']:,.0f}"
        )

    with col2:
        st.metric(
            "Total Return",
            f"{summary['return']['percentage']:.2f}%",
            delta=None
        )

    with col3:
        st.metric(
            "Investment Profit",
            f"₩{summary['profit']['investment']:,.0f}",
            delta=None
        )

    with col4:
        st.metric(
            "Days",
            summary['days'],
            delta=None
        )

    # Performance metrics
    st.subheader("Performance Metrics")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.info(f"**Sharpe Ratio**: {metrics['sharpe_ratio']:.2f}")
        st.info(f"**Volatility**: {metrics['volatility']:.2f}%")

    with col2:
        st.info(f"**Max Drawdown**: {metrics['max_drawdown']:.2f}%")
        st.info(f"**Win Rate**: {metrics['win_rate']:.2f}%")

    with col3:
        st.info(f"**Win/Loss Ratio**: {metrics['win_loss_ratio']:.2f}")
        st.info(f"**Best Day**: {metrics['best_day']:.2f}%")

    # Portfolio value chart
    st.subheader("Portfolio Value Over Time")

    chart_data = analyzer.get_daily_performance_chart_data()

    # Filter chart data by date range
    if start_date:
        chart_data = chart_data[chart_data['일자'] >= pd.to_datetime(start_date)]
    if end_date:
        chart_data = chart_data[chart_data['일자'] <= pd.to_datetime(end_date)]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=chart_data['일자'],
        y=chart_data['기말평가금액'],
        mode='lines',
        name='Portfolio Value',
        line=dict(color='#1f77b4', width=2)
    ))

    fig.update_layout(
        title="Portfolio Value",
        xaxis_title="Date",
        yaxis_title="Value (₩)",
        hovermode='x unified',
        height=400
    )

    st.plotly_chart(fig, use_container_width=True)

    # Cumulative return chart
    st.subheader("Cumulative Returns")

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=chart_data['일자'],
        y=chart_data['cumulative_return'],
        mode='lines',
        name='Cumulative Return',
        line=dict(color='#2ca02c', width=2),
        fill='tozeroy'
    ))

    fig2.update_layout(
        title="Cumulative Return (%)",
        xaxis_title="Date",
        yaxis_title="Return (%)",
        hovermode='x unified',
        height=400
    )

    st.plotly_chart(fig2, use_container_width=True)


def render_performance_analysis(start_date=None, end_date=None):
    """Render performance analysis page"""
    st.header("Performance Analysis")

    analyzer = load_analyzer()

    # Tabs
    tab1, tab2, tab3 = st.tabs(["Daily Performance", "Monthly Performance", "Risk Metrics"])

    with tab1:
        st.subheader("Daily Performance")

        chart_data = analyzer.get_daily_performance_chart_data()

        # Filter by date range
        if start_date:
            chart_data = chart_data[chart_data['일자'] >= pd.to_datetime(start_date)]
        if end_date:
            chart_data = chart_data[chart_data['일자'] <= pd.to_datetime(end_date)]

        # Daily return chart
        fig = go.Figure()
        colors = ['green' if x > 0 else 'red' for x in chart_data['daily_return']]

        fig.add_trace(go.Bar(
            x=chart_data['일자'],
            y=chart_data['daily_return'],
            marker_color=colors,
            name='Daily Return'
        ))

        fig.update_layout(
            title="Daily Returns (%)",
            xaxis_title="Date",
            yaxis_title="Return (%)",
            hovermode='x unified',
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

        # Daily performance table
        st.dataframe(
            chart_data.sort_values('일자', ascending=False),
            use_container_width=True,
            hide_index=True
        )

    with tab2:
        st.subheader("Monthly Performance")

        monthly = analyzer.get_monthly_performance()

        if not monthly.empty:
            # Convert period to string for display
            monthly_display = monthly.copy()
            monthly_display.index = monthly_display.index.astype(str)

            # Monthly return chart
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=monthly_display.index,
                y=monthly_display['월간수익률'],
                name='Monthly Return',
                marker_color='#1f77b4'
            ))

            fig.update_layout(
                title="Monthly Returns (%)",
                xaxis_title="Month",
                yaxis_title="Return (%)",
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # Monthly table
            st.dataframe(monthly_display, use_container_width=True)
        else:
            st.info("Not enough data for monthly analysis")

    with tab3:
        st.subheader("Risk Metrics")

        # Drawdown chart
        drawdown_series = analyzer.get_drawdown_series()

        # Filter by date range
        if start_date:
            drawdown_series = drawdown_series[drawdown_series.index >= pd.to_datetime(start_date)]
        if end_date:
            drawdown_series = drawdown_series[drawdown_series.index <= pd.to_datetime(end_date)]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=drawdown_series.index,
            y=drawdown_series.values,
            mode='lines',
            name='Drawdown',
            fill='tozeroy',
            line=dict(color='red', width=2)
        ))

        fig.update_layout(
            title="Drawdown Over Time",
            xaxis_title="Date",
            yaxis_title="Drawdown (%)",
            hovermode='x unified',
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

        # Risk metrics summary
        metrics = analyzer.calculate_performance_metrics(start_date, end_date)

        col1, col2 = st.columns(2)

        with col1:
            st.metric("Maximum Drawdown", f"{metrics['max_drawdown']:.2f}%")
            st.metric("Volatility (Annualized)", f"{metrics['volatility']:.2f}%")
            st.metric("Sharpe Ratio", f"{metrics['sharpe_ratio']:.2f}")

        with col2:
            st.metric("Best Day", f"{metrics['best_day']:.2f}%")
            st.metric("Worst Day", f"{metrics['worst_day']:.2f}%")
            st.metric("Win Rate", f"{metrics['win_rate']:.2f}%")


def render_holdings():
    """Render holdings page"""
    st.header("Portfolio Holdings")

    analyzer = load_analyzer()
    latest_holdings = data_loader.get_latest_holdings()

    if latest_holdings.empty:
        st.warning("No holdings data available")
        return

    # Summary metrics
    value_col = get_value_column(latest_holdings)
    total_value = latest_holdings[value_col].sum()
    total_profit = latest_holdings['평가손익'].sum()
    overall_return = (total_profit / (total_value - total_profit)) * 100 if (total_value - total_profit) != 0 else 0

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Value", f"₩{total_value:,.0f}")

    with col2:
        st.metric("Total Profit/Loss", f"₩{total_profit:,.0f}")

    with col3:
        st.metric("Overall Return", f"{overall_return:.2f}%")

    # Holdings by asset type
    st.subheader("Holdings by Asset Type")

    by_asset = data_loader.get_holdings_by_asset_type()
    type_col = get_asset_type_column(latest_holdings)

    col1, col2 = st.columns(2)

    with col1:
        if not by_asset.empty:
            fig = px.pie(
                values=by_asset[value_col] if value_col in by_asset.columns else by_asset.iloc[:, 0],
                names=by_asset.index,
                title="Asset Allocation (All)"
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # 해외주식 종목별 비중
        stock_holdings = latest_holdings[latest_holdings[type_col] == '해외주식'].copy() if type_col else pd.DataFrame()
        if not stock_holdings.empty:
            fig2 = px.pie(
                stock_holdings,
                values=value_col,
                names='종목명',
                title="Stock Holdings (Individual)"
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No stock holdings")

    # Holdings table
    st.subheader("Current Holdings")

    st.dataframe(
        latest_holdings[[
            '계좌번호', '상품유형', '종목명', '잔고수량',
            '현재가', '매수금액', value_col, '평가손익', '수익률(%)'
        ]],
        use_container_width=True,
        hide_index=True
    )

    # Stock holdings detail
    st.subheader("Stock Holdings Detail")

    stock_holdings = latest_holdings[latest_holdings[type_col] == '해외주식'].copy() if type_col else pd.DataFrame()
    if not stock_holdings.empty:
        # Add percentage column
        total_stock_value = stock_holdings[value_col].sum()
        stock_holdings['비중(%)'] = (stock_holdings[value_col] / total_stock_value * 100).round(2)

        st.dataframe(
            stock_holdings[[
                '종목명', '잔고수량', '현재가', '매수금액',
                value_col, '비중(%)', '평가손익', '수익률(%)'
            ]].sort_values(value_col, ascending=False),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No stock holdings")

    # Concentration and movers
    st.subheader("Concentration & Top Movers")
    top_weights = latest_holdings.sort_values(value_col, ascending=False).head(5)[['종목명', value_col, '수익률(%)']]
    top_gainers = latest_holdings.sort_values('수익률(%)', ascending=False).head(5)[['종목명', value_col, '수익률(%)']]
    top_losers = latest_holdings.sort_values('수익률(%)').head(5)[['종목명', value_col, '수익률(%)']]

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**Top 5 by Weight**")
        st.dataframe(top_weights, hide_index=True, use_container_width=True)
    with col2:
        st.markdown("**Top 5 Gainers**")
        st.dataframe(top_gainers, hide_index=True, use_container_width=True)
    with col3:
        st.markdown("**Top 5 Losers**")
        st.dataframe(top_losers, hide_index=True, use_container_width=True)

    # Account comparison
    st.subheader("Performance by Account")

    account_comp = analyzer.get_account_comparison()

    if not account_comp.empty:
        st.dataframe(account_comp, use_container_width=True)


def render_transactions(start_date=None, end_date=None):
    """Render transactions page"""
    st.header("Transaction History")

    transactions = data_loader.load_transaction_history()

    # Filter by date range
    if start_date:
        transactions = transactions[transactions['거래일자'] >= pd.to_datetime(start_date)]
    if end_date:
        transactions = transactions[transactions['거래일자'] <= pd.to_datetime(end_date)]

    if transactions.empty:
        st.warning("No transaction data available for selected period")
        return

    # Transaction summary
    trans_summary = data_loader.get_transaction_summary(start_date, end_date)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Transactions", trans_summary['total_transactions'])

    with col2:
        st.metric("Total Fees", f"₩{trans_summary['total_fees']:,.0f}")

    with col3:
        st.metric("Daily Average", f"{trans_summary['daily_average']:.1f}")

    # Transactions by type
    st.subheader("Transactions by Type")

    by_type = transactions.groupby('거래유형').size()

    fig = px.bar(
        x=by_type.index,
        y=by_type.values,
        labels={'x': 'Transaction Type', 'y': 'Count'},
        title="Transaction Counts by Type"
    )

    st.plotly_chart(fig, use_container_width=True)

    # Recent transactions
    st.subheader("Recent Transactions")

    recent = transactions.sort_values('거래일자', ascending=False).head(20)

    st.dataframe(
        recent[[
            '거래일자', '계좌번호', '거래유형', '종목명',
            '거래수량', '거래단가', '거래금액', '수수료/세금'
        ]],
        use_container_width=True,
        hide_index=True
    )


def render_dividends(start_date=None, end_date=None):
    """Render dividend cash flow and yield page."""
    st.header("Dividend Overview")

    transactions = data_loader.load_transaction_history()
    if transactions.empty:
        st.warning("No transaction data available")
        return

    if start_date:
        transactions = transactions[transactions['거래일자'] >= pd.to_datetime(start_date)]
    if end_date:
        transactions = transactions[transactions['거래일자'] <= pd.to_datetime(end_date)]

    div_tx = transactions[transactions['거래유형'].astype(str).str.contains('배당')].copy()
    if div_tx.empty:
        st.info("배당 거래가 없습니다.")
        return

    # Amount in native currency
    def native_amount(row):
        ccy = row.get('통화코드', None)
        if ccy == "KRW" or pd.isna(ccy):
            amt = row.get('정산금액', 0)
            if amt == 0:
                amt = row.get('거래금액', 0)
        else:
            amt = row.get('외화정산금액', 0)
            if amt == 0:
                amt = row.get('외화거래금액', 0)
        return abs(amt)

    div_tx['amount_native'] = div_tx.apply(native_amount, axis=1)

    # Convert to KRW
    div_tx['amount_krw'] = pd.NA
    for ccy, idx in div_tx.groupby('통화코드').groups.items():
        if ccy == "KRW" or pd.isna(ccy):
            div_tx.loc[idx, 'amount_krw'] = div_tx.loc[idx, 'amount_native']
            continue
        fx_series = load_fx_to_krw(str(ccy))
        if fx_series.empty:
            continue
        dates = div_tx.loc[idx, '거래일자']
        fx_rates = fx_series.reindex(dates, method='ffill').values
        div_tx.loc[idx, 'amount_krw'] = div_tx.loc[idx, 'amount_native'].values * fx_rates

    div_tx['amount_krw'] = pd.to_numeric(div_tx['amount_krw'], errors='coerce')

    # Monthly totals
    div_tx['월'] = div_tx['거래일자'].dt.to_period('M').astype(str)
    monthly = div_tx.groupby('월')['amount_krw'].sum().reset_index()

    st.subheader("Monthly Dividend (KRW)")
    fig = px.bar(
        monthly,
        x='월',
        y='amount_krw',
        title="월별 배당금 (KRW)",
        text='amount_krw',
        color_discrete_sequence=['#2ca02c']
    )
    fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig.update_layout(yaxis_title="KRW", height=360)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Monthly Dividend Table")
    st.dataframe(
        monthly.rename(columns={'amount_krw': '배당금(KRW)'}),
        use_container_width=True,
        hide_index=True
    )

    # Dividend yield by asset
    latest_holdings = data_loader.get_latest_holdings()
    if latest_holdings.empty:
        st.info("보유 자산 정보가 없어 배당 수익률을 계산할 수 없습니다.")
        return

    value_col = get_value_column(latest_holdings)
    holdings_value = latest_holdings.groupby('종목명')[value_col].sum()
    div_by_asset = div_tx.groupby('종목명')['amount_krw'].sum()

    yield_table = pd.DataFrame({
        '배당금(KRW)': div_by_asset,
        '보유금액(KRW)': holdings_value
    }).fillna(0)
    yield_table['배당수익률(%)'] = (
        yield_table['배당금(KRW)'] / yield_table['보유금액(KRW)'] * 100
    ).replace([pd.NA, pd.NaT], 0).fillna(0)

    st.subheader("Dividend Yield by Asset")
    yield_table = yield_table.sort_values('배당수익률(%)', ascending=False)
    st.dataframe(yield_table.reset_index(), use_container_width=True, hide_index=True)

    st.caption("배당금은 거래 내역 기준이며, 통화가 KRW가 아닌 경우 FX(toKRW)로 환산합니다.")


def render_cash_flow(start_date=None, end_date=None):
    """Render cash flow and contribution insights"""
    st.header("Cash Flow & Contributions")

    perf = data_loader.load_performance_history()
    if perf.empty:
        st.warning("No performance data available")
        return

    perf = data_loader.filter_by_date_range(perf, '일자', start_date, end_date)
    if perf.empty:
        st.warning("No data for selected period")
        return

    total_in = perf['입금액'].sum()
    total_out = perf['출금액'].sum()
    net_cf = total_in - total_out
    invest_pnl = perf['투자손익'].sum()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Deposits", format_won(total_in))
    with col2:
        st.metric("Total Withdrawals", format_won(total_out))
    with col3:
        st.metric("Net Contributions", format_won(net_cf))
    with col4:
        st.metric("Investment P&L", format_won(invest_pnl))

    # Monthly cash flow
    monthly = perf.copy()
    monthly['월'] = monthly['일자'].dt.to_period('M')
    monthly_cf = monthly.groupby('월').agg({
        '입금액': 'sum',
        '출금액': 'sum',
        '투자손익': 'sum'
    }).reset_index()
    monthly_cf['월'] = monthly_cf['월'].astype(str)
    monthly_cf['순입금'] = monthly_cf['입금액'] - monthly_cf['출금액']

    st.subheader("Monthly Cash Flow")
    fig_cf = go.Figure()
    fig_cf.add_bar(name="Deposits", x=monthly_cf['월'], y=monthly_cf['입금액'], marker_color='#2ca02c')
    fig_cf.add_bar(name="Withdrawals", x=monthly_cf['월'], y=-monthly_cf['출금액'], marker_color='#d62728')
    fig_cf.add_scatter(name="Net Contributions", x=monthly_cf['월'], y=monthly_cf['순입금'],
                       mode='lines+markers', line=dict(color='#1f77b4', width=2))
    fig_cf.update_layout(barmode='relative', xaxis_title="Month", yaxis_title="Amount (₩)",
                         height=360, hovermode='x unified')
    st.plotly_chart(fig_cf, use_container_width=True)

    st.subheader("Cumulative Contributions vs Portfolio Value")
    perf_sorted = perf.sort_values('일자').copy()
    perf_sorted['누적순입금'] = (perf_sorted['입금액'] - perf_sorted['출금액']).cumsum()
    fig_cum = go.Figure()
    fig_cum.add_trace(go.Scatter(
        x=perf_sorted['일자'], y=perf_sorted['누적순입금'],
        mode='lines', name='Cumulative Net Contributions', line=dict(color='#1f77b4', width=2)
    ))
    fig_cum.add_trace(go.Scatter(
        x=perf_sorted['일자'], y=perf_sorted['기말평가금액'],
        mode='lines', name='Portfolio Value', line=dict(color='#ff7f0e', width=2)
    ))
    fig_cum.update_layout(
        xaxis_title="Date", yaxis_title="Amount (₩)", hovermode='x unified', height=380
    )
    st.plotly_chart(fig_cum, use_container_width=True)

    st.subheader("Monthly Details")
    st.dataframe(
        monthly_cf[['월', '입금액', '출금액', '순입금', '투자손익']].sort_values('월'),
        use_container_width=True, hide_index=True
    )


def render_core_satellite_allocation():
    """Render momentum-based core/satellite/bond allocation."""
    st.header("Core / Satellite / Bond Allocation (Momentum)")

    st.caption("3/6/12개월 수익률 계산, 3개월 수익률 기반 계수 2 제곱으로 스코어 → 비중")

    col1, col2, col3 = st.columns(3)
    with col1:
        stock_weight = st.slider("주식 비중 (%)", min_value=50, max_value=90, value=70, step=5)
    with col2:
        bond_weight = 100 - stock_weight
        st.metric("채권 비중 (%)", f"{bond_weight}%")
    with col3:
        power = st.slider("모멘텀 계수 (3M)", min_value=1.0, max_value=3.0, value=DEFAULT_POWER, step=0.5)

    col1, col2 = st.columns(2)
    with col1:
        core_ratio = st.slider("코어 비중 (주식 내)", min_value=0.5, max_value=0.9, value=0.7, step=0.05)
    with col2:
        satellite_ratio = 1 - core_ratio
        st.metric("새틀라이트 비중 (주식 내)", f"{satellite_ratio*100:.0f}%")

    if st.button("Calculate Allocation", type="primary"):
        with st.spinner("시장 데이터 수집 중..."):
            try:
                df = compute_core_satellite_allocation(
                    stock_weight=stock_weight,
                    bond_weight=bond_weight,
                    core_ratio=core_ratio,
                    satellite_ratio=satellite_ratio,
                    power=power,
                )
            except Exception as e:
                st.error(f"계산 오류: {e}")
                return

        if df.empty:
            st.warning("데이터를 불러오지 못했습니다. 티커를 확인하세요.")
            return

        # Summary
        st.subheader("추천 비중")
        fig = px.bar(
            df,
            x="ticker",
            y="final_weight_pct",
            color="group",
            title="최종 비중 (%)",
            text="final_weight_pct",
            category_orders={"group": ["Core", "Satellite", "Bond"]}
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(yaxis_title="Weight (%)", height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("세부 지표")
        st.dataframe(
            df.rename(columns={
                "ret_3m": "3M 수익률 (%)",
                "ret_6m": "6M 수익률 (%)",
                "ret_12m": "12M 수익률 (%)",
                "score": "모멘텀 스코어",
                "group_weight_pct": "버킷 비중 (%)",
                "final_weight_pct": "최종 비중 (%)",
            }),
            use_container_width=True,
            hide_index=True
        )

        st.caption("스코어 = (1 + 3M 수익률) ** 계수; 각 버킷 내 스코어 비율로 배분.")


def render_risk_insights(start_date=None, end_date=None):
    """Render risk and return analytics"""
    st.header("Risk & Return Profile")

    analyzer = load_analyzer()
    perf = analyzer.performance.copy()

    if perf.empty:
        st.warning("No performance data available")
        return

    # Filter
    if start_date:
        perf = perf[perf['일자'] >= pd.to_datetime(start_date)]
    if end_date:
        perf = perf[perf['일자'] <= pd.to_datetime(end_date)]

    if perf.empty:
        st.warning("No data for selected period")
        return

    perf = perf.sort_values('일자')
    perf['daily_return'] = (perf['투자손익'] / perf['기초평가금액']) * 100

    metrics = analyzer.calculate_performance_metrics(start_date, end_date)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Volatility (Ann.)", format_pct(metrics.get('volatility', 0)))
        st.metric("Max Drawdown", format_pct(metrics.get('max_drawdown', 0)))
    with col2:
        st.metric("Sharpe Ratio", f"{metrics.get('sharpe_ratio', 0):.2f}")
        st.metric("Win Rate", format_pct(metrics.get('win_rate', 0)))
    with col3:
        st.metric("Best Day", format_pct(metrics.get('best_day', 0)))
        st.metric("Worst Day", format_pct(metrics.get('worst_day', 0)))

    # Distribution
    st.subheader("Distribution of Daily Returns")
    fig_hist = px.histogram(perf, x='daily_return', nbins=30, color_discrete_sequence=['#1f77b4'])
    fig_hist.update_layout(xaxis_title="Daily Return (%)", yaxis_title="Frequency", height=360)
    st.plotly_chart(fig_hist, use_container_width=True)

    # Rolling volatility
    st.subheader("30-Day Rolling Volatility (Ann.)")
    perf['rolling_vol'] = perf['daily_return'].rolling(30).std() * (252 ** 0.5)
    fig_vol = go.Figure()
    fig_vol.add_trace(go.Scatter(
        x=perf['일자'], y=perf['rolling_vol'],
        mode='lines', name='Rolling Volatility', line=dict(color='#9467bd', width=2)
    ))
    fig_vol.update_layout(xaxis_title="Date", yaxis_title="Volatility (%)", height=360, hovermode='x unified')
    st.plotly_chart(fig_vol, use_container_width=True)

    # Drawdown
    st.subheader("Drawdown Curve")
    drawdown = analyzer.get_drawdown_series()
    if start_date:
        drawdown = drawdown[drawdown.index >= pd.to_datetime(start_date)]
    if end_date:
        drawdown = drawdown[drawdown.index <= pd.to_datetime(end_date)]

    fig_dd = go.Figure()
    fig_dd.add_trace(go.Scatter(
        x=drawdown.index, y=drawdown.values,
        mode='lines', name='Drawdown', line=dict(color='#d62728', width=2), fill='tozeroy'
    ))
    fig_dd.update_layout(xaxis_title="Date", yaxis_title="Drawdown (%)", height=360, hovermode='x unified')
    st.plotly_chart(fig_dd, use_container_width=True)

    # Best / worst days
    st.subheader("Best & Worst Days")
    top = perf.nlargest(5, 'daily_return')[['일자', 'daily_return']]
    worst = perf.nsmallest(5, 'daily_return')[['일자', 'daily_return']]
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Top 5 Days**")
        st.dataframe(top.rename(columns={'daily_return': 'Return (%)'}), use_container_width=True, hide_index=True)
    with col2:
        st.markdown("**Bottom 5 Days**")
        st.dataframe(worst.rename(columns={'daily_return': 'Return (%)'}), use_container_width=True, hide_index=True)



def main():
    """Main app"""
    # Render sidebar and get current page and date range
    page, start_date, end_date = render_sidebar()

    # Render selected page with date filter
    if page == "Overview":
        render_overview(start_date, end_date)
    elif page == "Performance Analysis":
        render_performance_analysis(start_date, end_date)
    elif page == "Holdings":
        render_holdings()
    elif page == "Transactions":
        render_transactions(start_date, end_date)
    elif page == "Dividends":
        render_dividends(start_date, end_date)
    elif page == "Cash Flow":
        render_cash_flow(start_date, end_date)
    elif page == "Risk":
        render_risk_insights(start_date, end_date)
    elif page == "Core/Satellite Allocation":
        render_core_satellite_allocation()


if __name__ == "__main__":
    main()
