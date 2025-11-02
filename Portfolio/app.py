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
import momentum_calculator

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
            ["Overview", "Performance Analysis", "Holdings", "Transactions",
             "Momentum Allocation", "Absolute Momentum", "Asset Allocation"],
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
    total_value = latest_holdings['평가금액'].sum()
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

    col1, col2 = st.columns(2)

    with col1:
        if not by_asset.empty:
            fig = px.pie(
                values=by_asset['평가금액'],
                names=by_asset.index,
                title="Asset Allocation (All)"
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # 해외주식 종목별 비중
        stock_holdings = latest_holdings[latest_holdings['상품유형'] == '해외주식'].copy()
        if not stock_holdings.empty:
            fig2 = px.pie(
                stock_holdings,
                values='평가금액',
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
            '현재가', '매수금액', '평가금액', '평가손익', '수익률(%)'
        ]],
        use_container_width=True,
        hide_index=True
    )

    # Stock holdings detail
    st.subheader("Stock Holdings Detail")

    stock_holdings = latest_holdings[latest_holdings['상품유형'] == '해외주식'].copy()
    if not stock_holdings.empty:
        # Add percentage column
        total_stock_value = stock_holdings['평가금액'].sum()
        stock_holdings['비중(%)'] = (stock_holdings['평가금액'] / total_stock_value * 100).round(2)

        st.dataframe(
            stock_holdings[[
                '종목명', '잔고수량', '현재가', '매수금액',
                '평가금액', '비중(%)', '평가손익', '수익률(%)'
            ]].sort_values('평가금액', ascending=False),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No stock holdings")

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


def render_momentum_allocation():
    """Render momentum allocation calculator page"""
    st.header("Momentum-Based Country Allocation")

    st.markdown("""
    Calculate recommended allocation weights based on momentum strategy.
    Higher momentum assets receive higher weights.
    """)

    # Settings
    col1, col2, col3 = st.columns(3)

    with col1:
        power = st.selectbox("Momentum Coefficient", [1.0, 2.0, 3.0], index=1)

    with col2:
        primary_period = st.selectbox("Primary Period", [3, 6, 12], index=1)

    with col3:
        use_dual = st.checkbox("Use Dual Momentum (3M+12M)", value=False)

    # Custom tickers
    with st.expander("Customize Tickers (Optional)"):
        st.caption("Leave empty to use defaults")

        col1, col2 = st.columns(2)

        with col1:
            sp500_ticker = st.text_input("S&P500", value="SPY")
            kospi_ticker = st.text_input("KOSPI", value="069500.KS")
            csi300_ticker = st.text_input("CSI300", value="ASHR")

        with col2:
            europe_ticker = st.text_input("Europe", value="VGK")
            us_bond_ticker = st.text_input("US Treasury", value="TLT")
            kr_bond_ticker = st.text_input("KR Treasury", value="148070.KS")

        custom_tickers = {
            'S&P500': sp500_ticker,
            'KOSPI': kospi_ticker,
            'CSI300': csi300_ticker,
            'Europe': europe_ticker,
            'US Treasury': us_bond_ticker,
            'KR Treasury': kr_bond_ticker
        }

    # Calculate button
    if st.button("Calculate Allocation", type="primary"):
        with st.spinner("Fetching market data..."):
            try:
                # Get momentum allocation table
                allocation_table = momentum_calculator.get_momentum_allocation_table(
                    tickers=custom_tickers,
                    momentum_months=[3, 6, 12],
                    power=power,
                    use_dual=use_dual
                )

                if allocation_table.empty:
                    st.error("Failed to fetch data. Please check ticker symbols and try again.")
                    return

                # Display results
                st.success("Calculation complete!")

                # Allocation weights chart
                st.subheader("Recommended Allocation")

                col1, col2 = st.columns([1, 1])

                with col1:
                    # Pie chart
                    weights = allocation_table['Recommended Weight (%)']
                    weights = weights[weights > 0]  # Only show non-zero weights

                    if not weights.empty:
                        fig = px.pie(
                            values=weights.values,
                            names=weights.index,
                            title="Recommended Portfolio Allocation"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No assets with positive momentum")

                with col2:
                    # Bar chart of momentum scores
                    momentum_col = 'Dual Momentum (%)' if use_dual else f'{primary_period}M Return (%)'

                    fig2 = go.Figure()
                    fig2.add_trace(go.Bar(
                        x=allocation_table.index,
                        y=allocation_table[momentum_col],
                        marker_color=['green' if x > 0 else 'red' for x in allocation_table[momentum_col]],
                        name='Momentum Return'
                    ))

                    fig2.update_layout(
                        title=f"{momentum_col}",
                        xaxis_title="Asset",
                        yaxis_title="Return (%)",
                        height=400
                    )

                    st.plotly_chart(fig2, use_container_width=True)

                # Full table
                st.subheader("Detailed Analysis")
                st.dataframe(allocation_table, use_container_width=True)

                # Compare with current holdings
                st.subheader("Compare with Current Holdings")

                latest_holdings = data_loader.get_latest_holdings()

                if not latest_holdings.empty:
                    # Calculate current allocation
                    total_value = latest_holdings['평가금액'].sum()

                    current_allocation = pd.DataFrame({
                        'Current Weight (%)': [(latest_holdings['평가금액'].sum() / total_value * 100)]
                    }, index=['Total Portfolio'])

                    # Show comparison
                    st.info(f"**Current Total Portfolio Value**: ₩{total_value:,.0f}")

                    # Map holdings to index names (simplified)
                    st.caption("For detailed rebalancing, use the 'Asset Allocation' page")

                else:
                    st.info("No current holdings data available for comparison")

            except Exception as e:
                st.error(f"Error calculating allocation: {str(e)}")
                st.exception(e)


def render_absolute_momentum():
    """Render absolute momentum signals page"""
    st.header("Absolute Momentum Signals")

    st.markdown("""
    Absolute momentum strategy: Buy when momentum > 0%, Stop when momentum < 0%.
    This helps avoid holding assets during downtrends.
    """)

    # Settings
    col1, col2 = st.columns(2)

    with col1:
        threshold_months = st.selectbox("Momentum Period", [3, 6, 12], index=1)

    with col2:
        st.caption(f"Using {threshold_months}-month return as signal")

    # Custom tickers
    with st.expander("Customize Tickers (Optional)"):
        col1, col2 = st.columns(2)

        with col1:
            sp500_ticker = st.text_input("S&P500", value="SPY")
            kospi_ticker = st.text_input("KOSPI", value="069500.KS")
            csi300_ticker = st.text_input("CSI300", value="ASHR")

        with col2:
            europe_ticker = st.text_input("Europe", value="VGK")
            us_bond_ticker = st.text_input("US Treasury", value="TLT")
            kr_bond_ticker = st.text_input("KR Treasury", value="148070.KS")

        custom_tickers = {
            'S&P500': sp500_ticker,
            'KOSPI': kospi_ticker,
            'CSI300': csi300_ticker,
            'Europe': europe_ticker,
            'US Treasury': us_bond_ticker,
            'KR Treasury': kr_bond_ticker
        }

    # Calculate button
    if st.button("Get Signals", type="primary"):
        with st.spinner("Fetching market data..."):
            try:
                # Initialize allocator
                allocator = momentum_calculator.MomentumAllocator(tickers=custom_tickers)
                allocator.update_prices()

                # Get signals
                signals = allocator.get_absolute_signals(threshold_months=threshold_months)

                if signals.empty:
                    st.error("Failed to fetch data. Please check ticker symbols.")
                    return

                st.success("Signals updated!")

                # Display signals
                st.subheader("Current Signals")

                # Add colored background
                def highlight_signal(row):
                    if '🟢' in row['Signal']:
                        return ['background-color: #d4edda'] * len(row)
                    else:
                        return ['background-color: #f8d7da'] * len(row)

                styled_signals = signals.style.apply(highlight_signal, axis=1)
                st.dataframe(styled_signals, use_container_width=True)

                # Summary
                col1, col2, col3 = st.columns(3)

                buy_count = signals['Signal'].str.contains('🟢').sum()
                stop_count = signals['Signal'].str.contains('🔴').sum()

                with col1:
                    st.metric("Buy Signals", buy_count)

                with col2:
                    st.metric("Stop Signals", stop_count)

                with col3:
                    st.metric("Total Assets", len(signals))

                # Signal chart
                st.subheader("Momentum Returns")

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=signals.index,
                    y=signals['Return (%)'],
                    marker_color=['green' if '🟢' in s else 'red' for s in signals['Signal']],
                    text=signals['Signal'],
                    textposition='outside'
                ))

                fig.add_hline(y=0, line_dash="dash", line_color="gray")

                fig.update_layout(
                    title=f"{threshold_months}-Month Momentum Returns",
                    xaxis_title="Asset",
                    yaxis_title="Return (%)",
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

                # Interpretation
                st.subheader("Interpretation")

                st.markdown(f"""
                **Signal Rules**:
                - 🟢 **Buy**: {threshold_months}-month return > 0% → Positive momentum, safe to hold/buy
                - 🔴 **Stop**: {threshold_months}-month return < 0% → Negative momentum, consider reducing/avoiding

                **Current Market Status**:
                - {buy_count} out of {len(signals)} assets showing positive momentum
                - Market breadth: {(buy_count/len(signals)*100):.1f}% bullish
                """)

            except Exception as e:
                st.error(f"Error fetching signals: {str(e)}")
                st.exception(e)


def render_asset_allocation():
    """Render asset allocation simulator page"""
    st.header("Asset Allocation Simulator & Rebalancing Tool")

    st.markdown("""
    Simulate different allocation scenarios and generate rebalancing plans.
    """)

    # Get current holdings
    latest_holdings = data_loader.get_latest_holdings()

    if latest_holdings.empty:
        st.warning("No holdings data available")
        return

    total_value = latest_holdings['평가금액'].sum()

    st.info(f"**Current Portfolio Value**: ₩{total_value:,.0f}")

    # Current allocation analysis
    st.subheader("Current Allocation")

    by_asset = data_loader.get_holdings_by_asset_type()

    if not by_asset.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig = px.pie(
                values=by_asset['평가금액'],
                names=by_asset.index,
                title="Current Asset Allocation"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Show breakdown
            breakdown = by_asset.copy()
            breakdown['Weight (%)'] = (breakdown['평가금액'] / breakdown['평가금액'].sum() * 100).round(2)
            st.dataframe(breakdown[['평가금액', 'Weight (%)']], use_container_width=True)

    # Allocation sliders
    st.subheader("Target Allocation Settings")

    st.markdown("**Stock vs Bond Allocation**")
    stock_target = st.slider(
        "Stock Allocation (%)",
        min_value=50,
        max_value=80,
        value=70,
        step=5,
        help="Range: 50% (defensive) to 80% (aggressive)"
    )

    bond_target = 100 - stock_target
    st.caption(f"Bond Allocation: {bond_target}%")

    st.markdown("**Core vs Satellite Allocation**")
    core_target = st.slider(
        "Core Allocation (%)",
        min_value=50,
        max_value=100,
        value=70,
        step=10,
        help="Range: 50% (active) to 100% (passive index only)"
    )

    satellite_target = 100 - core_target
    st.caption(f"Satellite Allocation: {satellite_target}%")

    # Target allocation summary
    st.subheader("Target Portfolio Composition")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Stock Target", f"{stock_target}%")
        st.caption(f"₩{(total_value * stock_target / 100):,.0f}")

    with col2:
        st.metric("Bond Target", f"{bond_target}%")
        st.caption(f"₩{(total_value * bond_target / 100):,.0f}")

    with col3:
        st.metric("Core Target", f"{core_target}%")
        st.caption(f"₩{(total_value * core_target / 100):,.0f}")

    # Rebalancing plan
    st.subheader("Rebalancing Plan")

    st.markdown("""
    Based on your target allocation, here's what you need to do:
    """)

    # Calculate current stock/bond ratio (simplified)
    # This is a simplified version - in reality you'd need to classify each holding
    st.info("**Note**: Detailed rebalancing calculations require asset classification. Use momentum allocation tool for specific recommendations.")

    # Show sample rebalancing table
    st.markdown("**Sample Rebalancing Workflow**:")
    st.markdown("""
    1. Go to 'Momentum Allocation' page to get recommended country/index weights
    2. Compare recommended weights with your target allocation
    3. Calculate required trades based on current holdings
    4. Execute trades through your broker
    5. Update your holdings CSV and refresh the dashboard
    """)

    # Holdings detail
    st.subheader("Current Holdings Detail")

    display_cols = ['종목명', '평가금액', '수익률(%)']
    if all(col in latest_holdings.columns for col in display_cols):
        holdings_display = latest_holdings[display_cols].copy()
        holdings_display['Weight (%)'] = (latest_holdings['평가금액'] / total_value * 100).round(2)

        st.dataframe(
            holdings_display.sort_values('평가금액', ascending=False),
            use_container_width=True,
            hide_index=True
        )


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
    elif page == "Momentum Allocation":
        render_momentum_allocation()
    elif page == "Absolute Momentum":
        render_absolute_momentum()
    elif page == "Asset Allocation":
        render_asset_allocation()


if __name__ == "__main__":
    main()
