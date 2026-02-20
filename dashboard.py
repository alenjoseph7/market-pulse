import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import snowflake.connector
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(
    page_title="Market Pulse",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

:root {
    --bg-primary: #0a0a0a;
    --bg-secondary: #111111;
    --bg-card: #161616;
    --accent-orange: #ff6600;
    --accent-green: #00ff41;
    --accent-red: #ff3333;
    --accent-blue: #00aaff;
    --accent-yellow: #ffcc00;
    --text-primary: #e0e0e0;
    --text-secondary: #888888;
    --border: #2a2a2a;
}

.stApp { background-color: var(--bg-primary); }
section[data-testid="stSidebar"] {
    background-color: var(--bg-secondary) !important;
    border-right: 1px solid var(--border);
}
section[data-testid="stSidebar"] * {
    font-family: 'IBM Plex Mono', monospace !important;
    color: var(--text-primary) !important;
}
.stSelectbox > div > div {
    background-color: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    color: var(--text-primary) !important;
    font-family: 'IBM Plex Mono', monospace !important;
}
h1, h2, h3, h4 {
    font-family: 'IBM Plex Mono', monospace !important;
    color: var(--text-primary) !important;
}
.metric-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-left: 3px solid var(--accent-orange);
    padding: 16px 20px;
    border-radius: 2px;
    font-family: 'IBM Plex Mono', monospace;
    margin-bottom: 8px;
}
.metric-label {
    font-size: 10px;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.15em;
    margin-bottom: 6px;
}
.metric-value {
    font-size: 22px;
    font-weight: 600;
    color: var(--text-primary);
}
.section-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: var(--accent-orange);
    text-transform: uppercase;
    letter-spacing: 0.2em;
    margin-bottom: 12px;
    padding-bottom: 6px;
    border-bottom: 1px solid var(--border);
}
.info-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    padding: 20px;
    border-radius: 2px;
    font-family: 'IBM Plex Mono', monospace;
    margin-bottom: 12px;
}
.terminal-header {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-bottom: 2px solid var(--accent-orange);
    padding: 12px 20px;
    margin-bottom: 20px;
    font-family: 'IBM Plex Mono', monospace;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.status-dot {
    width: 8px; height: 8px;
    background: var(--accent-green);
    border-radius: 50%;
    display: inline-block;
    margin-right: 6px;
    animation: pulse 2s infinite;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ── SNOWFLAKE ──────────────────────────────────────────
@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        account="EBSQBLV-NJ71987",
        user="ALENJOSEPH7",
        password=st.secrets["snowflake"]["password"],
        warehouse="REPORTER_WH",
        database="ANALYTICS",
        schema="CORE",
        role="REPORTER"
    )

@st.cache_data(ttl=300)
def query(_conn, sql):
    return pd.read_sql(sql, _conn)

# ── HEADER ─────────────────────────────────────────────
st.markdown(f"""
<div class="terminal-header">
    <div style="font-family: IBM Plex Mono; font-size: 18px; font-weight: 600; color: #ff6600;">
        ▸ MARKET PULSE
    </div>
    <div style="font-size: 11px; color: #888; letter-spacing: 0.1em;">
        <span class="status-dot"></span>
        LIVE · {datetime.now().strftime('%Y-%m-%d %H:%M')} EST
    </div>
</div>
""", unsafe_allow_html=True)

# ── SIDEBAR ────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⬡ MARKET PULSE")
    st.markdown("---")
    page = st.selectbox("NAVIGATE", [
        "Overview",
        "Price Charts",
        "Sector Performance",
        "Daily Returns",
        "Volatility Rankings",
        "Stock Info"
    ])
    st.markdown("---")
    st.markdown("<div style='font-size:10px;color:#555;font-family:IBM Plex Mono'>MARKET PULSE v1.0<br>Source: Yahoo Finance<br>Warehouse: Snowflake<br>Refresh: 5 min</div>", unsafe_allow_html=True)

# ── LOAD DATA ──────────────────────────────────────────
PLOTLY_DARK = dict(
    paper_bgcolor='#111111', plot_bgcolor='#111111',
    font=dict(family='IBM Plex Mono', color='#888888', size=11),
    margin=dict(l=0, r=0, t=30, b=0)
)

try:
    conn = get_conn()

    df_returns = query(conn, """
        SELECT TICKER, TRADE_DATE, CLOSE_PRICE, DAILY_RETURN_PCT,
               CUMULATIVE_RETURN_PCT, SECTOR
        FROM ANALYTICS.CORE.MART_DAILY_RETURNS
        ORDER BY TRADE_DATE DESC
    """)
    df_ma = query(conn, """
        SELECT TICKER, TRADE_DATE, CLOSE_PRICE,
               MA_7_DAY, MA_30_DAY, MA_90_DAY,
               VOLATILITY_30_DAY, TREND_SIGNAL, SECTOR
        FROM ANALYTICS.CORE.MART_MOVING_AVERAGES
        ORDER BY TRADE_DATE DESC
    """)
    df_sector = query(conn, """
        SELECT SECTOR, TRADE_MONTH_START, AVG_DAILY_RETURN_PCT,
               COMPANY_COUNT, SECTOR_RANK_BY_RETURN, TRADE_YEAR, TRADE_MONTH
        FROM ANALYTICS.CORE.MART_SECTOR_PERFORMANCE
        ORDER BY TRADE_MONTH_START DESC, SECTOR_RANK_BY_RETURN
    """)

    df_returns['TRADE_DATE'] = pd.to_datetime(df_returns['TRADE_DATE'])
    df_ma['TRADE_DATE'] = pd.to_datetime(df_ma['TRADE_DATE'])
    df_sector['TRADE_MONTH_START'] = pd.to_datetime(df_sector['TRADE_MONTH_START'])

    TICKERS = sorted(df_returns['TICKER'].unique().tolist())
    SECTORS = sorted(df_returns['SECTOR'].unique().tolist())
    latest_ma = df_ma.groupby('TICKER').first().reset_index()
    latest_ret = df_returns.groupby('TICKER').first().reset_index()
    if 'SECTOR' not in latest_ma.columns:
        latest_ma = latest_ma.merge(latest_ret[['TICKER','SECTOR']], on='TICKER', how='left')

    COLORS = ['#ff6600','#00ff41','#00aaff','#ffcc00','#ff3333','#aa44ff','#00ffcc','#ff44aa','#88ff00','#ff8844']

    # ── PAGE: OVERVIEW ─────────────────────────────────
    if page == "Overview":
        st.markdown('<div class="section-title">▸ Market Overview</div>', unsafe_allow_html=True)

        gainers = latest_ret.nlargest(3, 'DAILY_RETURN_PCT')
        losers = latest_ret.nsmallest(3, 'DAILY_RETURN_PCT')
        avg_return = latest_ret['DAILY_RETURN_PCT'].mean()
        bullish = (latest_ma['TREND_SIGNAL'] == 'BULLISH').sum()
        avg_vol = latest_ma['VOLATILITY_30_DAY'].mean()

        c1, c2, c3, c4 = st.columns(4)
        for col, label, value, color in [
            (c1, "Total Tickers", str(len(TICKERS)), "#e0e0e0"),
            (c2, "Avg Daily Return", f"{'+' if avg_return>0 else ''}{avg_return:.2f}%", "#00ff41" if avg_return > 0 else "#ff3333"),
            (c3, "Bullish Signals", f"{bullish}/{len(TICKERS)}", "#00ff41"),
            (c4, "Avg 30D Volatility", f"{avg_vol:.1f}%", "#ffcc00"),
        ]:
            with col:
                st.markdown(f"""<div class="metric-card">
                    <div class="metric-label">{label}</div>
                    <div class="metric-value" style="color:{color}">{value}</div>
                </div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown('<div class="section-title">▸ Top Gainers</div>', unsafe_allow_html=True)
            for _, r in gainers.iterrows():
                st.markdown(f"""<div class="metric-card" style="border-left-color:#00ff41">
                    <b>{r['TICKER']}</b><span style="color:#00ff41;float:right">+{r['DAILY_RETURN_PCT']:.2f}%</span>
                    <br><small style="color:#555">{r['SECTOR']}</small>
                </div>""", unsafe_allow_html=True)
        with c2:
            st.markdown('<div class="section-title">▸ Top Losers</div>', unsafe_allow_html=True)
            for _, r in losers.iterrows():
                st.markdown(f"""<div class="metric-card" style="border-left-color:#ff3333">
                    <b>{r['TICKER']}</b><span style="color:#ff3333;float:right">{r['DAILY_RETURN_PCT']:.2f}%</span>
                    <br><small style="color:#555">{r['SECTOR']}</small>
                </div>""", unsafe_allow_html=True)
        with c3:
            st.markdown('<div class="section-title">▸ Most Volatile</div>', unsafe_allow_html=True)
            for _, r in latest_ma.nlargest(3, 'VOLATILITY_30_DAY').iterrows():
                st.markdown(f"""<div class="metric-card" style="border-left-color:#ffcc00">
                    <b>{r['TICKER']}</b><span style="color:#ffcc00;float:right">{r['VOLATILITY_30_DAY']:.1f}%</span>
                    <br><small style="color:#555">{r['SECTOR']}</small>
                </div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="section-title">▸ Cumulative Returns — Top 10</div>', unsafe_allow_html=True)
        top10 = latest_ret.nlargest(10, 'CUMULATIVE_RETURN_PCT')['TICKER'].tolist()
        fig = go.Figure()
        for i, t in enumerate(top10):
            d = df_returns[df_returns['TICKER']==t].sort_values('TRADE_DATE')
            fig.add_trace(go.Scatter(x=d['TRADE_DATE'], y=d['CUMULATIVE_RETURN_PCT'],
                name=t, line=dict(color=COLORS[i%len(COLORS)], width=1.5)))
        fig.update_layout(**PLOTLY_DARK,
            yaxis=dict(gridcolor='#1e1e1e', ticksuffix='%'),
            xaxis=dict(gridcolor='#1e1e1e'),
            legend=dict(bgcolor='#161616', bordercolor='#2a2a2a', borderwidth=1),
            height=350, hovermode='x unified')
        st.plotly_chart(fig, use_container_width=True)

    # ── PAGE: PRICE CHARTS ─────────────────────────────
    elif page == "Price Charts":
        st.markdown('<div class="section-title">▸ Price + Moving Averages</div>', unsafe_allow_html=True)
        c1, c2 = st.columns([2,1])
        with c1: ticker = st.selectbox("TICKER", TICKERS)
        with c2: days = st.selectbox("PERIOD", [30, 60, 90, 180], index=1)

        d = df_ma[df_ma['TICKER']==ticker].sort_values('TRADE_DATE').tail(days)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7,0.3], vertical_spacing=0.05)
        fig.add_trace(go.Scatter(x=d['TRADE_DATE'], y=d['CLOSE_PRICE'], name='Price', line=dict(color='#e0e0e0', width=2)), row=1, col=1)
        fig.add_trace(go.Scatter(x=d['TRADE_DATE'], y=d['MA_7_DAY'], name='MA7', line=dict(color='#ffcc00', width=1, dash='dot')), row=1, col=1)
        fig.add_trace(go.Scatter(x=d['TRADE_DATE'], y=d['MA_30_DAY'], name='MA30', line=dict(color='#00aaff', width=1.5)), row=1, col=1)
        fig.add_trace(go.Scatter(x=d['TRADE_DATE'], y=d['MA_90_DAY'], name='MA90', line=dict(color='#ff6600', width=1.5)), row=1, col=1)
        fig.add_trace(go.Bar(x=d['TRADE_DATE'], y=d['VOLATILITY_30_DAY'], name='Volatility', marker_color='rgba(255,102,0,0.33)'), row=2, col=1)
        fig.update_layout(**PLOTLY_DARK,
            yaxis=dict(gridcolor='#1e1e1e', tickprefix='$'),
            yaxis2=dict(gridcolor='#1e1e1e', ticksuffix='%'),
            xaxis2=dict(gridcolor='#1e1e1e'),
            legend=dict(bgcolor='#161616', bordercolor='#2a2a2a', orientation='h', y=1.02),
            height=500,
            title=dict(text=f"▸ {ticker} · {days}D", font=dict(color='#ff6600', size=14), x=0))
        st.plotly_chart(fig, use_container_width=True)

        row = d.iloc[-1]
        signal_color = "#00ff41" if row['TREND_SIGNAL'] == 'BULLISH' else "#ff3333"
        for col, label, value, color in zip(
            st.columns(4),
            ["Close Price", "MA 30D", "Volatility 30D", "Trend Signal"],
            [f"${row['CLOSE_PRICE']:.2f}", f"${row['MA_30_DAY']:.2f}" if pd.notna(row['MA_30_DAY']) else "N/A",
             f"{row['VOLATILITY_30_DAY']:.1f}%" if pd.notna(row['VOLATILITY_30_DAY']) else "N/A", row['TREND_SIGNAL']],
            ["#e0e0e0", "#00aaff", "#ffcc00", signal_color]
        ):
            with col:
                st.markdown(f"""<div class="metric-card">
                    <div class="metric-label">{label}</div>
                    <div class="metric-value" style="font-size:18px;color:{color}">{value}</div>
                </div>""", unsafe_allow_html=True)

    # ── PAGE: SECTOR PERFORMANCE ───────────────────────
    elif page == "Sector Performance":
        st.markdown('<div class="section-title">▸ Sector Performance</div>', unsafe_allow_html=True)
        latest_month = df_sector['TRADE_MONTH_START'].max()
        df_latest = df_sector[df_sector['TRADE_MONTH_START']==latest_month].sort_values('SECTOR_RANK_BY_RETURN')

        colors = ['#00ff41' if x > 0 else '#ff3333' for x in df_latest['AVG_DAILY_RETURN_PCT']]
        fig = go.Figure(go.Bar(
            x=df_latest['AVG_DAILY_RETURN_PCT'], y=df_latest['SECTOR'], orientation='h',
            marker_color=colors,
            text=[f"{v:.2f}%" for v in df_latest['AVG_DAILY_RETURN_PCT']],
            textposition='outside', textfont=dict(family='IBM Plex Mono', size=11, color='#e0e0e0')
        ))
        fig.update_layout(**PLOTLY_DARK,
            xaxis=dict(gridcolor='#1e1e1e', ticksuffix='%', zeroline=True, zerolinecolor='#2a2a2a'),
            yaxis=dict(gridcolor='#1e1e1e'),
            height=380,
            title=dict(text=f"▸ Monthly Returns · {latest_month.strftime('%b %Y')}", font=dict(color='#ff6600', size=13), x=0))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-title">▸ Historical Heatmap</div>', unsafe_allow_html=True)
        pivot = df_sector.pivot_table(values='AVG_DAILY_RETURN_PCT', index='SECTOR', columns='TRADE_MONTH_START', aggfunc='mean')
        pivot.columns = [c.strftime('%b %Y') for c in pivot.columns]
        fig2 = go.Figure(go.Heatmap(
            z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
            colorscale=[[0,'#ff3333'],[0.5,'#111111'],[1,'#00ff41']],
            text=[[f"{v:.1f}%" if pd.notna(v) else "" for v in row] for row in pivot.values],
            texttemplate="%{text}", textfont=dict(family='IBM Plex Mono', size=10),
        ))
        fig2.update_layout(**PLOTLY_DARK, height=320)
        st.plotly_chart(fig2, use_container_width=True)

    # ── PAGE: DAILY RETURNS ────────────────────────────
    elif page == "Daily Returns":
        st.markdown('<div class="section-title">▸ Daily Returns Analysis</div>', unsafe_allow_html=True)
        c1, c2 = st.columns([2,1])
        with c1: sector_filter = st.selectbox("SECTOR", ["All"] + SECTORS)
        with c2: days = st.selectbox("PERIOD", [30, 60, 90], index=0)

        df_filtered = df_returns.copy()
        if sector_filter != "All":
            df_filtered = df_filtered[df_filtered['SECTOR'] == sector_filter]
        tickers_filtered = df_filtered['TICKER'].unique().tolist()

        # Return distribution
        latest_day = df_filtered.groupby('TICKER').first().reset_index()
        fig = go.Figure()
        fig.add_trace(go.Histogram(
            x=latest_day['DAILY_RETURN_PCT'],
            nbinsx=20,
            marker_color='#ff6600',
            marker_line=dict(color='#000', width=1),
            opacity=0.8,
            name='Distribution'
        ))
        fig.add_vline(x=0, line_color='#888', line_dash='dash', line_width=1)
        fig.update_layout(**PLOTLY_DARK,
            xaxis=dict(gridcolor='#1e1e1e', ticksuffix='%', title='Daily Return'),
            yaxis=dict(gridcolor='#1e1e1e', title='Count'),
            height=300,
            title=dict(text="▸ Return Distribution (Latest Day)", font=dict(color='#ff6600', size=13), x=0))
        st.plotly_chart(fig, use_container_width=True)

        # Returns heatmap by ticker
        st.markdown('<div class="section-title">▸ Daily Returns — Recent Period</div>', unsafe_allow_html=True)
        df_heat = df_filtered[df_filtered['TICKER'].isin(tickers_filtered[:20])].copy()
        df_heat = df_heat.sort_values('TRADE_DATE').tail(days * len(tickers_filtered[:20]))
        pivot_r = df_heat.pivot_table(values='DAILY_RETURN_PCT', index='TICKER', columns='TRADE_DATE', aggfunc='mean')
        pivot_r.columns = [c.strftime('%m/%d') for c in pivot_r.columns]

        fig2 = go.Figure(go.Heatmap(
            z=pivot_r.values, x=pivot_r.columns.tolist(), y=pivot_r.index.tolist(),
            colorscale=[[0,'#ff3333'],[0.5,'#111111'],[1,'#00ff41']],
            zmid=0,
            text=[[f"{v:.1f}%" if pd.notna(v) else "" for v in row] for row in pivot_r.values],
            texttemplate="%{text}", textfont=dict(family='IBM Plex Mono', size=8),
        ))
        fig2.update_layout(**PLOTLY_DARK, height=500)
        st.plotly_chart(fig2, use_container_width=True)

    # ── PAGE: VOLATILITY RANKINGS ──────────────────────
    elif page == "Volatility Rankings":
        st.markdown('<div class="section-title">▸ Risk vs Return</div>', unsafe_allow_html=True)
        df_vol = latest_ma.merge(latest_ret[['TICKER','DAILY_RETURN_PCT','CUMULATIVE_RETURN_PCT','SECTOR']], on='TICKER', how='left', suffixes=('','_y'))
        if 'SECTOR_y' in df_vol.columns:
            df_vol = df_vol.drop(columns=['SECTOR_y'])

        fig = go.Figure()
        for i, sector in enumerate(SECTORS):
            d = df_vol[df_vol['SECTOR']==sector]
            fig.add_trace(go.Scatter(
                x=d['VOLATILITY_30_DAY'], y=d['CUMULATIVE_RETURN_PCT'],
                mode='markers+text', name=sector,
                text=d['TICKER'], textposition='top center',
                textfont=dict(family='IBM Plex Mono', size=9),
                marker=dict(size=10, color=COLORS[i%len(COLORS)], line=dict(width=1, color='#000'))
            ))
        fig.update_layout(**PLOTLY_DARK,
            xaxis=dict(gridcolor='#1e1e1e', title='30D Volatility (%)', ticksuffix='%'),
            yaxis=dict(gridcolor='#1e1e1e', title='Cumulative Return (%)', ticksuffix='%'),
            legend=dict(bgcolor='#161616', bordercolor='#2a2a2a', borderwidth=1),
            height=450,
            title=dict(text="▸ Volatility vs Cumulative Return", font=dict(color='#ff6600', size=13), x=0))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-title">▸ Full Rankings</div>', unsafe_allow_html=True)
        df_display = df_vol[['TICKER','SECTOR','CLOSE_PRICE','VOLATILITY_30_DAY','DAILY_RETURN_PCT','CUMULATIVE_RETURN_PCT','TREND_SIGNAL']].copy()
        df_display = df_display.sort_values('VOLATILITY_30_DAY', ascending=False)
        df_display.columns = ['Ticker','Sector','Price','Volatility 30D','Daily Return','Cumulative Return','Signal']
        df_display['Price'] = df_display['Price'].apply(lambda x: f"${x:.2f}")
        df_display['Volatility 30D'] = df_display['Volatility 30D'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
        df_display['Daily Return'] = df_display['Daily Return'].apply(lambda x: f"+{x:.2f}%" if x>0 else f"{x:.2f}%")
        df_display['Cumulative Return'] = df_display['Cumulative Return'].apply(lambda x: f"+{x:.2f}%" if x>0 else f"{x:.2f}%")
        st.dataframe(df_display, use_container_width=True, hide_index=True)

    # ── PAGE: STOCK INFO ───────────────────────────────
    elif page == "Stock Info":
        st.markdown('<div class="section-title">▸ Stock Information</div>', unsafe_allow_html=True)
        ticker = st.selectbox("SELECT TICKER", TICKERS)

        t_ret = df_returns[df_returns['TICKER']==ticker].sort_values('TRADE_DATE')
        t_ma = df_ma[df_ma['TICKER']==ticker].sort_values('TRADE_DATE')

        latest_r = t_ret.iloc[-1]
        latest_m = t_ma.iloc[-1]

        c1, c2 = st.columns([1,2])
        with c1:
            sector = latest_r['SECTOR']
            signal = latest_m['TREND_SIGNAL']
            signal_color = "#00ff41" if signal == "BULLISH" else "#ff3333"
            st.markdown(f"""
            <div class="info-card">
                <div style="font-size:32px;font-weight:700;color:#ff6600;margin-bottom:4px">{ticker}</div>
                <div style="font-size:12px;color:#888;margin-bottom:16px">{sector}</div>
                <div style="font-size:36px;font-weight:600;color:#e0e0e0;margin-bottom:4px">${latest_r['CLOSE_PRICE']:.2f}</div>
                <div style="font-size:14px;color:{'#00ff41' if latest_r['DAILY_RETURN_PCT']>0 else '#ff3333'}">
                    {'+' if latest_r['DAILY_RETURN_PCT']>0 else ''}{latest_r['DAILY_RETURN_PCT']:.2f}% today
                </div>
                <hr style="border-color:#2a2a2a;margin:16px 0">
                <div class="metric-label">Trend Signal</div>
                <div style="font-size:18px;font-weight:600;color:{signal_color}">{signal}</div>
                <hr style="border-color:#2a2a2a;margin:16px 0">
                <div class="metric-label">MA 7D</div>
                <div style="color:#ffcc00">${latest_m['MA_7_DAY']:.2f}</div>
                <div class="metric-label" style="margin-top:8px">MA 30D</div>
                <div style="color:#00aaff">${latest_m['MA_30_DAY']:.2f}</div>
                <div class="metric-label" style="margin-top:8px">MA 90D</div>
                <div style="color:#ff6600">${latest_m['MA_90_DAY']:.2f}</div>
                <hr style="border-color:#2a2a2a;margin:16px 0">
                <div class="metric-label">30D Volatility</div>
                <div style="color:#ffcc00">{latest_m['VOLATILITY_30_DAY']:.1f}%</div>
                <div class="metric-label" style="margin-top:8px">Cumulative Return</div>
                <div style="color:{'#00ff41' if latest_r['CUMULATIVE_RETURN_PCT']>0 else '#ff3333'}">
                    {'+' if latest_r['CUMULATIVE_RETURN_PCT']>0 else ''}{latest_r['CUMULATIVE_RETURN_PCT']:.2f}%
                </div>
            </div>
            """, unsafe_allow_html=True)

        with c2:
            fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                               row_heights=[0.7,0.3], vertical_spacing=0.05)
            fig.add_trace(go.Scatter(x=t_ma['TRADE_DATE'], y=t_ma['CLOSE_PRICE'],
                name='Price', line=dict(color='#e0e0e0', width=2)), row=1, col=1)
            fig.add_trace(go.Scatter(x=t_ma['TRADE_DATE'], y=t_ma['MA_7_DAY'],
                name='MA7', line=dict(color='#ffcc00', width=1, dash='dot')), row=1, col=1)
            fig.add_trace(go.Scatter(x=t_ma['TRADE_DATE'], y=t_ma['MA_30_DAY'],
                name='MA30', line=dict(color='#00aaff', width=1.5)), row=1, col=1)
            fig.add_trace(go.Scatter(x=t_ma['TRADE_DATE'], y=t_ma['MA_90_DAY'],
                name='MA90', line=dict(color='#ff6600', width=1.5)), row=1, col=1)
            fig.add_trace(go.Bar(x=t_ret['TRADE_DATE'], y=t_ret['DAILY_RETURN_PCT'],
                name='Daily Return',
                marker_color=['#00ff41' if v>0 else '#ff3333' for v in t_ret['DAILY_RETURN_PCT']]),
                row=2, col=1)
            fig.update_layout(**PLOTLY_DARK,
                yaxis=dict(gridcolor='#1e1e1e', tickprefix='$'),
                yaxis2=dict(gridcolor='#1e1e1e', ticksuffix='%', zeroline=True, zerolinecolor='#333'),
                xaxis2=dict(gridcolor='#1e1e1e'),
                legend=dict(bgcolor='#161616', bordercolor='#2a2a2a', orientation='h', y=1.02),
                height=450,
                title=dict(text=f"▸ {ticker} Full History", font=dict(color='#ff6600', size=13), x=0))
            st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Connection error: {e}")
    st.info("Set your Snowflake password in .streamlit/secrets.toml")