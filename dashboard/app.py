"""
dashboard/app.py
================
Crypto Market Intelligence Dashboard — Streamlit
Uses ONLY the two Gold tables that exist in ADLS.
"""

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from data_loader import get_loader, ADLSConnectionError

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Crypto Market Intelligence",
    page_icon="🪙",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS (CoinMarketCap Light Theme) ────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* Overriding Streamlit's base background for a subtle light gray */
.stApp { background-color: #F8F9FA; }

.main-header {
  background: #FFFFFF;
  padding: 1.5rem 2rem; border-radius: 16px; margin-bottom: 1.5rem;
  border: 1px solid #EFF2F5; box-shadow: 0px 4px 14px rgba(0, 0, 0, 0.03);
}
.main-header h1 { color: #000000; margin: 0; font-size: 1.75rem; font-weight: 700; }
.main-header p  { color: #58667E; margin: 0.25rem 0 0; font-size: 0.9rem; }

.kpi-card {
  background: #FFFFFF; border-radius: 14px;
  padding: 1.2rem 1.4rem; border: 1px solid #EFF2F5; height: 100%;
  box-shadow: 0px 4px 14px rgba(0, 0, 0, 0.03); transition: transform 0.2s ease;
}
.kpi-card:hover { transform: translateY(-2px); }
.kpi-label { color: #58667E; font-size: 0.75rem; font-weight: 600;
             text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.4rem; }
.kpi-value { color: #000000; font-size: 1.6rem; font-weight: 700;
             margin: 0.15rem 0 0.3rem; line-height: 1.15; }
.kpi-delta-pos { color: #16C784; font-size: 0.85rem; font-weight: 600; }
.kpi-delta-neg { color: #EA3943; font-size: 0.85rem; font-weight: 600; }
.kpi-delta-neu { color: #58667E; font-size: 0.85rem; }

.sec-head {
  color: #000000; font-size: 1.2rem; font-weight: 700;
  margin: 2rem 0 1rem; padding-bottom: 0.5rem;
  border-bottom: 1px solid #EFF2F5;
}

.rank-card-up {
  background: #FFFFFF; border: 1px solid #EFF2F5;
  border-radius: 10px; padding: 0.75rem 1rem; margin: 0.3rem 0;
  display: flex; justify-content: space-between; align-items: center;
  box-shadow: 0px 2px 8px rgba(0, 0, 0, 0.02);
}
.rank-card-dn {
  background: #FFFFFF; border: 1px solid #EFF2F5;
  border-radius: 10px; padding: 0.75rem 1rem; margin: 0.3rem 0;
  display: flex; justify-content: space-between; align-items: center;
  box-shadow: 0px 2px 8px rgba(0, 0, 0, 0.02);
}
.rank-sym  { font-weight: 700; color: #000000; font-size: 0.95rem; }
.rank-name { color: #58667E; font-size: 0.8rem; margin-left: 8px; }
.rank-pct-up { color: #16C784; font-weight: 600; font-size: 0.9rem; }
.rank-pct-dn { color: #EA3943; font-weight: 600; font-size: 0.9rem; }

.error-box {
  background: #FFF0F0; border: 1px solid #FCA5A5;
  border-radius: 16px; padding: 2rem 2.5rem; margin: 3rem auto;
  max-width: 680px; text-align: center;
}
.error-box h2 { color: #EA3943; margin-bottom: 0.5rem; }
.error-box pre {
  background: #FFFFFF; border-radius: 8px; padding: 1rem;
  color: #58667E; font-size: 0.8rem; text-align: left;
  overflow-x: auto; margin-top: 1rem; border: 1px solid #EFF2F5;
}

#MainMenu, footer, header { visibility: hidden; }
[data-testid="stSidebar"] { background-color: #FFFFFF; border-right: 1px solid #EFF2F5; }
[data-testid="stSidebar"] * { color: #000000 !important; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# Data loading — cached 5 min, hard-fail on error
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner=False)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    try:
        loader  = get_loader()
        data    = loader.load_all()
        daily   = data.get("daily_price_summary", pd.DataFrame())
        vol     = data.get("volatility_metrics",  pd.DataFrame())
        return daily, vol, None
    except ADLSConnectionError as exc:
        return pd.DataFrame(), pd.DataFrame(), str(exc)
    except Exception as exc:
        return pd.DataFrame(), pd.DataFrame(), f"Unexpected error: {exc}"


# ══════════════════════════════════════════════════════════════════════════════
# Format helpers
# ══════════════════════════════════════════════════════════════════════════════

def fmt_usd(v, short=True) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    if short:
        if abs(v) >= 1e12: return f"${v/1e12:.2f}T"
        if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
        if abs(v) >= 1e6:  return f"${v/1e6:.2f}M"
        if abs(v) >= 1e3:  return f"${v/1e3:.2f}K"
        return f"${v:.6g}"
    return f"${v:,.6g}"

def fmt_pct(v, is_fraction=False) -> str:
    """Format a percentage with CMC arrows."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    val = v * 100 if is_fraction else v
    if val > 0: return f"▲ {val:.2f}%"
    elif val < 0: return f"▼ {abs(val):.2f}%"
    return f"{val:.2f}%"

def delta_cls(v, is_fraction=False) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "kpi-delta-neu"
    val = v * 100 if is_fraction else v
    return "kpi-delta-pos" if val > 0 else "kpi-delta-neg"

def kpi_card(label: str, value: str, delta_text: str = "", delta_val=None,
             is_fraction=False) -> str:
    cls = delta_cls(delta_val, is_fraction)
    d   = f'<div class="{cls}">{delta_text}</div>' if delta_text else ""
    return f"""
    <div class="kpi-card">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
      {d}
    </div>"""

def plotly_theme(fig, title="", height=320):
    fig.update_layout(
        title       = dict(text=title, font=dict(color="#000000", size=15, family="Inter")),
        paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF",
        font        = dict(color="#58667E", family="Inter"),
        height      = height,
        margin      = dict(l=44, r=16, t=44, b=36),
        legend      = dict(bgcolor="rgba(255,255,255,0.8)", font=dict(color="#58667E", size=11)),
        xaxis       = dict(gridcolor="#EFF2F5", linecolor="#EFF2F5", tickfont=dict(color="#58667E")),
        yaxis       = dict(gridcolor="#EFF2F5", linecolor="#EFF2F5", tickfont=dict(color="#58667E")),
    )
    # Add subtle border around the entire plot to match the CMC cards
    fig.update_layout(
        shapes=[dict(
            type="rect", xref="paper", yref="paper", x0=0, y0=0, x1=1, y1=1,
            line=dict(color="#EFF2F5", width=1)
        )]
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# Data preparation helpers (work on actual schema)
# ══════════════════════════════════════════════════════════════════════════════

def enrich_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    d = df.copy()
    d["event_timestamp"] = pd.to_datetime(d["event_timestamp"], utc=True)
    d["date"] = d["event_timestamp"].dt.date
    if "daily_return" in d.columns:
        d["daily_return_pct"] = d["daily_return"] * 100
    d = d.sort_values(["symbol", "event_timestamp"]).reset_index(drop=True)
    return d

def latest_per_coin(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    return df.sort_values("event_timestamp").groupby("symbol", as_index=False).last()

def coin_history(df: pd.DataFrame, symbol: str, days: int) -> pd.DataFrame:
    if df.empty: return df
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
    mask   = (df["symbol"] == symbol) & (df["event_timestamp"] >= cutoff)
    return df[mask].sort_values("event_timestamp").reset_index(drop=True)

def compute_market_totals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    d = df.copy()
    d["date"] = d["event_timestamp"].dt.normalize()

    # --- THE FIX: Keep only the latest timestamp per coin per day ---
    d = d.sort_values("event_timestamp").drop_duplicates(subset=["symbol", "date"], keep="last")

    agg = (
        d.groupby("date")
         .agg(
             total_market_cap = ("market_cap", "sum"),
             total_volume_24h = ("volume_24h", "sum"),
             n_coins          = ("symbol", "nunique"),
         ).reset_index().sort_values("date")
    )
    agg["mc_pct_change"]  = agg["total_market_cap"].pct_change() * 100
    agg["vol_pct_change"] = agg["total_volume_24h"].pct_change() * 100
    return agg

def compute_gainers_losers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    latest = latest_per_coin(df)[["symbol", "name", "price_usd", "daily_return_pct", "market_cap"]].copy()
    latest = latest.dropna(subset=["daily_return_pct"])
    return latest.sort_values("daily_return_pct", ascending=False).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# Chart builders
# ══════════════════════════════════════════════════════════════════════════════

def chart_market_cap_trend(market_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=market_df["date"], y=market_df["total_market_cap"],
        mode="lines+markers", name="Total Market Cap",
        line=dict(color="#3861FB", width=2), # CMC Blue
        fill="tozeroy", fillcolor="rgba(56, 97, 251, 0.08)",
    ))
    ma = market_df["total_market_cap"].rolling(7, min_periods=1).mean()
    fig.add_trace(go.Scatter(
        x=market_df["date"], y=ma, mode="lines", name="7-day MA",
        line=dict(color="#F5A623", width=1.5, dash="dot"),
    ))
    plotly_theme(fig, "Crypto Market Cap Trend", 280)
    fig.update_yaxes(tickformat="$,.0s")
    return fig


def chart_volume_trend(market_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=market_df["date"], y=market_df["total_volume_24h"],
        name="24h Volume", marker_color="#8A9DAB", opacity=0.6,
    ))
    ma = market_df["total_volume_24h"].rolling(7, min_periods=1).mean()
    fig.add_trace(go.Scatter(
        x=market_df["date"], y=ma, mode="lines", name="7-day MA",
        line=dict(color="#3861FB", width=2, dash="dot"),
    ))
    plotly_theme(fig, "Trading Volume Trend", 280)
    fig.update_yaxes(tickformat="$,.0s")
    return fig


def chart_price_history(df: pd.DataFrame, vol_df: pd.DataFrame,
                         symbol: str, days: int) -> go.Figure:
    hist    = coin_history(df,     symbol, days)
    vol_sub = coin_history(vol_df, symbol, days) if not vol_df.empty else pd.DataFrame()

    if hist.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data for this period",
                           xref="paper", yref="paper", x=0.5, y=0.5,
                           showarrow=False, font=dict(color="#58667E", size=14))
        return plotly_theme(fig, f"{symbol} — {days}d", 400)

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.75, 0.25], vertical_spacing=0.03,
    )

    # Price line
    fig.add_trace(go.Scatter(
        x=hist["event_timestamp"], y=hist["price_usd"],
        mode="lines", name="Price",
        line=dict(color="#16C784", width=2), # CMC Green
        fill="tozeroy", fillcolor="rgba(22, 199, 132, 0.05)"
    ), row=1, col=1)

    # Moving averages
    if "7d_moving_avg" in hist.columns:
        fig.add_trace(go.Scatter(
            x=hist["event_timestamp"], y=hist["7d_moving_avg"],
            mode="lines", name="MA-7d",
            line=dict(color="#F5A623", width=1.4, dash="dot"), opacity=0.85,
        ), row=1, col=1)

    # Volume bars — color by daily_return sign
    bar_colors = []
    for _, row in hist.iterrows():
        r = row.get("daily_return", 0)
        bar_colors.append("#16C784" if (r and r >= 0) else "#EA3943")

    fig.add_trace(go.Bar(
        x=hist["event_timestamp"], y=hist["volume_24h"],
        name="Volume", marker_color=bar_colors, opacity=0.4,
    ), row=2, col=1)

    plotly_theme(fig, f"{symbol} Price & Volume Overview", 440)
    fig.update_layout(xaxis2_rangeslider_visible=False)
    fig.update_yaxes(tickformat="$,.4g", row=1, col=1)
    fig.update_yaxes(tickformat="$,.0s",  row=2, col=1)
    return fig


def chart_multi_comparison(df: pd.DataFrame, symbols: list[str], days: int) -> go.Figure:
    palette = ["#3861FB","#16C784","#EA3943","#F5A623","#A855F7","#475569"]
    cutoff  = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
    fig     = go.Figure()

    for i, sym in enumerate(symbols):
        sub = df[(df["symbol"] == sym) & (df["event_timestamp"] >= cutoff)
                ].sort_values("event_timestamp")
        if sub.empty or sub["price_usd"].iloc[0] == 0: continue
        rebased = sub["price_usd"] / sub["price_usd"].iloc[0] * 100
        fig.add_trace(go.Scatter(
            x=sub["event_timestamp"], y=rebased.round(2),
            mode="lines", name=sym,
            line=dict(color=palette[i % len(palette)], width=2),
        ))

    fig.add_hline(y=100, line_color="#A1A7BB", line_dash="dot", line_width=1, opacity=0.5)
    plotly_theme(fig, f"Normalised Performance — {days}d (Base = 100)", 340)
    return fig


def chart_volatility_by_coin(vol_df: pd.DataFrame, days: int = 30) -> go.Figure:
    if vol_df.empty or "rolling_std" not in vol_df.columns: return go.Figure()
    latest = (
        vol_df.sort_values("event_timestamp").dropna(subset=["rolling_std"])
              .groupby("symbol", as_index=False).last()[["symbol", "rolling_std"]]
              .sort_values("rolling_std", ascending=True)
    )
    colors = ["#EA3943" if v > latest["rolling_std"].median() else "#3861FB"
              for v in latest["rolling_std"]]
    fig = go.Figure(go.Bar(
        x=latest["rolling_std"], y=latest["symbol"],
        orientation="h", marker_color=colors,
        text=latest["rolling_std"].round(4),
        textposition="outside", textfont=dict(color="#58667E", size=11),
    ))
    plotly_theme(fig, "Rolling Std Dev by Coin (latest)", 380)
    return fig


def chart_dominance_pie(latest_df: pd.DataFrame) -> go.Figure:
    sub = latest_df[latest_df["market_cap"].notna() & (latest_df["market_cap"] > 0)]
    if sub.empty: return go.Figure()
    total  = sub["market_cap"].sum()
    top    = sub.nlargest(5, "market_cap")
    others = total - top["market_cap"].sum()
    labels = list(top["symbol"]) + (["Others"] if others > 0 else [])
    values = list(top["market_cap"]) + ([others] if others > 0 else [])
    palette= ["#3861FB","#16C784","#F5A623","#A855F7","#14B8A6","#8A9DAB"]
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.6,
        marker_colors=palette[:len(labels)],
        textinfo="label+percent",
        textfont=dict(color="#FFFFFF", size=12),
    ))
    plotly_theme(fig, "Market Cap Dominance", 300)
    fig.update_layout(showlegend=False)
    return fig


def chart_volume_spike(df: pd.DataFrame, symbol: str, days: int = 60) -> go.Figure:
    hist = coin_history(df, symbol, days)
    if hist.empty: return go.Figure()
    fig = go.Figure()
    colors = ["#EA3943" if s else "#8A9DAB" for s in hist.get("volume_spike", [0]*len(hist))]
    fig.add_trace(go.Bar(
        x=hist["event_timestamp"], y=hist["volume_24h"],
        name="Volume", marker_color=colors, opacity=0.6,
    ))
    plotly_theme(fig, f"{symbol} Volume (Red = Spike)", 220)
    fig.update_yaxes(tickformat="$,.0s")
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# Full-page error renderer
# ══════════════════════════════════════════════════════════════════════════════

def show_error_page(message: str):
    st.markdown(f"""
    <div class="error-box">
      <h2>⚠️ Cannot Load Dashboard Data</h2>
      <p style="color:#58667E">The dashboard could not connect to Azure Data Lake Storage.<br>
      Nothing will be displayed until the issue is resolved.</p>
      <pre>{message}</pre>
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

with st.spinner("Connecting to Azure and loading data …"):
    daily_raw, vol_raw, load_error = load_data()

if load_error:
    st.markdown("""
    <div class="main-header">
      <h1>🪙 Crypto Market Intelligence</h1>
      <p>Connection error</p>
    </div>""", unsafe_allow_html=True)
    show_error_page(load_error)
    st.stop()

daily      = enrich_daily(daily_raw)
vol        = vol_raw.copy()
if "event_timestamp" in vol.columns:
    vol["event_timestamp"] = pd.to_datetime(vol["event_timestamp"], utc=True)

latest     = latest_per_coin(daily)
market_agg = compute_market_totals(daily)
gl_df      = compute_gainers_losers(daily)

all_symbols = sorted(daily["symbol"].dropna().unique().tolist()) if not daily.empty else []

if daily.empty:
    st.markdown("""<div class="main-header">
      <h1>🪙 Crypto Market Intelligence</h1>
      <p>Connected — waiting for pipeline data</p></div>""", unsafe_allow_html=True)
    st.info("Connected to Azure successfully, but gold/daily_price_summary has no data yet.", icon="ℹ️")
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### 🪙 Crypto Intelligence")
    st.success("Connected to Azure", icon="✅")
    st.caption(f"Data: {len(daily):,} rows · {len(all_symbols)} coins")
    st.markdown("---")

    selected_symbol = st.selectbox(
        "Coin Overview",
        options=all_symbols,
        index=all_symbols.index("BTC") if "BTC" in all_symbols else 0,
    )

    compare_syms = st.multiselect(
        "Multi-Coin Comparison",
        options=all_symbols,
        default=all_symbols[:4] if len(all_symbols) >= 4 else all_symbols,
        max_selections=8,
    )

    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# Page header
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<div class="main-header">
  <h1>🪙 Crypto Market Overview</h1>
  <p>Live market data from Azure Data Lake Storage · Gold layer</p>
</div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# KPI 1 & 3 — Top metric cards
# ══════════════════════════════════════════════════════════════════════════════

def safe(row, col, default=None):
    try:
        v = row[col]
        return None if pd.isna(v) else v
    except Exception:
        return default

if not market_agg.empty:
    mr = market_agg.iloc[-1]
    total_mc     = safe(mr, "total_market_cap")
    total_vol    = safe(mr, "total_volume_24h")
    mc_pct_day   = safe(mr, "mc_pct_change")
    vol_pct_day  = safe(mr, "vol_pct_change")
else:
    total_mc = total_vol = mc_pct_day = vol_pct_day = None

sel_row = latest[latest["symbol"] == selected_symbol]
sel = sel_row.iloc[0] if not sel_row.empty else None

coin_price   = safe(sel, "price_usd")    if sel is not None else None
coin_ret     = safe(sel, "daily_return") if sel is not None else None
coin_vol     = safe(sel, "volume_24h")   if sel is not None else None
coin_mc      = safe(sel, "market_cap")   if sel is not None else None

if coin_mc and total_mc and total_mc > 0:
    calculated_dom = (coin_mc / total_mc) * 100
else:
    calculated_dom = None


c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.markdown(kpi_card(
        "Total Market Cap", fmt_usd(total_mc),
        fmt_pct(mc_pct_day) + " (24h)" if mc_pct_day is not None else "",
        mc_pct_day
    ), unsafe_allow_html=True)
with c2:
    st.markdown(kpi_card(
        "24h Trading Volume", fmt_usd(total_vol),
        fmt_pct(vol_pct_day) + " (24h)" if vol_pct_day is not None else "",
        vol_pct_day
    ), unsafe_allow_html=True)
with c3:
    st.markdown(kpi_card(
        f"{selected_symbol} Price", fmt_usd(coin_price, short=False),
        fmt_pct(coin_ret, is_fraction=True) + " (24h)" if coin_ret is not None else "",
        coin_ret, is_fraction=True
    ), unsafe_allow_html=True)
with c4:
    st.markdown(kpi_card(
        f"{selected_symbol} 24h Volume", fmt_usd(coin_vol),
        "", None
    ), unsafe_allow_html=True)
with c5:
    st.markdown(kpi_card(
        f"{selected_symbol} Market Cap", fmt_usd(coin_mc),
        f"Dominance: {calculated_dom:.2f}%" if calculated_dom is not None else "Dominance: N/A",
        None
    ), unsafe_allow_html=True)

st.markdown("<div style='margin-top:0.8rem'></div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# KPI 2 & 4 — Market Trends
# ══════════════════════════════════════════════════════════════════════════════

st.markdown('<div class="sec-head">Global Market Trends</div>', unsafe_allow_html=True)
tc1, tc2 = st.columns(2)
with tc1:
    st.plotly_chart(chart_market_cap_trend(market_agg), use_container_width=True)
with tc2:
    st.plotly_chart(chart_volume_trend(market_agg),     use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Top 5 Gainers & Losers
# ══════════════════════════════════════════════════════════════════════════════

st.markdown('<div class="sec-head">Top Movers (24h)</div>', unsafe_allow_html=True)

def render_rank_list(df: pd.DataFrame, ascending: bool, n: int = 5):
    sub = df.sort_values("daily_return_pct", ascending=ascending).head(n)
    for _, row in sub.iterrows():
        pct  = row.get("daily_return_pct", 0)
        sym  = row.get("symbol", "?")
        name = row.get("name",   "")
        card_cls = "rank-card-dn" if ascending else "rank-card-up"
        pct_cls  = "rank-pct-dn" if ascending else "rank-pct-up"
        sign     = "▼ " if pct < 0 else "▲ "
        st.markdown(f"""
        <div class="{card_cls}">
          <span>
            <span class="rank-sym">{sym}</span>
            <span class="rank-name">{name}</span>
          </span>
          <span class="{pct_cls}">{sign}{abs(pct):.2f}%</span>
        </div>""", unsafe_allow_html=True)

gc, lc = st.columns(2)
with gc:
    st.markdown("##### 🚀 Top Gainers")
    if not gl_df.empty: render_rank_list(gl_df, ascending=False)
with lc:
    st.markdown("##### 📉 Top Losers")
    if not gl_df.empty: render_rank_list(gl_df, ascending=True)


# ══════════════════════════════════════════════════════════════════════════════
# Price movement by selected coin
# ══════════════════════════════════════════════════════════════════════════════

st.markdown(f'<div class="sec-head">{selected_symbol} Overview</div>', unsafe_allow_html=True)

pt1, pt2, pt3 = st.tabs(["30 Days", "7 Days", "All Time"])
with pt1:
    st.plotly_chart(chart_price_history(daily, vol, selected_symbol, 30), use_container_width=True)
with pt2:
    st.plotly_chart(chart_price_history(daily, vol, selected_symbol, 7), use_container_width=True)
with pt3:
    st.plotly_chart(chart_price_history(daily, vol, selected_symbol, 999), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Technical indicators row
# ══════════════════════════════════════════════════════════════════════════════

st.markdown('<div class="sec-head">Technical Indicators</div>', unsafe_allow_html=True)
ti1, ti2 = st.columns([1.4, 1])

with ti1:
    st.plotly_chart(chart_volatility_by_coin(vol), use_container_width=True)

with ti2:
    if sel is not None:
        rows = {
            "Price":            fmt_usd(coin_price, short=False),
            "24h Return":       fmt_pct(coin_ret, is_fraction=True),
            "7d Moving Avg":    fmt_usd(safe(sel,"7d_moving_avg"),  short=False),
            "14d Moving Avg":   fmt_usd(safe(sel,"14d_moving_avg"), short=False),
            "Volume 24h":       fmt_usd(coin_vol),
            "7d Avg Volume":    fmt_usd(safe(sel,"7d_avg_volume")),
            "Market Cap":       fmt_usd(coin_mc),
            "Dominance":        f"{calculated_dom:.4f}%" if calculated_dom else "—",
        }
        vol_row = vol[vol["symbol"] == selected_symbol].sort_values("event_timestamp").tail(1)
        if not vol_row.empty:
            rows["Rolling Std"] = f"{vol_row['rolling_std'].iloc[0]:.6f}"

        tbl = "".join([
            f"<tr>"
            f"<td style='color:#58667E;padding:12px 14px;font-size:0.9rem;border-bottom:1px solid #EFF2F5;'>{k}</td>"
            f"<td style='color:#000000;padding:12px 14px;font-size:0.9rem;"
            f"font-weight:600;border-bottom:1px solid #EFF2F5;text-align:right;'>{v}</td>"
            f"</tr>"
            for k, v in rows.items()
        ])
        st.markdown(
            f"<table style='width:100%;background:#FFFFFF;border-radius:14px; border-collapse: collapse;"
            f"border:1px solid #EFF2F5; box-shadow: 0px 4px 14px rgba(0, 0, 0, 0.03);'>{tbl}</table>",
            unsafe_allow_html=True
        )

st.plotly_chart(chart_volume_spike(daily, selected_symbol, 60), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Multi-coin comparison + dominance
# ══════════════════════════════════════════════════════════════════════════════

st.markdown('<div class="sec-head">Comparison & Dominance</div>', unsafe_allow_html=True)
mc1, mc2 = st.columns([2, 1])

with mc1:
    cm1, cm2, cm3 = st.tabs(["30 Days", "7 Days", "All Time"])
    with cm1:
        st.plotly_chart(chart_multi_comparison(daily, compare_syms, 30), use_container_width=True)
    with cm2:
        st.plotly_chart(chart_multi_comparison(daily, compare_syms, 7), use_container_width=True)
    with cm3:
        st.plotly_chart(chart_multi_comparison(daily, compare_syms, 999), use_container_width=True)

with mc2:
    st.plotly_chart(chart_dominance_pie(latest), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Full coin snapshot table
# ══════════════════════════════════════════════════════════════════════════════

st.markdown('<div class="sec-head">Asset Directory</div>', unsafe_allow_html=True)

if not latest.empty:
    disp = latest[
        [c for c in ["symbol","name","price_usd","daily_return_pct",
                      "volume_24h","market_cap","7d_moving_avg"]
         if c in latest.columns]
    ].copy()

    rename = {
        "symbol":               "Symbol",
        "name":                 "Name",
        "price_usd":            "Price (USD)",
        "daily_return_pct":     "24h Return %",
        "volume_24h":           "Volume (24h)",
        "market_cap":           "Market Cap",
        "7d_moving_avg":        "MA-7d",
    }
    disp = disp.rename(columns={k: v for k, v in rename.items() if k in disp.columns})
    disp = disp.sort_values("Market Cap", ascending=False) if "Market Cap" in disp.columns else disp

    # Note: Streamlit's default st.dataframe respects the base light/dark mode of the app config.
    st.dataframe(disp.reset_index(drop=True), use_container_width=True, hide_index=True)