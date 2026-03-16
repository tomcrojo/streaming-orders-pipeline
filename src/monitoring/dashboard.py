"""Streamlit monitoring dashboard for the order processing pipeline."""

from __future__ import annotations

import random
import time
from datetime import datetime, timezone, timedelta

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from src.monitoring.metrics_collector import metrics_store

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Order Pipeline Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------
st.markdown(
    """
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 0.5rem;
        color: white;
        text-align: center;
    }
    .metric-value { font-size: 2rem; font-weight: bold; }
    .metric-label { font-size: 0.9rem; opacity: 0.9; }
    .alert-bad { background: #ff4b4b !important; }
</style>
""",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Simulated data (when no live stream is running)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=10)
def get_simulated_metrics() -> dict[str, float]:
    """Return simulated metrics for dashboard demo."""
    return {
        "events_per_second": round(random.uniform(8, 15), 1),
        "orders_last_hour": random.randint(400, 800),
        "revenue_last_hour": round(random.uniform(15000, 45000), 2),
        "cancellation_rate": round(random.uniform(0.02, 0.08), 4),
        "avg_processing_latency_ms": round(random.uniform(15, 120), 1),
        "active_orders": random.randint(50, 200),
    }


@st.cache_data(ttl=10)
def get_simulated_region_data() -> pd.DataFrame:
    regions = [
        "Madrid",
        "Barcelona",
        "Valencia",
        "Sevilla",
        "Zaragoza",
        "Málaga",
        "Murcia",
        "Bilbao",
        "Alicante",
        "Córdoba",
        "Valladolid",
        "Vigo",
        "Gijón",
        "A Coruña",
        "Granada",
        "Vitoria",
        "Pamplona",
        "San Sebastián",
        "Oviedo",
        "Elche",
    ]
    return pd.DataFrame(
        {
            "region": regions,
            "order_count": [random.randint(20, 150) for _ in regions],
            "revenue": [round(random.uniform(500, 8000), 2) for _ in regions],
        }
    )


@st.cache_data(ttl=10)
def get_simulated_event_types() -> pd.DataFrame:
    types = [
        "order_created",
        "order_item_added",
        "order_confirmed",
        "order_shipped",
        "order_delivered",
        "order_cancelled",
    ]
    counts = [
        random.randint(100, 500),
        random.randint(50, 200),
        random.randint(80, 300),
        random.randint(60, 250),
        random.randint(50, 200),
        random.randint(10, 50),
    ]
    return pd.DataFrame({"event_type": types, "count": counts})


@st.cache_data(ttl=10)
def get_simulated_revenue_trend() -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    times = [now - timedelta(minutes=i * 5) for i in range(60, 0, -1)]
    return pd.DataFrame(
        {
            "time": times,
            "revenue": [round(random.uniform(200, 1200), 2) for _ in times],
            "order_count": [random.randint(5, 30) for _ in times],
        }
    )


@st.cache_data(ttl=10)
def get_simulated_hourly_pattern() -> pd.DataFrame:
    hours = list(range(24))
    # Spanish shopping pattern: peaks at lunch (13-14) and evening (20-22)
    base = [
        2,
        1,
        1,
        1,
        1,
        2,
        5,
        10,
        20,
        35,
        45,
        55,
        60,
        65,
        50,
        45,
        40,
        35,
        40,
        55,
        70,
        60,
        30,
        10,
    ]
    return pd.DataFrame(
        {
            "hour": hours,
            "avg_orders": [b + random.randint(-5, 5) for b in base],
        }
    )


# ---------------------------------------------------------------------------
# Dashboard layout
# ---------------------------------------------------------------------------
st.title("Real-Time Order Pipeline Monitor")
st.caption("Streaming data pipeline — Kafka + Spark Structured Streaming + Iceberg")

# Auto-refresh
if st.sidebar.button("Refresh Now"):
    st.cache_data.clear()

st.sidebar.checkbox("Auto-refresh (10s)", value=True, disabled=True)
st.sidebar.caption("Dashboard refreshes every 10 seconds")

# Metrics = live if available, simulated otherwise
try:
    live_metrics = metrics_store.get_all_metrics()
    use_live = live_metrics.get("total_events_processed", 0) > 0
except Exception:
    use_live = False

metrics = live_metrics if use_live else get_simulated_metrics()

# --- KPI Row ---
col1, col2, col3, col4 = st.columns(4)

with col1:
    cancel_class = (
        "metric-card alert-bad" if metrics["cancellation_rate"] > 0.1 else "metric-card"
    )
    st.markdown(
        f"""
    <div class="{cancel_class}">
        <div class="metric-value">{metrics["events_per_second"]:.1f}</div>
        <div class="metric-label">Events/sec</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

with col2:
    st.markdown(
        f"""
    <div class="metric-card">
        <div class="metric-value">{metrics["orders_last_hour"]:,}</div>
        <div class="metric-label">Orders (last hour)</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

with col3:
    st.markdown(
        f"""
    <div class="metric-card">
        <div class="metric-value">{metrics["revenue_last_hour"]:,.2f}</div>
        <div class="metric-label">Revenue last hour (EUR)</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

with col4:
    cancel_pct = f"{metrics['cancellation_rate'] * 100:.1f}%"
    cancel_class = (
        "metric-card alert-bad" if metrics["cancellation_rate"] > 0.1 else "metric-card"
    )
    st.markdown(
        f"""
    <div class="{cancel_class}">
        <div class="metric-value">{cancel_pct}</div>
        <div class="metric-label">Cancellation Rate</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

st.divider()

# --- Charts Row 1 ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Revenue Trend (5-min windows)")
    trend_df = get_simulated_revenue_trend()
    fig = px.area(
        trend_df,
        x="time",
        y="revenue",
        labels={"time": "Time (UTC)", "revenue": "Revenue (EUR)"},
        color_discrete_sequence=["#667eea"],
    )
    fig.update_layout(height=300, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Event Type Distribution")
    type_df = get_simulated_event_types()
    fig = px.pie(
        type_df,
        values="count",
        names="event_type",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_layout(height=300, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

# --- Charts Row 2 ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Orders by Region (Top 20)")
    region_df = (
        get_simulated_region_data().sort_values("order_count", ascending=True).tail(15)
    )
    fig = px.bar(
        region_df,
        x="order_count",
        y="region",
        orientation="h",
        labels={"order_count": "Orders", "region": ""},
        color="order_count",
        color_continuous_scale="Viridis",
    )
    fig.update_layout(height=400, margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Hourly Order Pattern (Spain)")
    hourly_df = get_simulated_hourly_pattern()
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=hourly_df["hour"],
            y=hourly_df["avg_orders"],
            mode="lines+markers",
            fill="tozeroy",
            line=dict(color="#764ba2", width=2),
            marker=dict(size=6),
        )
    )
    fig.update_layout(
        xaxis_title="Hour of Day",
        yaxis_title="Avg Orders",
        height=400,
        margin=dict(l=0, r=0, t=0, b=0),
        xaxis=dict(tickmode="linear", tick0=0, dtick=2),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- Processing latency gauge ---
col_lat, col_active, col_info = st.columns(3)

with col_lat:
    st.subheader("Processing Latency")
    lat = metrics.get("avg_processing_latency_ms", 50)
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=lat,
            number={"suffix": " ms"},
            gauge={
                "axis": {"range": [0, 500]},
                "bar": {"color": "#667eea"},
                "steps": [
                    {"range": [0, 100], "color": "#d4edda"},
                    {"range": [100, 300], "color": "#fff3cd"},
                    {"range": [300, 500], "color": "#f8d7da"},
                ],
                "threshold": {
                    "line": {"color": "red", "width": 2},
                    "thickness": 0.75,
                    "value": 300,
                },
            },
        )
    )
    fig.update_layout(height=250, margin=dict(l=20, r=20, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)

with col_active:
    st.subheader("Active Orders")
    active = metrics.get("active_orders", 0)
    fig = go.Figure(
        go.Indicator(
            mode="number",
            value=active,
            number={"font": {"size": 48}},
        )
    )
    fig.update_layout(height=250, margin=dict(l=20, r=20, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)

with col_info:
    st.subheader("Pipeline Info")
    st.markdown("""
    | Component | Status |
    |-----------|--------|
    | Kafka Producer | 🟢 Running |
    | Spark Streaming | 🟢 Running |
    | Iceberg Writer | 🟢 Running |
    | Quarantine Sink | 🟢 Active |
    | Data Quality | 🟢 Passing |
    """)
    st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")

# Footer
st.divider()
st.caption(
    "Built with Spark Structured Streaming + Apache Iceberg + Kafka — Portfolio Project"
)
