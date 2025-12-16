import streamlit as st
import polars as pl
import plotly.graph_objects as go
import time
from pathlib import Path

# Config
SNAPSHOT_PATH = Path("output/candles_live_snapshot.parquet")
st.set_page_config(page_title="HFT Aggregator Dashboard", layout="wide")

st.title("⚡ HFT Pipeline Real-Time Dashboard")

# Refresh Logic
if st.button("Refresh"):
    st.rerun()

# 1. Read Data
if not SNAPSHOT_PATH.exists():
    st.warning("Waiting for Aggregator Snapshot...")
    time.sleep(1)
    st.rerun()

try:
    df = pl.read_parquet(SNAPSHOT_PATH)
except:
    st.warning("Reading Snapshot...")
    time.sleep(0.5)
    st.run()

if df.is_empty():
    st.write("No data yet.")
    st.stop()

# 2. Filters (Sidebar)
st.sidebar.header("Filters")
instruments = df["instrument"].unique().to_list()
windows = df["window_size"].unique().to_list()

selected_inst = st.sidebar.selectbox("Instrument", sorted(instruments))
selected_window = st.sidebar.selectbox("Window Size", sorted(windows))

# 3. Filter Data
subset = df.filter(
    (pl.col("instrument") == selected_inst) & 
    (pl.col("window_size") == selected_window)
).sort("window_start")

if subset.height == 0:
    st.write(f"No data for {selected_inst} ({selected_window})")
    st.stop()

# 4. Metrics
latest = subset.row(-1, named=True)
col1, col2, col3, col4 = st.columns(4)
col1.metric("Close Price", f"{latest['close']:.2f}")
col2.metric("Volume", f"{latest['volume']:.0f}")
col3.metric("Trade Count", f"{latest['trade_count']}")
col4.metric("Window End", f"🔴 {latest['window_end'].split('T')[-1][:12]}")

# 5. Chart
fig = go.Figure(data=[go.Candlestick(
    x=subset["window_start"],
    open=subset["open"],
    high=subset["high"],
    low=subset["low"],
    close=subset["close"]
)])

fig.update_layout(
    title=f"{selected_inst} - {selected_window} Candles (Last {len(subset)} bars)",
    height=600,
    xaxis_rangeslider_visible=False
)
st.plotly_chart(fig, use_container_width=True)

# Auto-refresh loop
time.sleep(0.2)
st.rerun()
