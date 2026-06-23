import json
import os
import time
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

# ==========================================
# CONFIGURATION & STREAM CONNECTIONS
# ==========================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

st.set_page_config(page_title="Live F1 Telemetry", layout="wide")
st.title("🏎️ Real-Time F1 Telemetry Dashboard")

# Use Streamlit session state to cache data points across screen re-renders
if "telemetry_buffer" not in st.session_state:
    st.session_state.telemetry_buffer = []

# Initialize the Kafka consumer once and cache it globally
@st.cache_resource
def get_live_kafka_consumer():
    try:
        return KafkaConsumer(
            "live-car-telemetry", # Listens to your exact telemetry stream topic
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest', # Jump straight to the live racing ticks
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=100     # Ultra-fast timeout to keep the UI from locking up
        )
    except Exception as e:
        st.error(f"Could not connect to Kafka Broker: {e}")
        return None

consumer = get_live_kafka_consumer()

# ==========================================
# REAL-TIME METRICS SWEEP
# ==========================================
if consumer:
    # Rapidly fetch any messages that arrived in the last 100 milliseconds
    message_pack = consumer.poll(timeout_ms=100)
    
    for topic_partition, records in message_pack.items():
        for record in records:
            st.session_state.telemetry_buffer.append(record.value)

    # Keep only the last 150 records in browser memory to avoid lagging
    st.session_state.telemetry_buffer = st.session_state.telemetry_buffer[-150:]

# ==========================================
# VISUALISATION LAYERS
# ==========================================
if st.session_state.telemetry_buffer:
    # Parse our real-time memory buffer into a clean pandas DataFrame
    df = pd.DataFrame(st.session_state.telemetry_buffer)
    latest_tick = st.session_state.telemetry_buffer[-1]

    # 1. Live KPI Indicators
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Current Speed", f"{latest_tick.get('speed', 0)} km/h")
    col2.metric("Engine RPM", f"{latest_tick.get('rpm', 0)}")
    col3.metric("Active Gear", f"G{latest_tick.get('gear', 0)}")
    
    # Render DRS flag dynamically
    drs_state = "OPEN (DRS Active)" if latest_tick.get('drs') in [1, 8, 10, 12, 14] else "CLOSED"
    col4.metric("DRS Flap", drs_state)

    # 2. Live Graph Interactivity
    st.subheader("🏁 Live Vehicle Performance Profiles")
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        if 'speed' in df.columns:
            st.caption("Live Velocity Timeline")
            st.line_chart(data=df, y="speed")
            
    with chart_col2:
        if 'throttle' in df.columns and 'brake' in df.columns:
            st.caption("Driver Pedal Inputs (Throttle vs Brake)")
            # Create a combined dataframe for pedal tracking
            pedal_df = df[['throttle', 'brake']]
            st.line_chart(pedal_df)

    # 3. Interactive Data Logs
    st.subheader("📋 Recent Message Ticks (Kafka Log)")
    st.dataframe(df.tail(10), use_container_width=True)

else:
    # Display message if the script is alive but your Kafka broker is empty
    st.warning("🔄 Listening... Stream connection is open, but no live telemetry has arrived yet.")
    st.info("💡 Start your live producer container (`f1_live_producer`) to feed racing data into this page.")

# ==========================================
# AUTO-UPDATE MECHANISM
# ==========================================
# Pause for a split second, then automatically trigger a full screen re-run
time.sleep(0.3)
st.rerun()
