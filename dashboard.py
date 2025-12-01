import streamlit as st
import duckdb
import time
import pandas as pd
import plotly.express as px
import uuid
import os

# Page Configuration
st.set_page_config(
    page_title="ShopPulse Real-Time Dashboard",
    page_icon="🛒",
    layout="wide"
)

# Title
st.title("🛒 ShopPulse: Real-Time E-Commerce Analytics")
st.markdown("### ⚡ Live Stream from Apache Kafka & Spark")

# Placeholder for Auto-Refresh
placeholder = st.empty()

def fetch_data():
    """Fetch the latest data from the Parquet Data Lake using DuckDB"""
    con = duckdb.connect(database=':memory:')

    # Path to your Silver Layer (Parquet)
    parquet_files = "spark_data/silver_layer/*.parquet"

    # Check if data exists
    try:
        # 1. Total Counts by Event Type
        df_events = con.execute(f"""
            SELECT event_type, COUNT(*) as count 
            FROM '{parquet_files}' 
            GROUP BY event_type
        """).fetchdf()

        # 2. Top 5 Active Users
        df_users = con.execute(f"""
            SELECT user_id, COUNT(*) as actions 
            FROM '{parquet_files}' 
            GROUP BY user_id 
            ORDER BY actions DESC 
            LIMIT 5
        """).fetchdf()

        # 3. Location Traffic
        df_locations = con.execute(f"""
            SELECT location, COUNT(*) as traffic 
            FROM '{parquet_files}' 
            GROUP BY location 
            ORDER BY traffic DESC 
            LIMIT 5
        """).fetchdf()

        return df_events, df_users, df_locations

    except Exception as e:
        # Return empty DataFrames if no data found yet (prevents crash)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# Dashboard Loop (Updates every 2 seconds)
while True:
    df_events, df_users, df_locations = fetch_data()

    with placeholder.container():
        if df_events.empty:
            st.warning("⏳ Waiting for data from Spark... (Ensure Docker is running)")
        else:
            # --- KPI METRICS ---
            total_views = df_events[df_events['event_type'] == 'view']['count'].sum()
            total_carts = df_events[df_events['event_type'] == 'add_to_cart']['count'].sum()
            total_purchases = df_events[df_events['event_type'] == 'purchase']['count'].sum()

            col1, col2, col3 = st.columns(3)
            col1.metric("👀 Total Views", total_views, delta=None)
            # Handle division by zero if views are 0
            conversion_rate = (total_carts / total_views) if total_views > 0 else 0
            col2.metric("🛒 Add to Carts", total_carts, delta=f"{conversion_rate:.1%}")
            col3.metric("💰 Purchases", total_purchases, delta="Revenue Growing")

            st.markdown("---")

            # --- CHARTS ROW 1 ---
            col_c1, col_c2 = st.columns(2)

            with col_c1:
                st.subheader("📊 Event Distribution")
                fig_events = px.bar(df_events, x="event_type", y="count",
                                    color="event_type", template="plotly_dark",
                                    title="Live Event Counts")
                # FIX 1: Added unique key
                st.plotly_chart(fig_events, use_container_width=True, key=f"events_{uuid.uuid4()}")

            with col_c2:
                st.subheader("🏆 Top Active Users")
                fig_users = px.bar(df_users, x="user_id", y="actions",
                                   color="actions", template="plotly_dark",
                                   title="Most Engaged Users")
                # FIX 2: Added unique key
                st.plotly_chart(fig_users, use_container_width=True, key=f"users_{uuid.uuid4()}")

            # --- CHARTS ROW 2 ---
            st.subheader("🌍 Top Locations")
            fig_loc = px.pie(df_locations, names="location", values="traffic",
                             title="Traffic by Region", hole=0.4, template="plotly_dark")
            # FIX 3: Added unique key
            st.plotly_chart(fig_loc, use_container_width=True, key=f"loc_{uuid.uuid4()}")

    # Wait before refreshing
    time.sleep(2)