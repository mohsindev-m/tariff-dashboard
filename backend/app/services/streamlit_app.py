import os
import json
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime
from streamlit_folium import st_folium
import folium

# Import the production-ready pipeline; ensure that your module is configured correctly.
from app.services.tariff_pipeline import TariffDataPipeline

# ---------------------------
# Streamlit Page Configuration
# ---------------------------
st.set_page_config(page_title="Global Tariff Dashboard", layout="wide")
st.title("Global Tariff Dashboard")

# ---------------------------
# Sidebar Controls
# ---------------------------
st.sidebar.header("Dashboard Controls")
# Button to trigger a full pipeline update (data ingestion from all sources)
if st.sidebar.button("Update Dashboard Data"):
    st.info("Updating dashboard data. Please wait while the latest data is being ingested...")
    pipeline = TariffDataPipeline()
    pipeline.run_full_pipeline()  # Triggers the complete pipeline that ingests data from News, White House, Census, BEA, WTO
    st.success("Dashboard data updated successfully!")

# ---------------------------
# Data Loading (with daily caching)
# ---------------------------
@st.cache_data(ttl=86400, show_spinner=True)
def load_dashboard_data():
    pipeline = TariffDataPipeline()
    # Retrieve the unified dashboard data (aggregated from multiple sources)
    data = pipeline.get_dashboard_api_data()
    return data

dashboard_data = load_dashboard_data()

# ---------------------------
# Sidebar Filters for Countries and Industries
# ---------------------------
country_options = ["All"]
if "heatmap_data" in dashboard_data and dashboard_data["heatmap_data"]:
    df_heat = pd.DataFrame(dashboard_data["heatmap_data"])
    if "country_name" in df_heat.columns:
        # Remove None values before sorting
        valid_countries = [c for c in df_heat["country_name"].unique().tolist() if c is not None]
        country_options += sorted(valid_countries)

industry_options = ["All"]
if "detail_table" in dashboard_data and "industries" in dashboard_data["detail_table"]:
    df_industries = pd.DataFrame(dashboard_data["detail_table"]["industries"])
    if "industry_name" in df_industries.columns:
        # Remove None values before sorting
        valid_industries = [i for i in df_industries["industry_name"].unique().tolist() if i is not None]
        industry_options += sorted(valid_industries)

selected_country = st.sidebar.selectbox("Filter by Country", options=country_options)
selected_industry = st.sidebar.selectbox("Filter by Industry", options=industry_options)

# ---------------------------
# Global Heatmap (Trade Deficit)
# ---------------------------
st.subheader("Global Trade Deficit Heatmap")
if "heatmap_data" in dashboard_data and dashboard_data["heatmap_data"]:
    df_heat = pd.DataFrame(dashboard_data["heatmap_data"])
    if selected_country != "All":
        df_heat = df_heat[df_heat["country_name"] == selected_country]
    
    # Use 'country_code' field, which is assumed to follow ISO 3-letter standards.
    fig_heat = px.choropleth(
        df_heat,
        locations="country_code",
        color="trade_deficit",
        hover_name="country_name",
        color_continuous_scale=px.colors.sequential.OrRd,
        projection="natural earth",
        title="Trade Deficit Heatmap"
    )
    st.plotly_chart(fig_heat, use_container_width=True)
else:
    st.warning("No heatmap data available.")

# ---------------------------
# Sector-Specific Pie Chart
# ---------------------------
st.subheader("Sector Impact Distribution")
if "sector_data" in dashboard_data and dashboard_data["sector_data"]:
    # Convert to DataFrame and use standardized lowercase column names
    df_sector = pd.DataFrame(dashboard_data["sector_data"])
    # In production, the pipeline produces keys like 'sector', 'trade_volume', etc.
    if selected_industry != "All":
        df_sector = df_sector[df_sector["sector"] == selected_industry]
    
    # Use the 'sector' as names and 'trade_volume' for values.
    fig_sector = px.pie(
        df_sector,
        names="sector",
        values="trade_volume",
        title="Distribution of Annual Trade Volume by Sector"
    )
    st.plotly_chart(fig_sector, use_container_width=True)
else:
    st.warning("No sector data available.")

# ---------------------------
# Historical Trade Trends (Line Chart)
# ---------------------------
st.subheader("Historical Trade Trends")
if "time_series" in dashboard_data and dashboard_data["time_series"]:
    df_time = pd.DataFrame(dashboard_data["time_series"])
    try:
        df_time["year"] = df_time["YEAR"].astype(int)
    except Exception:
        df_time["year"] = df_time["YEAR"]
    
    fig_time = px.line(
        df_time,
        x="year",
        y=["DEFICIT_BILLIONS", "EXPORTS_BILLIONS", "IMPORTS_BILLIONS"],
        markers=True,
        title="Historical Trade Deficit, Exports, and Imports (Billions)"
    )
    st.plotly_chart(fig_time, use_container_width=True)
else:
    st.warning("No time series data available.")

# ---------------------------
# Detailed Metrics Table (Countries & Industries)
# ---------------------------
st.subheader("Detailed Metrics")
if "detail_table" in dashboard_data:
    detail_table = dashboard_data["detail_table"]
    
    if "countries" in detail_table and detail_table["countries"]:
        st.markdown("#### Country Metrics")
        df_countries = pd.DataFrame(detail_table["countries"])
        if selected_country != "All":
            df_countries = df_countries[df_countries["country_name"] == selected_country]
        st.dataframe(df_countries, use_container_width=True)
    
    if "industries" in detail_table and detail_table["industries"]:
        st.markdown("#### Industry Metrics")
        df_industries = pd.DataFrame(detail_table["industries"])
        if selected_industry != "All":
            df_industries = df_industries[df_industries["industry_name"] == selected_industry]
        st.dataframe(df_industries, use_container_width=True)
else:
    st.warning("No detailed table data available.")

# ---------------------------
# Dashboard Metadata Display
# ---------------------------
st.subheader("Dashboard Metadata")
if "metadata" in dashboard_data:
    st.json(dashboard_data["metadata"])
else:
    st.info("No metadata available.")
