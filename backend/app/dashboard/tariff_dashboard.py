#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Complete Integrated Tariff Dashboard

This dashboard combines data from multiple sources:
1. WTO Timeseries API (tariff rates, trade data)
2. WTO Quantitative Restrictions API (trade restrictions)
3. Census Bureau API (trade balance data)
4. White House Website (policy announcements)
5. NewsAPI (global tariff news articles)

For a complete picture of the global tariff landscape.
"""

import os
import json
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pycountry
import folium
from streamlit_folium import folium_static
from folium.plugins import MarkerCluster

# Set page configuration
st.set_page_config(
    page_title="Global Tariff and Trade Dashboard",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Application title and description
st.title("Global Tariff and Trade Dashboard")
st.markdown("""
This dashboard visualizes comprehensive data on global tariffs, trade deficits, and trade restrictions,
integrating data from multiple sources:
- World Trade Organization (WTO) tariff and trade restriction data
- U.S. Census Bureau trade statistics
- White House policy announcements
- Global news articles on tariffs and trade
""")

# Sidebar configuration
st.sidebar.title("Data Selection")

# Function to display data sources information
def show_data_sources_info():
    st.sidebar.markdown("### Data Sources Information")
    st.sidebar.markdown("""
    **WTO Timeseries API**:  
    Provides tariff rates and historical trade data.
    
    **WTO QR API**:  
    Provides quantitative restrictions (trade barriers).
    
    **Census Bureau API**:  
    Provides trade balance data and import/export statistics.
    
    **White House Website**:  
    Provides official US tariff policy announcements.
    
    **NewsAPI**:  
    Provides global news coverage on tariffs and trade.
    """)

# Add information about data sources to sidebar
show_data_sources_info()

# Function to convert ISO2 to country name
def iso2_to_country_name(iso2_code):
    try:
        return pycountry.countries.get(alpha_2=iso2_code).name
    except:
        return iso2_code

# Function to convert country name to ISO3
def country_to_iso3(country_name):
    try:
        country = pycountry.countries.search_fuzzy(country_name)[0]
        return country.alpha_3
    except:
        # Fallback mappings for common mismatches
        mappings = {
            "United States": "USA",
            "Vietnam": "VNM",
            "Russia": "RUS",
            "South Korea": "KOR",
            "Taiwan": "TWN",
            "China": "CHN",
            "Mexico": "MEX",
            "Canada": "CAN",
            "Japan": "JPN",
            "Germany": "DEU",
            "United Kingdom": "GBR",
            "South Korea": "KOR",
            "Vietnam": "VNM",
            "Taiwan, China": "TWN",
            "India": "IND",
            "European Union": "EU"
        }
        return mappings.get(country_name, "Unknown")

# Function to load the latest data files
@st.cache_data
def load_data():
    data_dir = "data"
    
    # Define paths for different data files
    tariff_data_path = os.path.join(data_dir, "tariff_data_latest.json")
    qr_data_path = os.path.join(data_dir, "qr_data_latest.json")
    census_data_path = os.path.join(data_dir, "census_data_latest.json")
    whitehouse_data_path = os.path.join(data_dir, "whitehouse_data_latest.json")
    news_data_path = os.path.join(data_dir, "news_data_latest.json")
    bea_data_path = os.path.join(data_dir, "bea_data_latest.json")
    
    data = {
        "tariff_data": None,
        "qr_data": None,
        "census_data": None,
        "whitehouse_data": None,
        "news_data": None,
        "bea_gdp_data": None,
        "bea_personal_income_data": None,
        "bea_state_gdp_data": None,
        "tariff_timestamp": None,
        "qr_timestamp": None,
        "census_timestamp": None,
        "whitehouse_timestamp": None,
        "news_timestamp": None,
        "bea_timestamp": None
    }
    
    # Load WTO tariff data if available
    if os.path.exists(tariff_data_path):
        try:
            with open(tariff_data_path, 'r') as f:
                tariff_json = json.load(f)
                if isinstance(tariff_json, dict) and "data" in tariff_json:
                    data["tariff_data"] = pd.DataFrame(tariff_json["data"])
                    data["tariff_timestamp"] = tariff_json.get("timestamp")
        except Exception as e:
            st.error(f"Error loading tariff data: {e}")

    # Load BEA economic data
    if os.path.exists(bea_data_path):
        try:
            with open(bea_data_path, 'r') as f:
                bea_json = json.load(f)
                if bea_json:
                    # Load GDP data
                    if "gdp_data" in bea_json and bea_json["gdp_data"]:
                        data["bea_gdp_data"] = pd.DataFrame(bea_json["gdp_data"])
                    
                    # Load personal income data
                    if "personal_income_data" in bea_json and bea_json["personal_income_data"]:
                        data["bea_personal_income_data"] = pd.DataFrame(bea_json["personal_income_data"])
                    
                    # Load state GDP data
                    if "state_gdp_data" in bea_json and bea_json["state_gdp_data"]:
                        data["bea_state_gdp_data"] = pd.DataFrame(bea_json["state_gdp_data"])
                    
                    # Store timestamp
                    data["bea_timestamp"] = bea_json.get("timestamp")
        except Exception as e:
            st.error(f"Error loading BEA data: {e}")
    
    # Load WTO QR data if available
    if os.path.exists(qr_data_path):
        try:
            with open(qr_data_path, 'r') as f:
                qr_json = json.load(f)
                if isinstance(qr_json, dict) and "data" in qr_json:
                    data["qr_data"] = pd.DataFrame(qr_json["data"])
                    data["qr_timestamp"] = qr_json.get("timestamp")
        except Exception as e:
            st.error(f"Error loading QR data: {e}")
    
    # Load Census data if available
    if os.path.exists(census_data_path):
        try:
            with open(census_data_path, 'r') as f:
                census_json = json.load(f)
                if isinstance(census_json, dict):
                    # Convert each dataset to DataFrame
                    if "trade_balance" in census_json and census_json["trade_balance"]:
                        data["census_trade_balance"] = pd.DataFrame(census_json["trade_balance"])
                    
                    if "monthly_trade" in census_json and census_json["monthly_trade"]:
                        data["census_monthly_trade"] = pd.DataFrame(census_json["monthly_trade"])
                    
                    if "state_data" in census_json and census_json["state_data"]:
                        data["census_state_data"] = pd.DataFrame(census_json["state_data"])
                    
                    data["census_timestamp"] = census_json.get("timestamp")
        except Exception as e:
            st.error(f"Error loading Census data: {e}")
    
    # Load White House data if available
    if os.path.exists(whitehouse_data_path):
        try:
            with open(whitehouse_data_path, 'r') as f:
                whitehouse_json = json.load(f)
                if isinstance(whitehouse_json, dict) and "data" in whitehouse_json:
                    data["whitehouse_data"] = pd.DataFrame(whitehouse_json["data"])
                    data["whitehouse_timestamp"] = whitehouse_json.get("timestamp")
        except Exception as e:
            st.error(f"Error loading White House data: {e}")
    
    # Load News data if available
    if os.path.exists(news_data_path):
        try:
            with open(news_data_path, 'r') as f:
                news_json = json.load(f)
                if isinstance(news_json, dict) and "data" in news_json:
                    data["news_data"] = pd.DataFrame(news_json["data"])
                    data["news_timestamp"] = news_json.get("timestamp")
        except Exception as e:
            st.error(f"Error loading News data: {e}")
    
    return data

# Load data
data = load_data()

# Display data timestamps and status
st.sidebar.markdown("### Data Status")

if data["tariff_data"] is not None and not data["tariff_data"].empty:
    st.sidebar.success(f"✅ WTO Tariff Data: Available")
    st.sidebar.caption(f"Last updated: {data['tariff_timestamp']}")
else:
    st.sidebar.error("❌ WTO Tariff Data: Missing")
    st.sidebar.caption("Run the WTO Timeseries API scraper")

if data["qr_data"] is not None and not data["qr_data"].empty:
    st.sidebar.success(f"✅ WTO QR Data: Available")
    st.sidebar.caption(f"Last updated: {data['qr_timestamp']}")
else:
    st.sidebar.error("❌ WTO QR Data: Missing")
    st.sidebar.caption("Run the WTO QR API scraper")

if "census_trade_balance" in data and not data["census_trade_balance"].empty:
    st.sidebar.success(f"✅ Census Bureau Data: Available")
    st.sidebar.caption(f"Last updated: {data['census_timestamp']}")
else:
    st.sidebar.error("❌ Census Bureau Data: Missing")
    st.sidebar.caption("Run the Census API scraper")

if "whitehouse_data" in data and not data["whitehouse_data"].empty:
    st.sidebar.success(f"✅ White House Data: Available")
    st.sidebar.caption(f"Last updated: {data['whitehouse_timestamp']}")
else:
    st.sidebar.error("❌ White House Data: Missing")
    st.sidebar.caption("Run the White House scraper")

if "news_data" in data and not data["news_data"].empty:
    st.sidebar.success(f"✅ News Data: Available")
    st.sidebar.caption(f"Last updated: {data['news_timestamp']}")
else:
    st.sidebar.error("❌ News Data: Missing")
    st.sidebar.caption("Run the NewsAPI scraper")

# Main dashboard tabs
tab1, tab2, tab3, tab4, tab5, tab6,tab7 = st.tabs([
    "Global Tariff Overview", 
    "Trade Balance Analysis", 
    "Quantitative Restrictions", 
    "Policy Updates",
    "News Analysis",
    "Country Profiles",
     "U.S. Economic Data"
])

# Tab 1: Global Tariff Overview
with tab1:
    st.header("Global Tariff Overview")
    
    if data["tariff_data"] is None or data["tariff_data"].empty:
        st.warning("""
        No tariff data available. To get tariff data:
        
        1. Run the **WTO Timeseries API scraper** to collect global tariff rates:
           ```
           python wto_api_integration.py
           ```
           
        2. This scraper will fetch tariff data from the World Trade Organization's API and save it to the data directory.
        
        3. After running the scraper, refresh this dashboard to see the tariff visualizations.
        """)
    else:
        # Display metrics in a row
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Check if we have tariff_value (from our actual data) or tariff_rate (original dashboard expected)
            if "tariff_value" in data["tariff_data"].columns:
                avg_tariff = data["tariff_data"]["tariff_value"].mean()
                st.metric("Average Global Tariff Lines", f"{avg_tariff:.0f}")
            elif "tariff_rate" in data["tariff_data"].columns:
                avg_tariff = data["tariff_data"]["tariff_rate"].mean()
                st.metric("Average Global Tariff Rate", f"{avg_tariff:.2f}%")
            else:
                st.metric("Average Global Tariff Value", "N/A")
        
        with col2:
            # Count countries with tariff data
            if "country_name" in data["tariff_data"].columns:
                country_count = data["tariff_data"]["country_name"].nunique()
                st.metric("Countries with Tariff Data", f"{country_count}")
            else:
                st.metric("Countries with Tariff Data", "N/A")
        
        with col3:
            # Highest tariff value/rate
            if "tariff_value" in data["tariff_data"].columns and "country_name" in data["tariff_data"].columns:
                # Find country with highest average tariff value
                country_avg = data["tariff_data"].groupby("country_name")["tariff_value"].mean()
                highest_country = country_avg.idxmax()
                highest_value = country_avg.max()
                st.metric("Highest Tariff Lines", f"{highest_value:.0f} ({highest_country})")
            elif "tariff_rate" in data["tariff_data"].columns and "country_name" in data["tariff_data"].columns:
                highest_idx = data["tariff_data"]["tariff_rate"].idxmax()
                highest_country = data["tariff_data"].loc[highest_idx, "country_name"]
                highest_rate = data["tariff_data"].loc[highest_idx, "tariff_rate"]
                st.metric("Highest Tariff Rate", f"{highest_rate:.2f}% ({highest_country})")
            else:
                st.metric("Highest Tariff Value", "N/A")
    
        # Create world map of tariff values
        st.subheader("Global Tariff Map")
        
        # Prepare data for map based on what columns we have
        if "country_name" in data["tariff_data"].columns and "tariff_value" in data["tariff_data"].columns:
            # Group by country to get average value
            map_data = data["tariff_data"].groupby("country_name")["tariff_value"].mean().reset_index()
            map_data = map_data.dropna(subset=["tariff_value"])
            
            # Add ISO3 codes for choropleth map
            map_data["iso3"] = map_data["country_name"].apply(country_to_iso3)
            
            # Create choropleth map
            fig = px.choropleth(
                map_data,
                locations="iso3",
                color="tariff_value",
                hover_name="country_name",
                color_continuous_scale="Viridis",
                range_color=[0, max(map_data["tariff_value"]) * 1.1],
                title="Number of Applied Tariff Lines by Country",
                labels={"tariff_value": "Tariff Lines", "iso3": "Country"}
            )
            
            # Update layout
            fig.update_layout(
                geo=dict(
                    showframe=False,
                    showcoastlines=True,
                    projection_type='equirectangular'
                ),
                height=600,
                margin={"r":0,"t":30,"l":0,"b":0}
            )
            
            st.plotly_chart(fig, use_container_width=True)
        elif "country_name" in data["tariff_data"].columns and "tariff_rate" in data["tariff_data"].columns:
            # Original code path for tariff_rate
            map_data = data["tariff_data"][["country_name", "tariff_rate"]].drop_duplicates("country_name")
            map_data = map_data.dropna(subset=["tariff_rate"])
            
            # Add ISO3 codes for choropleth map
            map_data["iso3"] = map_data["country_name"].apply(country_to_iso3)
            
            # Create choropleth map
            fig = px.choropleth(
                map_data,
                locations="iso3",
                color="tariff_rate",
                hover_name="country_name",
                color_continuous_scale="Viridis",
                range_color=[0, max(map_data["tariff_rate"]) * 1.1],
                title="Global Tariff Rates by Country",
                labels={"tariff_rate": "Tariff Rate (%)", "iso3": "Country"}
            )
            
            # Update layout
            fig.update_layout(
                geo=dict(
                    showframe=False,
                    showcoastlines=True,
                    projection_type='equirectangular'
                ),
                height=600,
                margin={"r":0,"t":30,"l":0,"b":0}
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Insufficient data for global tariff map. Please check your tariff data.")
        
        # Bar chart of tariff values/rates by country
        st.subheader("Tariff Data by Country")
        
        if "country_name" in data["tariff_data"].columns and "tariff_value" in data["tariff_data"].columns:
            # Group by country and get average tariff value
            country_tariffs = data["tariff_data"].groupby("country_name")["tariff_value"].mean().reset_index()
            country_tariffs = country_tariffs.sort_values("tariff_value", ascending=False)
            
            # Limit to top 20 countries for better visualization
            top_countries = country_tariffs.head(20)
            
            # Create bar chart
            fig = px.bar(
                top_countries, 
                x="country_name", 
                y="tariff_value",
                title="Top 20 Countries by Number of Applied Tariff Lines",
                labels={"country_name": "Country", "tariff_value": "Number of Tariff Lines"},
                color="tariff_value",
                color_continuous_scale="Viridis"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Display data table with search and sort functionality
            st.subheader("All Countries Tariff Data")
            st.dataframe(country_tariffs, use_container_width=True)
        elif "country_name" in data["tariff_data"].columns and "tariff_rate" in data["tariff_data"].columns:
            # Original code path
            # Group by country and get average tariff rate
            country_tariffs = data["tariff_data"].groupby("country_name")["tariff_rate"].mean().reset_index()
            country_tariffs = country_tariffs.sort_values("tariff_rate", ascending=False)
            
            # Limit to top 20 countries for better visualization
            top_countries = country_tariffs.head(20)
            
            # Create bar chart
            fig = px.bar(
                top_countries, 
                x="country_name", 
                y="tariff_rate",
                title="Top 20 Countries by Tariff Rate",
                labels={"country_name": "Country", "tariff_rate": "Average Tariff Rate (%)"},
                color="tariff_rate",
                color_continuous_scale="Viridis"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Display data table with search and sort functionality
            st.subheader("All Countries Tariff Data")
            st.dataframe(country_tariffs, use_container_width=True)

# Tab 2: Trade Balance Analysis
with tab2:
    st.header("Trade Balance Analysis")
    
    # Check if Census trade balance data is available
    if "census_trade_balance" not in data or data["census_trade_balance"].empty:
        st.warning("""
        No trade balance data available. To get trade balance data:
        
        1. Run the **Census API scraper** to collect trade balance statistics:
           ```
           python census_api_scraper.py
           ```
           
        2. This scraper will fetch trade deficit data from the U.S. Census Bureau's API and save it to the data directory.
        
        3. After running the scraper, refresh this dashboard to see the trade balance visualizations.
        """)
    else:
        census_tb = data["census_trade_balance"]
        
        # Display summary metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Count countries with trade deficit data
            if "country" in census_tb.columns:
                country_count = census_tb["country"].nunique()
                st.metric("Countries with Trade Data", f"{country_count}")
            else:
                st.metric("Countries with Trade Data", "N/A")
        
        with col2:
            # Year range
            if "year" in census_tb.columns:
                year_min = census_tb["year"].min()
                year_max = census_tb["year"].max()
                st.metric("Data Time Range", f"{year_min} - {year_max}")
            else:
                st.metric("Data Time Range", "N/A")
        
        with col3:
            # Total trade deficit
            if "trade_deficit" in census_tb.columns:
                total_deficit = census_tb["trade_deficit"].sum() / 1_000_000_000  # Convert to billions
                st.metric("Total Trade Deficit", f"${total_deficit:.2f}B")
            else:
                st.metric("Total Trade Deficit", "N/A")
        
        # Prepare data for visualizations
        if all(col in census_tb.columns for col in ["country", "year", "trade_deficit"]):
            # Add ISO3 codes for choropleth map
            census_tb["iso3"] = census_tb["country"].apply(country_to_iso3)
            
            # Group by country (if there are multiple years)
            if census_tb["year"].nunique() > 1:
                country_totals = census_tb.groupby("country").agg({
                    "trade_deficit": "sum",
                    "iso3": "first"
                }).reset_index()
            else:
                country_totals = census_tb
            
            # Create choropleth map
            st.subheader("Global Trade Deficit Map")
            
            fig = px.choropleth(
                country_totals,
                locations="iso3",
                color="trade_deficit",
                hover_name="country",
                color_continuous_scale="RdBu_r",  # Red for deficit, blue for surplus
                range_color=[-max(abs(country_totals["trade_deficit"])), max(abs(country_totals["trade_deficit"]))],
                title="Trade Balance by Country",
                labels={"trade_deficit": "Trade Deficit (USD)", "iso3": "Country"}
            )
            
            # Update layout
            fig.update_layout(
                geo=dict(
                    showframe=False,
                    showcoastlines=True,
                    projection_type='equirectangular'
                ),
                height=600,
                margin={"r":0,"t":30,"l":0,"b":0}
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Create bar chart of top trade deficits/surpluses
            st.subheader("Top Trade Partners by Deficit/Surplus")
            
            # Sort and get top and bottom countries (highest deficit and surplus)
            sorted_countries = country_totals.sort_values("trade_deficit")
            top_deficit = sorted_countries.head(10)  # Top 10 deficit countries
            top_surplus = sorted_countries.tail(10)  # Top 10 surplus countries
            top_both = pd.concat([top_deficit, top_surplus])
            
            # Create bar chart
            fig = px.bar(
                top_both, 
                x="country", 
                y="trade_deficit",
                title="Trade Balance with Top Partners",
                labels={"country": "Country", "trade_deficit": "Trade Deficit (USD)"},
                color="trade_deficit",
                color_continuous_scale="RdBu_r"  # Red for deficit, blue for surplus
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # If we have multiple years, show time series chart
            if census_tb["year"].nunique() > 1:
                st.subheader("Trade Balance Over Time")
                
                # Group by year
                yearly_data = census_tb.groupby("year")["trade_deficit"].sum().reset_index()
                
                # Create line chart
                fig = px.line(
                    yearly_data,
                    x="year",
                    y="trade_deficit",
                    title="Total Trade Balance Over Time",
                    labels={"year": "Year", "trade_deficit": "Trade Deficit (USD)"},
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)
    
        # Check if Census monthly trade data is available
        if "census_monthly_trade" in data and not data["census_monthly_trade"].empty:
            st.subheader("Monthly Trade Data")
            
            census_mt = data["census_monthly_trade"]
            
            if all(col in census_mt.columns for col in ["country", "year", "month", "import_value"]):
                # Create treemap of imports by country
                fig = px.treemap(
                    census_mt,
                    path=["country"],
                    values="import_value",
                    title=f"Imports by Country ({census_mt['year'].iloc[0]}-{census_mt['month'].iloc[0]})",
                    color="import_value",
                    color_continuous_scale="Viridis"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Display data table
        st.subheader("Trade Balance Data")
        st.dataframe(census_tb, use_container_width=True)

# Tab 3: Quantitative Restrictions
with tab3:
    st.header("Quantitative Restrictions")
    
    if data["qr_data"] is None or data["qr_data"].empty:
        st.warning("""
        No quantitative restrictions data available. To get QR data:
        
        1. Run the **WTO QR API scraper** to collect trade restriction information:
           ```
           python wto_qr_scraper.py
           ```
           
        2. This scraper will fetch data on trade barriers and restrictions from the WTO's Quantitative Restrictions API.
        
        3. After running the scraper, refresh this dashboard to see the trade restrictions visualizations.
        """)
    else:
        qr_df = data["qr_data"]
        
        # Display QR summary metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Restrictions", f"{len(qr_df)}")
        
        # Count in-force restrictions (where termination_dt is null or in future)
        in_force_count = qr_df["termination_dt"].isna().sum()
        with col2:
            st.metric("In Force Restrictions", f"{in_force_count}")
        
        # Count unique countries
        country_count = qr_df["reporter_name_en"].nunique()
        with col3:
            st.metric("Countries with Restrictions", f"{country_count}")
        
        # Create visualizations
        st.subheader("Restrictions by Country")
        
        # Count restrictions by country
        country_counts = qr_df["reporter_name_en"].value_counts().reset_index()
        country_counts.columns = ["Country", "Restriction Count"]
        
        # Take top 20 for visualization
        top_countries = country_counts.head(20)
        
        # Create horizontal bar chart
        fig = px.bar(
            top_countries, 
            y="Country", 
            x="Restriction Count",
            title="Top 20 Countries by Number of Trade Restrictions",
            orientation='h',
            color="Restriction Count",
            color_continuous_scale="Reds"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Create map of restrictions by country
        st.subheader("Global Map of Trade Restrictions")
        
        # Add ISO3 codes for map
        country_counts["iso3"] = country_counts["Country"].apply(country_to_iso3)
        
        # Create choropleth map
        fig = px.choropleth(
            country_counts,
            locations="iso3",
            color="Restriction Count",
            hover_name="Country",
            color_continuous_scale="Reds",
            title="Number of Trade Restrictions by Country",
            labels={"Restriction Count": "Number of Restrictions"}
        )
        
        # Update layout
        fig.update_layout(
            geo=dict(
                showframe=False,
                showcoastlines=True,
                projection_type='equirectangular'
            ),
            height=600,
            margin={"r":0,"t":30,"l":0,"b":0}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Restrictions by type
        st.subheader("Restrictions by Type")
        
        # Extract the measure type and count
        measure_counts = {}
        for col in qr_df.columns:
            if col.startswith("measure_") and col.endswith("_symbol"):
                measures = qr_df[col].value_counts()
                for measure, count in measures.items():
                    if pd.notna(measure):
                        if measure in measure_counts:
                            measure_counts[measure] += count
                        else:
                            measure_counts[measure] = count
        
        # Convert to DataFrame
        measure_df = pd.DataFrame({
            "Measure Type": list(measure_counts.keys()),
            "Count": list(measure_counts.values())
        }).sort_values("Count", ascending=False)
        
        # Create pie chart
        fig = px.pie(
            measure_df, 
            values="Count", 
            names="Measure Type",
            title="Distribution of Restriction Types",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Show QR data table
        st.subheader("Restriction Data")
        # Select relevant columns for display
        display_cols = ["id", "reporter_name_en", "general_description", "in_force_from", 
                        "termination_dt", "restrictions", "measure_1_description_en"]
        display_cols = [col for col in display_cols if col in qr_df.columns]
        st.dataframe(qr_df[display_cols], use_container_width=True)

# Tab 4: Policy Updates (White House data)
with tab4:
    st.header("Tariff Policy Updates")
    
    if "whitehouse_data" not in data or data["whitehouse_data"].empty:
        st.warning("""
        No White House tariff policy data available. To get policy data:
        
        1. Run the **White House scraper** to collect official policy announcements:
           ```
           python whitehouse_scraper.py
           ```
           
        2. This scraper will extract tariff-related policies from the White House website and extract structured data.
        
        3. After running the scraper, refresh this dashboard to see the policy visualizations.
        """)
    else:
        wh_df = data["whitehouse_data"]
        
        # Display summary metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Count of policy documents
            st.metric("Tariff Policy Documents", f"{len(wh_df)}")
        
        with col2:
            # Count countries mentioned
            if "countries_mentioned" in wh_df.columns:
                # Extract all countries mentioned (from list columns)
                all_countries = []
                for countries in wh_df["countries_mentioned"]:
                    if isinstance(countries, list):
                        all_countries.extend(countries)
                unique_countries = len(set(all_countries))
                st.metric("Countries Mentioned", f"{unique_countries}")
            else:
                st.metric("Countries Mentioned", "N/A")
                
        with col3:
            # Average tariff rate mentioned
            if "tariff_rates" in wh_df.columns:
                # Extract all tariff rates (from list columns)
                all_rates = []
                for rates in wh_df["tariff_rates"]:
                    if isinstance(rates, list) and rates:
                        all_rates.extend(rates)
                
                if all_rates:
                    avg_rate = sum(all_rates) / len(all_rates)
                    st.metric("Average Tariff Rate", f"{avg_rate:.1f}%")
                else:
                    st.metric("Average Tariff Rate", "N/A")
            else:
                st.metric("Average Tariff Rate", "N/A")
        
        # Timeline of policy documents
        st.subheader("Timeline of Tariff Policy Documents")
        
        if "publication_date" in wh_df.columns:
            # Convert publication_date to datetime
            wh_df["pub_date_dt"] = pd.to_datetime(wh_df["publication_date"], errors="coerce")
            
            # Sort by date
            wh_df_sorted = wh_df.sort_values("pub_date_dt")
            
            # Create timeline chart
            fig = px.scatter(
                wh_df_sorted,
                x="pub_date_dt",
                y=["title"]*len(wh_df_sorted),  # Just to get a single row
                text="title",
                title="Timeline of White House Tariff Policy Documents",
                height=300,
                labels={"pub_date_dt": "Publication Date", "y": ""}
            )
            
            # Update layout
            fig.update_traces(marker=dict(size=12, symbol="diamond", color="blue"))
            fig.update_layout(
                yaxis=dict(showticklabels=False, title=""),
                xaxis=dict(title="Publication Date")
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Countries mentioned in policies
        st.subheader("Countries Mentioned in Tariff Policies")
        
        if "countries_mentioned" in wh_df.columns:
            # Count mentions for each country
            country_mentions = {}
            for countries in wh_df["countries_mentioned"]:
                if isinstance(countries, list):
                    for country in countries:
                        if country in country_mentions:
                            country_mentions[country] += 1
                        else:
                            country_mentions[country] = 1
            
            # Convert to DataFrame
            if country_mentions:
                country_df = pd.DataFrame({
                    "Country": list(country_mentions.keys()),
                    "Mentions": list(country_mentions.values())
                }).sort_values("Mentions", ascending=False)
                
                # Create bar chart
                fig = px.bar(
                    country_df,
                    x="Country",
                    y="Mentions",
                    title="Countries Mentioned in White House Tariff Policies",
                    color="Mentions",
                    color_continuous_scale="Blues"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Latest policy documents
        st.subheader("Latest Tariff Policy Documents")
        
        # Display the most recent documents with expandable details
        for _, row in wh_df.sort_values("pub_date_dt", ascending=False).head(5).iterrows():
            with st.expander(f"{row.get('title', 'Untitled')} ({row.get('publication_date', 'Unknown date')})"):
                st.write(f"**Published:** {row.get('publication_date', 'Unknown')}")
                
                if isinstance(row.get('countries_mentioned'), list) and row.get('countries_mentioned'):
                    st.write(f"**Countries mentioned:** {', '.join(row.get('countries_mentioned'))}")
                
                if isinstance(row.get('tariff_rates'), list) and row.get('tariff_rates'):
                    st.write(f"**Tariff rates mentioned:** {', '.join([f'{rate}%' for rate in row.get('tariff_rates')])}")
                
                if row.get('effective_date'):
                    st.write(f"**Effective date:** {row.get('effective_date')}")
                
                if row.get('relevant_excerpt'):
                    st.write("**Relevant excerpt:**")
                    st.markdown(f"> {row.get('relevant_excerpt')}")
                
                if row.get('url'):
                    st.write(f"[Read full document]({row.get('url')})")
        
        # Full data table
        st.subheader("All Tariff Policy Documents")
        
        # Prepare DataFrame for display
        display_cols = ["title", "publication_date", "countries_mentioned", "tariff_rates", "effective_date", "url"]
        display_cols = [col for col in display_cols if col in wh_df.columns]
        
        # Function to format lists for display
        def format_list_col(x):
            if isinstance(x, list):
                if all(isinstance(item, (int, float)) for item in x):
                    return ", ".join([f"{item}%" for item in x])
                else:
                    return ", ".join(x)
            return x
        
        # Apply formatting to list columns
        wh_display = wh_df[display_cols].copy()
        for col in ["countries_mentioned", "tariff_rates"]:
            if col in wh_display.columns:
                wh_display[col] = wh_display[col].apply(format_list_col)
        
        st.dataframe(wh_display, use_container_width=True)

# Tab 5: News Analysis (NewsAPI data)
with tab5:
    st.header("Global Tariff News Analysis")
    
    if "news_data" not in data or data["news_data"].empty:
        st.warning("""
        No tariff news data available. To get global news data:
        
        1. Run the **NewsAPI scraper** to collect news articles about tariffs:
           ```
           python newsapi_scraper.py
           ```
           
        2. This scraper will search for and categorize news articles related to tariffs and trade from around the world.
        
        3. After running the scraper, refresh this dashboard to see the news analysis.
        
        Note: This scraper requires an API key from NewsAPI.org to be set as the NEWSAPI_KEY environment variable.
        """)
    else:
        news_df = data["news_data"]
        
        # Display summary metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Count of news articles
            st.metric("Tariff News Articles", f"{len(news_df)}")
        
        with col2:
            # Count news sources
            if "source" in news_df.columns:
                source_count = news_df["source"].nunique()
                st.metric("News Sources", f"{source_count}")
            else:
                st.metric("News Sources", "N/A")
                
        with col3:
            # Count countries mentioned
            if "countries" in news_df.columns:
                # Extract all countries mentioned
                all_countries = []
                for countries in news_df["countries"]:
                    if isinstance(countries, list):
                        all_countries.extend(countries)
                unique_countries = len(set(all_countries))
                st.metric("Countries in News", f"{unique_countries}")
            else:
                st.metric("Countries in News", "N/A")
        
        # News Distribution by Sentiment
        st.subheader("News Sentiment Analysis")
        
        if "sentiment" in news_df.columns:
            # Extract sentiment classification
            try:
                # Extract sentiment classification from the sentiment column
                sentiment_counts = {}
                for sentiment in news_df["sentiment"]:
                    if isinstance(sentiment, dict) and "classification" in sentiment:
                        classification = sentiment["classification"]
                        if classification in sentiment_counts:
                            sentiment_counts[classification] += 1
                        else:
                            sentiment_counts[classification] = 1
                    
                # Convert to DataFrame
                if sentiment_counts:
                    sentiment_df = pd.DataFrame({
                        "Sentiment": list(sentiment_counts.keys()),
                        "Count": list(sentiment_counts.values())
                    })
                    
                    # Create pie chart
                    fig = px.pie(
                        sentiment_df,
                        values="Count",
                        names="Sentiment",
                        title="Distribution of News Sentiment",
                        color="Sentiment",
                        color_discrete_map={
                            "positive": "#2E8B57",  # Sea Green
                            "neutral": "#4682B4",   # Steel Blue
                            "negative": "#CD5C5C"   # Indian Red
                        }
                    )
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error processing sentiment data: {e}")
        
        # Countries mentioned in news
        st.subheader("Countries Mentioned in Tariff News")
        
        if "countries" in news_df.columns:
            # Count mentions for each country
            country_mentions = {}
            for countries in news_df["countries"]:
                if isinstance(countries, list):
                    for country in countries:
                        if country in country_mentions:
                            country_mentions[country] += 1
                        else:
                            country_mentions[country] = 1
            
            # Convert to DataFrame
            if country_mentions:
                country_df = pd.DataFrame({
                    "Country": list(country_mentions.keys()),
                    "Mentions": list(country_mentions.values())
                }).sort_values("Mentions", ascending=False)
                
                # Create bar chart
                fig = px.bar(
                    country_df.head(15),  # Top 15 countries
                    x="Country",
                    y="Mentions",
                    title="Top Countries Mentioned in Tariff News",
                    color="Mentions",
                    color_continuous_scale="Viridis"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Industries mentioned in news
        st.subheader("Industries Mentioned in Tariff News")
        
        if "industries" in news_df.columns:
            # Count mentions for each industry
            industry_mentions = {}
            for industries in news_df["industries"]:
                if isinstance(industries, list):
                    for industry in industries:
                        if industry in industry_mentions:
                            industry_mentions[industry] += 1
                        else:
                            industry_mentions[industry] = 1
            
            # Convert to DataFrame
            if industry_mentions:
                industry_df = pd.DataFrame({
                    "Industry": list(industry_mentions.keys()),
                    "Mentions": list(industry_mentions.values())
                }).sort_values("Mentions", ascending=False)
                
                # Create horizontal bar chart
                fig = px.bar(
                    industry_df,
                    y="Industry",
                    x="Mentions",
                    title="Industries Mentioned in Tariff News",
                    orientation='h',
                    color="Mentions",
                    color_continuous_scale="Viridis"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Action types mentioned in news
        st.subheader("Tariff Actions in the News")
        
        if "actions" in news_df.columns:
            # Count mentions for each action type
            action_mentions = {}
            for actions in news_df["actions"]:
                if isinstance(actions, list):
                    for action in actions:
                        if action in action_mentions:
                            action_mentions[action] += 1
                        else:
                            action_mentions[action] = 1
            
            # Convert to DataFrame
            if action_mentions:
                action_df = pd.DataFrame({
                    "Action": list(action_mentions.keys()),
                    "Mentions": list(action_mentions.values())
                }).sort_values("Mentions", ascending=False)
                
                # Create horizontal bar chart
                fig = px.bar(
                    action_df,
                    x="Mentions",
                    y="Action",
                    title="Tariff Actions Mentioned in News",
                    orientation='h',
                    color="Mentions",
                    color_continuous_scale="Purp"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Latest news articles
        st.subheader("Latest Tariff News Articles")
        
        # Convert publishedAt to datetime for sorting
        if "publishedAt" in news_df.columns:
            news_df["published_dt"] = pd.to_datetime(news_df["publishedAt"], errors="coerce")
            
            # Display the most recent articles with expandable details
            for _, row in news_df.sort_values("published_dt", ascending=False).head(10).iterrows():
                # Create a title with sentiment indicator
                sentiment_icon = "✅" if row.get("sentiment", {}).get("classification") == "positive" else "❌" if row.get("sentiment", {}).get("classification") == "negative" else "ℹ️"
                
                with st.expander(f"{sentiment_icon} {row.get('title', 'Untitled')} ({row.get('source', 'Unknown source')})"):
                    st.write(f"**Published:** {row.get('publishedAt', 'Unknown')}")
                    
                    if row.get('description'):
                        st.write(f"**Description:** {row.get('description')}")
                    
                    if isinstance(row.get('countries'), list) and row.get('countries'):
                        st.write(f"**Countries mentioned:** {', '.join(row.get('countries'))}")
                    
                    if isinstance(row.get('industries'), list) and row.get('industries'):
                        st.write(f"**Industries mentioned:** {', '.join(row.get('industries'))}")
                    
                    if isinstance(row.get('actions'), list) and row.get('actions'):
                        st.write(f"**Actions mentioned:** {', '.join(row.get('actions'))}")
                    
                    if isinstance(row.get('tariff_rates'), list) and row.get('tariff_rates'):
                        st.write(f"**Tariff rates mentioned:** {', '.join(row.get('tariff_rates'))}")
                    
                    sentiment = row.get("sentiment", {})
                    sentiment_class = sentiment.get("classification", "neutral") if isinstance(sentiment, dict) else "neutral"
                    sentiment_score = sentiment.get("score", 0) if isinstance(sentiment, dict) else 0
                    
                    st.write(f"**Sentiment:** {sentiment_class.capitalize()} (Score: {sentiment_score:.2f})")
                    
                    if row.get('url'):
                        st.write(f"[Read full article]({row.get('url')})")

# Tab 6: Country Profiles
with tab6:
    st.header("Country Profiles")
    
    # Create country selector
    countries = set()
    
    if data["tariff_data"] is not None and "country_name" in data["tariff_data"].columns:
        tariff_countries = data["tariff_data"]["country_name"].unique().tolist()
        countries.update(tariff_countries)
    
    if data["qr_data"] is not None and "reporter_name_en" in data["qr_data"].columns:
        qr_countries = data["qr_data"]["reporter_name_en"].unique().tolist()
        countries.update(qr_countries)
    
    if "census_trade_balance" in data and "country" in data["census_trade_balance"].columns:
        census_countries = data["census_trade_balance"]["country"].unique().tolist()
        countries.update(census_countries)
    
    # Add countries from White House data
    if "whitehouse_data" in data and "countries_mentioned" in data["whitehouse_data"].columns:
        for countries_list in data["whitehouse_data"]["countries_mentioned"]:
            if isinstance(countries_list, list):
                countries.update(countries_list)
    
    # Add countries from News data
    if "news_data" in data and "countries" in data["news_data"].columns:
        for countries_list in data["news_data"]["countries"]:
            if isinstance(countries_list, list):
                countries.update(countries_list)
    
    # Get unique countries and sort
    countries = sorted(list(countries))
    
    if countries:
        selected_country = st.selectbox("Select a country", countries)
        
        st.subheader(f"Trade Profile: {selected_country}")
        
        # Create tabs for different aspects of country profile
        profile_tab1, profile_tab2, profile_tab3, profile_tab4 = st.tabs([
            "Tariff & Trade Data", "Trade Restrictions", "Policy Mentions", "News Coverage"
        ])
        
        # Tab 1: Tariff & Trade Data
        with profile_tab1:
            col1, col2 = st.columns(2)
            
            with col1:
                # Display tariff information
                st.markdown("#### Tariff Information")
                
                if data["tariff_data"] is not None and "country_name" in data["tariff_data"].columns:
                    country_tariff = data["tariff_data"][data["tariff_data"]["country_name"] == selected_country]
                    
                    if not country_tariff.empty:
                        # Check if we have tariff_value (from our new data) or tariff_rate (original expected)
                        if "tariff_value" in country_tariff.columns:
                            avg_tariff = country_tariff["tariff_value"].mean()
                            st.metric("Average Applied Tariff Lines", f"{avg_tariff:.0f}")
                            
                            if "year" in country_tariff.columns:
                                # Create time series if year data available
                                yearly_tariffs = country_tariff.groupby("year")["tariff_value"].mean().reset_index()
                                
                                fig = px.line(
                                    yearly_tariffs, 
                                    x="year", 
                                    y="tariff_value",
                                    title=f"Applied Tariff Lines Trend for {selected_country}",
                                    labels={"year": "Year", "tariff_value": "Tariff Lines"},
                                    markers=True
                                )
                                st.plotly_chart(fig, use_container_width=True)
                        elif "tariff_rate" in country_tariff.columns:
                            # Original code path
                            avg_tariff = country_tariff["tariff_rate"].mean()
                            st.metric("Average Tariff Rate", f"{avg_tariff:.2f}%")
                            
                            if "year" in country_tariff.columns:
                                # Create time series if year data available
                                yearly_tariffs = country_tariff.groupby("year")["tariff_rate"].mean().reset_index()
                                
                                fig = px.line(
                                    yearly_tariffs, 
                                    x="year", 
                                    y="tariff_rate",
                                    title=f"Tariff Rate Trend for {selected_country}",
                                    labels={"year": "Year", "tariff_rate": "Tariff Rate (%)"},
                                    markers=True
                                )
                                st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info(f"No tariff data available for {selected_country}.")
                    else:
                        st.info(f"No tariff data available for {selected_country}.")
                else:
                    st.info("No tariff data available.")
            
            with col2:
                # Display Census trade data if available
                st.markdown("#### Trade Balance")
                
                if "census_trade_balance" in data:
                    census_country = data["census_trade_balance"][data["census_trade_balance"]["country"] == selected_country]
                    
                    if not census_country.empty:
                        latest_year = census_country["year"].max()
                        latest_data = census_country[census_country["year"] == latest_year]
                        
                        if not latest_data.empty and "trade_deficit" in latest_data.columns:
                            trade_deficit = latest_data["trade_deficit"].iloc[0]
                            
                            # Format with appropriate unit
                            if abs(trade_deficit) >= 1_000_000_000:
                                formatted_deficit = f"${trade_deficit/1_000_000_000:.2f}B"
                            elif abs(trade_deficit) >= 1_000_000:
                                formatted_deficit = f"${trade_deficit/1_000_000:.2f}M"
                            else:
                                formatted_deficit = f"${trade_deficit:,.2f}"
                            
                            st.metric(
                                f"Trade Deficit ({latest_year})", 
                                formatted_deficit, 
                                delta=None
                            )
                        
                        # Show trade balance trend if multiple years available
                        if census_country["year"].nunique() > 1:
                            fig = px.line(
                                census_country.sort_values("year"), 
                                x="year", 
                                y="trade_deficit",
                                title=f"Trade Balance Trend for {selected_country}",
                                labels={"year": "Year", "trade_deficit": "Trade Deficit (USD)"},
                                markers=True
                            )
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info(f"No Census trade data available for {selected_country}.")
                else:
                    st.info("No Census trade data available.")
                    
                # Add tariff details table if we have tariff data
                if data["tariff_data"] is not None and "country_name" in data["tariff_data"].columns:
                    country_tariff = data["tariff_data"][data["tariff_data"]["country_name"] == selected_country]
                    
                    if not country_tariff.empty:
                        st.markdown("#### Detailed Tariff Data")
                        
                        # Choose which columns to display based on what's available
                        display_cols = []
                        if "year" in country_tariff.columns:
                            display_cols.append("year")
                        if "tariff_value" in country_tariff.columns:
                            display_cols.append("tariff_value")
                            display_label = "Tariff Lines"
                        elif "tariff_rate" in country_tariff.columns:
                            display_cols.append("tariff_rate")
                            display_label = "Tariff Rate (%)"
                        
                        if "indicator_name" in country_tariff.columns:
                            display_cols.append("indicator_name")
                        
                        if display_cols:
                            # Group by year if available
                            if "year" in display_cols:
                                table_data = country_tariff.sort_values("year", ascending=False)[display_cols]
                            else:
                                table_data = country_tariff[display_cols]
                                
                            st.dataframe(table_data, use_container_width=True)
        
        # Tab 2: Trade Restrictions
        with profile_tab2:
            # Display QR information
            st.markdown("#### Trade Restrictions")
            
            if data["qr_data"] is not None and "reporter_name_en" in data["qr_data"].columns:
                country_qr = data["qr_data"][data["qr_data"]["reporter_name_en"] == selected_country]
                
                if not country_qr.empty:
                    total_restrictions = len(country_qr)
                    in_force = country_qr["termination_dt"].isna().sum()
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Restrictions", total_restrictions)
                    with col2:
                        st.metric("Active Restrictions", in_force)
                    
                    # Visualize restriction types
                    measure_counts = {}
                    for col in country_qr.columns:
                        if col.startswith("measure_") and col.endswith("_symbol"):
                            measures = country_qr[col].value_counts()
                            for measure, count in measures.items():
                                if pd.notna(measure):
                                    if measure in measure_counts:
                                        measure_counts[measure] += count
                                    else:
                                        measure_counts[measure] = count
                    
                    if measure_counts:
                        measure_df = pd.DataFrame({
                            "Measure Type": list(measure_counts.keys()),
                            "Count": list(measure_counts.values())
                        }).sort_values("Count", ascending=False)
                        
                        fig = px.bar(
                            measure_df, 
                            x="Measure Type", 
                            y="Count",
                            title=f"Restriction Types for {selected_country}",
                            color="Count"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Display all restrictions in a table
                    st.subheader("All Trade Restrictions")
                    
                    # Select relevant columns for display
                    display_cols = ["id", "in_force_from", "termination_dt", "general_description", 
                                    "restrictions", "measure_1_description_en"]
                    display_cols = [col for col in display_cols if col in country_qr.columns]
                    st.dataframe(country_qr[display_cols], use_container_width=True)
                else:
                    st.info(f"No trade restriction data available for {selected_country}.")
            else:
                st.info("No trade restriction data available.")
        
        # Tab 3: Policy Mentions
        with profile_tab3:
            st.markdown(f"#### White House Policy Mentions for {selected_country}")
            
            if "whitehouse_data" in data and "countries_mentioned" in data["whitehouse_data"].columns:
                # Filter to policies mentioning this country
                country_policies = data["whitehouse_data"].apply(
                    lambda row: selected_country in row.get("countries_mentioned", []) 
                    if isinstance(row.get("countries_mentioned"), list) else False, 
                    axis=1
                )
                
                country_wh_data = data["whitehouse_data"][country_policies]
                
                if not country_wh_data.empty:
                    st.write(f"Found {len(country_wh_data)} White House documents mentioning {selected_country}.")
                    
                    # Display the policies with expandable details
                    for _, row in country_wh_data.sort_values("publication_date", ascending=False).iterrows():
                        with st.expander(f"{row.get('title', 'Untitled')} ({row.get('publication_date', 'Unknown date')})"):
                            st.write(f"**Published:** {row.get('publication_date', 'Unknown')}")
                            
                            if isinstance(row.get('tariff_rates'), list) and row.get('tariff_rates'):
                                st.write(f"**Tariff rates mentioned:** {', '.join([f'{rate}%' for rate in row.get('tariff_rates')])}")
                            
                            if row.get('effective_date'):
                                st.write(f"**Effective date:** {row.get('effective_date')}")
                            
                            if row.get('relevant_excerpt'):
                                st.write("**Relevant excerpt:**")
                                st.markdown(f"> {row.get('relevant_excerpt')}")
                            
                            if row.get('url'):
                                st.write(f"[Read full document]({row.get('url')})")
                else:
                    st.info(f"No White House policy documents mention {selected_country}.")
            else:
                st.info("No White House policy data available.")
        
        # Tab 4: News Coverage
        with profile_tab4:
            st.markdown(f"#### News Coverage for {selected_country}")
            
            if "news_data" in data and "countries" in data["news_data"].columns:
                # Filter to news mentioning this country
                country_news = data["news_data"].apply(
                    lambda row: selected_country in row.get("countries", []) 
                    if isinstance(row.get("countries"), list) else False, 
                    axis=1
                )
                
                country_news_data = data["news_data"][country_news]
                
                if not country_news_data.empty:
                    # Count of news articles
                    st.write(f"Found {len(country_news_data)} news articles mentioning {selected_country}.")
                    
                    # Analyze sentiment distribution
                    if "sentiment" in country_news_data.columns:
                        # Extract sentiment classification
                        try:
                            # Extract sentiment classification from the sentiment column
                            sentiment_counts = {}
                            for sentiment in country_news_data["sentiment"]:
                                if isinstance(sentiment, dict) and "classification" in sentiment:
                                    classification = sentiment["classification"]
                                    if classification in sentiment_counts:
                                        sentiment_counts[classification] += 1
                                    else:
                                        sentiment_counts[classification] = 1
                                
                            # Convert to DataFrame
                            if sentiment_counts:
                                sentiment_df = pd.DataFrame({
                                    "Sentiment": list(sentiment_counts.keys()),
                                    "Count": list(sentiment_counts.values())
                                })
                                
                                # Create pie chart
                                fig = px.pie(
                                    sentiment_df,
                                    values="Count",
                                    names="Sentiment",
                                    title=f"News Sentiment for {selected_country}",
                                    color="Sentiment",
                                    color_discrete_map={
                                        "positive": "#2E8B57",  # Sea Green
                                        "neutral": "#4682B4",   # Steel Blue
                                        "negative": "#CD5C5C"   # Indian Red
                                    }
                                )
                                st.plotly_chart(fig, use_container_width=True)
                        except Exception as e:
                            st.error(f"Error processing sentiment data: {e}")
                    
                    # Display news articles
                    st.subheader(f"Recent News Articles about {selected_country}")
                    
                    # Sort by publication date if available
                    if "publishedAt" in country_news_data.columns:
                        country_news_data["published_dt"] = pd.to_datetime(country_news_data["publishedAt"], errors="coerce")
                        sorted_news = country_news_data.sort_values("published_dt", ascending=False)
                    else:
                        sorted_news = country_news_data
                    
                    # Display articles
                    for _, row in sorted_news.head(10).iterrows():
                        # Create a title with sentiment indicator
                        sentiment_icon = "✅" if row.get("sentiment", {}).get("classification") == "positive" else "❌" if row.get("sentiment", {}).get("classification") == "negative" else "ℹ️"
                        
                        with st.expander(f"{sentiment_icon} {row.get('title', 'Untitled')} ({row.get('source', 'Unknown source')})"):
                            st.write(f"**Published:** {row.get('publishedAt', 'Unknown')}")
                            
                            if row.get('description'):
                                st.write(f"**Description:** {row.get('description')}")
                            
                            if isinstance(row.get('industries'), list) and row.get('industries'):
                                st.write(f"**Industries mentioned:** {', '.join(row.get('industries'))}")
                            
                            if isinstance(row.get('actions'), list) and row.get('actions'):
                                st.write(f"**Actions mentioned:** {', '.join(row.get('actions'))}")
                            
                            if isinstance(row.get('tariff_rates'), list) and row.get('tariff_rates'):
                                st.write(f"**Tariff rates mentioned:** {', '.join(row.get('tariff_rates'))}")
                            
                            sentiment = row.get("sentiment", {})
                            sentiment_class = sentiment.get("classification", "neutral") if isinstance(sentiment, dict) else "neutral"
                            sentiment_score = sentiment.get("score", 0) if isinstance(sentiment, dict) else 0
                            
                            st.write(f"**Sentiment:** {sentiment_class.capitalize()} (Score: {sentiment_score:.2f})")
                            
                            if row.get('url'):
                                st.write(f"[Read full article]({row.get('url')})")
                else:
                    st.info(f"No news articles mention {selected_country}.")
            else:
                st.info("No news data available.")
    else:
        st.info("No country data available. Run the scrapers to collect data.")

us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "United States": "US"
}

# Tab 7: BEA Economic Data
with tab7:
    st.header("U.S. Economic Indicators")
    
    if (data["bea_gdp_data"] is None or data["bea_gdp_data"].empty) and \
       (data["bea_personal_income_data"] is None or data["bea_personal_income_data"].empty) and \
       (data["bea_state_gdp_data"] is None or data["bea_state_gdp_data"].empty):
        st.warning("""
        No BEA economic data available. To get BEA data:
        
        1. Run the **BEA API scraper** to collect economic indicators:
           ```
           python bea_api_scraper.py
           ```
           
        2. This scraper will fetch GDP, personal income, and state economic data from the Bureau of Economic Analysis and save it to the data directory.
        
        3. After running the scraper, refresh this dashboard to see the economic data visualizations.
        """)
    else:
        # Display metrics in a row
        col1, col2, col3 = st.columns(3)
        
        # Display last updated timestamp
        if data["bea_timestamp"]:
            st.caption(f"Last updated: {data['bea_timestamp']}")
        
        # GDP Data Section
        st.subheader("GDP Overview")
        
        if data["bea_gdp_data"] is not None and not data["bea_gdp_data"].empty:
            # Filter for GDP Total values - field may vary based on actual data structure
            gdp_df = data["bea_gdp_data"]
            
            # Create GDP trend over time
            if "TimePeriod" in gdp_df.columns and "DataValue" in gdp_df.columns:
                # Convert DataValue if it's string with commas
                if gdp_df["DataValue"].dtype == 'object':
                    gdp_df["DataValue"] = gdp_df["DataValue"].str.replace(',', '').astype(float)
                
                # Create line chart
                fig = px.line(
                    gdp_df.sort_values("TimePeriod"), 
                    x="TimePeriod", 
                    y="DataValue",
                    title="U.S. GDP Trend",
                    labels={"TimePeriod": "Time Period", "DataValue": "GDP Value"},
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("GDP data structure does not contain expected columns for time series visualization.")
        else:
            st.info("No GDP data available.")
        
        # Personal Income Data Section
        st.subheader("Personal Income by State")
        
        if data["bea_personal_income_data"] is not None and not data["bea_personal_income_data"].empty:
            income_df = data["bea_personal_income_data"]
            
            # Add state selection if showing per-capita income
            selected_time_period = None
            if "TimePeriod" in income_df.columns:
                # Get the most recent time period
                time_periods = sorted(income_df["TimePeriod"].unique())
                if time_periods:
                    selected_time_period = st.selectbox("Select Time Period", time_periods, index=len(time_periods)-1)
            
            if selected_time_period:
                # Filter for selected time period
                period_data = income_df[income_df["TimePeriod"] == selected_time_period]
                
                # Create choropleth map if GeoName and DataValue columns exist
                if "GeoName" in period_data.columns and "DataValue" in period_data.columns:
                    # Convert DataValue if it's string with commas
                    if period_data["DataValue"].dtype == 'object':
                        period_data["DataValue"] = period_data["DataValue"].str.replace(',', '').astype(float)
                    
                    # Add state codes for mapping
                    period_data["state_code"] = period_data["GeoName"].apply(lambda x: us_state_to_abbrev.get(x, ""))
                    
                    # Create choropleth map
                    fig = px.choropleth(
                        period_data,
                        locations="state_code",
                        color="DataValue",
                        scope="usa",
                        locationmode="USA-states",
                        color_continuous_scale="Viridis",
                        title=f"Per Capita Personal Income by State ({selected_time_period})",
                        labels={"DataValue": "Per Capita Income ($)"}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Personal income data structure does not contain expected columns for map visualization.")
        else:
            st.info("No personal income data available.")
        
        # State GDP Data Section
        if data["bea_state_gdp_data"] is not None and not data["bea_state_gdp_data"].empty:
            st.subheader("State GDP Comparison")
            
            state_gdp_df = data["bea_state_gdp_data"]
            
            # Select time period if available
            selected_gdp_period = None
            if "TimePeriod" in state_gdp_df.columns:
                gdp_periods = sorted(state_gdp_df["TimePeriod"].unique())
                if gdp_periods:
                    selected_gdp_period = st.selectbox("Select GDP Time Period", gdp_periods, index=len(gdp_periods)-1)
            
            if selected_gdp_period:
                # Filter for selected time period
                gdp_period_data = state_gdp_df[state_gdp_df["TimePeriod"] == selected_gdp_period]
                
                # Create bar chart for top states by GDP
                if "GeoName" in gdp_period_data.columns and "DataValue" in gdp_period_data.columns:
                    # Convert DataValue if it's string with commas
                    if gdp_period_data["DataValue"].dtype == 'object':
                        gdp_period_data["DataValue"] = gdp_period_data["DataValue"].str.replace(',', '').astype(float)
                    
                    # Get top 10 states by GDP
                    top_states = gdp_period_data.sort_values("DataValue", ascending=False).head(10)
                    
                    # Create bar chart
                    fig = px.bar(
                        top_states,
                        x="GeoName",
                        y="DataValue",
                        title=f"Top 10 States by GDP ({selected_gdp_period})",
                        labels={"GeoName": "State", "DataValue": "GDP Value"},
                        color="DataValue",
                        color_continuous_scale="Viridis"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("State GDP data structure does not contain expected columns for visualization.")
        else:
            st.info("No state GDP data available.")
        
        # Add a data table with all BEA data for reference
        st.subheader("Raw Economic Data")
        
        # Create tabs for different datasets
        data_tab1, data_tab2, data_tab3 = st.tabs(["GDP Data", "Personal Income Data", "State GDP Data"])
        
        with data_tab1:
            if data["bea_gdp_data"] is not None and not data["bea_gdp_data"].empty:
                st.dataframe(data["bea_gdp_data"], use_container_width=True)
            else:
                st.info("No GDP data available.")
        
        with data_tab2:
            if data["bea_personal_income_data"] is not None and not data["bea_personal_income_data"].empty:
                st.dataframe(data["bea_personal_income_data"], use_container_width=True)
            else:
                st.info("No personal income data available.")
        
        with data_tab3:
            if data["bea_state_gdp_data"] is not None and not data["bea_state_gdp_data"].empty:
                st.dataframe(data["bea_state_gdp_data"], use_container_width=True)
            else:
                st.info("No state GDP data available.")

# Footer
st.markdown("""
Data sourced from the World Trade Organization (WTO), U.S. Census Bureau, White House, and global news sources. Dashboard last updated: {}
""".format(datetime.now().strftime("%Y-%m-%d")))