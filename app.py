#!/usr/bin/env python3
"""
Airline Analytics Dashboard - Streamlit App
"""
import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Airline Analytics Dashboard",
    page_icon="✈️",
    layout="wide"
)

@st.cache_resource
def get_connection():
    """Get database connection"""
    return psycopg2.connect(os.getenv('DATABASE_URL'))

def run_query(query):
    """Execute query and return DataFrame"""
    conn = get_connection()
    return pd.read_sql_query(query, conn)

# Title
st.title("✈️ Airline Analytics Dashboard")
st.markdown("---")

# Sidebar
st.sidebar.header("Filters")

# Main metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_airports = run_query("SELECT COUNT(*) as count FROM DimAirport WHERE IsCurrent = TRUE")
    st.metric("Total Airports", total_airports['count'].iloc[0])

with col2:
    total_carriers = run_query("SELECT COUNT(*) as count FROM DimCarrier WHERE IsCurrent = TRUE")
    st.metric("Total Carriers", total_carriers['count'].iloc[0])

with col3:
    total_aircraft = run_query("SELECT COUNT(*) as count FROM DimAircraft WHERE IsCurrent = TRUE")
    st.metric("Total Aircraft", total_aircraft['count'].iloc[0])

with col4:
    total_customers = run_query("SELECT COUNT(*) as count FROM DimCustomer WHERE IsCurrent = TRUE")
    st.metric("Total Customers", total_customers['count'].iloc[0])

st.markdown("---")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["🌍 Airports", "✈️ Fleet", "👥 Customers", "📊 Analytics"])

with tab1:
    st.header("Airport Network")
    
    # Airport data
    airports_df = run_query("""
        SELECT 
            IATA, AirportName, City, Country, Region,
            Latitude, Longitude
        FROM DimAirport
        WHERE IsCurrent = TRUE
        ORDER BY AirportName
    """)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Airport Locations")
        if not airports_df.empty:
            fig = px.scatter_geo(
                airports_df,
                lat='latitude',
                lon='longitude',
                hover_name='airportname',
                hover_data=['iata', 'city', 'country'],
                title='Global Airport Network',
                projection='natural earth'
            )
            fig.update_traces(marker=dict(size=10, color='red'))
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Airports by Region")
        region_counts = airports_df['region'].value_counts().reset_index()
        region_counts.columns = ['Region', 'Count']
        fig = px.pie(region_counts, values='Count', names='Region', hole=0.4)
        st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Airport Details")
    st.dataframe(airports_df, use_container_width=True, hide_index=True)

with tab2:
    st.header("Aircraft Fleet")
    
    # Fleet data
    fleet_df = run_query("""
        SELECT 
            TailNumber, Manufacturer, Model, AircraftType,
            SeatingCapacity, ManufactureYear, Age, OwnershipType
        FROM DimAircraft
        WHERE IsCurrent = TRUE
        ORDER BY Age
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Fleet by Manufacturer")
        mfr_counts = fleet_df['manufacturer'].value_counts().reset_index()
        mfr_counts.columns = ['Manufacturer', 'Count']
        fig = px.bar(mfr_counts, x='Manufacturer', y='Count', color='Manufacturer')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Fleet Age Distribution")
        fig = px.histogram(fleet_df, x='age', nbins=20, 
                          title='Aircraft Age Distribution',
                          labels={'age': 'Age (years)', 'count': 'Number of Aircraft'})
        st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Fleet Details")
    st.dataframe(fleet_df, use_container_width=True, hide_index=True)

with tab3:
    st.header("Customer Analytics")
    
    # Customer data
    customers_df = run_query("""
        SELECT 
            CustomerID, FirstName, LastName, Email,
            LoyaltyTier, LoyaltyPoints, Country, City,
            EXTRACT(YEAR FROM AGE(CURRENT_DATE, BirthDate)) as Age
        FROM DimCustomer
        WHERE IsCurrent = TRUE
        ORDER BY LoyaltyPoints DESC
        LIMIT 100
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Loyalty Tier Distribution")
        tier_counts = customers_df['loyaltytier'].value_counts().reset_index()
        tier_counts.columns = ['Tier', 'Count']
        colors = {'Bronze': '#CD7F32', 'Silver': '#C0C0C0', 
                 'Gold': '#FFD700', 'Platinum': '#E5E4E2'}
        fig = px.bar(tier_counts, x='Tier', y='Count', 
                    color='Tier', color_discrete_map=colors)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Top 10 Customers by Points")
        top_customers = customers_df.head(10)
        fig = px.bar(top_customers, 
                    x='loyaltypoints', 
                    y='customerid',
                    orientation='h',
                    labels={'loyaltypoints': 'Loyalty Points', 'customerid': 'Customer'})
        st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Customer Details")
    st.dataframe(customers_df, use_container_width=True, hide_index=True)

with tab4:
    st.header("Data Warehouse Analytics")
    
    # Carrier info
    carriers_df = run_query("""
        SELECT 
            AirlineCode, CarrierName, AllianceCode, Country
        FROM DimCarrier
        WHERE IsCurrent = TRUE
        ORDER BY CarrierName
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Carriers")
        st.dataframe(carriers_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.subheader("Carriers by Alliance")
        alliance_counts = carriers_df['alliancecode'].value_counts().reset_index()
        alliance_counts.columns = ['Alliance', 'Count']
        fig = px.pie(alliance_counts, values='Count', names='Alliance')
        st.plotly_chart(fig, use_container_width=True)
    
    # Date dimension stats
    st.subheader("Date Dimension Coverage")
    date_stats = run_query("""
        SELECT 
            MIN(Date) as StartDate,
            MAX(Date) as EndDate,
            COUNT(*) as TotalDays,
            SUM(CASE WHEN IsWeekend THEN 1 ELSE 0 END) as WeekendDays,
            SUM(CASE WHEN HolidayFlag THEN 1 ELSE 0 END) as Holidays
        FROM DimDate
    """)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Start Date", date_stats['startdate'].iloc[0])
    with col2:
        st.metric("End Date", date_stats['enddate'].iloc[0])
    with col3:
        st.metric("Total Days", date_stats['totaldays'].iloc[0])
    with col4:
        st.metric("Weekend Days", date_stats['weekenddays'].iloc[0])
    with col5:
        st.metric("Holidays", date_stats['holidays'].iloc[0])

# Footer
st.markdown("---")
st.markdown("**Data Source:** Neon PostgreSQL Database | **Refresh:** Real-time")
