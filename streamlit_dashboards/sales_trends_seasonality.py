import streamlit as st
import pandas as pd
import boto3
import plotly.express as px
from io import StringIO
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker

s3 = boto3.client('s3')

# Custom y-axis formatter for millions/billions
def format_revenue(x, pos):
    if x >= 1e9:
        return f'${x/1e9:.1f}B'
    else:
        return f'${x/1e6:.1f}M'
                        
def sales_trend_seasonality(bucket):
    st.set_page_config(page_title="Sales Trends & Seasonality Dashboard", layout="wide")
    st.title("Sales Trends & Seasonality Dashboard")
    prefix = "athena-query-results/sales_trend/"
    date_prefix = "athena-query-results/get_date_detail"

    file = None
    for f in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', []):
        if f['Key'].endswith('.csv'):
              file = f['Key'] 
              break
    
        # Get the object from S3
    response = s3.get_object(Bucket=bucket, Key=file)
    df = pd.read_csv(StringIO(response['Body'].read().decode("utf-8")))

    date_file = None
    for f in s3.list_objects_v2(Bucket=bucket, Prefix=date_prefix).get('Contents', []):
        if f['Key'].endswith('.csv'):
              date_file = f['Key'] 
              break
    
        # Get the object from S3
    date_response = s3.get_object(Bucket=bucket, Key=date_file)
    date_df = pd.read_csv(StringIO(date_response['Body'].read().decode("utf-8")))

    # Convert appropriate columns to the correct data type for plotting
    # Check if the column is a numeric type that might be a timestamp
    if pd.api.types.is_numeric_dtype(df['period_start']):
        # Assuming the timestamp is in seconds, convert it to datetime
        df['period_start'] = pd.to_datetime(df['period_start'], unit='s')
    else:
        # Fallback to a standard conversion
        df['period_start'] = pd.to_datetime(df['period_start'])

    # Main page filter options
    st.header("Filter Options")
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_period = st.selectbox("Select Time Period", df['period_type'].unique())
    with col2:
        selected_restaurant = st.selectbox("Select Restaurant ID", ['All'] + list(df['restaurant_id'].unique()))
    with col3:
        selected_category = st.selectbox("Select Item Category", ['All'] + list(df['item_category'].unique()))

    filtered_df = df[df['period_type'] == selected_period]
    
    # Apply restaurant filter if not 'All'
    if selected_restaurant != 'All':
        filtered_df = filtered_df[filtered_df['restaurant_id'] == selected_restaurant]

    # Apply item category filter if not 'All'
    if selected_category != 'All':
        filtered_df = filtered_df[filtered_df['item_category'] == selected_category]

    # Display metrics
    st.markdown("### Key Metrics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Revenue", f"${filtered_df['revenue'].sum():,.2f}")
    with col2:
        st.metric("Number of Restaurants", filtered_df['restaurant_id'].nunique())
    with col3:
        st.metric("Number of Categories", filtered_df['item_category'].nunique())

     # Holiday filter options
    st.markdown("### Holiday Visualizations")
    col_show = st.columns(1)[0]
    with col_show:
        show_holidays = st.checkbox("Show holidays on chart", value=True)

    # Visualization Section using matplotlib for static charts
    st.markdown("### Revenue Visualizations")
    
    # Chart 1: Revenue over Time
    st.markdown("#### Revenue over Time")
    fig1, ax1 = plt.subplots(figsize=(12, 6))
    
    # Ensure period_start is a valid datetime before plotting
    filtered_df = filtered_df.dropna(subset=['period_start'])

    ax1.plot(filtered_df.sort_values('period_start')['period_start'], filtered_df.sort_values('period_start')['revenue'])
    ax1.set_title(f"Revenue over Time ({selected_period} Aggregation)")
    ax1.set_xlabel("Period Start")
    ax1.set_ylabel("Revenue ($)")
    
    # Dynamic date formatting based on selected period
    if selected_period == 'Daily':
        date_formatter = mdates.DateFormatter('%Y-%m-%d')
    elif selected_period == 'Weekly':
        date_formatter = mdates.DateFormatter('%Y-%W')
    else: # Monthly
        date_formatter = mdates.DateFormatter('%Y-%m')
    
    # Set a max number of ticks to prevent overcrowding
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=10))
    ax1.xaxis.set_major_formatter(date_formatter)
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(format_revenue))
    # Add vertical lines for holidays using the custom DataFrame
   
    print(date_df.columns)
    if show_holidays and not date_df.empty:
        holiday_dates = pd.to_datetime(date_df[date_df['is_holiday']]['date_key'])

    is_first_holiday = True
    for holiday_date in holiday_dates:
        if filtered_df['period_start'].min() <= holiday_date <= filtered_df['period_start'].max():
            if is_first_holiday:
                ax1.axvline(x=holiday_date, color='r', linestyle='--', linewidth=1, label='Public Holidays')
                is_first_holiday = False
            else:
                ax1.axvline(x=holiday_date, color='r', linestyle='--', linewidth=1)

    ax1.legend()

    plt.tight_layout()
    st.pyplot(fig1)
    
    # Chart 2: Revenue Breakdown by Restaurant
    st.markdown("#### Revenue Breakdown by Restaurant")
    fig2, ax2 = plt.subplots(figsize=(12, 6))
    revenue_by_restaurant = filtered_df.groupby('restaurant_id')['revenue'].sum().reset_index()
    ax2.bar(revenue_by_restaurant['restaurant_id'], revenue_by_restaurant['revenue'])
    ax2.set_title("Revenue Breakdown by Restaurant")
    ax2.set_xlabel("Restaurant ID")
    ax2.set_ylabel("Revenue ($)")
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(True)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(format_revenue))
    plt.tight_layout()
    st.pyplot(fig2)

    # Chart 3: Revenue Breakdown by Item Category
    st.markdown("#### Revenue Breakdown by Item Category")
    revenue_by_category = filtered_df.groupby('item_category')['revenue'].sum().reset_index().sort_values('revenue', ascending=False)
    # Truncate long category names
    trimmed_categories = [
        (cat[:20] + '...') if len(cat) > 20 else cat
        for cat in revenue_by_category['item_category']
    ]
    # Dynamically set figure size based on number of categories
    num_categories = len(revenue_by_category)
    fig_height = max(6, num_categories * 0.2)
    fig3, ax3 = plt.subplots(figsize=(12, fig_height))

    ax3.bar(revenue_by_category['item_category'], revenue_by_category['revenue'])
    ax3.set_title("Revenue Breakdown by Item Category")
    ax3.set_xlabel("Item Category")
    ax3.set_ylabel("Revenue ($)")
    # Get the current tick locations and apply the trimmed labels
    tick_locations = ax3.get_xticks()
    ax3.set_xticks(tick_locations)
    ax3.set_xticklabels(trimmed_categories, rotation=90)
    ax3.grid(True)
    ax3.yaxis.set_major_formatter(mticker.FuncFormatter(format_revenue))
    plt.tight_layout()
    st.pyplot(fig3)

    st.markdown("### Data Tables")
    
    st.markdown(f"#### {selected_period} Revenue")
    st.dataframe(filtered_df.sort_values('period_start', ascending=False), use_container_width=True)

    
