import streamlit as st
import pandas as pd
import boto3
import io
import plotly.express as px
from io import StringIO


s3 = boto3.client('s3')

def churn_indicator(bucket):
    st.set_page_config(page_title="Churn Indicator Dashboard", layout="wide")
    st.title("Churn Indicator Dashboard")

    st.write("Identify customers at risk based on recency, frequency, and spend trends.")
    
    prefix = "athena-query-results/churn_indicator/"
    s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
    file = None
    for f in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', []):
        if f['Key'].endswith('.csv'):
              file = f['Key'] 
              break
    
        # Get the object from S3
    response = s3.get_object(Bucket=bucket, Key=file)
    df = pd.read_csv(StringIO(response['Body'].read().decode("utf-8")))
    # st.dataframe(df.head(10))  # sample table 


    # If CSV doesn't already contain churn status, create it
    if "activity_status" not in df.columns:
        df["activity_status"] = df["days_since_last_order"].apply(
            lambda x: "At Risk" if x > 700 else "Active"
        )


    # KPIs
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Customers", df['user_id'].nunique())
    with col2:
        st.metric("At Risk Customers", df[df['activity_status']=="At Risk"].shape[0])
    with col3:
        st.metric("Avg Days Since Last Order", round(df['days_since_last_order'].mean(),1))

    st.divider()

    # Visualization 1: Bar chart of Active vs At Risk Customers
    status_counts = df['activity_status'].value_counts().reset_index()
    status_counts.columns = ['Status', 'Customer Count']
    fig1 = px.bar(status_counts, x='Status', y='Customer Count', color='Status', title="Active vs At Risk Customers")
    st.plotly_chart(fig1, use_container_width=True)

    # Visualization 2: Distribution of Days Since Last Order
    fig2 = px.histogram(df, x="days_since_last_order", nbins=30, title="Distribution: Days Since Last Order")
    st.plotly_chart(fig2, use_container_width=True)

    # Visualization 3: Spend Trends (last month % change)
    # if "pct_change_last_month" in df.columns:
    #     fig3 = px.histogram(df, x="pct_change_last_month", nbins=30, title="Spend Change % (Last Month)")
    #     st.plotly_chart(fig3, use_container_width=True)

    # Visualization 4: Scatter Plot - Churn Risk Profile
    if "avg_days_between_orders" in df.columns:
        fig4 = px.scatter(
            df, 
            x="avg_days_between_orders", 
            y="days_since_last_order",
            color="activity_status",
            hover_data=["user_id"],
            title="Customer Churn Risk Profile"
        )
        st.plotly_chart(fig4, use_container_width=True)   

    # Data Table
    st.subheader("Customer Activity Details")
    st.dataframe(df.head(20))
