import streamlit as st
import pandas as pd
import plotly.express as px
import seaborn as sns
import boto3
from io import StringIO


s3 = boto3.client('s3')

def customer_segmentation(bucket):
    st.set_page_config(page_title="Customer Segmentation Dashboard", layout="wide")
    st.title("Customer Segmentation Dashboard")
    st.header("RFM Segmentation")
    st.write("Low Monetary Rank - High Spending")
    st.write("Low Frequency Rank - High Order Count In Last 24 Months")
    st.write("Low Recency Rank - Few Days Passed Since Last Order")

    prefix = "athena-query-results/customer_segmentation_behavior/"
    s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
    file = None
    for f in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', []):
        if f['Key'].endswith('.csv'):
              file = f['Key'] 
              break
    
    # Get the object from S3
    response = s3.get_object(Bucket=bucket, Key=file)
    df = pd.read_csv(StringIO(response['Body'].read().decode("utf-8")))
    st.dataframe(df.head(20)) # sample table

    # --- Scatter Plot: Customer-level view ---
    st.subheader("Customer Distribution (RFM Scatter)")

    fig_scatter = px.scatter(
        df,
        x="days_passed",
        y="num_purchases_last_24_months",
        size="total_cost_per_user",
        color="customer_segment",
        hover_data=["user_id", "total_cost_per_user"],
        labels={
            "days_passed": "Recency (days since last purchase)",
            "num_purchases_last_24_months": "Frequency (purchases last 24 months)",
            "total_cost_per_user": "Monetary (total spend)"
        },
        title="Customer Segmentation by RFM"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    # --- Segment Summary: Aggregated view ---
    st.subheader("Customer Segment Summary")

    # Bar Chart: Count of customers per segment
    fig_bar = px.bar(
        df.groupby("customer_segment")["user_id"].count().reset_index(),
        x="customer_segment",
        y="user_id",
        text="user_id",
        labels={"user_id": "Number of Customers"},
        title="Customers per Segment"
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # Pie Chart: Revenue share per segment
    fig_pie = px.pie(
        df.groupby("customer_segment")["total_cost_per_user"].sum().reset_index(),
        values="total_cost_per_user",
        names="customer_segment",
        title="Revenue Contribution by Segment",
        hole=0.3
    )
    st.plotly_chart(fig_pie, use_container_width=True)

    # Table: Avg RFM values per segment
    segment_summary = (
        df.groupby("customer_segment")
        .agg(
            avg_recency=("days_passed", "mean"),
            avg_frequency=("num_purchases_last_24_months", "mean"),
            avg_monetary=("total_cost_per_user", "mean"),
            customer_count=("user_id", "count"),
            total_revenue=("total_cost_per_user", "sum")
        )
        .reset_index()
    )

    st.write("### Segment Summary Table")
    st.dataframe(segment_summary, use_container_width=True)
    