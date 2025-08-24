import streamlit as st
import pandas as pd
import boto3
from io import StringIO
import plotly.express as px

# Initialize S3 client
s3 = boto3.client("s3")

# Page config (call only ONCE at the top)
st.set_page_config(page_title="Location Performance Dashboard", layout="wide")

def location_performance(bucket):
    st.title("Location Performance Dashboard")

    # Get the latest CSV file from S3 prefix
    prefix = "athena-query-results/top_performing_location/"
    files = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    csv_files = [f["Key"] for f in files if f["Key"].endswith(".csv")]

    if not csv_files:
        st.error("No CSV file found in the S3 bucket under the given prefix.")
        return

    # Use the most recent file
    latest_file = max(csv_files, key=lambda x: x.split("/")[-1])
    response = s3.get_object(Bucket=bucket, Key=latest_file)
    df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")))

    # Format numbers
    df["total_revenue"] = df["total_revenue"].round(2)
    df["avg_order_value"] = df["avg_order_value"].round(2)
    df["orders_per_day"] = df["orders_per_day"].round(2)
    df["orders_per_week"] = df["orders_per_week"].round(2)

    st.subheader("Ranked Locations by Revenue")
    st.dataframe(df, use_container_width=True)

    # --- Top vs Bottom Locations ---
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Top 5 Locations by Revenue")
        top5 = df.sort_values("total_revenue", ascending=False).head(5)
        fig1 = px.bar(
            top5, x="location_id", y="total_revenue",
            text="total_revenue", title="Top 5 Revenue Generators",
            color="total_revenue", color_continuous_scale="Viridis"
        )
        fig1.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.markdown("### Bottom 5 Locations by Revenue")
        bottom5 = df.sort_values("total_revenue", ascending=True).head(5)
        fig2 = px.bar(
            bottom5, x="location_id", y="total_revenue",
            text="total_revenue", title="Bottom 5 Revenue Generators",
            color="total_revenue", color_continuous_scale="Reds"
        )
        fig2.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
        st.plotly_chart(fig2, use_container_width=True)

    # --- Metrics Breakdown ---
    st.markdown("### Metrics Comparison Across Locations")

    metric_choice = st.selectbox(
        "Choose a metric to compare:", 
        ["avg_order_value", "orders_per_day", "orders_per_week"]
    )

    fig3 = px.bar(
        df.sort_values(metric_choice, ascending=False),
        x="location_id", y=metric_choice, color="total_revenue",
        title=f"Location Comparison by {metric_choice.replace('_', ' ').title()}",
        color_continuous_scale="Blues"
    )
    st.plotly_chart(fig3, use_container_width=True)

 