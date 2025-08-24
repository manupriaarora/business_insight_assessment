import streamlit as st
import pandas as pd
import boto3
from io import StringIO
import plotly.express as px

# Initialize S3 client
s3 = boto3.client("s3")

# Page config (call only ONCE at the top)
st.set_page_config(page_title="Loyalty Program Impact Dashboard", layout="wide")

def loyalty_program_impact(bucket):
    st.title("Loyalty Program Impact Dashboard")

    # Get the latest CSV file from S3 prefix
    prefix = "athena-query-results/loyalty_program_impact/"
    files = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    csv_files = [f["Key"] for f in files if f["Key"].endswith(".csv")]

    if not csv_files:
        st.error("No CSV file found in the S3 bucket under the given prefix.")
        return

    # Use the most recent file
    latest_file = max(csv_files, key=lambda x: x.split("/")[-1])
    response = s3.get_object(Bucket=bucket, Key=latest_file)
    df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")))

    # Format numbers for better readability
    df["avg_spend_per_customer"] = df["avg_spend_per_customer"].round(2)
    df["avg_repeat_orders"] = df["avg_repeat_orders"].round(2)
    df["avg_order_value"] = df["avg_order_value"].round(2)

    # Metrics Table
    st.markdown("### Detailed Metrics Table")
    metrics_table_df = df.set_index("customer_type")
    metrics_table_df.columns = [
        "Avg. Spend per Customer ($)",
        "Avg. Repeat Orders",
        "Avg. Order Value ($)",
    ]
    st.dataframe(metrics_table_df.T, use_container_width=True)

    # --- Visualization Section ---
    st.markdown("### Visualizations")

    # Chart 1: Average Spend per Customer
    fig1 = px.bar(
        df,
        x="customer_type",
        y="avg_spend_per_customer",
        title="Average Spend per Customer",
        text_auto=True,
        color="customer_type",
    )
    st.plotly_chart(fig1, use_container_width=True)

    # Chart 2: Average Order Value
    fig2 = px.bar(
        df,
        x="customer_type",
        y="avg_order_value",
        title="Average Order Value",
        text_auto=True,
        color="customer_type",
    )
    st.plotly_chart(fig2, use_container_width=True)

    # Chart 3: Average Repeat Orders
    fig3 = px.bar(
        df,
        x="customer_type",
        y="avg_repeat_orders",
        title="Average Repeat Orders",
        text_auto=True,
        color="customer_type",
    )
    st.plotly_chart(fig3, use_container_width=True)
