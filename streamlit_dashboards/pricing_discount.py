import streamlit as st
import pandas as pd
import boto3
from io import StringIO
import plotly.express as px

# Initialize S3 client
s3 = boto3.client("s3")

# Page config (call only ONCE at the top)
st.set_page_config(page_title="Pricing & Discount Effectiveness Dashboard", layout="wide")

def pricing_discount(bucket):
    st.title("Pricing & Discount Effectiveness Dashboard")

    # Get the latest CSV file from S3 prefix
    prefix = "athena-query-results/pricing_discount_effectiveness/"
    files = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    csv_files = [f["Key"] for f in files if f["Key"].endswith(".csv")]

    if not csv_files:
        st.error("No CSV file found in the S3 bucket under the given prefix.")
        return

    # Use the most recent file
    latest_file = max(csv_files, key=lambda x: x.split("/")[-1])
    response = s3.get_object(Bucket=bucket, Key=latest_file)
    df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")))

    # Summary Metrics
    st.subheader("Key Metrics")
    col1, col2, col3 = st.columns(3)

    total_revenue = df["total_revenue"].sum()
    total_orders = df["total_orders"].sum()
    avg_order_value = (df["total_revenue"].sum() / df["total_orders"].sum()) if total_orders > 0 else 0

    col1.metric("Total Revenue", f"${total_revenue:,.2f}")
    col2.metric("Total Orders", f"{total_orders:,}")
    col3.metric("Avg Order Value", f"${avg_order_value:,.2f}")

    # Revenue by Discount Type
    st.subheader("Revenue by Order Type")
    fig1 = px.bar(
        df,
        x="order_type",
        y="total_revenue",
        color="order_type",
        text="total_revenue",
        title="Revenue from Discounted vs Non-Discounted Orders",
    )
    fig1.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
    st.plotly_chart(fig1, use_container_width=True)

    # Orders Count
    st.subheader("Orders by Type")
    fig2 = px.pie(
        df,
        names="order_type",
        values="total_orders",
        title="Share of Orders (Discounted vs Non-Discounted)",
    )
    st.plotly_chart(fig2, use_container_width=True)

    # Avg Order Value Comparison
    st.subheader("Average Order Value by Type")
    fig3 = px.bar(
        df,
        x="order_type",
        y="avg_order_value",
        color="order_type",
        text="avg_order_value",
        title="Avg Order Value Comparison",
    )
    fig3.update_traces(texttemplate="$%{text:,.2f}", textposition="outside")
    st.plotly_chart(fig3, use_container_width=True)

    # Insights
    st.subheader("Insights & Recommendations")
    discounted_rev = df[df["order_type"] == "Discounted Order"]["total_revenue"].sum()
    nondiscounted_rev = df[df["order_type"] == "Non-Discounted Order"]["total_revenue"].sum()

    if discounted_rev > nondiscounted_rev:
        st.success("Discounts are driving higher total revenue — they may be effective in boosting sales volume.")
    else:
        st.warning("Non-discounted orders contribute more revenue — discounts might be eroding profit margins without enough volume uplift.")

    st.info("Use these insights to refine promotion strategies: target discounts where they boost order frequency without cutting into margins.")


