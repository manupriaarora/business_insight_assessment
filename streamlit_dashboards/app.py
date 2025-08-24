import streamlit as st
import boto3

from churn_indicator import churn_indicator
from customer_segmentation import customer_segmentation
from sales_trends_seasonality import sales_trend_seasonality
from loyalty_program_impact import loyalty_program_impact
from location_performance import location_performance
from pricing_discount import pricing_discount

# -----------------------------
# Load Data (replace with your source)
# -----------------------------

bucket = "global-partners-de-project2"
s3 = boto3.client('s3')

st.set_page_config(layout="wide", page_title="Business Insights Dashboard")

# -----------------------------
# Sidebar for Dashboard Selection
# -----------------------------
st.sidebar.title("Dashboard Selector")
dashboard = st.sidebar.radio("Choose Dashboard:", [
    "Customer Segmentation",
    "Churn Risk Indicators",
    "Sales Trends & Seasonality",
    "Loyalty Program Impact",
    "Location Performance",
    "Pricing & Discount Effectiveness"
])

# -----------------------------
# Customer Segmentation Dashboard
# -----------------------------
if dashboard == "Customer Segmentation":
    customer_segmentation(bucket)

if dashboard == "Churn Risk Indicators":
    churn_indicator(bucket)

if dashboard == "Sales Trends & Seasonality":
    sales_trend_seasonality(bucket)

if dashboard == "Loyalty Program Impact":
    loyalty_program_impact(bucket)

if dashboard == "Location Performance":
    location_performance(bucket)

if dashboard == "Pricing & Discount Effectiveness":
    pricing_discount(bucket)
    
