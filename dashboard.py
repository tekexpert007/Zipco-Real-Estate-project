import streamlit as st
import pandas as pd
import altair as alt
from sqlalchemy import create_engine, text

# --- Database connection using SQLAlchemy ---
def get_engine():
    return create_engine("postgresql+psycopg2://postgres:sunset99NEW@localhost:5432/zipco_realestate2")

# --- Refresh Gold Tables ---
def refresh_gold_tables():
    engine = get_engine()
    with engine.connect() as conn:
        try:
            # Refresh Gold Sales
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS gold_sales_listings AS
                SELECT * FROM silver_sales WHERE 1=0;
                TRUNCATE TABLE gold_sales_listings;
                INSERT INTO gold_sales_listings
                SELECT 
                    id AS listing_id,
                    address AS formatted_address,
                    property_type,
                    price,
                    status,
                    city,
                    state,
                    zip_code,
                    bedrooms,
                    bathrooms,
                    sqft AS square_feet
                FROM silver_sales;
            """))

            # Refresh Gold Rentals
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS gold_rental_listings AS
                SELECT * FROM silver_rentals WHERE 1=0;
                TRUNCATE TABLE gold_rental_listings;
                INSERT INTO gold_rental_listings
                SELECT 
                    id AS listing_id,
                    address AS formatted_address,
                    property_type,
                    price,
                    status,
                    city,
                    state,
                    zip_code,
                    bedrooms,
                    bathrooms,
                    sqft AS square_feet
                FROM silver_rentals;
            """))

            st.success("✅ Gold tables refreshed successfully!")
        except Exception as e:
            st.error(f"Error refreshing gold tables: {e}")

# --- Load data safely ---
@st.cache_data
def load_data(table_name: str):
    engine = get_engine()
    try:
        df = pd.read_sql_table(table_name, con=engine)
        return df
    except Exception:
        return pd.DataFrame()  # Return empty DataFrame if table is missing

# --- Sidebar: Admin controls ---
st.sidebar.header("⚡ Admin Controls")
if st.sidebar.button("🔄 Refresh Gold Tables"):
    refresh_gold_tables()

# --- Sidebar: Dataset selection ---
st.sidebar.header("📂 Dataset")
dataset_choice = st.sidebar.radio("Choose dataset:", ["Sales Listings", "Rental Listings"])
table_name = "gold_sales_listings" if dataset_choice == "Sales Listings" else "gold_rental_listings"

# --- Load selected data ---
df = load_data(table_name)
if df.empty:
    st.warning(f"No data available for {dataset_choice}. Refresh gold tables or check silver tables.")
    st.stop()

# --- Sidebar: Filters ---
st.sidebar.header("🔍 Filters")
state_filter = st.sidebar.multiselect("State", sorted(df["state"].dropna().unique()))
city_filter = st.sidebar.multiselect("City", sorted(df["city"].dropna().unique()))
type_filter = st.sidebar.multiselect("Property Type", sorted(df["property_type"].dropna().unique()))
status_filter = st.sidebar.multiselect("Status", sorted(df["status"].dropna().unique()))

# --- Apply filters ---
filtered_df = df.copy()
if state_filter:
    filtered_df = filtered_df[filtered_df["state"].isin(state_filter)]
if city_filter:
    filtered_df = filtered_df[filtered_df["city"].isin(city_filter)]
if type_filter:
    filtered_df = filtered_df[filtered_df["property_type"].isin(type_filter)]
if status_filter:
    filtered_df = filtered_df[filtered_df["status"].isin(status_filter)]

# --- KPIs ---
st.title(f"🏡 Zipco Real Estate Dashboard — {dataset_choice}")

col1, col2, col3 = st.columns(3)
col1.metric("Total Listings", len(filtered_df))
col2.metric("Avg Price", f"${filtered_df['price'].mean():,.0f}" if not filtered_df.empty else "N/A")
col3.metric("Median Sqft", f"{filtered_df['square_feet'].median():,.0f}" if not filtered_df.empty else "N/A")

# --- Charts ---
st.subheader("📊 Price Distribution")
if not filtered_df.empty:
    price_chart = alt.Chart(filtered_df).mark_bar().encode(
        x=alt.X("price", bin=alt.Bin(maxbins=40), title="Price ($)"),
        y="count()"
    )
    st.altair_chart(price_chart, use_container_width=True)

st.subheader("🏙️ Average Price by City")
if not filtered_df.empty:
    city_chart = alt.Chart(filtered_df).mark_bar().encode(
        x="city",
        y="average(price)",
        color="city"
    )
    st.altair_chart(city_chart, use_container_width=True)

st.subheader("📐 Price vs. Square Feet")
if not filtered_df.empty:
    scatter = alt.Chart(filtered_df).mark_circle(size=60).encode(
        x="square_feet",
        y="price",
        tooltip=["formatted_address", "price", "square_feet", "bedrooms", "bathrooms"]
    ).interactive()
    st.altair_chart(scatter, use_container_width=True)

# --- Raw data view ---
st.subheader("📋 Detailed Listings")
st.dataframe(filtered_df)
