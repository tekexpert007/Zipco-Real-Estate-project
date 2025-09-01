import json
import psycopg2
from psycopg2.extras import execute_values
from config import POSTGRES
from utils import get_db_connection

# Define the fields we want to extract from the raw JSON for Silver
SALES_FIELDS = [
    "formattedAddress", 
    "city", 
    "state", 
    "zipCode", 
    "price", 
    "bedrooms", 
    "bathrooms", 
    "squareFootage", 
    "propertyType", 
    "status"
]

RENTAL_FIELDS = SALES_FIELDS + ["listedDate"]

def create_silver_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS silver_sales (
            id SERIAL PRIMARY KEY,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            price NUMERIC,
            bedrooms INT,
            bathrooms NUMERIC,
            sqft INT,
            property_type TEXT,
            status TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS silver_rentals (
            id SERIAL PRIMARY KEY,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            price NUMERIC,
            bedrooms INT,
            bathrooms NUMERIC,
            sqft INT,
            property_type TEXT,
            status TEXT,
            listed_date TIMESTAMP
        );
        """)
    conn.commit()

def transform_table(conn, bronze_table, silver_table, is_rental=False):
    """Read raw bronze JSON and insert into the structured silver table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT raw_json FROM {bronze_table}")
        rows = cur.fetchall()
    
    records = []
    for (raw_json,) in rows:
        if isinstance(raw_json, str):
            record = json.loads(raw_json)
        else:
            record = raw_json  # already JSONB/dict

        if not record:
            continue
        
        # Extract fields safely with .get()
        row = [
            record.get("formattedAddress"),
            record.get("city"),
            record.get("state"),
            record.get("zipCode"),
            record.get("price"),
            record.get("bedrooms"),
            record.get("bathrooms"),
            record.get("squareFootage"),
            record.get("propertyType"),
            record.get("status")
        ]
        if is_rental:
            # Add listedDate for rentals if available
            listed_date = record.get("listedDate")
            row.append(listed_date)
        records.append(row)
    
    if records:
        with conn.cursor() as cur:
            if is_rental:
                insert_query = """
                INSERT INTO silver_rentals 
                (address, city, state, zip_code, price, bedrooms, bathrooms, sqft, property_type, status, listed_date)
                VALUES %s
                """
            else:
                insert_query = """
                INSERT INTO silver_sales 
                (address, city, state, zip_code, price, bedrooms, bathrooms, sqft, property_type, status)
                VALUES %s
                """
            execute_values(cur, insert_query, records)
        conn.commit()

def run_silver():
    conn = get_db_connection(POSTGRES)
    create_silver_tables(conn)
    
    # Process all bronze_sales_* tables
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'bronze_sales_%'")
        bronze_sales_tables = [r[0] for r in cur.fetchall()]
    for table in bronze_sales_tables:
        transform_table(conn, table, "silver_sales", is_rental=False)

    # Process all bronze_rentals_* tables
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'bronze_rentals_%'")
        bronze_rentals_tables = [r[0] for r in cur.fetchall()]
    for table in bronze_rentals_tables:
        transform_table(conn, table, "silver_rentals", is_rental=True)

    conn.close()
    print("Silver transformation complete!")

if __name__ == "__main__":
    run_silver()

#To handle Logging & Error Handling issues at the silver Layer
import json
import psycopg2
import pandas as pd
from config import POSTGRES
from utils import get_db_connection, create_table_if_not_exists
from logging_config import logger

def load_bronze(table_name):
    """Load raw JSON from Bronze table into DataFrame"""
    try:
        conn = get_db_connection(POSTGRES)
        df = pd.read_sql(f"SELECT raw_json FROM {table_name}", conn)
        conn.close()
        logger.info(f"Loaded {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error loading Bronze table {table_name}: {e}")
        return pd.DataFrame()

def transform_sales(df):
    """Transform Bronze sales JSON into structured Silver schema"""
    try:
        records = df["raw_json"].apply(json.loads).tolist()
        sales_df = pd.json_normalize(records)

        # Select and rename important fields
        sales_df = sales_df[[
            "formattedAddress",
            "propertyType",
            "price",
            "status",
            "bedrooms",
            "bathrooms",
            "squareFootage",
            "listingOffice.name"
        ]].rename(columns={
            "formattedAddress": "address",
            "listingOffice.name": "listing_office"
        })

        logger.info(f"Transformed {len(sales_df)} sales records")
        return sales_df
    except Exception as e:
        logger.error(f"Error transforming sales data: {e}", exc_info=True)
        return pd.DataFrame()

def transform_rentals(df):
    """Transform Bronze rentals JSON into structured Silver schema"""
    try:
        records = df["raw_json"].apply(json.loads).tolist()
        rentals_df = pd.json_normalize(records)

        rentals_df = rentals_df[[
            "formattedAddress",
            "propertyType",
            "price",
            "status",
            "bedrooms",
            "bathrooms",
            "squareFootage",
            "listingOffice.name"
        ]].rename(columns={
            "formattedAddress": "address",
            "listingOffice.name": "listing_office"
        })

        logger.info(f"Transformed {len(rentals_df)} rental records")
        return rentals_df
    except Exception as e:
        logger.error(f"Error transforming rental data: {e}", exc_info=True)
        return pd.DataFrame()

def save_to_silver(table_name, df):
    """Save structured data into Silver tables"""
    if df.empty:
        logger.warning(f"No data to insert into {table_name}")
        return
    try:
        conn = get_db_connection(POSTGRES)
        cur = conn.cursor()
        create_table_if_not_exists(conn, table_name, df)  # utils should handle schema
        for _, row in df.iterrows():
            cur.execute(
                f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s']*len(df.columns))})",
                tuple(row)
            )
        conn.commit()
        logger.info(f"Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        logger.error(f"Error saving to Silver table {table_name}: {e}", exc_info=True)
    finally:
        conn.close()

def run_silver():
    try:
        logger.info("Starting Silver ETL stage...")

        sales_bronze = load_bronze("bronze_sales")
        silver_sales = transform_sales(sales_bronze)
        save_to_silver("silver_sales", silver_sales)

        rentals_bronze = load_bronze("bronze_rentals")
        silver_rentals = transform_rentals(rentals_bronze)
        save_to_silver("silver_rentals", silver_rentals)

        logger.info("Silver ETL completed successfully.")
    except Exception as e:
        logger.critical(f"Unexpected error in Silver ETL: {e}", exc_info=True)

if __name__ == "__main__":
    run_silver()

