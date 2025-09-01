import psycopg2
from psycopg2.extras import execute_values
from config import POSTGRES
from utils import get_db_connection

def create_gold_tables(conn):
    with conn.cursor() as cur:
        # Gold table for sales insights by state
        cur.execute("""
        CREATE TABLE IF NOT EXISTS gold_sales_summary (
            state TEXT,
            total_listings INT,
            avg_price NUMERIC,
            avg_bedrooms NUMERIC,
            avg_bathrooms NUMERIC,
            avg_sqft NUMERIC
        );
        """)
        
        # Gold table for rental insights by state
        cur.execute("""
        CREATE TABLE IF NOT EXISTS gold_rentals_summary (
            state TEXT,
            total_listings INT,
            avg_price NUMERIC,
            avg_bedrooms NUMERIC,
            avg_bathrooms NUMERIC,
            avg_sqft NUMERIC
        );
        """)
    conn.commit()

def aggregate_to_gold(conn):
    with conn.cursor() as cur:
        # Clear existing data (optional – ensures fresh loads)
        cur.execute("DELETE FROM gold_sales_summary;")
        cur.execute("DELETE FROM gold_rentals_summary;")
        
        # Aggregate sales data from silver
        cur.execute("""
        SELECT 
            state,
            COUNT(*) AS total_listings,
            AVG(price) AS avg_price,
            AVG(bedrooms) AS avg_bedrooms,
            AVG(bathrooms) AS avg_bathrooms,
            AVG(sqft) AS avg_sqft
        FROM silver_sales
        WHERE price IS NOT NULL
        GROUP BY state;
        """)
        sales_summary = cur.fetchall()
        
        if sales_summary:
            execute_values(cur, """
            INSERT INTO gold_sales_summary 
            (state, total_listings, avg_price, avg_bedrooms, avg_bathrooms, avg_sqft)
            VALUES %s
            """, sales_summary)

        # Aggregate rental data from silver
        cur.execute("""
        SELECT 
            state,
            COUNT(*) AS total_listings,
            AVG(price) AS avg_price,
            AVG(bedrooms) AS avg_bedrooms,
            AVG(bathrooms) AS avg_bathrooms,
            AVG(sqft) AS avg_sqft
        FROM silver_rentals
        WHERE price IS NOT NULL
        GROUP BY state;
        """)
        rentals_summary = cur.fetchall()
        
        if rentals_summary:
            execute_values(cur, """
            INSERT INTO gold_rentals_summary 
            (state, total_listings, avg_price, avg_bedrooms, avg_bathrooms, avg_sqft)
            VALUES %s
            """, rentals_summary)
        
    conn.commit()

def run_gold():
    conn = get_db_connection(POSTGRES)
    create_gold_tables(conn)
    aggregate_to_gold(conn)
    conn.close()
    print("Gold aggregation complete!")

if __name__ == "__main__":
    run_gold()

#To handle Logging & Error Handling issues at the Gold Layer
import pandas as pd
from config import POSTGRES
from utils import get_db_connection, create_table_if_not_exists
from logging_config import logger

def load_silver(table_name):
    """Load Silver table into DataFrame"""
    try:
        conn = get_db_connection(POSTGRES)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        logger.info(f"Loaded {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error loading Silver table {table_name}: {e}")
        return pd.DataFrame()

def aggregate_sales(df):
    """Aggregate sales by propertyType"""
    try:
        agg_df = (
            df.groupby("propertyType")
              .agg(avg_price=("price", "mean"),
                   count=("price", "count"))
              .reset_index()
        )
        logger.info(f"Aggregated {len(agg_df)} sales records")
        return agg_df
    except Exception as e:
        logger.error(f"Error aggregating sales data: {e}", exc_info=True)
        return pd.DataFrame()

def aggregate_rentals(df):
    """Aggregate rentals by propertyType"""
    try:
        agg_df = (
            df.groupby("propertyType")
              .agg(avg_rent=("price", "mean"),
                   count=("price", "count"))
              .reset_index()
        )
        logger.info(f"Aggregated {len(agg_df)} rental records")
        return agg_df
    except Exception as e:
        logger.error(f"Error aggregating rental data: {e}", exc_info=True)
        return pd.DataFrame()

def save_to_gold(table_name, df):
    """Save aggregated results into Gold tables"""
    if df.empty:
        logger.warning(f"No data to insert into {table_name}")
        return
    try:
        conn = get_db_connection(POSTGRES)
        cur = conn.cursor()
        create_table_if_not_exists(conn, table_name, df)
        for _, row in df.iterrows():
            cur.execute(
                f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s']*len(df.columns))})",
                tuple(row)
            )
        conn.commit()
        logger.info(f"Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        logger.error(f"Error saving to Gold table {table_name}: {e}", exc_info=True)
    finally:
        conn.close()

def run_gold():
    try:
        logger.info("Starting Gold ETL stage...")

        sales_silver = load_silver("silver_sales")
        gold_sales = aggregate_sales(sales_silver)
        save_to_gold("gold_sales_summary", gold_sales)

        rentals_silver = load_silver("silver_rentals")
        gold_rentals = aggregate_rentals(rentals_silver)
        save_to_gold("gold_rentals_summary", gold_rentals)

        logger.info("Gold ETL completed successfully.")
    except Exception as e:
        logger.critical(f"Unexpected error in Gold ETL: {e}", exc_info=True)

if __name__ == "__main__":
    run_gold()
