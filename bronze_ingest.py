import requests
import json
from config import API_KEY, BASE_URL, POSTGRES
from utils import get_db_connection, create_table_if_not_exists

#  Define U.S. states once, at the top
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

def fetch_listings(endpoint, params):
    headers = {
        "accept": "application/json",
        "X-Api-Key": API_KEY   #  API_KEY comes from config.py
    }
    response = requests.get(endpoint, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def save_to_bronze(table_name, data):
    conn = get_db_connection(POSTGRES)
    create_table_if_not_exists(conn, table_name)
    with conn.cursor() as cur:
        for record in data:
            cur.execute(
                f"INSERT INTO {table_name} (raw_json) VALUES (%s)",
                [json.dumps(record)]
            )
    conn.commit()
    conn.close()

def run_bronze():
    # Loop over all states for Active Sales Listings
    sales_url = f"{BASE_URL}/sale"
    for state in US_STATES:
        sales_params = {"state": state, "status": "Active", "limit": 50}
        sales_data = fetch_listings(sales_url, sales_params)
        save_to_bronze(f"bronze_sales_{state.lower()}", sales_data)

    # Loop over all states for Active Rental Listings
    rentals_url = f"{BASE_URL}/rental/long-term"
    for state in US_STATES:
        rentals_params = {"state": state, "status": "Active", "limit": 50}
        rentals_data = fetch_listings(rentals_url, rentals_params)
        save_to_bronze(f"bronze_rentals_{state.lower()}", rentals_data)

    print("Bronze ingestion complete for all states!")

if __name__ == "__main__":
    run_bronze()


# To handle Logging & Error Handling issues at the  Bronze Layer
import requests
import json
from config import API_KEY, BASE_URL, POSTGRES
from utils import get_db_connection, create_table_if_not_exists
from logging_config import logger

def fetch_listings(endpoint, params):
    headers = {"accept": "application/json", "X-Api-Key": API_KEY}
    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        logger.info(f"Fetched data from {endpoint} with params {params}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data: {e}")
        return []  # Return empty list instead of crashing

def save_to_bronze(table_name, data):
    if not data:
        logger.warning(f"No data to insert into {table_name}")
        return
    try:
        conn = get_db_connection(POSTGRES)
        create_table_if_not_exists(conn, table_name)
        with conn.cursor() as cur:
            for record in data:
                cur.execute(f"INSERT INTO {table_name} (raw_json) VALUES (%s)", [json.dumps(record)])
        conn.commit()
        logger.info(f"Inserted {len(data)} rows into {table_name}")
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {e}")
    finally:
        conn.close()

def run_bronze():
    try:
        logger.info("Starting Bronze ETL stage...")
        sales_url = f"{BASE_URL}/sale"
        sales_params = {"city": "Austin", "state": "TX", "status": "Active", "limit": 50}
        sales_data = fetch_listings(sales_url, sales_params)
        save_to_bronze("bronze_sales", sales_data)

        rentals_url = f"{BASE_URL}/rental/long-term"
        rentals_params = {"city": "Austin", "state": "TX", "status": "Active", "limit": 50}
        rentals_data = fetch_listings(rentals_url, rentals_params)
        save_to_bronze("bronze_rentals", rentals_data)

        logger.info("Bronze ETL completed successfully.")
    except Exception as e:
        logger.critical(f"Unexpected error in Bronze ETL: {e}", exc_info=True)

if __name__ == "__main__":
    run_bronze()
