import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection(config):
    conn = psycopg2.connect(
        host=config["host"],
        port=config["port"],
        database=config["database"],
        user=config["user"],
        password=config["password"]
    )
    return conn

def create_table_if_not_exists(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                raw_json JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        
from config import POSTGRES
from utils import get_db_connection

conn = get_db_connection(POSTGRES)
print("Connected to PostgreSQL!")
conn.close()
