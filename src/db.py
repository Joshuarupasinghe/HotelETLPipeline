import psycopg2
from src.config import Config
import logging

logger = logging.getLogger("etl_pipeline")

def get_connection():
    return psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        dbname=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD
    )

def init_db(schema_file_path: str):
    logger.info("Initializing database schema...")
    with get_connection() as conn:
        with conn.cursor() as cur:
            with open(schema_file_path, "r") as f:
                cur.execute(f.read())
        conn.commit()
    logger.info("Database schema initialized successfully.")