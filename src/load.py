import psycopg2
from psycopg2.extras import execute_values
from src.db import get_connection
import pandas as pd
import logging

logger = logging.getLogger("etl_pipeline")

def load_data_to_postgres(df: pd.DataFrame, batch_size: int = 5000):
    logger.info(f"Loading {len(df):,} records into PostgreSQL...")
    
    columns = list(df.columns)
    query = f"""
        INSERT INTO hotel_bookings ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (booking_id) DO UPDATE SET
            is_canceled = EXCLUDED.is_canceled,
            reservation_status = EXCLUDED.reservation_status,
            reservation_status_date = EXCLUDED.reservation_status_date,
            adr = EXCLUDED.adr;
    """

    records = [tuple(x) for x in df.to_numpy()]
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, records, page_size=batch_size)
        conn.commit()
        
    logger.info("Data successfully loaded into PostgreSQL.")

def load_audit_rejections(rejections: list):
    if not rejections:
        return
    logger.info(f"Writing {len(rejections)} rejection logs to database audit table...")
    query = """
        INSERT INTO rejected_records_audit (raw_payload, rejection_reason)
        VALUES %s;
    """
    records = [(r["raw_payload"], r["rejection_reason"]) for r in rejections]
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, records)
        conn.commit()
    logger.info("Rejection audit log updated.")