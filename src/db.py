import psycopg2
from sqlalchemy import create_engine
from src.config import Config

def get_engine():
    return create_engine(Config.DATABASE_URI)

def db_connection():
    engine = get_engine()
    
    try:
        with engine.connect() as connection:
            print("Database connection established.")
            return connection
    
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
    