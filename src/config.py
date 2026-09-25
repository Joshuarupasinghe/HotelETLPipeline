import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

class Config:
    # AWS
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

    # Database
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "hotel_reservations")
    DB_USER = os.getenv("DB_USER", "etl_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "etl_password")
    DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Paths
    DATA_DIR = BASE_DIR / "data"
    RAW_DATA_PATH = DATA_DIR / "raw" / "hotel_bookings_raw.csv"
    PROCESSED_DATA_PATH = DATA_DIR / "processed" / "hotel_bookings_cleaned.csv"
    REJECTED_DATA_PATH = DATA_DIR / "rejected" / "rejected_records.csv"
    LOG_DIR = BASE_DIR / "logs"