import sys
import argparse
import logging
from src.config import Config
from src.db import init_db
from src.aws_client import S3Client
from src.extract import extract_raw_data
from src.validate import validate_data
from src.transform import transform_data
from src.load import load_data_to_postgres, load_audit_rejections


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s"
)
logger = logging.getLogger("etl_pipeline")

def main(init_schema: bool = False, upload_s3: bool = True):
    logger.info("STARTING HOTEL BOOKING DATA ETL PIPELINE")
    
    # Database Schema Setup
    if init_schema:
        init_db("sql/01_schema.sql")
        
    # Extract
    raw_df = extract_raw_data(str(Config.RAW_DATA_PATH))
    
    # Validate
    valid_df, rejections = validate_data(raw_df)
    load_audit_rejections(rejections)
    
    # Transform
    transformed_df = transform_data(valid_df)
    
    # Save local copy of transformed data
    Config.PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    transformed_df.to_csv(Config.PROCESSED_DATA_PATH, index=False)
    logger.info(f"Transformed data saved to {Config.PROCESSED_DATA_PATH}")
    
    # Load
    load_data_to_postgres(transformed_df)
    
    # Upload cleaned output and Backup to S3
    if upload_s3:
        s3 = S3Client()
        s3.upload_file(str(Config.RAW_DATA_PATH), "raw/hotel_bookings_raw.csv")
        s3.upload_file(str(Config.PROCESSED_DATA_PATH), "processed/hotel_bookings_cleaned.csv")
    
    logger.info("ETL PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Hotel Bookings ETL Pipeline")
    parser.add_argument("--init-db", action="store_true", help="Recreate DB schema before loading")
    parser.add_argument("--skip-s3", action="store_true", help="Skip AWS S3 upload")
    args = parser.parse_args()

    main(init_schema=args.init_db, upload_s3=not args.skip_s3)