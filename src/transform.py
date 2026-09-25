import pandas as pd
import numpy as np
import hashlib
import logging

logger = logging.getLogger("etl_pipeline")

MONTH_MAP = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12
}

def generate_booking_id(row: pd.Series) -> str:
    """Generate a primary key based on the row's data."""
    raw_key = (
            f"{row['hotel']}_{row['arrival_date']}_{row['lead_time']}_"
            f"{row['adults']}_{row['children']}_{row['adr']}_{row['market_segment']}"
    )
    return hashlib.sha256(raw_key.encode('utf-8')).hexdigest()[:16]

def clean_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize null representations across the dataset."""
    return df.replace({"NULL": None, "null": None, "undefined": None, "Undefined": None, "": None})

def standardize_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize casing and strip whitespace for categorical string columns."""
    string_columns = [
        "hotel", "meal", "market_segment", "distribution_channel", 
        "deposit_type", "customer_type", "reservation_status"
    ]
    
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()
            
    if "country" in df.columns:
        df["country"] = df["country"].fillna("UNK").astype(str).str.upper().str.strip()
        
    return df

def apply_categorical_defaults(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing categorical values and map specific term replacements."""
    if "meal" in df.columns:
        df["meal"] = df["meal"].fillna("SC")
    if "market_segment" in df.columns:
        df["market_segment"] = df["market_segment"].fillna("Direct")
    if "distribution_channel" in df.columns:
        df["distribution_channel"] = df["distribution_channel"].fillna("Direct")
    
    # Replace Non Refundable and Refundable with their actual meaning
    if "deposit_type" in df.columns:
        deposit_mapping = {
            "Non Refund": "Paid Total",
            "Non Refundable": "Paid Total",
            "Refundable": "Paid Advance"
        }
        df["deposit_type"] = df["deposit_type"].replace(deposit_mapping)
        
    return df

def standardize_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Cast numeric columns to correct types and handle missing values."""
    numeric_int_cols = [
        "is_canceled", "lead_time", "stays_in_weekend_nights", "stays_in_week_nights",
        "adults", "children", "babies", "is_repeated_guest", "previous_cancellations",
        "previous_bookings_not_canceled", "booking_changes", "required_car_parking_spaces",
        "total_of_special_requests" 
    ]
    
    for col in numeric_int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
    if "adr" in df.columns:
        df["adr"] = pd.to_numeric(df["adr"], errors='coerce').fillna(0.0).round(2)
        
    return df

def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse and combine date components into standardized datetime structures."""
    if set(["arrival_date_year", "arrival_date_month", "arrival_date_day_of_month"]).issubset(df.columns):
        month_num = df["arrival_date_month"].map(MONTH_MAP).fillna(1).astype(int).astype(str).str.zfill(2)
        day_num = df["arrival_date_day_of_month"].astype(str).str.zfill(2)
        
        df["arrival_date"] = pd.to_datetime(
            df["arrival_date_year"].astype(str) + "-" + month_num + "-" + day_num,
            format="%Y-%m-%d",
            errors="coerce"
        )
        # Default fallback for bad dates
        df["arrival_date"] = df["arrival_date"].fillna(pd.Timestamp("2015-01-01")).dt.date

    if "reservation_status_date" in df.columns:
        df["reservation_status_date"] = pd.to_datetime(df["reservation_status_date"], errors="coerce").dt.date
        
    return df

def deduplicate_records(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact row-level duplicates."""
    initial_count = len(df)
    df = df.drop_duplicates()
    logger.info(f"Deduplicated dataset: dropped {initial_count - len(df)} duplicate records.")
    return df

def generate_and_deduplicate_pk(df: pd.DataFrame) -> pd.DataFrame:
    """Generate booking_id and remove any collisions."""
    df["booking_id"] = df.apply(generate_booking_id, axis=1)
    df = df.drop_duplicates(subset=["booking_id"])
    logger.info(f"Final dataset size after primary key deduplication: {len(df)} records.")
    return df

def select_and_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to match target schema and filter unused data."""
    df = df.rename(columns={"agent": "agent_id", "company": "company_id"})
    
    target_columns = [
        "booking_id", "hotel", "is_canceled", "lead_time", "arrival_date",
        "stays_in_weekend_nights", "stays_in_week_nights", "adults", "children", "babies",
        "meal", "country", "market_segment", "distribution_channel", "is_repeated_guest",
        "previous_cancellations", "previous_bookings_not_canceled", "reserved_room_type",
        "assigned_room_type", "booking_changes", "deposit_type", "agent_id", "company_id",
        "customer_type", "adr", "required_car_parking_spaces", "total_of_special_requests",
        "reservation_status", "reservation_status_date"
    ]
    
    # Only select columns that actually exist in the dataframe to prevent KeyError
    existing_target_cols = [col for col in target_columns if col in df.columns]
    return df[existing_target_cols].copy()

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the raw DataFrame into a cleaned and structured format using a pipeline.
    """
    logger.info("Starting data transformation...")
    
    final_df = (
        df.copy()
          .pipe(clean_missing_values)
          .pipe(standardize_strings)
          .pipe(apply_categorical_defaults)
          .pipe(standardize_numerics)
          .pipe(standardize_dates)
          .pipe(deduplicate_records)
          .pipe(generate_and_deduplicate_pk)
          .pipe(select_and_rename_columns)
    )
    
    logger.info(f"Transformation complete. {len(final_df):,} clean rows ready for loading.")
    return final_df