import pandas as pd
import numpy as np
import json
import logging
from src.config import Config

logger = logging.getLogger("etl_pipeline")

def validate_data(df: pd.DataFrame):
    logger.info("Starting data validation...")

    rejections = []
    valid_indices = []

    # Set of valid meal codes for fast lookup
    valid_meals = {"Undefined", "SC", "BB", "HB", "FB"}

    for index, row in df.iterrows():
        reasons = []
        
        try:
            adults = int(row.get("adults") or 0)
            children = int(row.get("children") or 0)
            babies = int(row.get("babies") or 0)
            total_guests = adults + children + babies
            
            # No empty bookings, must have at least one guest
            if total_guests <= 0:
                reasons.append("No guests in booking")
        except (ValueError, TypeError):
            reasons.append("Invalid guest count values (must be numbers)")

        adr = row.get("adr")
        meal = row.get("meal")

        if adr is None:
            reasons.append("Missing ADR value")
        else:
            try:
                if float(adr) < 0:
                    reasons.append(f"Negative price ADR: {adr}")
            except (ValueError, TypeError):
                reasons.append(f"Invalid ADR value: {adr}")

        # Valid year
        try:
            year = int(row.get("arrival_date_year", 0))
            if year < 2010 or year > 2027:
                reasons.append(f"Invalid arrival year: {year}")
        except (ValueError, TypeError):
            reasons.append("Invalid arrival year value")

        # Valid meal
        if meal not in valid_meals:
            reasons.append(f"Invalid meal type: {meal}")

        if reasons:
            clean_dict = {k: (None if pd.isna(v) else v) for k, v in row.items()}
            
            rejections.append({
                "index": index,
                "raw_payload": json.dumps(clean_dict, default=str),
                "rejection_reason": "; ".join(reasons)
            })
        else:
            valid_indices.append(index)

    # Save rejected records
    if rejections:
        rejected_df = pd.DataFrame(rejections)
        Config.REJECTED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
        rejected_df.to_csv(Config.REJECTED_DATA_PATH, index=False)
        logger.warning(f"Quarantined {len(rejections)} rejected records to {Config.REJECTED_DATA_PATH}")
    else:
        logger.info("Zero records rejected.")

    valid_df = df.loc[valid_indices].copy()
    return valid_df, rejections