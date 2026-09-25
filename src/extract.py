import pandas as pd
from src.config import Config
import logging
from typing import Optional

logger = logging.getLogger("etl_pipeline")

def extract_raw_data(source_path: Optional[str] = None) -> pd.DataFrame:
    path = source_path or Config.RAW_DATA_PATH
    logger.info(f"Extracting raw dataset from {path}...")
    # Read with string types initially to avoid silent cast failures
    df = pd.read_csv(path, dtype=str)
    logger.info(f"Extracted {len(df):,} records with {len(df.columns)} columns.")
    return df