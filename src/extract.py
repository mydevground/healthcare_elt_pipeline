import pandas as pd
import logging

def read_excel_file(path):
    try:
        df = pd.read_excel(path)
        logging.info(f"Loaded {len(df)} rows from {path}")
        return df
    except Exception as e:
        logging.error(f"Error reading {path}: {e}")
        return pd.DataFrame()