import pandas as pd
import logging
import datetime
from src.schema_definitions import PROVIDER_COLUMNS, PATIENT_COLUMNS, CLAIM_COLUMNS

def align_columns(df: pd.DataFrame, expected_cols: list) -> pd.DataFrame:
    """
    Ensures the DataFrame has all expected columns in correct order.
    Missing columns are filled with None. Extra columns are dropped.
    """
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    return df[expected_cols]

# def convert_timestamps(df: pd.DataFrame, date_columns: list) -> pd.DataFrame:
#     """
#     Converts pandas Timestamp or datetime columns to ISO strings for SQLite compatibility.
#     Skips conversion for existing strings or nulls.
#     """
#     for col in date_columns:
#         if col in df.columns:
#             df[col] = pd.to_datetime(df[col], errors="coerce").apply(
#                 lambda x: x.isoformat() if isinstance(x, (pd.Timestamp, datetime.datetime)) else x
#             )
#     return df

# def convert_timestamps(df: pd.DataFrame, date_columns: list) -> pd.DataFrame:
    # """
    # Converts pandas Timestamp or datetime columns to ISO strings for SQLite compatibility.
    # Skips conversion for existing strings or nulls.
    # Ensures that invalid dates become None, not 'NaT'
    # """
#     for col in date_columns:
#         if col in df.columns:
#             df[col] = pd.to_datetime(df[col], errors="coerce").apply(
#                 lambda x: x.isoformat() if isinstance(x, (pd.Timestamp, datetime.datetime)) else None
#             )
#     return df
def convert_timestamps(df: pd.DataFrame, date_columns: list) -> pd.DataFrame:
    """
    Converts pandas Timestamp or datetime columns to ISO strings for SQLite compatibility.
    Skips conversion for existing strings or nulls.
    Ensures that invalid dates become None, not 'NaT'
    """
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").apply(
                lambda x: x.isoformat() if pd.notnull(x) else None
            )
    return df

def clean_providers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aligns and cleans provider data.
    Drops rows missing ProviderID and removes duplicates.
    """
    df = align_columns(df, PROVIDER_COLUMNS)
    df.dropna(subset=["ProviderID"], inplace=True)
    df = df.drop_duplicates(subset=["ProviderID"])
    return df

def clean_patients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aligns and cleans patient data.
    Drops rows missing PatientID and removes duplicates.
    """
    df = align_columns(df, PATIENT_COLUMNS)
    df = convert_timestamps(df, ["DateOfBirth"])
    df.dropna(subset=["PatientID"], inplace=True)
    df = df.drop_duplicates(subset=["PatientID"])
    return df

def clean_claims(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Aligns and cleans claims data.
    Converts timestamps, drops rows missing PatientID or ProviderID,
    logs dropped rows, and removes duplicates.
    Returns cleaned DataFrame and count of dropped rows.
    """
    df = align_columns(df, CLAIM_COLUMNS)
    df = convert_timestamps(df, ["ServiceDate"])

    # Identify and log invalid rows
    invalid_rows = df[df["PatientID"].isna() | df["ProviderID"].isna()]
    dropped = invalid_rows.shape[0]

    for _, row in invalid_rows.iterrows():
        logging.warning({
            "event": "claim_dropped",
            "reason": "Missing PatientID or ProviderID",
            "claim_id": row.get("ClaimID"),
            "patient_id": row.get("PatientID"),
            "provider_id": row.get("ProviderID"),
            "service_date": row.get("ServiceDate"),
            "status": row.get("Status")
        })

    # Drop and deduplicate
    df = df.dropna(subset=["PatientID", "ProviderID"])
    df = df.drop_duplicates(subset=["ClaimID"])
    return df, dropped