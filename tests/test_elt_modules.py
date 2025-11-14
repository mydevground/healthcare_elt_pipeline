import pandas as pd
import pytest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.transform import clean_providers, clean_patients, clean_claims, align_columns, convert_timestamps
from src.schema_definitions import PROVIDER_COLUMNS, PATIENT_COLUMNS, CLAIM_COLUMNS

def test_align_columns_adds_missing_and_others_correctly():
    df = pd.DataFrame({"Name": ["Dr. A"], "ProviderID": ["P001"]})
    aligned = align_columns(df, PROVIDER_COLUMNS)
    assert aligned.columns.tolist() == PROVIDER_COLUMNS
    assert aligned["ProviderID"].iloc[0] == "P001"

def test_convert_timestamps_handles_mixed_types():
    df = pd.DataFrame({
        "DateOfBirth": [pd.Timestamp("1990-01-01"), "2000-02-02", None]
    })
    converted = convert_timestamps(df, ["DateOfBirth"])
    assert converted["DateOfBirth"].iloc[0] == "1990-01-01T00:00:00"
    assert converted["DateOfBirth"].iloc[1] == "2000-02-02T00:00:00"
    assert pd.isna(converted["DateOfBirth"].iloc[2]) or converted["DateOfBirth"].iloc[2] is None

def test_clean_providers_removes_null_and_duplicates():
    df = pd.DataFrame({
        "ProviderID": ["P001", None, "P001"],
        "Name": ["Dr. A", "Dr. B", "Dr. A"]
    })
    cleaned = clean_providers(df)
    assert len(cleaned) == 1
    assert cleaned["ProviderID"].iloc[0] == "P001"

def test_clean_patients_converts_dob_and_removes_nulls():
    df = pd.DataFrame({
        "PatientID": ["PT001", None],
        "Name": ["Alice", "Bob"],
        "DateOfBirth": [pd.Timestamp("1985-05-05"), "1990-06-06"]
    })
    cleaned = clean_patients(df)
    assert len(cleaned) == 1
    assert cleaned["DateOfBirth"].iloc[0] == "1985-05-05T00:00:00"

def test_clean_claims_drops_invalid_and_logs(caplog):
    df = pd.DataFrame({
        "ClaimID": ["C001", "C002", "C003"],
        "PatientID": ["PT001", None, "PT003"],
        "ProviderID": ["PR001", "PR002", None],
        "ServiceDate": [pd.Timestamp("2025-01-01"), "2025-02-02", "2025-03-03"],
        "ClaimAmount": [100, 200, 300],
        "Status": ["Approved", "Denied", "Pending"]
    })
    cleaned, dropped = clean_claims(df)
    assert len(cleaned) == 1
    assert dropped == 2
    assert "claim_dropped" in caplog.text
    assert "Missing PatientID or ProviderID" in caplog.text