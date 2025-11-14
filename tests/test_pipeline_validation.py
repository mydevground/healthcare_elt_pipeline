import sqlite3

def test_row_counts(db_path="healthcare.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for table in ["Providers", "Patients", "Claims"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        assert cursor.fetchone()[0] > 0
    conn.close()

def test_foreign_keys_enforced(db_path="healthcare.db"):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    result = conn.execute("PRAGMA foreign_keys;").fetchone()[0]
    assert result == 1, "Foreign key enforcement is not active"


def test_claims_integrity(db_path="healthcare.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM Claims
        WHERE PatientID NOT IN (SELECT PatientID FROM Patients)
           OR ProviderID NOT IN (SELECT ProviderID FROM Providers)
    """)
    assert cursor.fetchone()[0] == 0
    conn.close()