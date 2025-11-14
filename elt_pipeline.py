from src.extract import read_excel_file
from src.transform import clean_providers, clean_patients, clean_claims
from src.load import create_tables, upsert_to_data 
from src.utils import setup_logging
from src.config_loader import load_config
import logging
import sqlite3



def main():

    # Get Config
    config = load_config()
    setup_logging(config['logging']['log_file'], config['logging']['level'])
    
    #Connect to Database
    conn = sqlite3.connect(config['database']['path'])
    conn.execute("PRAGMA foreign_keys = ON;")  # Enforce FK constraints
    create_tables(conn)

    # Extract
    providers_raw = read_excel_file(config['files']['providers'])
    patients_raw = read_excel_file(config['files']['patients'])
    claims_raw = read_excel_file(config['files']['claims'])

    # Transform
    providers_clean = clean_providers(providers_raw)
    patients_clean = clean_patients(patients_raw)
    claims_clean, dropped = clean_claims(claims_raw)

    # Log
    logging.info(f"Providers cleaned: {len(providers_clean)} rows")
    logging.info(f"Patients cleaned: {len(patients_clean)} rows")
    logging.info(f"Claims cleaned: {len(claims_clean)} rows, dropped: {dropped}")

    # Load (Upsert)
    p_inserted, p_updated = upsert_to_data(providers_clean, "Providers", conn, key_column="ProviderID")
    pt_inserted, pt_updated = upsert_to_data(patients_clean, "Patients", conn, key_column="PatientID")
    c_inserted, c_updated = upsert_to_data(claims_clean, "Claims", conn, key_column="ClaimID")

    # Log Summary
    logging.info("ETL Summary:")
    logging.info(f"Providers: {p_inserted} inserted, {p_updated} updated")
    logging.info(f"Patients: {pt_inserted} inserted, {pt_updated} updated")
    logging.info(f"Claims: {c_inserted} inserted, {c_updated} updated")
    logging.info("ETL pipeline completed successfully.")

    
    conn.close()

if __name__ == "__main__":
    main()