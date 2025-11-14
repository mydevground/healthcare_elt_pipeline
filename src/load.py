import sqlite3
import logging

def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Providers (
            ProviderID TEXT PRIMARY KEY,
            Name TEXT,
            Specialty TEXT,
            City TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Patients (
            PatientID TEXT PRIMARY KEY,
            Name TEXT,
            DateOfBirth DATE,
            Gender TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Claims (
            ClaimID TEXT PRIMARY KEY,
            PatientID TEXT,
            ProviderID TEXT,
            ServiceDate DATE,
            ClaimAmount NUMERIC,
            Status TEXT,
            FOREIGN KEY(PatientID) REFERENCES Patients(PatientID),
            FOREIGN KEY(ProviderID) REFERENCES Providers(ProviderID)
        )
    """)
    conn.commit()

# def load_data(conn, df, table_name):
#     df.to_sql(table_name, conn, if_exists='replace', index=False)
#     logging.info(f"Loaded {len(df)} rows into {table_name}")

# def upsert_to_data(df, table_name, conn, key_column):
#     cursor = conn.cursor()
#     columns = df.columns.tolist()
#     placeholders = ", ".join(["?"] * len(columns))
#     update_clause = ", ".join([f"{col}=excluded.{col}" for col in columns if col != key_column])

#     sql = f"""
#     INSERT INTO {table_name} ({', '.join(columns)})
#     VALUES ({placeholders})
#     ON CONFLICT({key_column}) DO UPDATE SET
#     {update_clause};
#     """

#     for row in df.itertuples(index=False):
#         cursor.execute(sql, tuple(row))
#     conn.commit()
#     logging.info(f"Upserted {len(df)} rows into {table_name}")

# def upsert_to_data(df, table_name, conn, key_column):
#     cursor = conn.cursor()

#     # Get actual table columns
#     cursor.execute(f"PRAGMA table_info({table_name})")
#     table_cols = [row[1] for row in cursor.fetchall()]

#     # Filter DataFrame to match table columns
#     df = df[[col for col in df.columns if col in table_cols]]

#     # Skip if key column is missing
#     if key_column not in df.columns:
#         print(f"⚠️ Skipping {table_name}: missing key column '{key_column}'")
#         return

#     placeholders = ", ".join(["?"] * len(df.columns))
#     update_clause = ", ".join([f"{col}=excluded.{col}" for col in df.columns if col != key_column])

#     sql = f"""
#     INSERT INTO {table_name} ({', '.join(df.columns)})
#     VALUES ({placeholders})
#     ON CONFLICT({key_column}) DO UPDATE SET
#     {update_clause};
#     """

#     for row in df.itertuples(index=False):
#         try:
#             cursor.execute(sql, tuple(row))
#         except Exception as e:
#             print(f"⚠️ Row skipped due to error: {e}")
#     conn.commit()
#     logging.info(f"Upserted {len(df)} rows into {table_name}")

# def upsert_to_data(df, table_name, conn, key_column):
#     cursor = conn.cursor()

#     # Get existing keys
#     cursor.execute(f"SELECT {key_column} FROM {table_name}")
#     existing_keys = {row[0] for row in cursor.fetchall()}

#     insert_count = 0
#     update_count = 0

#     for _, row in df.iterrows():
#         key = row[key_column]
#         values = tuple(row)
#         placeholders = ", ".join(["?"] * len(row))
#         columns = ", ".join(df.columns)
#         updates = ", ".join([f"{col}=excluded.{col}" for col in df.columns if col != key_column])

#         if key in existing_keys:
#             update_count += 1
#         else:
#             insert_count += 1

#         sql = f"""
#             INSERT INTO {table_name} ({columns})
#             VALUES ({placeholders})
#             ON CONFLICT({key_column}) DO UPDATE SET {updates}
#         """
#         try:
#             cursor.execute(sql, values)
#         except Exception as e:
#             logging.warning(f"Row skipped due to error: {e}")

#     conn.commit()
#     logging.info(f"{table_name}: {insert_count} inserted, {update_count} updated")
#     return insert_count, update_count

# def upsert_to_data(df, table_name, conn, key_column):
#     cursor = conn.cursor()

#     # Fetch existing keys once before upserting
#     cursor.execute(f"SELECT {key_column} FROM {table_name}")
#     existing_keys = {row[0] for row in cursor.fetchall()}

#     insert_count = 0
#     update_count = 0

#     for _, row in df.iterrows():
#         key = row[key_column]
#         values = tuple(row)
#         placeholders = ", ".join(["?"] * len(row))
#         columns = ", ".join(df.columns)
#         updates = ", ".join([f"{col}=excluded.{col}" for col in df.columns if col != key_column])

#         if key in existing_keys:
#             update_count += 1
#         else:
#             insert_count += 1

#         sql = f"""
#             INSERT INTO {table_name} ({columns})
#             VALUES ({placeholders})
#             ON CONFLICT({key_column}) DO UPDATE SET {updates}
#         """
#         try:
#             cursor.execute(sql, values)
#         except Exception as e:
#             logging.warning(f"Row skipped due to error: {e}")

#     conn.commit()
#     logging.info(f"{table_name}: {insert_count} inserted, {update_count} updated")
#     return insert_count, update_count


# def upsert_to_data(df, table_name, conn, key_column):
#     cursor = conn.cursor()

#     insert_count = 0
#     update_count = 0

#     for _, row in df.iterrows():
#         key = row[key_column]
#         values = tuple(row)
#         placeholders = ", ".join(["?"] * len(row))
#         columns = ", ".join(df.columns)
#         updates = ", ".join([f"{col}=excluded.{col}" for col in df.columns if col != key_column])

#         # Check if the key exists in the DB
#         cursor.execute(f"SELECT 1 FROM {table_name} WHERE {key_column} = ?", (key,))
#         exists = cursor.fetchone() is not None

#         if exists:
#             update_count += 1
#         else:
#             insert_count += 1

#         sql = f"""
#             INSERT INTO {table_name} ({columns})
#             VALUES ({placeholders})
#             ON CONFLICT({key_column}) DO UPDATE SET {updates}
#         """
#         try:
#             cursor.execute(sql, values)
#         except Exception as e:
#             logging.warning(f"Row skipped due to error: {e}")

#     conn.commit()
#     logging.info(f"{table_name}: {insert_count} inserted, {update_count} updated")
#     return insert_count, update_count


def upsert_to_data(df, table_name, conn, key_column):
    cursor = conn.cursor()

    insert_count = 0
    update_count = 0

    for _, row in df.iterrows():
        key = row[key_column]
        values = tuple(row)
        placeholders = ", ".join(["?"] * len(row))
        columns = ", ".join(df.columns)
        updates = ", ".join([f"{col}=excluded.{col}" for col in df.columns if col != key_column])

        # Check if the key exists in the DB
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE {key_column} = ?", (key,))
        exists = cursor.fetchone() is not None

        sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT({key_column}) DO UPDATE SET {updates}
        """
        try:
            cursor.execute(sql, values)
            if exists:
                update_count += 1
            else:
                insert_count += 1
        except Exception as e:
            logging.warning(f"Row skipped due to error: {e}")

    conn.commit()
    logging.info(f"{table_name}: {insert_count} inserted, {update_count} updated")
    return insert_count, update_count