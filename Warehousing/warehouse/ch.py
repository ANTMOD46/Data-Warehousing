# # from clickhouse_driver import Client
# # import psycopg2
# # from datetime import datetime, date

# # # PostgreSQL Connection Info
# # PG_DB = "warehousing"
# # PG_USER = "warehouse"
# # PG_PASSWORD = "Warehouse59"
# # PG_HOST = "localhost"
# # PG_PORT = "5432"

# # # ClickHouse Connection Info
# # CH_HOST = "localhost"
# # CH_PORT = 9000
# # CH_USER = "default"
# # CH_PASSWORD = ""
# # CH_DB = "market"

# # def ch_client():
# #     """Establishes and returns a ClickHouse client."""
# #     try:
# #         client = Client(
# #             host=CH_HOST,
# #             port=CH_PORT,
# #             user=CH_USER,
# #             password=CH_PASSWORD,
# #             database=CH_DB,
# #             settings={"use_numpy": False},
# #         )
# #         print("Connected to ClickHouse successfully.")
# #         return client
# #     except Exception as e:
# #         print(f"Error connecting to ClickHouse: {e}")
# #         return None

# # def pg_connect():
# #     """Establishes and returns a PostgreSQL connection."""
# #     try:
# #         conn = psycopg2.connect(
# #             dbname=PG_DB,
# #             user=PG_USER,
# #             password=PG_PASSWORD,
# #             host=PG_HOST,
# #             port=PG_PORT,
# #         )
# #         print("Connected to PostgreSQL successfully.")
# #         return conn
# #     except Exception as e:
# #         print(f"Error connecting to PostgreSQL: {e}")
# #         return None

# # def main():
# #     """Main function to perform ETL from PostgreSQL to ClickHouse."""
# #     conn_pg = pg_connect()
# #     if conn_pg is None:
# #         return

# #     client_ch = ch_client()
# #     if client_ch is None:
# #         conn_pg.close()
# #         return

# #     cursor_pg = conn_pg.cursor()

# #     try:
# #         # Fetch data from PostgreSQL. We're explicitly selecting columns to ensure order.
# #         cursor_pg.execute("""
# #             SELECT 
# #                 id, "open", high, low, close, adj_close, volume, "source", load_ts, date_id, symbol_id 
# #             FROM fact_price_daily
# #         """)
# #         rows_pg = cursor_pg.fetchall()
# #         print(f"Fetched {len(rows_pg)} rows from PostgreSQL.")

# #         data_to_insert = []
# #         for row in rows_pg:
# #             # Unpack the row based on the select statement's column order
# #             (
# #                 id, open_price, high_price, low_price, close_price, adj_close,
# #                 volume, source, load_ts, date_id, symbol_id
# #             ) = row

# #             # Data Type Conversions for ClickHouse
# #             # `id`, `date_id`, `symbol_id`: Ensure they are integers (UInt64)
# #             id = int(id) if id is not None else 0
# #             date_id = int(date_id) if date_id is not None else 0
# #             symbol_id = int(symbol_id) if symbol_id is not None else 0

# #             # `volume`: Ensure it's a 64-bit integer
# #             volume = int(volume) if volume is not None else 0

# #             # `load_ts`: Convert to a format ClickHouse's DateTime type expects
# #             # PostgreSQL's TIMESTAMP WITH TIME ZONE might need to be naive.
# #             if load_ts and load_ts.tzinfo is not None:
# #                 load_ts = load_ts.replace(tzinfo=None)

# #             data_to_insert.append((
# #                 id, open_price, high_price, low_price, close_price, adj_close,
# #                 volume, source, load_ts, date_id, symbol_id
# #             ))

# #         if data_to_insert:
# #             print(f"Inserting {len(data_to_insert)} rows into ClickHouse...")
# #             # Use the correct table name and columns, or use the VALUES syntax
# #             client_ch.execute(
# #                 "INSERT INTO fact_price_daily VALUES",
# #                 data_to_insert,
# #                 types_check=True
# #             )
# #             print("Data insertion successful.")
# #         else:
# #             print("No data to insert.")

# #     except Exception as e:
# #         print(f"Error during data migration: {e}")
# #     finally:
# #         # Close all connections
# #         if 'cursor_pg' in locals() and cursor_pg:
# #             cursor_pg.close()
# #         if 'conn_pg' in locals() and conn_pg:
# #             conn_pg.close()
# #         if 'client_ch' in locals() and client_ch:
# #             client_ch.disconnect()
# #         print("Connections closed.")

# # if __name__ == "__main__":
# #     main()


# from clickhouse_driver import Client
# import psycopg2
# from datetime import datetime, date

# # PostgreSQL Connection Info
# PG_DB = "warehousing"
# PG_USER = "warehouse"
# PG_PASSWORD = "Warehouse59"
# PG_HOST = "localhost"
# PG_PORT = "5432"

# # ClickHouse Connection Info
# CH_HOST = "localhost"
# CH_PORT = 9000
# CH_USER = "default"
# CH_PASSWORD = ""
# CH_DB = "market"

# def pg_connect():
#     """Establishes and returns a PostgreSQL connection."""
#     try:
#         conn = psycopg2.connect(
#             dbname=PG_DB,
#             user=PG_USER,
#             password=PG_PASSWORD,
#             host=PG_HOST,
#             port=PG_PORT,
#         )
#         print("Connected to PostgreSQL successfully.")
#         return conn
#     except Exception as e:
#         print(f"Error connecting to PostgreSQL: {e}")
#         return None

# def ch_client():
#     """Establishes and returns a ClickHouse client."""
#     try:
#         client = Client(
#             host=CH_HOST,
#             port=CH_PORT,
#             user=CH_USER,
#             password=CH_PASSWORD,
#             database=CH_DB,
#             settings={"use_numpy": False},
#         )
#         print("Connected to ClickHouse successfully.")
#         return client
#     except Exception as e:
#         print(f"Error connecting to ClickHouse: {e}")
#         return None

# def migrate_table(pg_conn, ch_client, pg_table_name, ch_table_name, column_info):
#     """
#     Migrates data from a single PostgreSQL table to a ClickHouse table.
    
#     Args:
#         pg_conn: The PostgreSQL connection object.
#         ch_client: The ClickHouse client object.
#         pg_table_name (str): The name of the table in PostgreSQL.
#         ch_table_name (str): The name of the table in ClickHouse.
#         column_info (list): A list of tuples with (column_name, data_type_converter_function).
#     """
#     try:
#         cursor_pg = pg_conn.cursor()
        
#         # Build the SELECT query with specific column names
#         select_columns = ", ".join([info[0] for info in column_info])
#         cursor_pg.execute(f"SELECT {select_columns} FROM {pg_table_name}")
#         rows_pg = cursor_pg.fetchall()
        
#         print(f"Fetched {len(rows_pg)} rows from {pg_table_name}.")
        
#         data_to_insert = []
#         for row in rows_pg:
#             # Apply data type converters for each column
#             processed_row = [converter(value) for converter, value in zip([info[1] for info in column_info], row)]
#             data_to_insert.append(processed_row)
            
#         if data_to_insert:
#             print(f"Inserting {len(data_to_insert)} rows into {ch_table_name}...")
#             ch_client.execute(
#                 f"INSERT INTO {ch_table_name} ({select_columns}) VALUES",
#                 data_to_insert,
#                 types_check=True
#             )
#             print("Data insertion successful.")
#         else:
#             print(f"No data to insert for table {ch_table_name}.")

#     except Exception as e:
#         print(f"Error migrating data for table {pg_table_name}: {e}")
#     finally:
#         if cursor_pg:
#             cursor_pg.close()

# # ---
# # Main script execution
# # ---
# if __name__ == "__main__":
#     conn_pg = pg_connect()
#     client_ch = ch_client()

#     if not conn_pg or not client_ch:
#         print("Failed to connect to one of the databases. Exiting.")
#     else:
#         # Define the tables to migrate with their column names and type converters
#         tables_to_migrate = [
#             {
#                 "pg_table": "dim_date", 
#                 "ch_table": "dim_date",
#                 "columns": [
#                     ("id", int), ("date", lambda x: x), ("year", int), ("quarter", int),
#                     ("month", int), ("day", int), ("day_of_week", int), ("is_weekend", int) # Convert bool to int (0 or 1)
#                 ]
#             },
#             {
#                 "pg_table": "dim_symbol", 
#                 "ch_table": "dim_symbol",
#                 "columns": [
#                     ("id", int), ("ticker", str), ("name", str), ("currency", str),
#                     ("is_active", int), ("exchange_id", int), ("sector_id", int)
#                 ]
#             },
#             # Add other tables here following the same structure
#             {
#                 "pg_table": "dim_exchange",
#                 "ch_table": "dim_exchange",
#                 "columns": [("id", int), ("code", str), ("name", str)]
#             },
#             {
#                 "pg_table": "dim_sector",
#                 "ch_table": "dim_sector",
#                 "columns": [("id", int), ("code", str), ("name", str)]
#             },
#             {
#                 "pg_table": "dim_econ_indicator",
#                 "ch_table": "dim_econ_indicator",
#                 "columns": [("id", int), ("code", str), ("name", str), ("unit", str)]
#             },
#             {
#                 "pg_table": "fact_econ_daily",
#                 "ch_table": "fact_econ_daily",
#                 "columns": [
#                     ("id", int), ("value", lambda x: float(x) if x is not None else 0.0),
#                     ("source", str), ("load_ts", lambda x: x.replace(tzinfo=None) if x and x.tzinfo is not None else x),
#                     ("date_id", int), ("indicator_id", int)
#                 ]
#             },
#             {
#                 "pg_table": "fact_price_daily",
#                 "ch_table": "fact_price_daily",
#                 "columns": [
#                     ("id", int), ("open", lambda x: float(x) if x is not None else 0.0), 
#                     ("high", lambda x: float(x) if x is not None else 0.0), 
#                     ("low", lambda x: float(x) if x is not None else 0.0), 
#                     ("close", lambda x: float(x) if x is not None else 0.0), 
#                     ("adj_close", lambda x: float(x) if x is not None else 0.0), 
#                     ("volume", lambda x: int(x) if x is not None else 0), 
#                     ("source", str), 
#                     ("load_ts", lambda x: x.replace(tzinfo=None) if x and x.tzinfo is not None else x), 
#                     ("date_id", int), ("symbol_id", int)
#                 ]
#             }
#         ]

#         # Execute migration for each table in the list
#         for table_config in tables_to_migrate:
#             print(f"\n--- Starting migration for {table_config['pg_table']} ---")
#             migrate_table(
#                 conn_pg,
#                 client_ch,
#                 table_config["pg_table"],
#                 table_config["ch_table"],
#                 table_config["columns"]
#             )

#         # Close connections
#         conn_pg.close()
#         client_ch.disconnect()
#         print("\nAll connections closed.")




from clickhouse_driver import Client
import psycopg2
from datetime import datetime, date

# PostgreSQL Connection Info
PG_DB = "warehousing"
PG_USER = "warehouse"
PG_PASSWORD = "Warehouse59"
PG_HOST = "localhost"
PG_PORT = "5432"

# ClickHouse Connection Info
CH_HOST = "localhost"
CH_PORT = 9000
CH_USER = "default"
CH_PASSWORD = ""
CH_DB = "market"

def pg_connect():
    """Establishes and returns a PostgreSQL connection with autocommit."""
    try:
        conn = psycopg2.connect(
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
        )
        conn.autocommit = True  # Added this line to ensure changes are committed.
        print("Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def ch_client():
    """Establishes and returns a ClickHouse client."""
    try:
        client = Client(
            host=CH_HOST,
            port=CH_PORT,
            user=CH_USER,
            password=CH_PASSWORD,
            database=CH_DB,
            settings={"use_numpy": False},
        )
        print("Connected to ClickHouse successfully.")
        return client
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        return None

def migrate_table(pg_conn, ch_client, pg_table_name, ch_table_name, column_info):
    """
    Migrates data from a single PostgreSQL table to a ClickHouse table.
    """
    cursor_pg = pg_conn.cursor()
    
    try:
        select_columns = ", ".join([info[0] for info in column_info])
        cursor_pg.execute(f"SELECT {select_columns} FROM {pg_table_name}")
        rows_pg = cursor_pg.fetchall()
        
        print(f"Fetched {len(rows_pg)} rows from {pg_table_name}.")
        
        data_to_insert = []
        for row in rows_pg:
            try:
                # Apply data type converters for each column
                processed_row = [converter(value) for converter, value in zip([info[1] for info in column_info], row)]
                data_to_insert.append(processed_row)
            except Exception as e:
                # Print specific error for each row
                print(f"Skipping row due to data conversion error: {e} | Row: {row}")
                continue
                
        if data_to_insert:
            print(f"Inserting {len(data_to_insert)} rows into {ch_table_name}...")
            ch_client.execute(
                f"INSERT INTO {ch_table_name} ({select_columns}) VALUES",
                data_to_insert,
                types_check=True
            )
            print("Data insertion successful.")
        else:
            print(f"No valid data to insert for table {ch_table_name}.")

    except Exception as e:
        print(f"Error migrating data for table {pg_table_name}: {e}")
    finally:
        if cursor_pg:
            cursor_pg.close()

# ---
# Main script execution
# ---
if __name__ == "__main__":
    conn_pg = pg_connect()
    client_ch = ch_client()

    if not conn_pg or not client_ch:
        print("Failed to connect to one of the databases. Exiting.")
    else:
        # Define the tables to migrate with their column names and type converters
        tables_to_migrate = [
            {
                "pg_table": "dim_date",
                "ch_table": "dim_date",
                "columns": [
                    ("id", int), ("date", lambda x: x), ("year", int), ("quarter", int),
                    ("month", int), ("day", int), ("day_of_week", int), ("is_weekend", int)
                ]
            },
            {
                "pg_table": "dim_symbol",
                "ch_table": "dim_symbol",
                "columns": [
                    ("id", int), ("ticker", str), ("name", str), ("currency", str),
                    ("is_active", int), ("exchange_id", int), ("sector_id", int)
                ]
            },
            {
                "pg_table": "dim_exchange",
                "ch_table": "dim_exchange",
                "columns": [("id", int), ("code", str), ("name", str)]
            },
            {
                "pg_table": "dim_sector",
                "ch_table": "dim_sector",
                "columns": [("id", int), ("code", str), ("name", str)]
            },
            {
                "pg_table": "dim_econ_indicator",
                "ch_table": "dim_econ_indicator",
                "columns": [("id", int), ("code", str), ("name", str), ("unit", str)]
            },
            {
                "pg_table": "fact_econ_daily",
                "ch_table": "fact_econ_daily",
                "columns": [
                    ("id", int),
                    ("value", lambda x: float(x) if x is not None else 0.0),
                    ("source", str),
                    ("load_ts", lambda x: x.replace(tzinfo=None) if x and x.tzinfo is not None else x),
                    ("date_id", int),
                    ("indicator_id", int)
                ]
            },
            {
                "pg_table": "fact_price_daily",
                "ch_table": "fact_price_daily",
                "columns": [
                    ("id", int),
                    ("open", lambda x: float(x) if x is not None else 0.0), 
                    ("high", lambda x: float(x) if x is not None else 0.0), 
                    ("low", lambda x: float(x) if x is not None else 0.0), 
                    ("close", lambda x: float(x) if x is not None else 0.0), 
                    ("adj_close", lambda x: float(x) if x is not None else 0.0), 
                    ("volume", lambda x: int(x) if x is not None else 0), 
                    ("source", str), 
                    ("load_ts", lambda x: x.replace(tzinfo=None) if x and x.tzinfo is not None else x), 
                    ("date_id", int), ("symbol_id", int)
                ]
            }
        ]

        # Execute migration for each table in the list
        for table_config in tables_to_migrate:
            print(f"\n--- Starting migration for {table_config['pg_table']} ---")
            migrate_table(
                conn_pg,
                client_ch,
                table_config["pg_table"],
                table_config["ch_table"],
                table_config["columns"]
            )

        # Close connections
        conn_pg.close()
        client_ch.disconnect()
        print("\nAll connections closed.")