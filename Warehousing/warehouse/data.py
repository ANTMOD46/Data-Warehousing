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

def pg_connect():
    """Establishes and returns a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
        )
        print("Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def main():
    """Main function to perform ETL from PostgreSQL to ClickHouse."""
    conn_pg = pg_connect()
    if conn_pg is None:
        return

    client_ch = ch_client()
    if client_ch is None:
        conn_pg.close()
        return

    cursor_pg = conn_pg.cursor()

    try:
        # Fetch data from PostgreSQL. We're explicitly selecting columns to ensure order.
        cursor_pg.execute("""
            SELECT 
                id, "open", high, low, close, adj_close, volume, "source", load_ts, date_id, symbol_id 
            FROM fact_price_daily
        """)
        rows_pg = cursor_pg.fetchall()
        print(f"Fetched {len(rows_pg)} rows from PostgreSQL.")

        data_to_insert = []
        for row in rows_pg:
            # Unpack the row based on the select statement's column order
            (
                id, open_price, high_price, low_price, close_price, adj_close,
                volume, source, load_ts, date_id, symbol_id
            ) = row

            # Data Type Conversions for ClickHouse
            # `id`, `date_id`, `symbol_id`: Ensure they are integers (UInt64)
            id = int(id) if id is not None else 0
            date_id = int(date_id) if date_id is not None else 0
            symbol_id = int(symbol_id) if symbol_id is not None else 0

            # `volume`: Ensure it's a 64-bit integer
            volume = int(volume) if volume is not None else 0

            # `load_ts`: Convert to a format ClickHouse's DateTime type expects
            # PostgreSQL's TIMESTAMP WITH TIME ZONE might need to be naive.
            if load_ts and load_ts.tzinfo is not None:
                load_ts = load_ts.replace(tzinfo=None)

            data_to_insert.append((
                id, open_price, high_price, low_price, close_price, adj_close,
                volume, source, load_ts, date_id, symbol_id
            ))

        if data_to_insert:
            print(f"Inserting {len(data_to_insert)} rows into ClickHouse...")
            # Use the correct table name and columns, or use the VALUES syntax
            client_ch.execute(
                "INSERT INTO fact_price_daily VALUES",
                data_to_insert,
                types_check=True
            )
            print("Data insertion successful.")
        else:
            print("No data to insert.")

    except Exception as e:
        print(f"Error during data migration: {e}")
    finally:
        # Close all connections
        if 'cursor_pg' in locals() and cursor_pg:
            cursor_pg.close()
        if 'conn_pg' in locals() and conn_pg:
            conn_pg.close()
        if 'client_ch' in locals() and client_ch:
            client_ch.disconnect()
        print("Connections closed.")

if __name__ == "__main__":
    main()