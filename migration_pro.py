import os
import sys
import csv
import json
import time
import logging
import argparse
import configparser
import threading
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import oracledb
import psycopg2
from psycopg2 import extras
from tqdm import tqdm

# --- Global Configuration ---
TEMP_DIR = "temp_data"
LOG_DIR = "logs"
STATUS_FILENAME = "migration_status.json"
ORACLE_TO_POSTGRES_TYPEMAP = {
    'VARCHAR2': 'TEXT', 'NVARCHAR2': 'TEXT', 'CHAR': 'CHAR', 'NCHAR': 'CHAR',
    'CLOB': 'TEXT', 'NCLOB': 'TEXT', 'NUMBER': 'NUMERIC', 'FLOAT': 'DOUBLE PRECISION',
    'BINARY_FLOAT': 'REAL', 'BINARY_DOUBLE': 'DOUBLE PRECISION', 'DATE': 'TIMESTAMP',
    'TIMESTAMP': 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE': 'TIMESTAMP WITH TIME ZONE',
    'TIMESTAMP WITH LOCAL TIME ZONE': 'TIMESTAMP WITH TIME ZONE', 'RAW': 'BYTEA', 'BLOB': 'BYTEA'
}
status_lock = threading.Lock()

# --- 1. Setup and Initialization ---

def setup_logging():
    """Configures logging to both console and a file with a timestamp."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_filename = os.path.join(LOG_DIR, f"migration_{datetime.now():%Y-%m-%d_%H-%M-%S}.log")
    
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout) # Also print to console
        ]
    )
    logging.info("Logging initialized.")

def load_config(path='config.ini'):
    """Loads configuration from the INI file."""
    if not os.path.exists(path):
        logging.error(f"Configuration file not found: {path}")
        sys.exit(1)
    config = configparser.ConfigParser()
    config.read(path)
    
    # Initialize Oracle Client if specified
    client_lib_dir = config.get('ORACLE', 'CLIENT_LIB_DIR', fallback=None)
    if client_lib_dir and os.path.isdir(client_lib_dir):
        try:
            oracledb.init_oracle_client(lib_dir=client_lib_dir)
            logging.info(f"Oracle Instant Client initialized from: {client_lib_dir}")
        except Exception as e:
            logging.error(f"Failed to initialize Oracle Client: {e}")
            sys.exit(1)
            
    return config

def load_or_create_status():
    """Loads the migration status file, or creates a new one if it doesn't exist."""
    if os.path.exists(STATUS_FILENAME):
        logging.info(f"Found existing status file: {STATUS_FILENAME}")
        with open(STATUS_FILENAME, 'r') as f:
            return json.load(f)
    logging.info("No status file found, creating a new one.")
    return {}

def save_status(status_data):
    """Saves the current migration status to the JSON file thread-safely."""
    with status_lock:
        with open(STATUS_FILENAME, 'w') as f:
            json.dump(status_data, f, indent=4)

def parse_input_file(file_path):
    """Parses the CSV input file to get table names and their ordering keys."""
    if not os.path.exists(file_path):
        logging.error(f"Input file not found: {file_path}")
        sys.exit(1)
    
    tables_to_migrate = []
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].strip().startswith('#'): # Skip empty or commented lines
                continue
            table_name = row[0].strip()
            keys = [k.strip() for k in row[1:] if k.strip()]
            if not table_name or not keys:
                logging.warning(f"Skipping invalid row in {file_path}: {row}")
                continue
            tables_to_migrate.append({'name': table_name, 'keys': keys})
    logging.info(f"Found {len(tables_to_migrate)} tables to migrate in {file_path}.")
    return tables_to_migrate

# --- 2. Resilient Connection Handling ---

def get_db_connection(config, db_type, max_retries=5, delay=5, chunk_num=None):
    """Establishes a database connection with retry logic."""
    attempt = 0
    while attempt < max_retries:
        try:
            if db_type == 'oracle':
                dsn = f"{config['ORACLE']['HOST']}:{config['ORACLE']['PORT']}/{config['ORACLE']['SERVICE_NAME']}"
                conn = oracledb.connect(user=config['ORACLE']['USER'], password=config['ORACLE']['PASSWORD'], dsn=dsn)
                if chunk_num != None:
                    logging.info(f"Successfully connected to Oracle for chunk:{chunk_num}")
                else:
                    logging.info(f"Successfully connected to Oracle.")
                return conn
            elif db_type == 'postgres':
                conn = psycopg2.connect(
                    host=config['POSTGRES']['HOST'], port=config['POSTGRES']['PORT'],
                    dbname=config['POSTGRES']['DBNAME'], user=config['POSTGRES']['USER'],
                    password=config['POSTGRES']['PASSWORD']
                )
                if chunk_num != None:
                    logging.info(f"Successfully connected to PostgreSQL for chunk:{chunk_num}")
                else:
                    logging.info(f"Successfully connected to PostgreSQL.")
                return conn
        except (oracledb.DatabaseError, psycopg2.OperationalError) as e:
            attempt += 1
            logging.warning(f"Connection to {db_type} failed (attempt {attempt}/{max_retries}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
    logging.error(f"Could not connect to {db_type} after {max_retries} attempts.")
    return None

# --- 3. Schema and Metadata Functions ---

def get_oracle_table_metadata(ora_conn, schema_name, table_name):
    """Fetches column definitions and total row count from Oracle."""
    cursor = ora_conn.cursor()
    full_table_name = f"{schema_name.upper()}.{table_name.upper()}"
    
    # Get column schema
    query_cols = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :1 AND TABLE_NAME = :2
        ORDER BY COLUMN_ID
    """
    cursor.execute(query_cols, [schema_name.upper(), table_name.upper()])
    columns = []
    for row in cursor.fetchall():
        col_name, data_type, precision, scale, char_len = row
        pg_type = ORACLE_TO_POSTGRES_TYPEMAP.get(data_type, 'TEXT')
        if data_type == 'NUMBER' and precision is not None:
            pg_type = f"NUMERIC({precision}, {scale or 0})"
        columns.append({'name': col_name, 'pg_type': pg_type})
    
    if not columns:
        raise ValueError(f"Table not found or has no columns: {full_table_name}")

    # Get total row count
    cursor.execute(f"SELECT COUNT(1) FROM {full_table_name}")
    row_count = cursor.fetchone()[0]
    cursor.close()
    
    return columns, row_count

def create_postgres_table(pg_conn, schema_name, table_name, columns):
    """Creates the table in PostgreSQL, dropping it if it exists."""
    pg_cursor = pg_conn.cursor()
    pg_table_name = f"{schema_name.lower()}.{table_name.lower()}"
    
    pg_cols_defs = [f'"{col["name"].lower()}" {col["pg_type"]}' for col in columns]
    drop_sql = f"DROP TABLE IF EXISTS {pg_table_name};"
    create_sql = f"CREATE TABLE {pg_table_name} ({', '.join(pg_cols_defs)});"

    logging.info(f"Recreating table '{pg_table_name}' in PostgreSQL.")
    logging.debug(f"Executing: {drop_sql}")
    pg_cursor.execute(drop_sql)
    logging.debug(f"Executing: {create_sql}")
    pg_cursor.execute(create_sql)

    pg_conn.commit()
    pg_cursor.close()


# --- 4. Producer-Consumer Core Logic ---
def producer_task(config, schema_name, table_info, chunk_num, chunk_size, total_rows):
    """
    Fetches a single chunk of data from Oracle and saves it to a file.
    This version uses the ROW_NUMBER() approach for compatibility with older Oracle DBs.
    """
    table_name = table_info['name']
    order_by_keys = ', '.join([f'"{k}"' for k in table_info['keys']])
    columns = table_info['columns']
    col_names = ', '.join([f'"{c["name"]}"' for c in columns])

    # --- CHANGE 1: Calculate start and end rows instead of offset ---
    # Calculate the row range for the current chunk number.
    # For chunk 0, this will be 1 to chunk_size.
    # For chunk 1, this will be chunk_size + 1 to 2 * chunk_size, and so on.
    start_row = chunk_num * chunk_size + 1
    end_row = (chunk_num + 1) * chunk_size

    # --- CHANGE 2: Use the ROW_NUMBER() query ---
    # This query is more compatible with Oracle versions before 12c.
    chunk_query = f"""
        SELECT {col_names} FROM (
            SELECT
                {col_names},
                ROW_NUMBER() OVER (ORDER BY {order_by_keys}) AS rn
            FROM {schema_name.upper()}.{table_name.upper()}
        )
        WHERE rn BETWEEN :start_row AND :end_row
    """

    ora_conn = get_db_connection(config, 'oracle', chunk_num = chunk_num)
    if not ora_conn:
        raise ConnectionError("Producer failed to connect to Oracle.")

    try:
        cursor = ora_conn.cursor()
        # --- CHANGE 3: Update the bind variables for the new query ---
        cursor.execute(chunk_query, start_row=start_row, end_row=end_row)
        rows = cursor.fetchall()
        cursor.close()

        if rows:
            chunk_dir = os.path.join(TEMP_DIR, table_name.lower())
            os.makedirs(chunk_dir, exist_ok=True)
            chunk_filepath = os.path.join(chunk_dir, f"chunk_{chunk_num}.pkl")

            with open(chunk_filepath, 'wb') as f:
                pickle.dump(rows, f)
            return chunk_num, len(rows)
    finally:
        if ora_conn:
            ora_conn.close()
    return chunk_num, 0

def consumer_task(config, table_info, chunk_num):
    """Reads a chunk file and loads its data into PostgreSQL."""
    table_name = table_info['name']
    pg_table_name = table_name.lower()
    columns = table_info['columns']
    
    chunk_filepath = os.path.join(TEMP_DIR, pg_table_name, f"chunk_{chunk_num}.pkl")
    if not os.path.exists(chunk_filepath):
        logging.warning(f"Chunk file not found, skipping: {chunk_filepath}")
        return chunk_num, 0

    with open(chunk_filepath, 'rb') as f:
        rows = pickle.load(f)

    if not rows:
        os.remove(chunk_filepath) # Clean up empty file
        return chunk_num, 0

    pg_conn = get_db_connection(config, 'postgres')
    if not pg_conn:
        raise ConnectionError("Consumer failed to connect to Postgres.")
    
    try:
        cursor = pg_conn.cursor()
        pg_cols_names = ', '.join([f'"{col["name"].lower()}"' for col in columns])
        insert_sql = f"INSERT INTO {pg_table_name} ({pg_cols_names}) VALUES %s"
        
        extras.execute_values(cursor, insert_sql, rows, page_size=len(rows))
        pg_conn.commit()
        cursor.close()
        
        # Clean up the processed chunk file
        os.remove(chunk_filepath)
        return chunk_num, len(rows)
    except Exception as e:
        logging.error(f"Error inserting chunk {chunk_num} for table {table_name}: {e}")
        pg_conn.rollback()
        raise  # Re-raise to let the main loop know it failed
    finally:
        if pg_conn:
            pg_conn.close()
    return chunk_num, 0


# --- 5. Main Orchestration ---

def process_table(config, schema_name, table_info, status_data):
    """Orchestrates the entire migration process for a single table."""
    table_name = table_info['name']
    table_key = f"{schema_name}.{table_name}"
    logging.info(f"--- Starting migration for table: {table_key} ---")

    # Initialize status for this table if it doesn't exist
    if table_key not in status_data:
        status_data[table_key] = {
            "status": "pending", "total_rows": 0, "total_chunks": 0,
            "fetched_chunks": [], "consumed_chunks": [], "failed_chunks": [],
            "columns": []
        }
    
    table_status = status_data[table_key]

    # --- Step 1: Metadata and Schema Setup (if not already done) ---
    if table_status['status'] == 'pending':
        try:
            ora_conn = get_db_connection(config, 'oracle')
            if not ora_conn: return
            
            columns, row_count = get_oracle_table_metadata(ora_conn, schema_name, table_name)
            table_info['columns'] = columns # Add columns to the main info dict
            table_status['columns'] = columns
            table_status['total_rows'] = row_count
            
            chunk_size = config.getint('SETTINGS', 'CHUNK_SIZE')
            table_status['total_chunks'] = (row_count + chunk_size - 1) // chunk_size
            
            pg_conn = get_db_connection(config, 'postgres')
            if not pg_conn: return
            
            # This will wipe the table, so we reset consumed chunks
            create_postgres_table(pg_conn, schema_name, table_name, columns)
            table_status['consumed_chunks'] = []
            
            table_status['status'] = 'fetching'
            save_status(status_data)
            logging.info(f"Table {table_key}: Schema created. Total rows: {row_count}, Chunks: {table_status['total_chunks']}.")
            ora_conn.close()
            pg_conn.close()
        except (Exception, ValueError) as e:
            logging.error(f"Failed during metadata/schema setup for {table_key}: {e}")
            table_status['status'] = 'failed'
            save_status(status_data)
            return
    else:
        # Load columns from status if resuming
        table_info['columns'] = table_status['columns']
        logging.info(f"Resuming migration for {table_key} from state: {table_status['status']}")

    total_chunks = table_status['total_chunks']
    
    # --- Step 2: Producer Phase (Fetch from Oracle) ---
    if table_status['status'] in ['fetching', 'consuming']:
        chunks_to_fetch = [i for i in range(total_chunks) if i not in table_status['fetched_chunks']]
        if chunks_to_fetch:
            logging.info(f"Starting producer phase for {len(chunks_to_fetch)} chunks...")
            with ThreadPoolExecutor(max_workers=config.getint('SETTINGS', 'PRODUCER_WORKERS'), thread_name_prefix='Producer') as executor:
                futures = {executor.submit(producer_task, config, schema_name, table_info, c, config.getint('SETTINGS', 'CHUNK_SIZE'), table_status['total_rows']): c for c in chunks_to_fetch}
                
                with tqdm(total=len(chunks_to_fetch), desc=f"Fetching {table_name}") as pbar:
                    for future in as_completed(futures):
                        try:
                            chunk_num, rows_fetched = future.result()
                            if rows_fetched > 0:
                                table_status['fetched_chunks'].append(chunk_num)
                                save_status(status_data)
                        except Exception as e:
                            logging.error(f"A producer task failed for chunk {futures[future]}: {e}")
                        pbar.update(1)
            logging.info("Producer phase finished.")
        else:
            logging.info("All chunks already fetched. Skipping producer phase.")
        
        table_status['status'] = 'consuming'
        save_status(status_data)

    # --- Step 3: Consumer Phase (Load to Postgres) ---
    if table_status['status'] == 'consuming':
        chunks_to_consume = sorted([c for c in table_status['fetched_chunks'] if c not in table_status['consumed_chunks']])
        if chunks_to_consume:
            logging.info(f"Starting consumer phase for {len(chunks_to_consume)} chunks...")
            with ThreadPoolExecutor(max_workers=config.getint('SETTINGS', 'CONSUMER_WORKERS'), thread_name_prefix='Consumer') as executor:
                futures = {executor.submit(consumer_task, config, table_info, c): c for c in chunks_to_consume}
                
                with tqdm(total=len(chunks_to_consume), desc=f"Loading {table_name}") as pbar:
                    for future in as_completed(futures):
                        try:
                            chunk_num, rows_consumed = future.result()
                            if rows_consumed >= 0: # 0 is a valid success case for empty chunks
                                table_status['consumed_chunks'].append(chunk_num)
                                save_status(status_data)
                        except Exception as e:
                            logging.error(f"A consumer task failed for chunk {futures[future]}: {e}")
                        pbar.update(1)
            logging.info("Consumer phase finished.")
        else:
            logging.info("All fetched chunks already consumed.")

    # --- Step 4: Final Verification ---
    if len(table_status['consumed_chunks']) == total_chunks:
        table_status['status'] = 'completed'
        logging.info(f"+++ Successfully completed migration for table: {table_key} +++")
    else:
        table_status['status'] = 'failed'
        logging.error(f"--- Migration failed for {table_key}. Fetched: {len(table_status['fetched_chunks'])}/{total_chunks}, Consumed: {len(table_status['consumed_chunks'])}/{total_chunks} ---")
    save_status(status_data)


def main():
    setup_logging()
    
    parser = argparse.ArgumentParser(description="A resilient, parallel tool to migrate tables from Oracle to PostgreSQL.")
    parser.add_argument("schema", help="The source Oracle schema name (e.g., HR).")
    parser.add_argument("input_file", help="Path to the CSV file listing tables and their keys.")
    args = parser.parse_args()

    config = load_config()
    status_data = load_or_create_status()
    tables_to_migrate = parse_input_file(args.input_file)
    
    os.makedirs(TEMP_DIR, exist_ok=True)

    logging.info("Starting migration process...")
    for table_info in tables_to_migrate:
        table_key = f"{args.schema}.{table_info['name']}"
        if status_data.get(table_key, {}).get('status') == 'completed':
            logging.info(f"Skipping already completed table: {table_key}")
            continue
        process_table(config, args.schema, table_info, status_data)

    logging.info("All specified tasks are finished. Check logs and status file for details.")


if __name__ == "__main__":
    main()
