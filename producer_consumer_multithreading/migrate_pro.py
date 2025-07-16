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
import queue
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
    """Configures logging to a timestamped file and the console."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_filename = os.path.join(LOG_DIR, f"migration_{datetime.now():%Y-%m-%d_%H-%M-%S}.log")
    
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Logging initialized.")

def load_config(path='config.ini'):
    """Loads configuration and initializes the Oracle client if specified."""
    if not os.path.exists(path):
        logging.error(f"Configuration file not found: {path}")
        sys.exit(1)
    config = configparser.ConfigParser()
    config.read(path)
    
    client_lib_dir = config.get('ORACLE', 'CLIENT_LIB_DIR', fallback=None)
    if client_lib_dir and os.path.isdir(client_lib_dir):
        try:
            oracledb.init_oracle_client(lib_dir=client_lib_dir)
            logging.info(f"Oracle Instant Client initialized from: {client_lib_dir}")
        except Exception as e:
            logging.error(f"Failed to initialize Oracle Client from {client_lib_dir}: {e}")
            # Do not exit, it might still work if the client is in the system path
            
    return config

def load_or_create_status():
    """Loads migration status from a file or creates a new one."""
    if os.path.exists(STATUS_FILENAME):
        logging.info(f"Found existing status file: {STATUS_FILENAME}")
        with open(STATUS_FILENAME, 'r') as f:
            return json.load(f)
    logging.info("No status file found, creating a new one.")
    return {}

def save_status(status_data):
    """Saves the current migration status to a file thread-safely."""
    with status_lock:
        with open(STATUS_FILENAME, 'w') as f:
            json.dump(status_data, f, indent=4)

def parse_input_file(file_path):
    """Parses the CSV input file for table names and their ordering keys."""
    if not os.path.exists(file_path):
        logging.error(f"Input file not found: {file_path}")
        sys.exit(1)
    
    tables_to_migrate = []
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].strip().startswith('#'):
                continue
            table_name = row[0].strip()
            # The rest of the row are keys
            keys = [k.strip() for k in row[1:] if k.strip()]
            if not table_name or not keys:
                logging.warning(f"Skipping invalid row in {file_path} (requires table_name and at least one key): {row}")
                continue
            tables_to_migrate.append({'name': table_name, 'keys': keys})
    logging.info(f"Found {len(tables_to_migrate)} tables to migrate in '{file_path}'.")
    return tables_to_migrate

# --- 2. Resilient Connection Handling ---

def get_db_connection(config, db_type, max_retries=10000, delay=10, chunk_num=None):
    """Establishes a database connection with retry logic for network issues."""
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
    logging.error(f"Could not connect to {db_type} after {max_retries} attempts. Aborting.")
    return None

# --- 3. Schema and Metadata Functions ---

def get_oracle_table_metadata(ora_conn, schema_name, table_name):
    """Fetches column definitions and total row count from Oracle."""
    full_table_name = f"{schema_name.upper()}.{table_name.upper()}"
    with ora_conn.cursor() as cursor:
        query_cols = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :owner AND TABLE_NAME = :table_name
            ORDER BY COLUMN_ID
        """
        cursor.execute(query_cols, owner=schema_name.upper(), table_name=table_name.upper())
        columns = [{'name': col[0], 'type': col[1], 'precision': col[2], 'scale': col[3]} for col in cursor.fetchall()]

        if not columns:
            raise ValueError(f"Table not found or has no columns: {full_table_name}")

        cursor.execute(f"SELECT COUNT(1) FROM {full_table_name}")
        row_count = cursor.fetchone()[0]
    return columns, row_count

def map_oracle_to_postgres_types(columns):
    """Converts Oracle column types to PostgreSQL equivalents."""
    pg_columns = []
    for col in columns:
        data_type = col['type']
        pg_type = ORACLE_TO_POSTGRES_TYPEMAP.get(data_type, 'TEXT')
        if data_type == 'NUMBER' and col['precision'] is not None and col['scale'] is not None:
             if col['precision'] > 0:
                pg_type = f"NUMERIC({col['precision']}, {col['scale']})"
             else: # Handle NUMBER without precision as just NUMERIC
                pg_type = 'NUMERIC'
        pg_columns.append({'name': col['name'], 'pg_type': pg_type})
    return pg_columns

def create_postgres_table(pg_conn, schema_name, table_name, columns):
    """Creates the target table in PostgreSQL, dropping it if it exists for a clean run."""
    pg_table_name = f'"{schema_name.lower()}"."{table_name.lower()}"'
    pg_cols_defs = [f'"{col["name"].lower()}" {col["pg_type"]}' for col in columns]
    
    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name.lower()};")
        logging.info(f"Ensured schema '{schema_name.lower()}' exists in PostgreSQL.")

        drop_sql = f"DROP TABLE IF EXISTS {pg_table_name};"
        create_sql = f"CREATE TABLE {pg_table_name} ({', '.join(pg_cols_defs)});"
        
        logging.info(f"Recreating table '{pg_table_name}' in PostgreSQL.")
        pg_cursor.execute(drop_sql)
        pg_cursor.execute(create_sql)
        pg_conn.commit()

# --- 4. Producer-Consumer Core Logic ---

def producer_task(config, schema_name, table_info, chunk_num, chunk_size, ready_queue, shared_state, status_data):
    """Fetches a single data chunk from Oracle, saves it, and puts its ID on the queue."""
    table_name = table_info['name']
    order_by_keys = table_info['keys']
    columns = table_info['columns']
    col_names = ', '.join([f'"{c["name"]}"' for c in columns])
    order_by_clause = ', '.join([f'"{k}"' for k in order_by_keys])
    table_key = f"{schema_name}.{table_name}"
    table_status = status_data[table_key]  
    query = f"""
        SELECT {col_names} FROM (
            SELECT /*+ PARALLEL */ {col_names}, ROW_NUMBER() OVER (ORDER BY {order_by_clause}) AS rn
            FROM "{schema_name.upper()}"."{table_name.upper()}"
        ) WHERE rn BETWEEN :start_row AND :end_row
    """
    
    start_row = chunk_num * chunk_size + 1
    end_row = (chunk_num + 1) * chunk_size
    
    for attempt in range(shared_state['max_retries'] + 1):
        try:
            with get_db_connection(config, 'oracle',chunk_num=chunk_num) as ora_conn:
                if not ora_conn: raise ConnectionError("Producer failed to connect to Oracle.")
                with ora_conn.cursor() as cursor:
                    cursor.arraysize = chunk_size
                    cursor.execute(query, start_row=start_row, end_row=end_row)
                    rows = cursor.fetchall()
            
            chunk_dir = os.path.join(TEMP_DIR, table_name.lower())
            os.makedirs(chunk_dir, exist_ok=True)
            chunk_filepath = os.path.join(chunk_dir, f"chunk_{chunk_num}.pkl")
            temp_filepath = chunk_filepath + '.tmp'

            with open(temp_filepath, 'wb') as f:
                pickle.dump(rows, f)
            os.rename(temp_filepath, chunk_filepath) # Atomic write
            
            ready_queue.put(chunk_num)
            logging.info(f"Producer SUCCESS: Chunk {chunk_num} ({len(rows)} rows) for {table_name} saved.")
            table_status['fetched_chunks'].append(chunk_num)
            save_status(status_data)
            return chunk_num, "fetched"
        
        except Exception as e:
            logging.warning(f"Producer FAILED on chunk {chunk_num} (attempt {attempt+1}/{shared_state['max_retries']}): {e}")
            if not os.path.exists(chunk_filepath):
                os.remove(chunk_filepath)
            if attempt < shared_state['max_retries'] -1:
                time.sleep(5 * (attempt + 1)) # Exponential backoff
            else:
                logging.error(f"Producer gave up on chunk {chunk_num} for table {table_name} after {shared_state['max_retries']} retries.")
                return chunk_num, "failed"

def consumer_task(config, schema_name, table_info, chunk_num, shared_state, status_data):
    """Reads a chunk file and loads its data into PostgreSQL with retries."""
    table_name = table_info['name']
    pg_table_name = f'"{schema_name.lower()}"."{table_name.lower()}"'
    columns = table_info['pg_columns']
    table_key = f"{schema_name}.{table_name}"
    table_status = status_data[table_key]    

    chunk_filepath = os.path.join(TEMP_DIR, table_name.lower(), f"chunk_{chunk_num}.pkl")

    for attempt in range(shared_state['max_retries']):
        try:
            if not os.path.exists(chunk_filepath):
                logging.warning(f"Consumer SKIPPING: Chunk file not found for chunk {chunk_num}. It might have failed during production.")
                return chunk_num, 0, "skipped"
            
            with open(chunk_filepath, 'rb') as f:
                rows = pickle.load(f)

            if not rows:
                os.remove(chunk_filepath)
                logging.info(f"Consumer SUCCESS: Chunk {chunk_num} was empty. Cleaned up.")
                return chunk_num, 0, "consumed"

            with get_db_connection(config, 'postgres', chunk_num=chunk_num) as pg_conn:
                if not pg_conn: raise ConnectionError("Consumer failed to connect to Postgres.")
                with pg_conn.cursor() as cursor:
                    pg_cols_names = ', '.join([f'"{col["name"].lower()}"' for col in columns])
                    insert_sql = f"INSERT INTO {pg_table_name} ({pg_cols_names}) VALUES %s"
                    extras.execute_values(cursor, insert_sql, rows, page_size=len(rows))
                    pg_conn.commit()
            
            os.remove(chunk_filepath) # Clean up successful chunk
            #table_status['consumed_chunks'].add(chunk_num)
            logging.info(f"Consumer SUCCESS: Chunk {chunk_num} ({len(rows)} rows) loaded into {table_name}.")
            return chunk_num, len(rows), "consumed"

        except Exception as e:
            logging.warning(f"Consumer FAILED on chunk {chunk_num} (attempt {attempt+1}/{shared_state['max_retries']}): {e}")
            time.sleep(5 * (attempt + 1)) # Exponential backoff

    logging.error(f"Consumer gave up on chunk {chunk_num} for table {table_name} after {shared_state['max_retries']} retries.")
    return chunk_num, 0, "failed"

# --- 5. Main Orchestration ---

def consumer_worker(config, schema_name, table_info, work_queue, shared_state, status_data):
    """Worker thread that continuously processes chunks from the queue."""
    while True:
        try:
            table_name = table_info['name']
            table_key = f"{schema_name}.{table_name}"
            table_status = status_data[table_key]
            chunk_num = work_queue.get() # Wait 2s for a new item
            if chunk_num is None: # Sentinel value to stop
                work_queue.put(None) # Propagate sentinel to other workers
                break
            
            time.sleep(2) # User-requested wait
            
            chunk_id, rows_processed, status = consumer_task(config, schema_name, table_info, chunk_num, shared_state, status_data)
            
            with shared_state['lock']:
                if status == 'consumed':
                    shared_state['consumed_chunks'].add(chunk_id)
                    table_status['consumed_chunks'].append(chunk_id)
                    shared_state['pbar'].update(1)
                elif status == 'failed':
                    shared_state['failed_chunks'].add(chunk_id)
                    table_status['failed_chunks'].append(chunk_id)
            save_status(status_data)
            work_queue.task_done()

        except queue.Empty:
            # If the queue is empty, check if producers are done
            if shared_state['producers_finished'].is_set():
                logging.info("Consumer worker exiting: producers are finished and queue is empty.")
                break

def process_table(config, schema_name, table_info, status_data):
    """Orchestrates the migration for a single table."""
    table_name = table_info['name']
    table_key = f"{schema_name}.{table_name}"
    logging.info(f"--- Starting migration for table: {table_key} ---")

    # --- Step 1: Initialize or Resume Status ---
    if table_key not in status_data:
        status_data[table_key] = {"status": "pending", "total_rows": 0, "total_chunks": 0, "fetched_chunks": [], "consumed_chunks": [], "failed_chunks": [], "columns": []}


    table_status = status_data[table_key]

    if status_data[table_key].get('status') == 'failed':
        logging.warning(f"Table {table_key} was in a 'failed' state. Resetting to retry.")
        table_status['status'] =  "pending"
    
    if table_status['status'] == 'pending':
        try:
            with get_db_connection(config, 'oracle') as ora_conn, get_db_connection(config, 'postgres') as pg_conn:
                if not ora_conn or not pg_conn: return
                
                ora_columns, row_count = get_oracle_table_metadata(ora_conn, schema_name, table_name)
                pg_columns = map_oracle_to_postgres_types(ora_columns)
                
                table_info['columns'], table_status['columns'] = ora_columns, ora_columns
                table_info['pg_columns'], table_status['pg_columns'] = pg_columns, pg_columns
                table_status['total_rows'] = row_count
                
                chunk_size = config.getint('SETTINGS', 'CHUNK_SIZE')
                table_status['total_chunks'] = (row_count + chunk_size - 1) // chunk_size if row_count > 0 else 0
                
                create_postgres_table(pg_conn, schema_name, table_name, pg_columns)
                
                table_status['status'] = 'processing'
                save_status(status_data)
                logging.info(f"Table {table_key}: Schema created. Total rows: {row_count}, Chunks: {table_status['total_chunks']}.")
        except Exception as e:
            logging.error(f"Failed during metadata/schema setup for {table_key}: {e}", exc_info=True)
            table_status['status'] = 'failed'
            save_status(status_data)
            return
    else: # Resuming
        table_info['columns'] = table_status['columns']
        table_info['pg_columns'] = table_status['pg_columns']
        logging.info(f"Resuming migration for {table_key}. {len(table_status['consumed_chunks'])}/{table_status['total_chunks']} chunks already processed.")

    if table_status['total_chunks'] == 0:
        table_status['status'] = 'completed'
        save_status(status_data)
        logging.info(f"Table {table_key} is empty. Marked as complete.")
        return

    # --- Step 2: Setup Parallel Processing ---
    ready_to_consume_queue = queue.Queue(maxsize=config.getint('SETTINGS', 'PRODUCER_WORKERS') * 2)
    
    all_chunks = set(range(table_status['total_chunks']))
    consumed_chunks = set(table_status['consumed_chunks'])
    failed_chunks = set(table_status['failed_chunks']) # Failed from previous runs
    
    chunks_to_fetch = sorted(list(all_chunks - consumed_chunks - failed_chunks))
    
    if not chunks_to_fetch:
        logging.info(f"No new chunks to process for {table_key}.")
    
    shared_state = {
        'lock': threading.Lock(),
        'max_retries': config.getint('SETTINGS', 'MAX_RETRIES'),
        'consumed_chunks': consumed_chunks,
        'failed_chunks': failed_chunks,
        'producers_finished': threading.Event(),
        'pbar': tqdm(total=table_status['total_chunks'], initial=len(consumed_chunks), desc=f"Migrating {table_name}", unit="chunk")
    }

    # --- Step 3: Start Consumers ---
    consumer_threads = []
    for i in range(config.getint('SETTINGS', 'CONSUMER_WORKERS')):
        thread = threading.Thread(target=consumer_worker, args=(config, schema_name, table_info, ready_to_consume_queue, shared_state, status_data), name=f"Consumer-{i}", daemon=True)
        thread.start()
        consumer_threads.append(thread)

    # --- Step 4: Start Producers ---
    producer_futures = []
    chunk_size = config.getint('SETTINGS', 'CHUNK_SIZE')
    with ThreadPoolExecutor(max_workers=config.getint('SETTINGS', 'PRODUCER_WORKERS'), thread_name_prefix='Producer') as executor:
        for chunk_num in chunks_to_fetch:
            future = executor.submit(producer_task, config, schema_name, table_info, chunk_num, chunk_size, ready_to_consume_queue, shared_state, status_data)
            producer_futures.append(future)

        for future in as_completed(producer_futures):
            try:
                chunk_id, status = future.result()
                if status == 'failed':
                    with shared_state['lock']:
                        shared_state['failed_chunks'].add(chunk_id)
            except Exception as e:
                logging.error(f"A producer future raised an unexpected exception: {e}", exc_info=True)
    
    # --- Step 5: Shutdown and Finalize ---
    shared_state['producers_finished'].set()

    ready_to_consume_queue.join() # Wait for consumers to process everything in the queue
    ready_to_consume_queue.put(None) # Signal consumers to stop
    for t in consumer_threads:
        t.join() # Wait for consumer threads to terminate

    shared_state['pbar'].close()
    
    if not shared_state['failed_chunks']:
        # Verify all chunks were consumed
        if len(table_status['consumed_chunks']) == table_status['total_chunks']:
            table_status['status'] = 'completed'
            logging.info(f"+++ Successfully completed migration for table: {table_key} +++")
        else:
            table_status['status'] = 'failed'
            logging.error(f"--- Migration INCOMPLETE for {table_key}. Expected {table_status['total_chunks']} chunks, but only {len(table_status['consumed_chunks'])} were consumed successfully. Check logs for producer errors.")
    else:
        table_status['status'] = 'failed'
        logging.error(f"--- Migration FAILED for {table_key}. Failed chunks: {table_status['failed_chunks']} ---")
    
    save_status(status_data)

# --- 6. Main Entry Point ---

def main():
    setup_logging()
    
    parser = argparse.ArgumentParser(description="A resilient, parallel tool to migrate tables from Oracle to PostgreSQL.")
    parser.add_argument("schema", help="The source Oracle schema name (e.g., HR).")
    parser.add_argument("input_file", help="Path to a CSV file listing tables and their ordering keys (e.g., table_name,key1,key2).")
    args = parser.parse_args()

    config = load_config()
    status_data = load_or_create_status()
    tables_to_migrate = parse_input_file(args.input_file)
    
    os.makedirs(TEMP_DIR, exist_ok=True)

    logging.info("Starting migration process...")
    try:
        for table_info in tables_to_migrate:
            table_key = f"{args.schema}.{table_info['name']}"
            if status_data.get(table_key, {}).get('status') == 'completed':
                logging.info(f"Skipping already completed table: {table_key}")
                continue
            try:
                process_table(config, args.schema, table_info, status_data)
            except Exception as e:
                logging.critical(f"A critical unhandled error occurred while processing {table_key}: {e}", exc_info=True)
                # Mark the table as failed so it can be retried on the next run
                if table_key in status_data:
                    status_data[table_key]['status'] = 'failed'
                    save_status(status_data)
        logging.info("All specified tasks are finished. Check logs and status file for details.")
    except KeyboardInterrupt:
        logging.warning("\nCtrl+C detected! Shutting down gracefully. Please wait...")
        sys.exit(0)

    

if __name__ == "__main__":
    main()