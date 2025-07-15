import configparser
import csv
import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime
import os
import oracledb
import psycopg2
from psycopg2 import extras
from tqdm import tqdm

# ### CHANGE 1: Use multiprocessing components exclusively ###
from multiprocessing import Process, Queue, Lock, current_process
from logging.handlers import QueueHandler, QueueListener

# --- Global Placeholders (for clarity, not for use by workers) ---
# Queues and locks will be created and managed within main()
# No global objects should be accessed by worker processes.

# --- Data Type Mappings (Unchanged) ---
ORACLE_TO_POSTGRES_TYPEMAP = {
    'VARCHAR2': 'TEXT', 'NVARCHAR2': 'TEXT', 'CHAR': 'CHAR', 'NCHAR': 'CHAR',
    'CLOB': 'TEXT', 'NCLOB': 'TEXT', 'NUMBER': 'NUMERIC', 'FLOAT': 'DOUBLE PRECISION',
    'BINARY_FLOAT': 'REAL', 'BINARY_DOUBLE': 'DOUBLE PRECISION', 'DATE': 'TIMESTAMP',
    'TIMESTAMP': 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE': 'TIMESTAMP WITH TIME ZONE',
    'TIMESTAMP WITH LOCAL TIME ZONE': 'TIMESTAMP WITH TIME ZONE', 'RAW': 'BYTEA', 'BLOB': 'BYTEA'
}

def log_app_restart(environment="DATA-PULLING"):
    # (Unchanged)
    banner = (
        "\n"
        "============================================================\n"
        f"  The Application Has Been Restarted!\n"
        f"  Time       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"  Environment: {environment}\n"
        f"  Status     : Ready to serve requests!\n"
        "============================================================"
    )
    logging.info(banner)

def load_config(path='config_multiprocessing.ini'):
    # (Unchanged)
    config = configparser.ConfigParser()
    if not config.read(path):
        print(f"Error: Configuration file '{path}' not found or is empty.", file=sys.stderr)
        sys.exit(1)
    return config

### CHANGE 2: setup_logging now creates and RETURNS the handler for the listener ###
def setup_logging(log_file_path):
    """Sets up file-based logging and returns the file handler."""
    # This handler will be used by the main process's QueueListener
    file_handler = logging.FileHandler(log_file_path, mode='a')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s'))
    
    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    
    # Silence the noisy oracledb logger unless it's an error
    logging.getLogger("oracledb").setLevel(logging.ERROR)
    
    return file_handler

### CHANGE 3: Helper functions now accept connection/cursor objects ###
# This avoids creating a new connection for every single operation.

def get_source_schema(ora_cursor, schema_name, table_name):
    """Fetches schema using a pre-existing Oracle cursor."""
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
        FROM all_tab_columns
        WHERE OWNER = :owner AND TABLE_NAME = :table_name
        ORDER BY COLUMN_ID
    """
    ora_cursor.execute(query, owner=schema_name.upper(), table_name=table_name.upper())
    columns = []
    for row in ora_cursor.fetchall():
        col_name, data_type, precision, scale = row
        pg_type = ORACLE_TO_POSTGRES_TYPEMAP.get(data_type, 'TEXT')
        if data_type == 'NUMBER' and precision is not None and scale is not None:
             # A scale of -127 indicates a FLOAT, let Postgres handle it as NUMERIC
            if scale > 0 or scale == -127:
                pg_type = "NUMERIC"
            else:
                pg_type = "BIGINT"
        columns.append((col_name, pg_type))
    
    if not columns:
        raise ValueError(f"Table '{table_name}' not found or has no columns in source.")
    return columns

def replicate_table(ora_conn, pg_conn, table_name, oracle_schema, pg_schema, config, process_name):
    """Replicates a single table using pre-existing, long-lived connections."""
    logging.info(f"[{process_name}] Starting replication for table: {table_name}")
    pg_table_name = f'{pg_schema.lower()}.{table_name.lower()}'
    oracle_table_name = f'{oracle_schema.upper()}.{table_name.upper()}'
    output_dir = os.path.join(config['settings']['data_path'], table_name.lower())
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        logging.error(f"[{process_name} | {table_name}] Could not create output directory '{output_dir}': {e}")
        return False, 0 # Indicate failure and 0 rows processed

    try:
        # Use the passed-in connections
        ora_cursor = ora_conn.cursor()
        pg_cursor = pg_conn.cursor()

        # --- Phase 1: Setup ---
        logging.info(f"[{process_name} | {table_name}] Fetching schema and preparing target table...")
        columns = get_source_schema(ora_cursor, oracle_schema, table_name)
        #print(columns)
        pg_cols_defs = [f'"{col[0].lower()}" {col[1]}' for col in columns]
        pg_cursor.execute(f"DROP TABLE IF EXISTS {pg_table_name};")
        create_sql = f"CREATE TABLE {pg_table_name} ({', '.join(pg_cols_defs)});"
        pg_cursor.execute(create_sql)
        pg_conn.commit()
        logging.info(f"[{process_name} | {table_name}] Table created/reset in PostgreSQL.")
        logging.info(f"SELECT COUNT(1) FROM {oracle_table_name}")
        ora_cursor.execute(f"SELECT COUNT(1) FROM {oracle_table_name}")
        logging.info(f"[{process_name} | {table_name}] fetchone")
        total_rows = ora_cursor.fetchone()[0]
        logging.info(f"[{process_name} | {table_name}] fetchone fetch")
        if total_rows == 0:
            logging.info(f"[{process_name} | {table_name}] Source table is empty. Replication considered successful.")
            header_path = os.path.join(output_dir, "header.csv")
            with open(header_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='|')
                writer.writerow(columns)
            return True


        # --- Phase 2: Data Transfer ---
        chunk_size = int(config['settings']['chunk_size'])
        logging.info(f"[{process_name} | {table_name}] chunk size set in array")
        ora_cursor.arraysize = chunk_size
        source_cols = ', '.join([f'"{col[0]}"' for col in columns])
        logging.info(f"[{process_name} | {table_name}] SELECT {source_cols} FROM {oracle_table_name}")
        ora_cursor.execute(f"SELECT {source_cols} FROM {oracle_table_name}")
        logging.info(f"[{process_name} | {table_name}] fetching")
        pg_cols_names = ', '.join([f'"{col[0].lower()}"' for col in columns])
        insert_sql = f"INSERT INTO {pg_table_name} ({pg_cols_names}) VALUES %s"
        chunk_number = 0
        total_fetched = 0
        
        with tqdm(total=total_rows, desc=f"[{process_name}] Copying {table_name}", unit="row", leave=False) as pbar:
            while True:
                
                rows_chunk = ora_cursor.fetchmany(numRows=chunk_size)
                if not rows_chunk:
                    break
                # psycopg2.extras.execute_values is highly optimized
                #extras.execute_values(pg_cursor, insert_sql, rows_chunk, page_size=len(rows_chunk))
                chunk_number += 1
                file_path = os.path.join(output_dir, f'chunk_{chunk_number}.csv')

                # Write the current chunk to its own CSV file
                try:
                    with open(file_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f, delimiter='|')
                        if chunk_number == 1: # Write header only for the first file
                           writer.writerow(columns)
                        writer.writerows(rows_chunk)
                except IOError as e:
                     logging.error(f"[{process_name} | {table_name}] Failed to write to file '{file_path}': {e}")
                     # Depending on requirements, you might want to stop or retry
                     raise # Re-raise the exception to fail the whole table process

                fetched_in_chunk = len(rows_chunk)
                total_fetched += fetched_in_chunk
                pbar.update(fetched_in_chunk)
        
        #pg_conn.commit()
        logging.info(f"[{process_name} | {table_name}] Successfully copied all {total_rows} rows.")
        return True

    except Exception as e:
        logging.error(f"[{process_name} | {table_name}] FATAL ERROR during replication: {e}", exc_info=True)
        # Rollback any partial transaction in Postgres
        #if pg_conn:
            #pg_conn.rollback()
        return False
    finally:
        # Cursors are cheap, close them. Connections are expensive, leave them open.
        #if 'pg_cursor' in locals() and pg_cursor: pg_cursor.close()
        if 'ora_cursor' in locals() and ora_cursor: ora_cursor.close()

def get_table_counts(conn, schema_name, table_name, db_type):
    """Gets counts using a pre-existing connection."""
    full_table_name = f'"{schema_name}"."{table_name}"'
    try:
        cursor = conn.cursor()
        if db_type == 'oracle':
            cursor.execute(f"SELECT COUNT(1) FROM {full_table_name}")
            row_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(1) FROM all_tab_columns WHERE OWNER = :owner AND TABLE_NAME = :table_name",
                           owner=schema_name.upper(), table_name=table_name.upper())
            col_count = cursor.fetchone()[0]
        else: # postgres
            cursor.execute(f"SELECT COUNT(1) FROM {full_table_name}")
            row_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(1) FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                           (schema_name.lower(), table_name.lower()))
            col_count = cursor.fetchone()[0]
        return row_count, col_count
    except Exception as e:
        logging.error(f"[{current_process().name} | {table_name}] Failed to get counts for {db_type}: {e}")
        return -1, -1
    finally:
        if 'cursor' in locals() and cursor: cursor.close()

### CHANGE 4: The completion logger now accepts a process-safe lock ###
def log_completion_status(config, log_data, lock):
    """Appends a detailed record to the CSV using a process-safe lock."""
    # (Function body is the same, but it now uses the passed-in `lock`)
    report_path = config['settings']['completion_report_path_producer']
    fieldnames = ['timestamp', 'schema_name', 'table_name', 'oracle_rows', 'oracle_cols', 'postgres_rows', 'postgres_cols', 'status', 'details']
    
    log_entry = {'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), **log_data}

    with lock:
        try:
            file_exists = False
            with open(report_path, 'r', newline='') as f_check:
                if f_check.readline(): file_exists = True
        except FileNotFoundError:
            file_exists = False

        with open(report_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists: writer.writeheader()
            writer.writerow(log_entry)

def worker(work_queue, results_queue, log_queue, completion_lock, config, completed_tables):
    """Worker process that connects ONCE, then processes tables from the queue."""
    # --- 1. SETUP WORKER ENVIRONMENT ---
    process_name = current_process().name

    # a) Configure logging to send to the main process's listener
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(QueueHandler(log_queue))
    root_logger.setLevel(logging.INFO)

    # b) Initialize Oracle Client for this process
    try:
        oracledb.init_oracle_client(lib_dir=config['oracle']['client_lib_dir'])
    except Exception as e:
        logging.critical(f"[{process_name}] Failed to initialize Oracle Client: {e}")
        return

    # c) ### CHANGE 5: Create long-lived, resilient connections ONCE ###
    ora_conn, pg_conn = None, None
    try:
        logging.info(f"[{process_name}] Establishing database connections...")
        ora_conn = oracledb.connect(user=config['oracle']['user'], password=config['oracle']['password'], dsn=f"{config['oracle']['host']}:{config['oracle']['port']}/{config['oracle']['service_name']}")
        pg_conn = psycopg2.connect(host=config['postgres']['host'], port=config['postgres']['port'], dbname=config['postgres']['db_name'], user=config['postgres']['user'], password=config['postgres']['password'])
        logging.info(f"[{process_name}] Connections established successfully.")
    except Exception as e:
        logging.critical(f"[{process_name}] Could not establish initial DB connections: {e}")
        return # Cannot work without connections

    oracle_schema = config['oracle']['schema']
    pg_schema = config['postgres']['schema']

    # --- 2. PROCESS QUEUE ---
    while True:
        table_name = work_queue.get()
        if table_name is None: break

        if table_name in completed_tables:
            logging.info(f"[{process_name} | {table_name}] Skipping already completed table.")
            results_queue.put({'status': 'SKIPPED', 'table_name': table_name})
            continue
        
        copy_success = replicate_table(ora_conn, pg_conn, table_name, oracle_schema, pg_schema, config, process_name)
        
        if not copy_success:
            results_queue.put({'status': 'FAIL-COPY', 'table_name': table_name})
            log_data = {'schema_name': oracle_schema, 'table_name': table_name, 'status': 'FAILED', 'details': 'Error during table replication process.'}
            log_completion_status(config, log_data, completion_lock)
        else:
            logging.info(f"[{process_name} | {table_name}] Copy complete. Verifying...")
            ora_rows, ora_cols = get_table_counts(ora_conn, oracle_schema, table_name, 'oracle')
            pg_rows, pg_cols = get_table_counts(pg_conn, pg_schema, table_name, 'postgres')

            log_data = {'schema_name': oracle_schema, 'table_name': table_name, 'oracle_rows': ora_rows, 'oracle_cols': ora_cols, 'postgres_rows': pg_rows, 'postgres_cols': pg_cols}

            if ora_rows == pg_rows and ora_cols == pg_cols and ora_rows != -1:
                log_data['status'], log_data['details'] = 'SUCCESS', 'Row & column counts match.'
                results_queue.put({'status': 'OK-VERIFIED', 'table_name': table_name})
                logging.info(f"[{process_name} | {table_name}] VERIFIED SUCCESSFULLY | Oracle({ora_rows}R, {ora_cols}C) vs Postgres({pg_rows}R, {pg_cols}C)")
                # Also log to simple completion log for re-runs
                with completion_lock:
                    with open(config['settings']['completed_log_path_producer'], 'a') as f: f.write(f"{table_name}\n")
            else:
                log_data['status'], log_data['details'] = 'FAILED - MISMATCH', f"Count mismatch! Oracle({ora_rows}R, {ora_cols}C) vs Postgres({pg_rows}R, {pg_cols}C)"
                results_queue.put({'status': 'FAIL-VERIFY', 'table_name': table_name})
                logging.error(f"[{process_name} | {table_name}] VERIFICATION FAILED | Oracle({ora_rows}R, {ora_cols}C) vs Postgres({pg_rows}R, {pg_cols}C)")
            
            log_completion_status(config, log_data, completion_lock)

    # --- 3. CLEANUP ---
    if pg_conn: pg_conn.close()
    if ora_conn: ora_conn.close()
    logging.info(f"[{process_name}] Connections closed. Worker finished.")

def main():
    config = load_config()
    
    # ### CHANGE 6: Capture the file_handler from setup_logging ###
    file_handler = setup_logging(config['settings']['log_file_path_producer'])
    log_app_restart()
    
    # Load tables (unchanged)
    try:
        with open(config['settings']['tables_csv_path'], 'r', newline='') as f:
            all_tables = [row[0] for row in csv.reader(f)][1:]
        logging.info(f"Loaded {len(all_tables)} tables to process from CSV.")
    except FileNotFoundError:
        logging.critical(f"Tables CSV file not found at '{config['settings']['tables_csv_path']}'")
        sys.exit(1)

    completed_tables = set()
    try:
        with open(config['settings']['completed_log_path_producer'], 'r') as f:
            completed_tables = {line.strip() for line in f if line.strip()}
        logging.info(f"Loaded {len(completed_tables)} completed tables from log.")
    except FileNotFoundError:
        logging.info("Completed tables log not found. Starting fresh.")
    
    # ### CHANGE 7: Create all queues and the process-safe Lock here ###
    work_queue = Queue()
    results_queue = Queue()
    log_queue = Queue()
    completion_lock = Lock() # Process-safe lock for file writing

    # ### CHANGE 8: Pass the valid file_handler to the listener ###
    listener = QueueListener(log_queue, file_handler)
    listener.start()

    tasks_to_do = [table for table in all_tables if table not in completed_tables]
    for table in tasks_to_do:
        work_queue.put(table)
    
    num_workers = int(config['settings']['num_workers_producer'])
    processes = []
    print(f"Starting replication with {num_workers} processes for {len(tasks_to_do)} tables...")

    for i in range(num_workers):
        work_queue.put(None) # Sentinel value for each worker
        process = Process(
            target=worker,
            name=f"Worker-{i+1}",
            # ### Pass the process-safe lock to the worker ###
            args=(work_queue, results_queue, log_queue, completion_lock, config, completed_tables)
        )
        processes.append(process)
        process.start()
        
    # Main process manages the progress bar by listening for results
    with tqdm(total=len(all_tables), desc="Overall Progress", unit=" tables") as pbar:
        pbar.update(len(completed_tables))
        for _ in range(len(tasks_to_do)):
            result = results_queue.get()
            pbar.set_postfix_str(f"{result['status']}: {result['table_name']}")
            pbar.update(1)
    
    for p in processes: p.join()
    listener.stop()
    print("\n" * (num_workers + 3))
    print("\nReplication process has finished.")
    logging.info("All worker processes have completed.")

if __name__ == '__main__':
    main()