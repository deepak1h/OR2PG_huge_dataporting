import configparser
import csv
import logging
import queue
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime
import oracledb
import psycopg2
from psycopg2 import extras
from tqdm import tqdm

# --- Global Objects ---
# Thread-safe queue for distributing table names to worker threads
work_queue = queue.Queue()
# Lock to ensure thread-safe writing to the completed tables log file
completed_file_lock = threading.Lock()
# Main tqdm progress bar for overall progress
main_pbar = None


# --- Data Type Mappings (as before) ---
ORACLE_TO_POSTGRES_TYPEMAP = {
    'VARCHAR2': 'TEXT', 'NVARCHAR2': 'TEXT', 'CHAR': 'CHAR', 'NCHAR': 'CHAR',
    'CLOB': 'TEXT', 'NCLOB': 'TEXT', 'NUMBER': 'NUMERIC', 'FLOAT': 'DOUBLE PRECISION',
    'BINARY_FLOAT': 'REAL', 'BINARY_DOUBLE': 'DOUBLE PRECISION', 'DATE': 'TIMESTAMP',
    'TIMESTAMP': 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE': 'TIMESTAMP WITH TIME ZONE',
    'TIMESTAMP WITH LOCAL TIME ZONE': 'TIMESTAMP WITH TIME ZONE', 'RAW': 'BYTEA', 'BLOB': 'BYTEA'
}



def convert_lobs(row):
    try:
        r=[]
        text=""
        for col in row:
            if isinstance(col, oracledb.LOB):
                if col is None:
                    col = None
                if col.size() > 0:
                    col = col.read()
                else:
                    col=""
            r.append(col)
        return r
    except Exception as e:
        raise Exception(f"Error converting CLOB to text: {str(e)}")



def log_app_restart(environment="DATA-PULLING"):
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

# --- Configuration & Logging Setup ---
def load_config(path='config.ini'):
    """Loads configuration from an INI file."""
    config = configparser.ConfigParser()
    if not config.read(path):
        print(f"Error: Configuration file '{path}' not found or is empty.", file=sys.stderr)
        sys.exit(1)
    return config

def setup_logging(log_file_path):
    """Sets up file-based logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
        filename=log_file_path,
        filemode='a' # Append to the log file
    )
    # Silence the noisy oracledb logger unless it's an error
    logging.getLogger("oracledb").setLevel(logging.ERROR)


# --- Resilient Connection Functions ---
@contextmanager
def resilient_oracle_connection(config, thread_name, schema_name, table_name):
    """A context manager for a resilient Oracle DB connection."""
    conn = None
    retry_delay = int(config['settings']['connection_retry_delay'])
    while conn is None:
        try:
            conn = oracledb.connect(
                user=config['oracle']['user'],
                password=config['oracle']['password'],
                dsn=f"{config['oracle']['host']}:{config['oracle']['port']}/{config['oracle']['service_name']}"
            )
            logging.info(f"[{thread_name}]-[{schema_name.upper()}.{table_name.lower()}] Oracle connection successful.")
            yield conn
            break # Exit loop on success
        except oracledb.DatabaseError as e:
            logging.error(f"Oracle connection failed: {e}. Retrying in {retry_delay}s...")
            print(f"[{thread_name}]-[{schema_name.upper()}.{table_name.lower()}] Oracle connection lost. Waiting to reconnect...")
            time.sleep(retry_delay)
        finally:
            if conn:
                conn.close()
                logging.info(f"[{thread_name}]-[{schema_name.upper()}.{table_name.lower()}] Oracle connection closed ")

@contextmanager
def resilient_postgres_connection(config, thread_name, schema_name, table_name):
    """A context manager for a resilient PostgreSQL DB connection."""
    conn = None
    retry_delay = int(config['settings']['connection_retry_delay'])
    while conn is None:
        try:
            conn = psycopg2.connect(
                host=config['postgres']['host'],
                port=config['postgres']['port'],
                dbname=config['postgres']['db_name'],
                user=config['postgres']['user'],
                password=config['postgres']['password']
            )
            logging.info(f"[{thread_name}]-[{schema_name.upper()}.{table_name.lower()}] PostgreSQL connection successful.")
            yield conn
            break
        except psycopg2.OperationalError as e:
            logging.error(f"PostgreSQL connection failed: {e}. Retrying in {retry_delay}s...")
            print(f"[{thread_name}]-[{schema_name.upper()}.{table_name.lower()}]  PostgreSQL connection lost. Waiting to reconnect...")
            time.sleep(retry_delay)
        finally:
            if conn:
                conn.close()
                logging.info(f"[{thread_name}]-[{schema_name.upper()}.{table_name.lower()}] PostgreSQL connection closed.")


def connect_to_postgres_with_retry(config, thread_name, schema_name, table_name):
    """
    Connects to PostgreSQL, retrying indefinitely on failure.
    Returns the connection object for manual management.
    The caller is responsible for closing the connection.
    """
    retry_delay = int(config['settings']['connection_retry_delay'])
    while True:
        try:
            conn = psycopg2.connect(
                host=config['postgres']['host'],
                port=config['postgres']['port'],
                dbname=config['postgres']['db_name'],
                user=config['postgres']['user'],
                password=config['postgres']['password']
            )
            logging.info(f"[{thread_name}]-[{schema_name.upper()}.{table_name.lower()}] PostgreSQL connection successful.")
            return conn # Return the actual connection object
        except psycopg2.OperationalError as e:
            logging.error(f"[{thread_name}] PostgreSQL connection failed: {e}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)


# --- Core Replication Logic ---

def get_source_schema(source_cursor, schema_name, table_name):
    """Fetches and maps the schema for a given Oracle table."""

    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
        FROM all_tab_columns
        WHERE OWNER = '{schema_name.upper()}' AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY COLUMN_ID
    """

    source_cursor.execute(query)
    columns = []
    has_lob_columns=False
    for row in source_cursor.fetchall():

        col_name, data_type, precision, scale = row
        has_lob_columns = True if (has_lob_columns or data_type in ('CLOB', 'BLOB', 'NCLOB')) else False
        pg_type = ORACLE_TO_POSTGRES_TYPEMAP.get(data_type, 'TEXT')
        if data_type == 'NUMBER' and precision is not None and precision > 0:
            pg_type = f"NUMERIC({precision}, {scale or 0})"

        columns.append((col_name, pg_type))
    
    if not columns:
        raise ValueError(f"Table '{table_name}' not found or has no columns in source.")
    return columns, has_lob_columns


def replicate_table(table_name, oracle_schema, pg_schema, config, thread_id):
    """
    Replicates a single table with resilient PostgreSQL connection handling
    during the data transfer phase.
    """
    thread_name = threading.current_thread().name
    logging.info(f"Starting replication for table: {table_name}")

    pg_conn = None
    pg_cursor = None
    has_lob_columns = False
    # Use the resilient context manager for Oracle, as it should stay open.
    try:
        with resilient_oracle_connection(config, thread_name, oracle_schema, table_name) as ora_conn:
            ora_cursor = ora_conn.cursor()

            # --- Phase 1: Setup (Requires both Oracle and Postgres) ---
            # This part can fail and restart the whole table, which is acceptable.
            logging.info(f"[{table_name}] Fetching schema and preparing target table...")
            columns, has_lob_columns = get_source_schema(ora_cursor, oracle_schema, table_name)
            
            pg_table_name = f"{pg_schema.lower()}.{table_name.lower()}"
            oracle_table_name = f"{oracle_schema.lower()}.{table_name.lower()}"

            # We need a PG connection for the initial table setup.
            # We use your resilient context manager here to ensure we can connect initially.
            with resilient_postgres_connection(config, thread_name, pg_schema, table_name) as pg_conn_setup:
                pg_cursor_setup = pg_conn_setup.cursor()
                pg_cols_defs = [f'"{col[0].lower()}" {col[1]}' for col in columns]
                pg_cursor_setup.execute(f"DROP TABLE IF EXISTS {pg_table_name};")
                create_sql = f"CREATE TABLE {pg_table_name} ({', '.join(pg_cols_defs)});"
                pg_cursor_setup.execute(create_sql)
                pg_conn_setup.commit()
                pg_cursor_setup.close()
            logging.info(f"[{table_name}] Table '{pg_table_name}' created/reset in PostgreSQL.")

            ora_cursor.execute(f"SELECT COUNT(*) FROM {oracle_table_name}")
            total_rows = ora_cursor.fetchone()[0]

            if total_rows == 0:
                logging.info(f"[{oracle_schema.upper()}.{table_name.lower(())}] Source table is empty. Replication complete.")
                log_completion(table_name, config['settings']['completed_log_path'])
                return True

            # --- Phase 2: The Resilient Data Transfer Loop ---
            # This is the critical part that needs to survive PG disconnections.
            
            chunk_size = int(config['settings']['chunk_size'])
            ora_cursor.arraysize = chunk_size
            
            source_cols = ', '.join([f'"{col[0]}"' for col in columns])
            ora_cursor.execute(f"SELECT {source_cols} FROM {oracle_table_name}")

            pg_cols_names = ', '.join([f'"{col[0].lower()}"' for col in columns])
            insert_sql = f"INSERT INTO {pg_table_name} ({pg_cols_names}) VALUES %s"

            rows_chunk = []
            
            # Establish the first PG connection for the copy process.
            pg_conn = connect_to_postgres_with_retry(config, thread_name, pg_schema, table_name)
            pg_cursor = pg_conn.cursor()
            total_copied=0
            with tqdm(total=total_rows, desc=f"Copying {table_name}", unit="row", ncols=100) as pbar:
                for row in ora_cursor:
                    if has_lob_columns:
                        row = convert_lobs(row)
                    rows_chunk.append(row)

                    if len(rows_chunk) >= chunk_size:
                        # ** THE RESILIENT WRITE BLOCK **
                        while True: # Keep trying to write this chunk until it succeeds
                            try:

                                extras.execute_values(pg_cursor, insert_sql, rows_chunk, page_size=chunk_size)
                                pg_conn.commit()
                                
                                # If successful, update progress and break the retry loop
                                pbar.update(len(rows_chunk))
                                total_copied += len(rows_chunk)
                                rows_chunk.clear()
                                if total_copied%1000000 == 0:
                                    logging.info(f"[{table_name}] Successfully copied {total_copied} rows | {round(total_copied*100/total_rows,3)}%")
                                break # Success! Exit the 'while True' loop.

                            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                                logging.error(f"[{table_name}] PG connection lost during write: {e}. "
                                              "The Oracle cursor position is preserved. "
                                              "Attempting to reconnect to PostgreSQL...")
                                print("\n-------CONNECTION LOST..........Connect VPN----------")
                                # Close dead resources first
                                if pg_cursor: pg_cursor.close()
                                if pg_conn: pg_conn.close()

                                # Use your resilient logic to get a new connection
                                pg_conn = connect_to_postgres_with_retry(config, thread_name, pg_schema, table_name)
                                pg_cursor = pg_conn.cursor()
                                logging.info(f"[{table_name}] Reconnected to PostgreSQL. Retrying the failed chunk...")
                                print((f"[{table_name}] Reconnected to PostgreSQL. Retrying the failed chunk..."))
                                # The 'while True' loop will now retry the 'try' block with the new connection

                # After the loop, handle the final partial chunk with the same resilient logic
                if rows_chunk:
                    while True:
                        try:
                            extras.execute_values(pg_cursor, insert_sql, rows_chunk)
                            pg_conn.commit()
                            pbar.update(len(rows_chunk))
                            break
                        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                            logging.error(f"[{table_name}] PG connection lost during final write: {e}. Reconnecting...")
                            if pg_cursor: pg_cursor.close()
                            if pg_conn: pg_conn.close()
                            pg_conn = connect_to_postgres_with_retry(config, thread_name, pg_schema, table_name)
                            pg_cursor = pg_conn.cursor()
                            logging.info(f"[{table_name}] Reconnected to PG. Retrying the final chunk...")

            total_copied = pbar.n
            if total_copied == total_rows:
                logging.info(f"[{table_name}] Successfully copied all {total_copied} rows.")
                log_completion(table_name, config['settings']['completed_log_path'])
            else:
                logging.warning(f"[{table_name}] Copy mismatch. Copied {total_copied}, expected {total_rows}.")
            return True

    except Exception as e:
        # This will now catch Oracle errors or unrecoverable setup errors
        logging.error(f"FATAL UNRECOVERABLE ERROR processing table '{table_name}': {e}", exc_info=True)
        return False
    finally:
        # Final cleanup of the PostgreSQL connection
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            pg_conn.close()
        logging.info(f"Finished replication process for table: {table_name}")


def replicate_table_1(table_name, oracle_schema, pg_schema, config, thread_id):
    """The main logic to replicate a single table, designed to be run in a thread."""
    thread_name = threading.current_thread().name
    logging.info(f"Starting replication for table: {table_name}")

    try:
        with resilient_oracle_connection(config, thread_name, oracle_schema, table_name) as ora_conn, \
             resilient_postgres_connection(config, thread_name, pg_schema, table_name) as pg_conn:
            
            ora_cursor = ora_conn.cursor()
            pg_cursor = pg_conn.cursor()

            # 1. Get schema and create table in Postgres
            logging.info(f"[{table_name}] Fetching schema...")
            columns = get_source_schema(ora_cursor, oracle_schema, table_name)

            pg_table_name = f"{pg_schema.lower()}.{table_name.lower()}"
            oracle_table_name = f"{oracle_schema.lower()}.{table_name.lower()}"

            pg_cols_defs = [f'"{col[0].lower()}" {col[1]}' for col in columns]
            pg_cursor.execute(f"DROP TABLE IF EXISTS {pg_table_name};")
            create_sql = f"CREATE TABLE {pg_table_name} ({', '.join(pg_cols_defs)});"
            pg_cursor.execute(create_sql)
            logging.info(f"[{table_name}] Table '{pg_table_name}' created in PostgreSQL.")

            # 2. Get total row count for the progress bar
            ora_cursor.execute(f"SELECT COUNT(*) FROM {oracle_table_name}")
            total_rows = ora_cursor.fetchone()[0]
            
            if total_rows == 0:
                logging.info(f"[{table_name}] Source table is empty. Replication complete.")
                pg_conn.commit()
                return True # Success

            # 3. Stream data from Oracle and load into Postgres
            chunk_size = int(config['settings']['chunk_size'])
            ora_cursor.arraysize = chunk_size
            
            source_cols = ', '.join([f'"{col[0]}"' for col in columns])
            ora_cursor.execute(f"SELECT {source_cols} FROM {oracle_table_name}")
            
            pg_cols_names = ', '.join([f'"{col[0].lower()}"' for col in columns])
            insert_sql = f"INSERT INTO {pg_table_name} ({pg_cols_names}) VALUES %s"
            
            rows_chunk = []
            total_copied = 0

            # Setup the per-table progress bar
            with tqdm(total=total_rows, desc=f"Copying {table_name}", unit="row", ncols=100) as pbar:
                for row in ora_cursor:
                    rows_chunk.append(row)
                    if len(rows_chunk) >= chunk_size:
                        extras.execute_values(pg_cursor, insert_sql, rows_chunk, page_size=chunk_size)
                        pg_conn.commit()
                        pbar.update(len(rows_chunk))
                        # if total_copied%100000 == 0:
                        #     print(f"{table_name} copied {total_copied}/{total_rows} - round({total_copied*100/total_rows},3)%")
                        if total_copied%1000000 == 0:
                            logging.info(f"[{table_name}] Successfully copied {total_copied} rows | {round(total_copied*100/total_rows,3)}%")

                        total_copied += len(rows_chunk)
                        rows_chunk.clear()

                if rows_chunk: # Insert final partial chunk
                    extras.execute_values(pg_cursor, insert_sql, rows_chunk)
                    pg_conn.commit()
                    total_copied += len(rows_chunk)
                    print(f"{table_name} copied {total_copied}/{total_rows}")
                    pbar.update(len(rows_chunk))
                
                total_copied = pbar.n
        
            # A final log message to confirm completion
            if total_copied == total_rows:
                logging.info(f"[{table_name}] Successfully copied all {total_copied} rows.")
                log_completion(table_name, config['settings']['completed_log_path'])

            else:
                logging.warning(
                    f"[{table_name}] Copy completed. "
                    f"Copied {total_copied} rows, but expected {total_rows}."
                )    

            return True

    except Exception as e:
        logging.error(f"FATAL ERROR processing table '{table_name}': {e}", exc_info=True)
        # We don't rollback pg_conn here, as the connection will be closed by the context manager
        return False




def log_completion(table_name: str, log_path: str):
    """
    Appends a successfully completed table name to the log file.

    Args:
        table_name (str): The name of the table that was successfully processed.
        log_path (str): The path to the completion log file.
    """
    try:
        # Open the file in append mode ('a').
        # This will create the file if it doesn't exist.
        with open(log_path, 'a') as f:
            f.write(f"{table_name}\n")
        logging.info(f"Successfully logged completion for table: {table_name}")
    except IOError as e:
        logging.error(f"CRITICAL: Failed to write to completion log for '{table_name}'. "
                      f"This may cause it to be re-processed next run. Error: {e}")



def get_table_counts(config, schema_name, table_name, db_type):
    """
    Connects to a database and returns the row and column count for a table.
    Returns (row_count, col_count) or (-1, -1) on error.
    """
    thread_name = threading.current_thread().name
    full_table_name = f"{schema_name}.{table_name}"

    try:
        if db_type == 'oracle':
            with resilient_oracle_connection(config, thread_name, schema_name, table_name) as conn:
                cursor = conn.cursor()
                # Get row count
                cursor.execute(f"SELECT COUNT(1) FROM {full_table_name}")
                row_count = cursor.fetchone()[0]
                # Get column count
                cursor.execute(f"""
                    SELECT COUNT(1) FROM all_tab_columns
                    WHERE OWNER = '{schema_name.upper()}' AND TABLE_NAME = '{table_name.upper()}'
                """)

                col_count = cursor.fetchone()[0]
                return row_count, col_count
        
        elif db_type == 'postgres':
            with resilient_postgres_connection(config, thread_name, schema_name, table_name) as conn:
                cursor = conn.cursor()
                # Get row count
                cursor.execute(f'SELECT COUNT(1) FROM {full_table_name}')
                row_count = cursor.fetchone()[0]
                # Get column count
                cursor.execute(f"""
                    SELECT COUNT(1) FROM information_schema.columns
                    WHERE table_schema = '{schema_name.lower()}' AND table_name = '{table_name.lower()}'
                """)
                col_count = cursor.fetchone()[0]
                #print(col_count, row_count)
                return row_count, col_count

    except Exception as e:
        logging.error(f"[{table_name}] Failed to get counts for {db_type}: {e}")

        return -1, -1 # Return error values

def log_completion_status(config, log_data):
    """
    Appends a detailed record to the completion report CSV.
    """
    report_path = config['settings']['completion_report_path']
    fieldnames = [
        'timestamp', 'schema_name', 'table_name', 
        'oracle_rows', 'oracle_cols', 
        'postgres_rows', 'postgres_cols', 
        'status', 'details'
    ]
    
    # Ensure all fields are present
    log_entry = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'schema_name': log_data.get('schema_name', 'N/A'),
        'table_name': log_data.get('table_name', 'N/A'),
        'oracle_rows': log_data.get('oracle_rows', -1),
        'oracle_cols': log_data.get('oracle_cols', -1),
        'postgres_rows': log_data.get('postgres_rows', -1),
        'postgres_cols': log_data.get('postgres_cols', -1),
        'status': log_data.get('status', 'UNKNOWN'),
        'details': log_data.get('details', '')
    }

    with completed_file_lock:
        file_exists = False
        try:
            # Check if file needs a header
            with open(report_path, 'r', newline='') as f_check:
                if f_check.readline():
                    file_exists = True
        except FileNotFoundError:
            file_exists = False

        with open(report_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(log_entry)


# --- Worker Thread and Main Execution ---

def worker(config, completed_tables):
    """
    Worker thread that replicates a table, then verifies it, and logs the detailed result.
    """
    # Note: `thread_id` is not needed for this logic.
    thread_name = threading.current_thread().name
    oracle_schema = config['oracle']['schema']
    pg_schema = config['postgres']['schema']

    while not work_queue.empty():
        try:
            table_name = work_queue.get_nowait()
        except queue.Empty:
            break

        # --- Step 1: Check if already completed (unchanged) ---
        if table_name in completed_tables:
            logging.info(f"[{table_name}] Skipping already completed and verified table.")
            with main_pbar.get_lock():
                main_pbar.update(1)
                main_pbar.set_postfix_str(f"Skipped: {table_name}")
            work_queue.task_done()
            continue
        
        # --- Step 2: Replicate the table data ---
        # The `thread_id` argument to replicate_table is removed as it's no longer needed for tqdm
        copy_success = replicate_table(table_name, oracle_schema, pg_schema, config, oracle_schema)
        
        # --- Step 3: Verify the result and log status ---
        if not copy_success:
            # The copy process itself failed (e.g., SQL error, connection drop during copy).
            with main_pbar.get_lock():
                main_pbar.set_postfix_str(f"FAIL-COPY: {table_name}")
            log_data = {
                'schema_name': oracle_schema, 'table_name': table_name,
                'status': 'FAILED', 'details': 'Error during table replication process.'
            }
            log_completion_status(config, log_data)
        else:
            # The copy process finished, now verify the data integrity.
            logging.info(f"[{thread_name} | {table_name}] Copy complete. Now verifying...")
            
            ora_rows, ora_cols = get_table_counts(config, oracle_schema, table_name, 'oracle')
            pg_rows, pg_cols = get_table_counts(config, pg_schema, table_name, 'postgres')

            log_data = {
                'schema_name': oracle_schema, 'table_name': table_name,
                'oracle_rows': ora_rows, 'oracle_cols': ora_cols,
                'postgres_rows': pg_rows, 'postgres_cols': pg_cols,
            }

            # Determine the final status
            if ora_rows == pg_rows and ora_cols == pg_cols and ora_rows != -1:
                log_data['status'] = 'SUCCESS'
                log_data['details'] = 'Row & column counts match.'
                with main_pbar.get_lock():
                    main_pbar.set_postfix_str(f"OK-VERIFIED: {table_name}")
                logging.info(f"[{thread_name}]-[{table_name}] VERIFIED SUCCESSFULLY | Oracle({ora_rows}R, {ora_cols}C) vs Postgres({pg_rows}R, {pg_cols}C)")
            else:
                log_data['status'] = 'FAILED - MISMATCH'
                log_data['details'] = (
                    f"Count mismatch! Oracle({ora_rows}R, {ora_cols}C) vs "
                    f"Postgres({pg_rows}R, {pg_cols}C)"
                )
                with main_pbar.get_lock():
                    main_pbar.set_postfix_str(f"FAIL-VERIFY: {table_name}")
                logging.ERROR(f"[{thread_name}]-[{table_name}] VERIFICATION FAILED | Oracle({ora_rows}R, {ora_cols}C) vs Postgres({pg_rows}R, {pg_cols}C)")
            
            # Write the detailed log
            log_completion_status(config, log_data)
            

        # Mark the task as done for the queue and update the overall progress bar.
        main_pbar.update(1)
        work_queue.task_done()




def main():
    """Main function to orchestrate the replication process."""

    global main_pbar

    
    config = load_config()
    setup_logging(config['settings']['log_file_path'])
    log_app_restart()
    
    # Initialize Oracle client once at the start
    try:
        oracledb.init_oracle_client(lib_dir=config['oracle']['client_lib_dir'])
    except Exception as e:
        print(f"Error initializing Oracle Client: {e}", file=sys.stderr)
        logging.critical(f"Failed to initialize Oracle Client: {e}")
        sys.exit(1)

    # 1. Load lists of tables to process and those already completed
    try:
        with open(config['settings']['tables_csv_path'], 'r', newline='') as f:
            # Assumes CSV has a header and the first column is the table name
            all_tables = [row[0] for row in csv.reader(f)][1:]
        logging.info(f"Loaded {len(all_tables)} tables to process from CSV.")
    except FileNotFoundError:
        print(f"Error: Tables CSV file not found at '{config['settings']['tables_csv_path']}'", file=sys.stderr)
        sys.exit(1)

    completed_tables = set()
    try:
        with open(config['settings']['completed_log_path'], 'r') as f:
            completed_tables = {line.strip() for line in f if line.strip()}
            #print(len(completed_tables))
        logging.info(f"Loaded {len(completed_tables)} completed tables from log.")
    except FileNotFoundError:
        logging.info("Completed tables log not found. Starting fresh.")
    
    # 2. Populate the work queue
    for table in all_tables:
        work_queue.put(table)
    
    # 3. Setup progress bar and start worker threads
    num_workers = int(config['settings']['num_workers'])
    threads = []

    print(f"Starting replication with {num_workers} workers...")
    
    with tqdm(total=len(all_tables), desc="Overall Progress", unit=" tables", position=num_workers) as pbar:
        main_pbar = pbar
        #main_pbar.update(len(completed_tables)) # Account for already completed tables
        #print(len(completed_tables))

        for i in range(num_workers):
            thread = threading.Thread(
                target=worker,
                name=f"Worker-{i+1}",
                args=(config, completed_tables)
            )
            thread.start()
            threads.append(thread)
        
        # 4. Wait for all work to be done
        for thread in threads:
            thread.join()

    # Move cursor below all progress bars
    print("Replication process has finished.")
    logging.info("All worker threads have completed.")

if __name__ == '__main__':
    main()