import configparser
import csv
import logging
import os
import sys
import time
from datetime import datetime
from multiprocessing import Process, Queue, Lock, current_process
from logging.handlers import QueueHandler, QueueListener

import psycopg2

# --- Configuration & Setup ---

def load_config(path='config_multiprocessing.ini'):
    """Loads configuration from an INI file."""
    config = configparser.ConfigParser()
    if not config.read(path):
        logging.critical(f"Configuration file '{path}' not found or is empty.")
        sys.exit(1)
    return config

def setup_logging(log_file_path):
    """Sets up file-based logging and returns the file handler for the listener."""
    file_handler = logging.FileHandler(log_file_path, mode='a')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s'))
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    
    return file_handler

def load_uploaded_chunks(log_path):
    """Loads the set of already processed chunk file paths from a log."""
    if not os.path.exists(log_path):
        return set()
    with open(log_path, 'r') as f:
        return {line.strip() for line in f if line.strip()}

def find_new_chunks(root_dir, uploaded_chunks_set):
    """Scans the directory for new chunk files that haven't been uploaded."""
    new_chunks = []
    try:
        for table_dir in os.listdir(root_dir):
            table_path = os.path.join(root_dir, table_dir)
            if not os.path.isdir(table_path):
                continue
            
            for filename in os.listdir(table_path):
                if filename.startswith('chunk_') and filename.endswith('.csv'):
                    chunk_path = os.path.join(table_path, filename)
                    if chunk_path not in uploaded_chunks_set:
                        new_chunks.append(chunk_path)
    except FileNotFoundError:
        logging.warning(f"Output directory '{root_dir}' not found. Waiting for it to be created.")
    
    return new_chunks


def connect_to_pg(process_name, config):
    while True:
        try:
            conn = psycopg2.connect(
            host=config['postgres']['host'], port=config['postgres']['port'],
            dbname=config['postgres']['db_name'], user=config['postgres']['user'],
            password=config['postgres']['password']
        )
            logging.info(f"[{process_name}] Successfully connected to PostgreSQL.")
            return conn
        except Exception as e:
            logging.error(f"[{process_name}] PostgreSQL connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(60)
# --- Database Core Logic ---

def upload_chunk_to_postgres(pg_conn, chunk_path, table_name, pg_schema):
    """Uploads a single CSV chunk file to a PostgreSQL table using the COPY command."""
    
    # Read the header from the CSV to get column names
    try:
        with open(chunk_path, 'r', encoding='utf-8') as f:
            if 'chunk_1.csv' in chunk_path:
                header = next(csv.reader(f, delimiter='|'))
            header = ['GSTIN','RET_PRD','SERIAL_NUM','RT','TAX_VAL','I_AMT','C_AMT','S_AMT','CS_AMT','INV_NUM', 'LOG_DT']
    except (IOError, StopIteration) as e:
        logging.error(f"Could not read header from '{chunk_path}': {e}")
        return False
        
    pg_table_name = f'"{pg_schema.lower()}"."{table_name.lower()}"'
    # Format columns for the SQL statement: "col1", "col2", ...
    formatted_cols = ', '.join([f'"{col.lower()}"' for col in header])
    #print(formatted_cols)
    copy_options = "WITH (FORMAT CSV, DELIMITER '|', NULL '')"

    # If it's the specific file that HAS a header, add the HEADER option
    if 'chunk_1.csv' in chunk_path:
        copy_options = "WITH (FORMAT CSV, DELIMITER '|', HEADER, NULL '')"
        logging.info(f"Processing '{os.path.basename(chunk_path)}' with HEADER option.")
    else:
        logging.info(f"Processing '{os.path.basename(chunk_path)}' without HEADER option.")
    # The COPY command is the most performant way to bulk-load data in PostgreSQL
    sql_copy =  f"""COPY {pg_table_name} ({formatted_cols}) 
            FROM STDIN 
            {copy_options}"""
    
    cursor = pg_conn.cursor()
    try:
        with open(chunk_path, 'r', encoding='utf-8') as f:
            # copy_expert streams the file data directly to the database
            cursor.copy_expert(sql=sql_copy, file=f)
        pg_conn.commit()
        logging.info(f"Successfully uploaded '{os.path.basename(chunk_path)}' to table '{table_name}'.")
        os.remove(chunk_path)
        return True
    except Exception as e:
        logging.error(f"Failed to upload chunk '{chunk_path}' to table '{table_name}': {e}")
        pg_conn.rollback() # Rollback the transaction on any error
        return False
    finally:
        cursor.close()

# --- Worker Process ---

def uploader_worker(work_queue, log_queue, upload_log_lock, config):
    """A worker process that pulls chunk file paths from a queue and uploads them."""
    process_name = current_process().name
    
    # Setup logging for this worker
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(QueueHandler(log_queue))
    root_logger.setLevel(logging.INFO)

    pg_conn = connect_to_pg(process_name, config)


    pg_schema = config['postgres']['schema']
    upload_log_path = config['settings']['completion_report_path_consumer']

    while True:
        chunk_path = work_queue.get()
        if chunk_path is None: # Sentinel value to stop the worker
            break

        logging.info(f"[{process_name}] Picked up task: {chunk_path}")
        
        # Infer table name from the parent directory of the chunk file
        table_name = os.path.basename(os.path.dirname(chunk_path))
        
        while True:
            success = upload_chunk_to_postgres(pg_conn, chunk_path, table_name, pg_schema)

            if success:
                logging.info(f"[{process_name}] Successfully uploaded {chunk_path}")
                # If upload was successful, log it to the state file to prevent re-uploading
                with upload_log_lock:
                    with open(upload_log_path, 'a') as f:
                        f.write(f"{chunk_path}\n")
                    #os.remove(chunk_path)
                break
            else:
                logging.warning(f"[{process_name}] Upload failed for {chunk_path}. Reconnecting and retrying...")
                try:
                    pg_conn.close()
                except Exception:
                    pass
                pg_conn = connect_to_pg(process_name, config)
    # Cleanups
    if pg_conn:
        pg_conn.close()
    logging.info(f"[{process_name}] Worker shutting down.")


# --- Main Orchestrator ---

def main():
    config = load_config()
    
    # Setup central logging
    uploader_log_handler = setup_logging(config['settings']['log_file_path_consumer'])
    
    # Load configuration
    output_data_path = config['settings']['data_path']
    uploaded_chunks_log_path = config['settings']['completed_log_path_consumer']
    num_workers = int(config['settings']['num_workers_consumer'])
    scan_interval_seconds = int(config['settings']['connection_retry_delay'])

    # Create multiprocessing components
    work_queue = Queue()
    log_queue = Queue()
    upload_log_lock = Lock() # To safely write to the uploaded_chunks.log

    # Start the listener that writes logs from all processes to the central file
    listener = QueueListener(log_queue, uploader_log_handler)
    listener.start()

    logging.info("="*50)
    logging.info("Uploader service started. Watching for new files...")
    logging.info(f"Watching directory: {output_data_path}")
    logging.info(f"Number of workers: {num_workers}")
    
    # Start worker processes
    processes = []
    for i in range(num_workers):
        process = Process(
            target=uploader_worker,
            name=f"Uploader-{i+1}",
            args=(work_queue, log_queue, upload_log_lock, config)
        )
        processes.append(process)
        process.start()
        
    # Main watcher loop
    try:
        while True:
            # 1. Load the current state of what's already been uploaded
            uploaded_set = load_uploaded_chunks(uploaded_chunks_log_path)
            
            # 2. Find new files to process
            new_files_to_upload = find_new_chunks(output_data_path, uploaded_set)
            
            if new_files_to_upload:
                logging.info(f"Watcher found {len(new_files_to_upload)} new chunk(s) to upload.")
                for file_path in new_files_to_upload:
                    work_queue.put(file_path)
            
            # 3. Wait before the next scan
            time.sleep(scan_interval_seconds)

    except KeyboardInterrupt:
        logging.info("Shutdown signal received. Notifying workers to stop.")
    finally:
        # Graceful shutdown procedure
        # 1. Send sentinel values to stop workers after they finish current task
        for _ in range(num_workers):
            work_queue.put(None)
            
        # 2. Wait for all worker processes to terminate
        for p in processes:
            p.join()
            
        # 3. Stop the logging listener
        listener.stop()
        logging.info("All workers have shut down. Uploader service stopped.")

if __name__ == '__main__':
    main()