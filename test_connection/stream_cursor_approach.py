# replicate_db.py
import argparse
import sys
import oracledb
import pyodbc
import psycopg2
from psycopg2 import extras

# This line should be at the top of your script's execution path.
oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_21_18")

# --- Configuration: Data Type Mappings ---
# This is a crucial part. It might need adjustments based on your specific schema.

ORACLE_TO_POSTGRES_TYPEMAP = {
    'VARCHAR2': 'TEXT',
    'NVARCHAR2': 'TEXT',
    'CHAR': 'CHAR',
    'NCHAR': 'CHAR',
    'CLOB': 'TEXT',
    'NCLOB': 'TEXT',
    'NUMBER': 'NUMERIC', # NUMBER(p,s) is handled in the schema fetch function
    'FLOAT': 'DOUBLE PRECISION',
    'BINARY_FLOAT': 'REAL',
    'BINARY_DOUBLE': 'DOUBLE PRECISION',
    'DATE': 'TIMESTAMP',
    'TIMESTAMP': 'TIMESTAMP',
    'TIMESTAMP WITH TIME ZONE': 'TIMESTAMP WITH TIME ZONE',
    'TIMESTAMP WITH LOCAL TIME ZONE': 'TIMESTAMP WITH TIME ZONE',
    'RAW': 'BYTEA',
    'BLOB': 'BYTEA',
}

SQLSERVER_TO_POSTGRES_TYPEMAP = {
    'varchar': 'TEXT',
    'nvarchar': 'TEXT',
    'char': 'CHAR',
    'nchar': 'CHAR',
    'text': 'TEXT',
    'ntext': 'TEXT',
    'int': 'INTEGER',
    'bigint': 'BIGINT',
    'smallint': 'SMALLINT',
    'tinyint': 'SMALLINT',
    'bit': 'BOOLEAN',
    'decimal': 'NUMERIC',
    'numeric': 'NUMERIC',
    'money': 'MONEY',
    'float': 'DOUBLE PRECISION',
    'real': 'REAL',
    'date': 'DATE',
    'datetime': 'TIMESTAMP',
    'datetime2': 'TIMESTAMP',
    'smalldatetime': 'TIMESTAMP',
    'time': 'TIME',
    'uniqueidentifier': 'UUID',
    'binary': 'BYTEA',
    'varbinary': 'BYTEA',
    'image': 'BYTEA',
}

# --- Connection Functions ---

def connect_to_postgres(args):
    """Establishes a connection to the PostgreSQL database."""
    try:
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host=args.pg_host,
            port=args.pg_port,
            dbname=args.pg_db,
            user=args.pg_user,
            password=args.pg_password
        )
        print("PostgreSQL connection successful.")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)

def connect_to_source(args):
    """Establishes a connection to the source database (Oracle or SQL Server)."""
    if args.source_db == 'oracle':
        return connect_to_oracle(args)
    elif args.source_db == 'sqlserver':
        return connect_to_sqlserver(args)
    else:
        print(f"Unsupported source database: {args.source_db}", file=sys.stderr)
        sys.exit(1)

def connect_to_oracle(args):
    """Establishes a connection to the Oracle database."""
    try:
        print("Connecting to Oracle...")
        dsn = f"{args.ora_host}:{args.ora_port}/{args.ora_service}"
        conn = oracledb.connect(
            user=args.ora_user,
            password=args.ora_password,
            dsn=dsn
        )
        print("Oracle connection successful.")
        return conn
    except Exception as e:
        print(f"Error connecting to Oracle: {e}", file=sys.stderr)
        print("Hint: Make sure Oracle Instant Client is installed and in your system's PATH.", file=sys.stderr)
        sys.exit(1)

def connect_to_sqlserver(args):
    """Establishes a connection to the SQL Server database."""
    try:
        print("Connecting to SQL Server...")
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={args.sql_host},{args.sql_port};"
            f"DATABASE={args.sql_db};"
            f"UID={args.sql_user};"
            f"PWD={args.sql_password};"
        )
        conn = pyodbc.connect(conn_str)
        print("SQL Server connection successful.")
        return conn
    except Exception as e:
        print(f"Error connecting to SQL Server: {e}", file=sys.stderr)
        print("Hint: Make sure the Microsoft ODBC Driver for SQL Server is installed.", file=sys.stderr)
        sys.exit(1)


# --- Schema and Data Replication Logic ---

def get_source_schema(source_conn, table_name, source_db_type):
    """Fetches the schema of a table from the source database."""
    cursor = source_conn.cursor()
    columns = []
    
    # Oracle schema/table names are case-sensitive but often stored as uppercase.
    # It's safer to query using uppercase.
    owner, table_part = table_name.split('.') if '.' in table_name else (cursor.username, table_name)

    if source_db_type == 'oracle':
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH
            FROM all_tab_columns
            WHERE OWNER = :owner AND TABLE_NAME = :table_name
            ORDER BY COLUMN_ID
        """
        cursor.execute(query, owner=owner.upper(), table_name=table_part.upper())
        
        for row in cursor.fetchall():
            col_name, data_type, precision, scale, char_len = row
            pg_type = ORACLE_TO_POSTGRES_TYPEMAP.get(data_type, 'TEXT') # Default to TEXT if not found
            if data_type == 'NUMBER' and precision is not None and precision > 0:
                pg_type = f"NUMERIC({precision}, {scale or 0})"
            elif data_type in ('VARCHAR2', 'CHAR') and char_len > 0:
                pass # Sticking with TEXT is generally fine
            columns.append((col_name, pg_type))

    elif source_db_type == 'sqlserver':
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query, table_name)
        for row in cursor.fetchall():
            col_name, data_type, precision, scale, char_len = row
            pg_type = SQLSERVER_TO_POSTGRES_TYPEMAP.get(data_type, 'TEXT')
            if data_type in ('decimal', 'numeric') and precision is not None:
                pg_type = f"NUMERIC({precision}, {scale or 0})"
            columns.append((col_name, pg_type))

    cursor.close()
    if not columns:
        raise ValueError(f"Table '{table_name}' not found or has no columns in source.")
    print(f"Source schema for '{table_name}': {columns}")
    return columns

def replicate_table(source_conn, pg_conn, table_name, chunk_size, source_db_type):
    """Replicates a single table's schema and data."""
    print(f"\n--- Processing table: {table_name} ---")

    # 1. Get schema from source and create the table in Postgres
    try:
        print(source_db_type,table_name)
        columns = get_source_schema(source_conn, table_name.lower(), source_db_type)
        pg_cursor = pg_conn.cursor()
        
        pg_cols_defs = [f'"{col[0].lower()}" {col[1]}' for col in columns]
        
        # We lowercase table names for Postgres convention
        pg_table_name = table_name.lower()


        print(f"Dropping and recreating table '{pg_table_name}' in PostgreSQL...")
        pg_cursor.execute(f"DROP TABLE IF EXISTS {pg_table_name};")
        create_table_sql = f"CREATE TABLE {pg_table_name} ({', '.join(pg_cols_defs)});"
        print(f"Executing: {create_table_sql}")
        pg_cursor.execute(create_table_sql)
        
    except Exception as e:
        print(f"Error creating schema for table {table_name}: {e}", file=sys.stderr)
        if pg_conn: pg_conn.rollback()
        return

    # 2. Copy data in chunks
    source_cursor = source_conn.cursor()
    source_cols = ', '.join([f'"{col[0]}"' for col in columns])
    total_rows_copied = 0

    print(f"Starting data copy for '{table_name}' in chunks of {chunk_size} rows...")

    if source_db_type == 'oracle':
        # ############################################################### #
        # ###               MODIFIED ORACLE DATA COPY LOGIC             ### #
        # ############################################################### #
        
        # 1. Set the arraysize. This is the key to efficient fetching.
        #    The driver will fetch this many rows at a time from the server.
        source_cursor.arraysize = chunk_size

        # 2. Use a simple SELECT statement without any expensive ORDER BY or pagination.
        select_query = f"SELECT {source_cols} FROM {table_name}"
        print(f"Executing source query: {select_query}")
        source_cursor.execute(select_query)
        
        # 3. Prepare the insert statement for Postgres
        pg_cols_names = ', '.join([f'"{col[0].lower()}"' for col in columns])
        insert_sql = f"INSERT INTO {pg_table_name} ({pg_cols_names}) VALUES %s"
        
        rows_chunk = []
        
        # 4. Iterate over the cursor. The driver handles fetching data in chunks.
        for row in source_cursor:
            rows_chunk.append(row)
            if len(rows_chunk) >= chunk_size:
                try:
                    extras.execute_values(pg_cursor, insert_sql, rows_chunk, page_size=chunk_size)
                    total_rows_copied += len(rows_chunk)
                    pg_conn.commit()
                    print(f"  Copied {total_rows_copied} rows...")
                    rows_chunk.clear() # Clear the chunk for the next batch
                except Exception as e:
                    print(f"Error inserting data chunk into {pg_table_name}: {e}", file=sys.stderr)
                    pg_conn.rollback()
                    # Exit the loop on error
                    break
        
        # 5. Insert any remaining rows in the last chunk
        if rows_chunk:
            try:
                extras.execute_values(pg_cursor, insert_sql, rows_chunk, page_size=len(rows_chunk))
                total_rows_copied += len(rows_chunk)
                pg_conn.commit()
                print(f"  Copied final {len(rows_chunk)} rows...")
            except Exception as e:
                print(f"Error inserting final data chunk into {pg_table_name}: {e}", file=sys.stderr)
                pg_conn.rollback()

    elif source_db_type == 'sqlserver':
        # (Original SQL Server logic using OFFSET/FETCH remains unchanged)
        order_by_col = f'"{columns[0][0]}"' # Required for OFFSET FETCH
        start_row = 0
        while True:
            chunk_query = f"""
                SELECT {source_cols} FROM {table_name}
                ORDER BY {order_by_col}
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            print(f"Fetching rows starting at offset {start_row}")
            rows = source_cursor.execute(chunk_query, start_row, chunk_size).fetchall()

            if not rows:
                break

            try:
                pg_cols_names = ', '.join([f'"{col[0].lower()}"' for col in columns])
                insert_sql = f"INSERT INTO {pg_table_name} ({pg_cols_names}) VALUES %s"
                extras.execute_values(pg_cursor, insert_sql, rows, page_size=chunk_size)
                total_rows_copied += len(rows)
                pg_conn.commit()
                print(f"  Copied {total_rows_copied} rows...")
            except Exception as e:
                print(f"Error inserting data chunk into {pg_table_name}: {e}", file=sys.stderr)
                pg_conn.rollback()
                break # Stop on error
            
            start_row += chunk_size

    source_cursor.close()
    pg_cursor.close()
    print(f"--- Finished table: {table_name}. Total rows copied: {total_rows_copied} ---")


# --- Main Execution Block ---

def main():
    parser = argparse.ArgumentParser(description="Replicate tables from Oracle/SQL Server to PostgreSQL.")
    parser.add_argument('source_db', choices=['oracle', 'sqlserver'], help="The source database type.")
    parser.add_argument('--tables', nargs='+', required=True, help="A space-separated list of table names to replicate. For Oracle, use SCHEMA.TABLE_NAME format.")
    parser.add_argument('--chunk-size', type=int, default=100000, help="Number of rows to process in each chunk. Also used as Oracle cursor arraysize.")

    # Postgres Args
    parser.add_argument('--pg-host', default='LOCALHOST', help="PostgreSQL host.")
    parser.add_argument('--pg-port', type=int, default=5432, help="PostgreSQL port.")
    parser.add_argument('--pg-db', default='test_db', help="PostgreSQL database name.")
    parser.add_argument('--pg-user', default='user', help="PostgreSQL username.")
    parser.add_argument('--pg-password', default='pass', help="PostgreSQL password.")

    # Oracle Args
    parser.add_argument('--ora-host', default='localhost', help="Oracle host.")
    parser.add_argument('--ora-port', type=int, default=1525, help="Oracle port.")
    parser.add_argument('--ora-service', default='db', help="Oracle service name.")
    parser.add_argument('--ora-user', default='user', help="Oracle username.")
    parser.add_argument('--ora-password', default='pass', help="Oracle password.")
    
    # SQL Server Args
    parser.add_argument('--sql-host', help="SQL Server host.")
    parser.add_argument('--sql-port', type=int, default=1433, help="SQL Server port.")
    parser.add_argument('--sql-db', help="SQL Server database name.")
    parser.add_argument('--sql-user', help="SQL Server username.")
    parser.add_argument('--sql-password', help="SQL Server password.")

    args = parser.parse_args()

    # A small fix in the original code: handle sql-host help text better.
    if args.source_db == 'sqlserver' and not args.sql_host:
        parser.error("--sql-host is required for source_db 'sqlserver'")

    source_conn = None
    pg_conn = None

    try:
        source_conn = connect_to_source(args)
        pg_conn = connect_to_postgres(args)
        
        for table_name in args.tables:
            replicate_table(source_conn, pg_conn, table_name, args.chunk_size, args.source_db)

        print("\nAll specified tables have been processed successfully.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if source_conn:
            source_conn.close()
            print("Source database connection closed.")
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed.")


if __name__ == '__main__':
    main()