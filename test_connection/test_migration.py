# replicate_db.py
import argparse
import sys
import oracledb
import pyodbc
import psycopg2
from psycopg2 import extras

# --- Configuration: Data Type Mappings ---
# This is a crucial part. It maps source DB data types to Postgres data types.
# It might need adjustments based on your specific schema.

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
        # For Oracle Instant Client, it needs to be initialized.
        # It's better to set the path in your environment variables.
        # If not, you can specify it here (not recommended for production).
        # oracledb.init_oracle_client(lib_dir="/path/to/your/instantclient")

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

    if source_db_type == 'oracle':
        # Oracle schema names are typically uppercase
        table_name = table_name.upper()
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = :1
            ORDER BY COLUMN_ID
        """
        cursor.execute(query, [table_name])
        for row in cursor.fetchall():
            col_name, data_type, precision, scale, char_len = row
            pg_type = ORACLE_TO_POSTGRES_TYPEMAP.get(data_type, 'TEXT') # Default to TEXT if not found
            if data_type == 'NUMBER' and precision is not None:
                pg_type = f"NUMERIC({precision}, {scale or 0})"
            elif data_type in ('VARCHAR2', 'CHAR') and char_len > 0:
                # In Postgres TEXT is often better than VARCHAR(n), but this shows how to map it
                # pg_type = f"VARCHAR({char_len})"
                pass
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
        raise ValueError(f"Table '{table_name}' not found or has no columns.")
    return columns

def replicate_table(source_conn, pg_conn, table_name, chunk_size, source_db_type):
    """Replicates a single table's schema and data."""
    print(f"\n--- Processing table: {table_name} ---")

    # 1. Get schema from source and create the table in Postgres
    try:
        columns = get_source_schema(source_conn, table_name, source_db_type)
        pg_cursor = pg_conn.cursor()

        # Sanitize column names for Postgres (e.g., lowercase and quote if needed)
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
        pg_conn.rollback()
        return

    # 2. Copy data in chunks
    source_cursor = source_conn.cursor()
    
    # Construct the SELECT statement for the source table
    # Quote column names to handle special characters or reserved words
    source_cols = ', '.join([f'"{col[0]}"' for col in columns])
    select_query=""
    # Oracle and newer SQL Server support OFFSET/FETCH for pagination
    if source_db_type == 'oracle':
        # Oracle requires an ORDER BY for OFFSET FETCH
        # We use the first column as the default ordering key
        order_by_col = f'"{columns[0][0]}"'
        select_query = f"SELECT {source_cols} FROM {table_name.upper()} ORDER BY {order_by_col}"
    elif source_db_type == 'sqlserver':
        order_by_col = f'"{columns[0][0]}"'
        select_query = f"SELECT {source_cols} FROM {table_name} ORDER BY {order_by_col}"

    print(f"Starting data copy for '{table_name}' in chunks of {chunk_size} rows...")
    offset = 0
    total_rows_copied = 0
    while True:
        if source_db_type == 'oracle':
            chunk_query = f"{select_query} OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
            source_cursor.execute(chunk_query)
        elif source_db_type == 'sqlserver':
            chunk_query = f"{select_query} OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
            source_cursor.execute(chunk_query)

        rows = source_cursor.fetchall()
        if not rows:
            break # No more data to fetch

        # Insert the chunk into Postgres
        try:
            # Prepare the INSERT statement for Postgres
            pg_cols_names = ', '.join([f'"{col[0].lower()}"' for col in columns])
            insert_sql = f"INSERT INTO {pg_table_name} ({pg_cols_names}) VALUES %s"
            
            # Use execute_values for efficient batch inserting
            extras.execute_values(pg_cursor, insert_sql, rows, page_size=chunk_size)
            
            total_rows_copied += len(rows)
            print(f"  Copied {total_rows_copied} rows...")

        except Exception as e:
            print(f"Error inserting data chunk into {pg_table_name}: {e}", file=sys.stderr)
            pg_conn.rollback() # Rollback the failed transaction
            source_cursor.close()
            pg_cursor.close()
            return
            
        offset += chunk_size

    source_cursor.close()
    pg_conn.commit() # Commit the transaction for this table
    pg_cursor.close()
    print(f"--- Finished table: {table_name}. Total rows copied: {total_rows_copied} ---")


# --- Main Execution Block ---

def main():
    parser = argparse.ArgumentParser(description="Replicate tables from Oracle/SQL Server to PostgreSQL.")
    parser.add_argument('source_db', choices=['oracle', 'sqlserver'], help="The source database type.")
    parser.add_argument('--tables', nargs='+', required=True, help="A space-separated list of table names to replicate.")
    parser.add_argument('--chunk-size', type=int, default=1000, help="Number of rows to process in each chunk.")

    # Postgres Args
    parser.add_argument('--pg-host', default='localhost', help="PostgreSQL host.")
    parser.add_argument('--pg-port', type=int, default=5432, help="PostgreSQL port.")
    parser.add_argument('--pg-db', default='mydb', help="PostgreSQL database name.")
    parser.add_argument('--pg-user', default='admin', help="PostgreSQL username.")
    parser.add_argument('--pg-password', default='admin123', help="PostgreSQL password.")

    # Oracle Args
    parser.add_argument('--ora-host', default='localhost', help="Oracle host.")
    parser.add_argument('--ora-port', type=int, default=1521, help="Oracle port.")
    parser.add_argument('--ora-service', default='orclpdb1', help="Oracle service name.")
    parser.add_argument('--ora-user', default='hr', help="Oracle username.")
    parser.add_argument('--ora-password', default='hr', help="Oracle password.")
    
    # SQL Server Args
    parser.add_argument('--sql-host', help="SQL Server host.")
    parser.add_argument('--sql-port', type=int, default=1433, help="SQL Server port.")
    parser.add_argument('--sql-db', help="SQL Server database name.")
    parser.add_argument('--sql-user', help="SQL Server username.")
    parser.add_argument('--sql-password', help="SQL Server password.")

    args = parser.parse_args()

    source_conn = None
    pg_conn = None

    try:
        # Establish connections
        source_conn = connect_to_source(args)
        pg_conn = connect_to_postgres(args)
        
        # Replicate each table
        for table_name in args.tables:
            replicate_table(source_conn, pg_conn, table_name, args.chunk_size, args.source_db)

        print("\nAll specified tables have been processed successfully.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
    finally:
        # Ensure connections are closed
        if source_conn:
            source_conn.close()
            print("Source database connection closed.")
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed.")


if __name__ == '__main__':
    main()