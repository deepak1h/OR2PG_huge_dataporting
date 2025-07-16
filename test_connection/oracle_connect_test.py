import oracledb

oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_21_18")

user = 'USER'
password = 'pass'
dsn = 'HOST:PORT/SERVICE_NAME'
# Connect using Thin mode (no Oracle client required)
connection = oracledb.connect(
    user=user,
    password=password,
    dsn=dsn,  # or ORCLCDB
    mode=oracledb.DEFAULT_AUTH
)

print("✅ Connected to Oracle DB")
cursor = connection.cursor()

cursor.execute("SELECT table_name FROM all_tab_columns")
tables = cursor.fetchall()

# For each table, fetch the row count
for table_name_tuple in tables:
    table_name = table_name_tuple[0]
    try:
        count_query = f'SELECT COUNT(*) FROM "{table_name}"'
        cursor.execute(count_query)
        row_count = cursor.fetchone()[0]
        print(f" - {table_name}: {row_count} rows")
    except Exception as e:
        print(f" - {table_name}: Failed to count rows ({e})")

cursor.close()
connection.close()
