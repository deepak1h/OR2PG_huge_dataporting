import psycopg2


user = 'USER'
password = 'pass'
host = 'HOST'
port = 1234
db = 'dbname'

# Connect to the PostgreSQL database
connection = psycopg2.connect(
    host=host,     # or your server IP / domain
    port=port,            # default PostgreSQL port
    database=db,   # replace with your database name
    user=user,     # replace with your username
    password=password  # replace with your password
)

print("✅ Connected to PostgreSQL DB")

cursor = connection.cursor()

# Query to list tables in the current schema
cursor.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
""")

# Print table namess
for table_name, in cursor.fetchall():
    print(" -", table_name)

# Cleanup
cursor.close()
connection.close()
