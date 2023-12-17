import csv
from main import PostgresDB

data_source = {
    "dbname": "db_lab3_petrov",
    "username": "postgres",
    "password": "1000dollars",
    "host": "localhost",
    "port": 5432
}
database = PostgresDB(data_source)
database.connect()

table_names = ["users", "conversation", "message", "user_conversation"]

for table_name in table_names:
    file_path = f"csv_files/export/{table_name}.csv"
    database.execute(f"SELECT * FROM {table_name}")
    rows = database.fetch_all()

    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow([desc[0] for desc in database.cursor.description])
        # Write data
        writer.writerows(rows)

database.close_connection()
