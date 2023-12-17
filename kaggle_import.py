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
    file_path = f"csv_files/import/{table_name}.csv"

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        columns = next(reader)  # Read the header row for column names
        for row in reader:
            values = "', '".join(row)
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ('{values}');"
            database.execute(query)

database.commit()
database.close_connection()
