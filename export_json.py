import json
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

output_path = "exported_data.json"

data = {}

for table_name in table_names:
    query = f"SELECT * FROM {table_name}"
    database.execute(f"SELECT * FROM {table_name}")
    rows = []
    fields = [x[0] for x in database.cursor.description]

    for row in database.cursor:
        rows.append(dict(zip(fields, row)))

    data[table_name] = rows

with open(output_path, 'w') as json_file:
    json.dump(data, json_file, default=str)

database.close_connection()
