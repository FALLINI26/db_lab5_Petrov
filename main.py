import psycopg2
from matplotlib import pyplot as plt
from matplotlib.dates import DateFormatter, DayLocator

views = {
    "conversation_messages_count": """
            create view conversation_messages_count as select 
                name, 
                count(message_id) as message_count
            from conversation c
                left join message m on c.conversation_id = m.conversation_id
            group by name;
        """,

    "user_messages_count": """
            create view user_messages_count as select 
                username, 
                count(message_id) as message_count
            from users u
                left join message m on u.user_id = m.sender_id
            group by username;
        """,

    "daily_messages_count": """
            create view daily_messages_count as select 
                DATE(timestamp) as date, 
                COUNT(*) as message_count
            from message
            where DATE(timestamp) between '2023-12-12' and '2023-12-15'
            group by DATE(timestamp)
            order by DATE(timestamp);
        """
}


class PostgresDB:
    def __init__(self, data_source):
        self.dbname = data_source["dbname"]
        self.user = data_source["username"]
        self.password = data_source["password"]
        self.host = data_source["host"]
        self.port = data_source["port"]
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def execute(self, query):
        try:
            self.cursor.execute(query)
        except Exception as e:
            print(f"Error executing query: {e}")

    def fetch_all(self):
        return self.cursor.fetchall()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()


class StatisticsRepository:
    def __init__(self):
        self.database = PostgresDB({
            "dbname": "db_lab3_petrov",
            "username": "postgres",
            "password": "1000dollars",
            "host": "localhost",
            "port": 5432
        })

    def get_conversation_messages_count(self):
        view_name = "conversation_messages_count"
        return self.__execute_view(view_name)

    def get_user_messages_count(self):
        view_name = "user_messages_count"
        return self.__execute_view(view_name)

    def get_daily_messages_count(self):
        view_name = "daily_messages_count"
        return self.__execute_view(view_name)

    def __execute_view(self, view_name):
        self.database.connect()
        self.__create_view(view_name)
        result = self.__read_view(view_name)
        self.database.close_connection()

        return result

    def __create_view(self, view_name):
        create_view_sql = views[view_name]
        self.database.execute(create_view_sql)

    def __read_view(self, view_name):
        select_sql = 'SELECT * FROM ' + view_name
        self.database.execute(select_sql)

        return self.database.fetch_all()


class StatisticsVisualizer:
    def __init__(self):
        self.statistics_provider = StatisticsRepository()

    def showHistogram(self):
        conversation_messages_count = self.statistics_provider.get_conversation_messages_count()
        conversation_names, message_counts = zip(*conversation_messages_count)

        plt.figure(figsize=(11, 6))
        plt.bar(conversation_names, message_counts, color='lightblue')
        plt.title('Message count for conversations')
        plt.show()

    def showCircleDiagram(self):
        user_messages_count = self.statistics_provider.get_user_messages_count()
        usernames, message_counts = zip(*user_messages_count)

        plt.pie(message_counts, labels=usernames, autopct='%1.1f%%', startangle=90, colors=plt.cm.Paired.colors)
        plt.title('Message percentage for users')
        plt.show()

    def showGraph(self):
        daily_messages_count = self.statistics_provider.get_daily_messages_count()
        dates, message_counts = zip(*daily_messages_count)

        plt.plot(dates, message_counts)
        plt.ylabel('Time (seconds)')
        plt.title('Daily message count')

        plt.gca().xaxis.set_major_formatter(DateFormatter('%d.%m.%y'))
        plt.gca().xaxis.set_major_locator(DayLocator())
        plt.ylim(bottom=0)

        plt.show()


statistics_visualizer = StatisticsVisualizer()
statistics_visualizer.showHistogram()
statistics_visualizer.showCircleDiagram()
statistics_visualizer.showGraph()
