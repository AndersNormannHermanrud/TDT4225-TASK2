from DbConnector import DbConnector
from tabulate import tabulate


class Connection:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor


def create_tables(connection):
    queries = []
    queries.append("""
    CREATE TABLE IF NOT EXISTS User (
                   id VARCHAR(30) NOT NULL PRIMARY KEY,
                   has_labels BOOL NOT NULL)
    """)
    queries.append("""
    CREATE TABLE IF NOT EXISTS Activity (
                   id INT NOT NULL PRIMARY KEY,
                   user_id VARCHAR(30) NOT NULL,
                   transportation_mode VARCHAR(30),
                   start_date_time VARCHAR(30) NOT NULL,
                   end_date_time VARCHAR(30) NOT NULL,
                   FOREIGN KEY (user_id)
                        REFERENCES User(id)
                        ON DELETE CASCADE)
    """)
    queries.append("""
    CREATE TABLE IF NOT EXISTS TrackPoint (
                   id INT NOT NULL PRIMARY KEY,
                   activity_id INT NOT NULL,
                   lat DOUBLE NOT NULL,
                   lon DOUBLE NOT NULL,
                   altitude INT NOT NULL,
                   date_days DOUBLE NOT NULL,
                   date_time DATETIME,
                   FOREIGN KEY (activity_id)
                        REFERENCES Activity(id)
                        ON DELETE CASCADE)
    """)
    for q in queries:
        connection.cursor.execute(q)
    connection.db_connection.commit()


def show_tables(connection):
    connection.cursor.execute("SHOW TABLES")
    rows = connection.cursor.fetchall()
    print(tabulate(rows, headers=connection.cursor.column_names))


def drop_table_lazy(connection, table_name):
    try:
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        connection.cursor.execute(query % table_name)
        connection.db_connection.commit()
    except:
        print("Could not drop table " + table_name)

def main():
    connection = Connection()
    drop_table_lazy(connection, "TrackPoint")
    drop_table_lazy(connection, "Activity")
    drop_table_lazy(connection, "User")
    create_tables(connection)
    show_tables(connection)

if __name__ == '__main__':
    main()
