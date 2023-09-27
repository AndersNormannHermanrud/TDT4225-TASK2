from DbConnector import DbConnector
from tabulate import tabulate


class Connection:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor


    def create_tables(self):
        queries = []
        queries.append("""
        CREATE TABLE IF NOT EXISTS User (
                       id INT NOT NULL PRIMARY KEY,
                       has_labels BOOL NOT NULL)
        """)
        queries.append("""
        CREATE TABLE IF NOT EXISTS Activity (
                       id BIGINT NOT NULL PRIMARY KEY,
                       user_id INT NOT NULL,
                       transportation_mode VARCHAR(30),
                       start_date_time VARCHAR(30),
                       end_date_time VARCHAR(30),
                       FOREIGN KEY (user_id)
                            REFERENCES User(id)
                            ON DELETE CASCADE)
        """)
        queries.append("""
        CREATE TABLE IF NOT EXISTS TrackPoint (
                       id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                       activity_id BIGINT NOT NULL,
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
            self.cursor.execute(q)
        self.db_connection.commit()


    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    """
    Drops the table if it can, ignores it if not, only for testing purposes
    Do not use in finished program
    """
    def drop_table_lazy(self, table_name):
        try:
            print("Dropping table %s..." % table_name)
            query = "DROP TABLE %s"
            self.cursor.execute(query % table_name)
            self.db_connection.commit()
        except:
            print("Could not drop table " + table_name)

    # insert into User (id, has_labels) Values (1,False);
    def insert_row(self, table_name, cols, values):
        #query = "INSERT INTO %s (name) VALUES ('%s')"
        query = "".join(["INSERT INTO ", table_name, " (", ', '.join(cols), ") VALUES (", ', '.join(values), ")"])
        print(query)
        self.cursor.execute(query)
        self.db_connection.commit()

def main():
    connection = Connection()
    connection.drop_table_lazy("TrackPoint")
    connection.drop_table_lazy("Activity")
    connection.drop_table_lazy("User")
    connection.create_tables()
    connection.show_tables()


if __name__ == '__main__':
    main()
