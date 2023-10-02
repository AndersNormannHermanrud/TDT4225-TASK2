from tabulate import tabulate
from sqlalchemy import create_engine, text


class Connection:
    def __init__(self):
        self.engine = create_engine('mysql+mysqlconnector://and:123@localhost:3306/gps',
                                    echo=False)  # engine = create_engine('dialect+driver://username:password@host:port/database', echo=False)

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
        connection = self.engine.connect()
        trans = connection.begin()
        for q in queries:
            connection.execute(text(q))
        trans.commit()

    def show_tables(self):
        result = self.engine.connect().execute("SHOW TABLES")
        print(tabulate(result, headers=self.cursor.column_names))

    """
    Drops the table if it can, ignores it if not, only for testing purposes
    Do not use in finished program
    """

    def drop_table_lazy(self, table_name):
        try:
            connection = self.engine.connect()
            trans = connection.begin()
            print("Dropping table %s..." % table_name)
            query = "DROP TABLE {0}".format(table_name)
            connection.execute(text(query))
            trans.commit()
        except:
            print("Could not drop table " + table_name)

    def insert_row(self, table_name, cols, values):
        try:
            connection = self.engine.connect()
            trans = connection.begin()
            query = "".join(["INSERT INTO ", table_name, " (", ', '.join(cols), ") VALUES (", ', '.join(values), ")"])
            connection.execute(text(query))
            trans.commit()
        except:
            print("Could not execute query on table " + table_name)

def main():
    connection = Connection()
    connection.drop_table_lazy("TrackPoint")
    connection.drop_table_lazy("Activity")
    connection.drop_table_lazy("User")
    connection.create_tables()
    # connection.show_tables()


if __name__ == '__main__':
    main()
