import pandas as pd
from sqlalchemy import text
import Connector


def task1(con):
    print("\n\nTASK 1")
    with con.engine.connect() as connection:
        num_users = connection.execute(text("SELECT COUNT(*) FROM User;"))
        num_activities = connection.execute(text("SELECT COUNT(*) FROM Activity"))
        num_trackpoints = connection.execute(text("SELECT COUNT(*) FROM TrackPoint"))
        print(num_users.first()[0] + num_activities.first()[0] + num_trackpoints.first()[0])


def task2(con):
    """
    My motivation here was to create a generic query that could be used to get the prerequisite data, then returning all the values at once
    Spent some time learning how to do nested queries
    :param con:
    :return:
    """
    print("\n\nTASK 2")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            """
            SELECT AVG(trackpoints) as average, MAX(trackpoints) as maximum, MIN(trackpoints) as minimum
            FROM (
                SELECT User.id as id, COUNT(TrackPoint.id) as trackpoints 
                FROM User INNER JOIN Activity ON User.id = Activity.User_id
                INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                GROUP BY User.id
            ) as just_here_cus_mysql_needs_it
            """))
        for average, maximum, minimum in result:
            print("Average {0}, Max {1}, Min {2}".format(average, maximum, minimum))

def task3(con):
    print("\n\nTASK 3")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            """
            SELECT User.id, COUNT(Activity.id) AS activities
            FROM User JOIN Activity ON User.id = Activity.user_id
            GROUP BY User.id ORDER BY activities DESC LIMIT 15
            """))
        for id, activities in result:
            print("User {0} with {1} activities".format(id, activities))

def task4(con):
    print("\n\nTASK 4")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            """
            SELECT DISTINCT User.id 
            FROM User JOIN Activity ON User.id = Activity.user_id
            WHERE Activity.transportation_mode = 'bus'
            """))
        for id in result:
            print("User {0}".format(id))


def task5(con): # Kjapp modifisering av oppgave 4 og 3
    print("\n\nTASK 5")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            """
            SELECT DISTINCT User.id, COUNT(DISTINCT Activity.transportation_mode)
            FROM User JOIN Activity ON User.id = Activity.user_id
            GROUP BY User.id ORDER BY COUNT(DISTINCT Activity.transportation_mode) DESC LIMIT 10;
            """))
        for id, activities in result:
            print("User {0} with {1} transportation modes".format(id, activities))


def task6(con):
    print("\n\nTASK 6")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            """
            SELECT transportation_mode, start_date_time, end_date_time, COUNT(*) AS count FROM Activity
            GROUP BY transportation_mode, start_date_time, end_date_time HAVING count > 1;
            """))
        try:
            for row in result:
                print("User {0} with {1} transportation modes".format(row))
        except:
            print("No rows was returned")


def task7(con):
    print("\n\nTASK 7")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            """
            
            """))
        for id, activities in result:
            print("User {0} with {1} transportation modes".format(id, activities))


def task(con):
    print("\n\nTASK 6")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            """
            
            """))
        for id, activities in result:
            print("User {0} with {1} transportation modes".format(id, activities))


def main():
    """
    Much of the code is inspired from the sqlalchemy documentation here:
    https://docs.sqlalchemy.org/en/20/core/connections.html
    :return:
    """
    con = Connector.Connection()
    #task1(con)
    #task2(con)
    #task3(con)
    task4(con)
    task5(con)
    task6(con)


if __name__ == '__main__':
    main()
