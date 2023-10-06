from collections import defaultdict

import numpy as np
import pandas as pd
from geopy.distance import geodesic
from haversine import haversine
from sqlalchemy import text
from rtree import index
import Connector


pd.set_option('display.max_columns', None)

def task1(con):
    print("\n\nTASK 1")
    with con.engine.connect() as connection:
        num_users = connection.execute(text("SELECT COUNT(*) FROM User;"))
        num_activities = connection.execute(text("SELECT COUNT(*) FROM Activity"))
        num_trackpoints = connection.execute(text("SELECT COUNT(*) FROM TrackPoint"))
        print("Users: {0}, Activities: {1}, Trackpoints: {2}".format(num_users.first()[0], num_activities.first()[0], num_trackpoints.first()[0]))


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
            GROUP BY User.id ORDER BY COUNT(DISTINCT Activity.transportation_mode) DESC LIMIT 10
            """))
        for id, activities in result:
            print("User {0} with {1} transportation modes".format(id, activities))


def task6(con):
    print("\n\nTASK 6")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            """
            SELECT id, transportation_mode, start_date_time, end_date_time, COUNT(*) AS count FROM Activity
            GROUP BY transportation_mode, start_date_time, end_date_time HAVING count > 1
            """))
        try:
            i = 0
            for row in result:
                print("Activity {0} is registered multiple times".format(row))
                i+=1
                if i>15:
                    break
            print("number of activities: {0}".format(len(result.fetchall())))
        except:
            print("No rows was returned")


def task7(con):
    """
    I am unsure if i am supposed to only return activities that has a transportation mode or not
    If the latter is the case add 'AND Activity.transportation_mode <> '' '
    :param con:
    :return:
    """
    print("\n\nTASK 7")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            """
            SELECT User.id, Activity.transportation_mode, TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time)
            FROM User JOIN Activity ON User.id = Activity.user_id
            WHERE DATEDIFF(end_date_time, start_date_time) >= 1
            """))
        for id, mode, time in result:
            print("User {0}, activity {1}, time {2}".format(id,mode,time))


def task8(con):
    print("\n\nTASK 8")
    query = """
    SELECT user_id, TrackPoint.lon, TrackPoint.lat, TrackPoint.date_time 
    FROM Activity INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
    """
    trackpoints = con.engine.connect().execute(text(query))
    users = dict()
    for line in trackpoints:
        user_id, lon, lat, date_time = line
        users.setdefault(user_id, []).append((lon, lat, date_time))
    idx = index.Index()
    for user_id, trackpoints in users.items():
        for lon, lat, date_time in trackpoints:
            idx.insert(user_id, (lon, lat, lon, lat), obj=date_time)

    close_users = set()
    for user_id, trackpoints in users.items():
        for lon, lat, date_time in trackpoints:
            for potential_close_id in idx.intersection((lon-0.0005, lat-0.0005, lon+0.0005, lat+0.0005), objects=True):
                if potential_close_id.id != user_id:
                    potential_distance = haversine((lat, lon), (potential_close_id.bbox[1], potential_close_id.bbox[0]))
                    if potential_distance <= 0.05:
                        if abs((date_time - potential_close_id.object).total_seconds()) <= 30:
                            close_users.add(user_id)
                            close_users.add(potential_close_id.id)
    print("Number of users that have been close to each other:", len(close_users))



""" Scrapped due to high memory demand, with both the sparse and dense approach
def task8(con):
    print("\n\nTASK 8")
    query = \"""
    SELECT TrackPoint.*, Activity.user_id FROM TrackPoint
    JOIN Activity ON TrackPoint.activity_id = Activity.id
    WHERE Activity.transportation_mode <> ''
    \"""
    trackpoints = pd.read_sql(query, con=con.engine)
    close_users = set()

    # Pulled from the example pandas itertools documentation and repurposed for this task
    for (i1, row1), (i2, row2) in combinations(trackpoints.iterrows(), 2):
        distance = geodesic((row1['lat'], row1['lon']), (row2['lat'], row2['lon'])).meters
        if distance <= 50 and abs((row1['date_time'] - row2['date_time']).total_seconds()) <= 30:
            user_pair = frozenset({row1['user_id'], row2['user_id']})
            close_users.add(user_pair)
    print(len(close_users))
    unique_users = set()
    for user_pair in close_users:
        unique_users.update(user_pair)
    print(len(unique_users))


def task8(con):
    print("\n\nTASK 8")
    query = \"""
    SELECT TrackPoint.*, Activity.user_id FROM TrackPoint
    JOIN Activity ON TrackPoint.activity_id = Activity.id
    \"""
    trackpoints = pd.read_sql(query, con=con.engine)
    print("Data collected, starting preprossesing")
    scaler = StandardScaler()
    x = trackpoints[['lat', 'lon', 'date_days']].values
    x = csr_matrix(pairwise_distances(x))
    x = scaler.fit_transform(x)
    print("Data transformed, starting DBscan")
    dbscan = DBSCAN(metric='precomputed')
    trackpoints['cluster'] = dbscan.fit_predict(x)
    trackpoints = trackpoints[trackpoints['cluster'] != -1]
    print(trackpoints.groupby('cluster'))
"""

def task9(con):
    print("\n\nTASK 9")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            """
            SELECT a.user_id, t.activity_id, t.id as trackpoint_id, t.altitude, t.date_time
            FROM TrackPoint t JOIN Activity a ON t.activity_id = a.id
            ORDER BY a.user_id, t.activity_id, t.date_time
            """)).fetchall()
        altitude_gains = defaultdict(int)
        prev_row = None
        for row in result:
            if prev_row and prev_row.user_id == row.user_id and prev_row.activity_id == row.activity_id:
                altitude_difference = row.altitude - prev_row.altitude
                if altitude_difference > 0:
                    altitude_gains[row.user_id] += altitude_difference
            prev_row = row
        top_users = sorted(altitude_gains.items(), key=lambda x: x[1], reverse=True)
        print("Top 15 users that gained the most altitude:")
        print(top_users[:15])


def approx_distance(row):
    if pd.notna(row['next_lat']):
        lat1, lon1, lat2, lon2 = np.radians(row['lat']), np.radians(row['lon']), np.radians(row['next_lat']), np.radians(row['next_lon'])
        x = (lon2 - lon1) * np.cos(0.5 * (lat2 + lat1))
        y = lat2 - lat1
        return 6371.01 * np.sqrt(x*x + y*y)  # 6371.01 is Earth's radius in km
    else:
        return 0

def task10(con):
    print("\n\nTASK 10")
    query = """
    SELECT a.user_id,a.transportation_mode,DATE(t.date_time) AS travel_date,t.lat,t.lon,t.date_time
    FROM Activity a JOIN TrackPoint t ON a.id = t.activity_id
    ORDER BY a.user_id, a.transportation_mode, t.date_time
    """
    result = pd.read_sql(con=con.engine, sql=query)
    print("Data fetched")
    result['next_lat'] = result.groupby(['user_id', 'transportation_mode'])['lat'].shift(-1)
    result['next_lon'] = result.groupby(['user_id', 'transportation_mode'])['lon'].shift(-1)
    print("Data grouped")
    result['distance'] = result.apply(approx_distance, axis=1)
    print("Data lamda applied")
    result = result.groupby(['user_id', 'transportation_mode', 'travel_date'])['distance'].sum().reset_index()
    result = result.groupby('transportation_mode').apply(lambda x: x.nlargest(1, 'distance')).reset_index(drop=True)
    print(result)


def task11(con):
    print("\n\nTASK 11")
    query = """
    SELECT a.user_id, t1.activity_id, t1.date_time as start_time, t2.date_time as end_time
    FROM TrackPoint t1
    JOIN TrackPoint t2 ON t1.activity_id = t2.activity_id AND t1.id = t2.id - 1
    JOIN Activity a ON t1.activity_id = a.id
    """
    df = pd.read_sql(con=con.engine, sql=query)
    df['time_diff'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60
    invalid_activities = df[df['time_diff'] >= 5]['activity_id'].unique()
    result = df[df['activity_id'].isin(invalid_activities)].groupby('user_id')['activity_id'].nunique().reset_index()
    result.columns = ['user_id', 'inv_act_count']
    print("Number of invalid activities per user:")
    print(result)


def task12(con):
    print("\n\nTASK 12")
    query = """
        SELECT user_id, transportation_mode
        FROM Activity 
        WHERE transportation_mode IS NOT NULL AND transportation_mode <> ''
        ORDER BY user_id
    """
    activities = pd.read_sql(con=con.engine, sql=query)
    activities = activities.groupby(['user_id', 'transportation_mode']).size().reset_index(name='count')
    idx = activities.groupby('user_id')['count'].idxmax()  # Get max count
    result = activities.loc[idx][['user_id', 'transportation_mode']].rename(columns={'transportation_mode': 'most_used_transportation_mode'})
    result = result.sort_values(by='user_id')
    print("Users with transportation mode and their most used mode:")
    print(result)

""" A baseline method for the tasks
def task(con):
    print("\n\nTASK 2")
    with con.engine.connect() as connection:
        result = connection.execute(text(
            \"""
            
            \"""))
        for id, activities in result:
            print("User {0} with {1} transportation modes".format(id, activities))
"""

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
    #task4(con)
    #task5(con)
    #task6(con)
    #task7(con)
    #task9(con)
    #task11(con)
    #task12(con)
    #task10(con)
    task8(con)

if __name__ == '__main__':
    main()
