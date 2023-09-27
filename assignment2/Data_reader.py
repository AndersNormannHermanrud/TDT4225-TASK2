import os
import sys
import numpy as np
import pandas as pd
import Connector
pd.set_option('display.max_columns', None)
from sqlalchemy import create_engine

def read_data():
    os.chdir("..")  # Platform independent file traversal to the dataset
    os.chdir("dataset")
    os.chdir("Data")

    Connector.main() # Reset and reload db for testing purposes
    con = Connector.Connection()
    #engine = create_engine('dialect+driver://username:password@host:port/database', echo=False)
    engine = create_engine('mysql+mysqlconnector://and:123@localhost:3306/gps', echo=False)

    for user_id in os.listdir(os.curdir):
        path = os.path.join(os.getcwd(), user_id, "Trajectory")
        con.insert_row("User", ["id", "has_labels"], [int(user_id).__str__(), "False"])
        activities = []
        activity_trackpoints = pd.DataFrame(columns=["activity_id","lat", "lon", "altitude", "date_days", "Date", "Time"])
        for activity_file in os.listdir(path):
            trackpoint_file = pd.read_csv(os.path.join(path, activity_file), skiprows=6, names=["lat", "lon", "Unused", "altitude", "date_days", "Date", "Time"]).drop(columns=["Unused"])
            activityID = activity_file.split('.', 1)[0] + user_id  # I append the user ID to the activity ID to make it truly unique
            trackpoint_file.insert(0, "activity_id", activityID)
            activity_trackpoints = pd.concat([activity_trackpoints, trackpoint_file])
            activities.append([activityID, user_id, "", "", ""])

        activities = pd.DataFrame(activities, columns=["id","user_id", "transportation_mode", "start_date_time", "end_date_time"])
        activities.to_sql(con=engine, name="Activity", if_exists="append", index=False)
        print("Added activities")

        #activity_trackpoints["date_time"] = activity_trackpoints["Date"] + " " + activity_trackpoints["Time"]
        #activity_trackpoints['id'] = activity_trackpoints.index
        activity_trackpoints["date_time"] = pd.to_datetime(activity_trackpoints["date_days"], origin=pd.Timestamp('12-30-1899'), unit='D')  # Datetime from origin given in the task description
        activity_trackpoints["activity_id"] = pd.to_numeric(activity_trackpoints["activity_id"])
        activity_trackpoints["altitude"] = pd.to_numeric(activity_trackpoints["altitude"])
        activity_trackpoints.drop(columns=["Date", "Time"], inplace=True)
        activity_trackpoints.astype({"activity_id" : "long"})
        print(activity_trackpoints.head())
        print(activity_trackpoints.dtypes)
        activity_trackpoints.to_sql(con=engine, name="TrackPoint", if_exists="append", index=False)
        if int(user_id) > 3:
            break

def main():
    read_data()


if __name__ == '__main__':
    main()
