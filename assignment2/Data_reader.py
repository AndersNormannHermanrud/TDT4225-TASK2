import logging
import os
import pandas as pd
import Connector
import time
import warnings


pd.set_option('display.max_columns', None)
DATA_CUTOFF = 2500
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
warnings.filterwarnings("ignore")


def clean_trackpoints(activity_trackpoints):
    """
    Some cleaning of data types
    Corrects the datatypes and datetime format so that the dataframe matches the database
    """
    activity_trackpoints["date_time"] = pd.to_datetime(activity_trackpoints["date_days"], origin=pd.Timestamp('12-30-1899'), unit='D').values.astype('datetime64[s]')  # Datetime from origin given in the task description
    activity_trackpoints["activity_id"] = pd.to_numeric(activity_trackpoints["activity_id"])
    activity_trackpoints["altitude"] = pd.to_numeric(activity_trackpoints["altitude"])
    activity_trackpoints.astype({"activity_id": "long"})
    activity_trackpoints.drop(columns=["Date", "Time"], inplace=True)
    return activity_trackpoints


def create_labels(user_id):
    labels = pd.read_csv(os.path.join(os.getcwd(), user_id, "labels.txt"), header=0, delimiter="\t")
    labels["Start Time"] = pd.to_datetime(labels["Start Time"]).values.astype('datetime64[s]')
    labels["End Time"] = pd.to_datetime(labels["End Time"]).values.astype('datetime64[s]')
    return labels

def get_transportation_mode(user_id, labeled_ids, activity_start, activity_end):
    trans_mode = ""
    if user_id in labeled_ids:
        labels = create_labels(user_id)
        where_do_start_and_end_time_match = labels["Start Time"].isin([activity_start]) & labels["End Time"].isin([activity_end])
        if where_do_start_and_end_time_match.any():
            trans_mode = labels.loc[where_do_start_and_end_time_match, "Transportation Mode"].iloc[0]
    return trans_mode

def fint_activity_and_trackpoints(file_path, user_id, activity_file,activities, activity_trackpoints, labeled_ids):
    """"
    Finds all activities and trackpoints belonging to one user and appends them to the correct list
    """
    trackpoints = pd.read_csv(file_path, skiprows=6, names=["lat", "lon", "Unused", "altitude", "date_days", "Date", "Time"]).drop(columns=["Unused"])
    if trackpoints.shape[0] <= DATA_CUTOFF:  # Ignore entries that are too big
        activity_id = activity_file.split('.', 1)[0] + user_id  # I append the user ID to the activity ID to make it unique, else multiple activities in the dataset will have the same id
        trackpoints.insert(0, "activity_id", activity_id)
        trackpoints = clean_trackpoints(trackpoints)
        activity_trackpoints = pd.concat([activity_trackpoints, trackpoints])
        activity_start = trackpoints["date_time"].iloc[0]
        activity_end = trackpoints["date_time"].iloc[-1]
        trans_mode = get_transportation_mode(user_id, labeled_ids, activity_start, activity_end)
        activities.append([activity_id, user_id, trans_mode, activity_start, activity_end])
    else:
        logging.debug("Ignored activity in file" + activity_file + " since it was bigger that the cutoff of size " + str(DATA_CUTOFF))
    return activities, activity_trackpoints


def insert_to_db(con, user_id, activities, activity_trackpoints):
    con.insert_row("User", ["id", "has_labels"], [int(user_id).__str__(), "False"])  # Insert the User
    activities = pd.DataFrame(activities, columns=["id","user_id", "transportation_mode", "start_date_time", "end_date_time"])
    activities.to_sql(con=con.engine, name="Activity", if_exists="append", index=False)  # Insert all activities for one user in bulk (technically sends an insert for each row, but its the fastest configuration)
    activity_trackpoints.to_sql(con=con.engine, name="TrackPoint", if_exists="append", index=False, chunksize=40000)  # Insert all trackpints for all activities for one user in bulk


def reset_and_fill_db():
    os.chdir("..")  # Platform independent file traversal to the dataset
    os.chdir("dataset")
    labeled_ids = open(os.path.join(os.path.join(os.getcwd()), "labeled_ids.txt"), "r").read().split("\n")
    os.chdir("Data")

    Connector.main()  # Reset and reload db for testing purposes
    con = Connector.Connection()

    start_time = time.time()
    for user_id in os.listdir(os.curdir):
        path = os.path.join(os.getcwd(), user_id, "Trajectory")
        activities = [] #activities = pd.DataFrame(columns=["id","user_id", "transportation_mode", "start_date_time", "end_date_time"])  # activities = [] #  Even though its more messy with multiple data types, avoiding a dataframe here decreases runtime
        activity_trackpoints = pd.DataFrame(columns=["activity_id","lat", "lon", "altitude", "date_time", "date_days"])
        for activity_file in os.listdir(path):
           activities, activity_trackpoints = fint_activity_and_trackpoints(os.path.join(path, activity_file), user_id, activity_file, activities, activity_trackpoints, labeled_ids)
        insert_to_db(con, user_id, activities, activity_trackpoints)
        logging.info(" At time: " + time.strftime("%H:%M:%S",time.gmtime(time.time() - start_time)) + ", Inserted user with id: " + user_id + ", " + str(len(activities)) + " Activities and " + str(activity_trackpoints.shape[0]) + " TrackPoints Successfully")
    #logging.info("Execution time:", time.strftime("%H:%M:%S",time.gmtime(time.time() - start_time)))

def main():
    reset_and_fill_db()


if __name__ == '__main__':
    main()
