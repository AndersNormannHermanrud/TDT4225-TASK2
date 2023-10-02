import mysql.connector as mysql
from sqlalchemy import create_engine

class DbConnector:
    """
    Connects to the MySQL server on the Ubuntu virtual machine.
    Connector needs HOST, DATABASE, USER and PASSWORD to connect,
    while PORT is optional and should be 3306.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    DATABASE = "testdb" // Database name, if you just want to connect to MySQL server, leave it empty
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(self):
        # Connect to the database
        try:
            self.db_connection = create_engine('mysql+mysqlconnector://and:123@localhost:3306/gps', echo=False)  # engine = create_engine('dialect+driver://username:password@host:port/database', echo=False)
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)