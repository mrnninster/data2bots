# imports
import os
import logging
import psycopg2
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Setup Logging
db_logger = logging.getLogger(__name__)
db_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
file_handler = logging.FileHandler("log_files/db.log")
file_handler.setFormatter(formatter)
db_logger.addHandler(file_handler)

class database():

    def __init__(self) -> None:
        self.host = os.getenv("DB_HOST")
        self.password = os.getenv("DB_PASS")
        self.username = os.getenv("DB_USER")
        self.port = os.getenv("DB_PORT")
        self.db_name = os.getenv("DB_NAME")


    # Create DB Connection
    def conn(self) -> psycopg2.connect:
        try:
            connect = psycopg2.connect(dbname=self.db_name,user=self.username,password=self.password,host=self.host,port=self.port)
            return connect

        except Exception as e:
            db_logger.critical(f"DBConnectError: Failed to create database connection {e}.")


    # Create Cursor
    def create_cursor(self,connect):
        try:
            cursor = connect.cursor()
            return cursor

        except Exception as e:
            db_logger.warning(f"CreateCursorError: Failed to create cursor {e}")


    # Create Table
    def create_table(self,cursor,connect,columns_data,tablename,schemaname) -> bool:
        """
        connect: Database connection
        cursor: Database connection cursor
        schemaname: Name of schema under which table would be created
        tablename: Name of table getting created
        columns_data: The name of each column along with their characters 
            sample: (id SERIAL PRIMARY KEY, name VARCHAR)
        """
        try:
            Query = f"CREATE TABLE IF NOT EXISTS {schemaname}.{tablename} {columns_data}"
            cursor.execute(Query)
            connect.commit()
            return True

        except Exception as e:
            db_logger.critical(f"CreateTableError: Failed to create database table {tablename} {e}.")
            return False


    # Fetch From DB
    def fetch_data(self,cursor,columns_name,tablename,secondary_tablename="",all=True,one=False,filtered=False,filter=None,join=False,join_condition=None):
        try:
            # Queries
            if (filtered == True) and (join == True):
                Query = f"SELECT {columns_name} FROM {tablename} INNER JOIN {secondary_tablename} ON {join_condition} WHERE {filter} ORDER BY {tablename}.id;"

            elif (filtered == False) and (join == True):
                Query = f"SELECT {columns_name} FROM {tablename} INNER JOIN {secondary_tablename} ON {join_condition} ORDER BY {tablename}.id;"

            elif filtered == True:
                Query = f"SELECT {columns_name} from {tablename} WHERE {filter};"
                
            elif (filtered == False) and (join == False):
                Query = f"SELECT {columns_name} from {tablename};"

            db_logger.debug(f"FetchDataQuery: {Query}")
            
            # Execute Query
            cursor.execute(Query)

            # Fetch
            if(one == True):
                data = cursor.fetchone()

            elif(all == True):
                data = cursor.fetchall()

            # Response
            return data

        except Exception as e:
            db_logger.critical(f"FetchDataError: Failed to fetch data from database table {tablename}, {e}")
    
    # Write To DB
    def Thread_write(self,cursor,connect,dict_data,tablename,schemaname):
        try:
            names = tuple(dict_data.keys())
            column_names = ""
            for name in names:
                if name == names[-1]:
                    column_names = f"{column_names}{name}"
                else:
                    column_names = f"{column_names}{name},"
            uncleaned_values = tuple(dict_data.values())
            cleaned_values = []
            for x in uncleaned_values:
                if x != "nan":
                    cleaned_values.append(x)
                else:
                    cleaned_values.append("-")
            column_values = tuple(cleaned_values)
            Query = f"INSERT INTO {schemaname}.{tablename} ({column_names}) VALUES {column_values};"
            cursor.execute(Query)
            connect.commit()
        except Exception as e:
            db_logger.critical(f"ThreadWriteToDBError: failed to write to {tablename}, {e}")

    def write_data(self,cursor,connect,columns_data,tablename,schemaname,check=True,check_row="id"):
        """
        connect: Database connection
        cursor: Database connection cursor
        schemaname: Name of schema where table is located
        tablename: Name of table where data is written into
        columns_data: The name of each column along with their values
            sample: ({"column_name":"column_value",...,"column_name":"column_value"},...,{"column_name":"column_value"})
        """
        try:
            db_logger.debug(f"{tablename}: total => {len(columns_data)}")

            # Preprocess for existing Data
            if check == True:
                res = self.fetch_data(cursor,check_row,f"{schemaname}.{tablename}")
                db_logger.debug(f"{tablename}: written => {len(res)}")
            else:
                res = ()

            if len(res) != 0:
                count = len(res)
                columns_data = columns_data[count:len(columns_data)]

            db_logger.debug(f"{tablename}:pending {len(columns_data)}")

            if len(columns_data) != 0:
                # Actual Write to DB
                db_logger.info(f"{tablename}: Writing Data")
                for dict_data in columns_data:
                    with ThreadPoolExecutor() as executor:
                        executor.submit(self.Thread_write,cursor,connect,dict_data,tablename,schemaname)
                db_logger.info(f"{tablename}: Write Complete")

        except Exception as e:
            db_logger.critical(f"WriteDataError: Failed to write data to table {tablename}, {e}")


    # Close Connections
    def close(self,connect=None, cursor=None):

        # Close DB Connection
        if connect != None:
            try:
                connect.close()
            
            except Exception as e:
                db_logger.critical(f"DBDisconnectError: Failed to disconnect from database {e}.")

        # Close Cursor Connection
        if cursor != None:
            try:
                cursor.close()
            
            except Exception as e:
                db_logger.warning(f"CloseCursorError: Failed to clsoe cursor {e}")