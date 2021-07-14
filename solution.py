import pymysql
import pymysql.cursors
import pandas as pd


class MySQL:
    def __init__(self, db='employees', dict_flag=0):
        
        # Check dict_flag, 0: return result in list, 1: return result in dict 
        if dict_flag:
            conn_properties[cursorclass] = pymysql.cursors.DictCursor
        
        
        conn_properties = {'host': 'localhost',
                           'user': 'root',
                           'password': 'admin',
                           'database': db,
                           'charset':'utf8mb4'
                           }
        self.db = db
        self.connection = pymysql.connect(**conn_properties)
        self.cursor = self.connection.cursor()
        
    def get_(self):
        """Print out all databases."""
        # execute show databases
        self.cursor.execute("SHOW DATABASES")
        self.connection.commit()
        
        results = self.cursor.fetchall()
        
if __name__ == "__main__":
    sql = MySQL()
        
        print("Databases:")
        for i,result in enumerate(results):
            print("\t{}: {}".format(i+1, result['Database']))
        
    def print_tables(self):
        """Print out all tables for current database."""
        # execute show tables
        self.cursor.execute("SHOW TABLES")
        self.connection.commit()
        
        # Set the key for result.
        key = "Tables_in_{}".format(self.db)
        
        results = self.cursor.fetchall()
        print("Databases:")
        for i,result in enumerate(results):
            print("\t{}: {}".format(i+1, result[key]))