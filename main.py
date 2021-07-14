import pymysql
import pymysql.cursors
import pandas as pd


class MySQL:
    def __init__(self, db='employees'):
        conn_properties = {'host': 'localhost',
                           'user': 'root',
                           'password': 'admin',
                           'database': db,
                           'charset':'utf8mb4',
                           'cursorclass':pymysql.cursors.DictCursor # Return result with dictionary type
                           }
        self.db = db
        self.connection = pymysql.connect(**conn_properties)
        self.cursor = self.connection.cursor()
        
    def set_db(self, db):
        """Set database for connection."""
        self.connection.select_db(db)
        self.db = db
        
    def print_databases(self):
        """Print out all databases."""
        # execute show databases
        self.cursor.execute("SHOW DATABASES")
        self.connection.commit()
        
        results = self.cursor.fetchall()
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
    
    def set_select_msg(msg):
        """Create the select msg"""
        
        

    def read_database(self, table='employees', filters=['gender','F'], order='birth_date', limit=10):
        """Read data from database."""
        
        # Initiallize msg.
        filter_msg = ""
        order_msg = ""
        limit_msg = ""
        
        # Create filter msg.
        if filters:
            filter_msg = "WHERE `{}`='{}'".format(filters[0], filters[1])
            
        # Create order msg.
        if order:
            order_msg = "ORDER BY `{}` ".format(order)
        
        # Create limit msg.
        if limit:
            limit_msg = "LIMIT {}".format(limit)
        
        sql = "select * FROM `{}` {} {} {}".format(table, filter_msg, order_msg, limit_msg)
        self.cursor.execute(sql)
        self.connection.commit()
        results = self.cursor.fetchall()

        df = pd.DataFrame(results)
        print(df)
    
if __name__ == "__main__":
    sql_class = MySQL()
    
    #Question 2
    sql_class.print_databases()
    sql_class.print_tables()
    
    sql_class.read_database(table='employees',filters=['gender','F'],order='birth_date',limit=10)
