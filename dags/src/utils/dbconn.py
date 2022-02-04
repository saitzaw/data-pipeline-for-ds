import psycopg2 as sql
import psycopg2.extras as extras
from configs import EnvData

class DBConn: 
    """ database connection for postgresql"""
    def __init__(self): 
        self.env = EnvData()

    def connect(self):
        conn = None
        try: 
            conn=sql.connect(
                 host = self.env.get_values["host"],
                 database = self.env.get_values["dbname"],
                 user = self.env.get_values["user"],
                 password = self.env.get_values["password"])
        except ConnectionError as exc:
            raise RuntimeError('Failed to open database') from exc
        finally:
            conn.close()
        return conn
       
    def query_to_database(self,query):
        conn = self.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
        except (Exception, sql.DatabaseError) as error:
            print(error)

        data = cursor.fetchall()
        return data

    def execute_values(self,dataframe, table):
        df = dataframe.copy()
        conn = self.connect()
        data_list = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        
        # SQL query to execute
        query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, data_list)
            conn.commit()
        except (Exception, sql.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
        print("execute_values() done")
        
    def truncate_table(self,table):
        conn = self.connect()
        cursor = conn.cursor()
        try: 
            query = "TRUNCATE TABLE %s" % table
            cursor.execute(query)
            conn.commit() 
        except (Exception, sql.DatabaseError) as error:
            conn.rollback()
            raise error
        print("Turncate the Table")