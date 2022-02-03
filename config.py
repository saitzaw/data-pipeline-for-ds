import os
from dotenv import load_dotenv
from os.path import join, dirname
dotenv_path = join(dirname(__file__), '.env')

class EnvData:
    """
    GET required values from dotenv file
    """
    def __init__(self):
        self.host = os.getenv("HOST")
        self.dbname = os.getenv("DBNAME")
        self.user = os.getenv("DBUSER")
        self.password = os.getenv("DATABASE_SECRETE")
    
    def get_values(self):
        return {"host": self.host,
                "dbname": self.dbname,
                "user": self.user,
                "password": self.password}

class FilePath:
    """
    list the file path use in this application 
    """
    def __init__(self):
        self.path = 