import os
import pathlib
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
        self.pfolder_path = pathlib.Path(__file__).parent.resolve()
        self.data_file_path = 'data'
        self.include_file_path = 'includes'

    def data_files(self):
        return os.path.join( self.pfolder_path, self.pfolder_path)

    def src_files(self):
        return os.path.join( self.pfolder_path, self.include_file_path)