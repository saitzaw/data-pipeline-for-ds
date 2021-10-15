import os
import pandas as pd
import pyarrow.feather as feather

class FileReader:
    def __init__(self, file_name, datatype, column, path):
        self.file_name = file_name
        self.datatype = datatype
        self.column = column
        self.path = path
    
    def take_csv(self):
        filename = os.path.join(self.path,self.file_name)
        dtype_dict = {self.column: self.datatype}
        return pd.read(self.file_name, dtype=dtype_dict,low_memory=False)

    def take_feather(self):
        filename = os.path.join(self.path,self.file_name)
        return feather.read_feather(filename)