import os
import pandas as pd
from src.utils.configs import FilePath

def data_fetching():
    train_url = 'https://raw.githubusercontent.com/geodra/Titanic-Dataset/master/data/train.csv'
    test_url = 'https://raw.githubusercontent.com/geodra/Titanic-Dataset/master/data/test.csv'

    df_train = pd.read_csv(train_url)
    df_test = pd.read_csv(test_url)

    file_path = FilePath().data_files()
    file_name1 = 'train_titanics.csv'
    file_name2 = 'test_titanics.csv'
    download_file1 = os.path.join(file_path, file_name1)
    download_file2 = os.path.join(file_path, file_name2)
    df_train.to_csv(download_file1, index=False)
    df_test.to_csv(download_file2, index=False)

