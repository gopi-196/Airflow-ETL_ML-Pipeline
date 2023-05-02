#import opendatasets as od
import pandas as pd
#from tqdm import tqdm
import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def run_preprocess():
    #Downloading dataset from kaggle
    # dataset_url = 'https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset'
    # od.download(dataset_url)
    # data_dir = 'stock-market-dataset'
    
    #Extracting Symbols and Security Names from the symbols_valid_meta csv file
    nasdaq_symbols = pd.read_csv('/opt/airflow/stock-market-dataset/symbols_valid_meta.csv')
    nasdaq_symbols_req = nasdaq_symbols[['Symbol', 'Security Name']]
  

    #Extracting etf and stock file names up 
    etf_path = '/opt/airflow/stock-market-dataset/etfs'
    stock_path = '/opt/airflow/stock-market-dataset/stocks'

    etf_filenames = os.listdir(etf_path)
    stock_filenames = os.listdir(stock_path)


    files = [etf_filenames, stock_filenames]
    paths = [etf_path, stock_path]

    #Calculating the total number of rows required to join all the etf and stock files
    total_rows = 0
    for i in range(len(files)):
        for element in files[i]: 
            file_df = pd.read_csv(paths[i]+ '/' + element)
            total_rows += len(file_df)


    #Creating an empty dataframe with number of rows found in the above step
    df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume',
        'file_name'], index = range(total_rows))


    #Appending a new column (file name - stock/etf name) to each file, and then assigning its values to the newly created dataframe
    current_row = 0
    for i in range(len(files)):
        for file in files[i]:
            file_name = file.split('.')[0]
            file_df = pd.read_csv(paths[i]+ '/' + file)
            file_df['file_name'] = file_name
            #print(current_row, len(file_df))
            df.iloc[current_row: (len(file_df)+current_row)] = file_df.values
            current_row += len(file_df)


    #Merging the dataframes using symbols
    data = pd.merge (df,nasdaq_symbols_req, left_on='file_name', right_on='Symbol')

    #Dropping the duplicate Column
    data.drop(columns = ['file_name'], axis = 1, inplace = True)

    #Dropping Null values
    data.dropna(inplace=True)

    #Assigning suitable datatypes to the features
    data['Open'] = data['Open'].astype("float64")
    data['High'] = data['High'].astype("float64")
    data['Low'] = data['Low'].astype("float64")
    data['Close'] = data['Close'].astype("float64")
    data['Adj Close'] = data['Adj Close'].astype("float64")
    data['Volume'] = data['Volume'].astype("int64")

    #Converting the pandas dataframe to a pyarrow object and then saving it as a parquet file
    table = pa.Table.from_pandas(data)
    filename = "/opt/airflow/pre_process/" + "preprocess_" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".parquet"
    pq.write_table(table, filename)