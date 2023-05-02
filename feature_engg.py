import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os

def run_feature_engg():
    #Accessing the data stored in parquet file
    data = pd.read_parquet("/opt/airflow/pre_process/preprocess.parquet",
                           columns=['Adj Close', 'Volume'])


    #Creating a new feature 'vol_moving_avg' (Moving average of Volume feature for 30 days)
    data["vol_moving_avg"] = data['Volume'].rolling(30).mean()
 

    #Creating a new feature 'adj_close_rolling_med' (Rolling Median of Adj Close feature for 30 days)
    data["adj_close_rolling_med"] = data['Adj Close'].rolling(30).median()


    #Storing the table with derived features as a parquet file
    table = pa.Table.from_pandas(data)
    filename =  "/opt/airflow/feat_eng/" + "feat_engg_" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".parquet"
    pq.write_table(table,  filename)
