import pandas as pd
import json
import pickle
from sklearn.model_selection import train_test_split

from datetime import datetime
from sklearn.metrics import mean_absolute_error, mean_squared_error
import lightgbm as ltb
 
def run_model_training():
    #Accessing the feature engineered columns from the parquet file
    data = pd.read_parquet("/opt/airflow/feat_eng/feat_eng.parquet",
                           columns=['vol_moving_avg', 'adj_close_rolling_med', 'Volume'])

    #Dropping null values
    data.dropna(inplace=True)

    #Using 'vol_moving_avg', 'adj_close_rolling_med' features to predict the target 'Volume'
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'

    X = data[features]
    y = data[target]

    #Splitting the dataset (Train = 80%,  Test = 20%)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    #Defining the ML Model
    model = ltb.LGBMRegressor(num_leaves = 50, tree_learner='data', learning_rate=0.01, num_threads = 8)

    #Model Training
    model.fit(X_train, y_train)

    #Making Predictions
    y_pred = model.predict(X_test)

    # Calculating Errors
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred, squared = False)


    print("mae: {}, mse: {}, rmse: {}".format(mae,mse,rmse))

    #Logging the errors
    error_log = {"mean_absolute_error": mae, 
             "mean_squared_error": mse, 
             "root_mean_squared_error": rmse}

    filename = "/opt/airflow/model_logs/" + "error_log_" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".txt"
    with open(filename, "w") as fp:
        json.dump(error_log, fp)  # encode dict into JSON

    #Saving the trained model in a pickle file
    filename = "/opt/airflow/model/" + "LightGBM_Reg_" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".pkl"
    pickle.dump(model, open(filename, "wb"))