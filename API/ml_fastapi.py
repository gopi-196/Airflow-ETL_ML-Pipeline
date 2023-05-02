from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
import uvicorn
import pickle
import os

app = FastAPI(debug=True)
filename = 'C:/Users/gobis/VS Code/airflow_rt/model/LightGBM_model.pkl'
if os.path.getsize(filename) > 0:
    with open(filename, 'rb') as f:
        unpickler = pickle.Unpickler(f)
        model = unpickler.load()

@app.get('/')
def home():
    return {'text': 'RiskThinking WorkSample'}

@app.get('/predict/')
def predict(vol_moving_avg : float = 12345, adj_close_rolling_med : float = 25): 
    make_prediction = model.predict([[vol_moving_avg, adj_close_rolling_med]])
    output = round(make_prediction[0])
    return jsonable_encoder({'vol_moving_avg': vol_moving_avg,'adj_close_rolling_med':adj_close_rolling_med,'volume': output})

if __name__ == '__main__':
    uvicorn.run(app)