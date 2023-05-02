from fastapi import FastAPI
import uvicorn
import pickle
import os

app = FastAPI(debug=True)

@app.get('/')
def home():
    return {'text': 'RiskThinking WorkSample'}

@app.get('/predict/')
def predict(vol_moving_avg : float = 12345, adj_close_rolling_med : float = 25):
    filename = 'C:/Users/gobis/VS Code/airflow_rt/model/LightGBM_model.pkl'
    if os.path.getsize(filename) > 0:
        with open(filename, 'rb') as f:
            unpickler = pickle.Unpickler(f)
            model = unpickler.load()
        make_prediction = model.predict([[vol_moving_avg, adj_close_rolling_med]])
        output = round(make_prediction[0],2)
        return output

if __name__ == '__main__':
    uvicorn.run(app)