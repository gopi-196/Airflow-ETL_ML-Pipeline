from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from preprocess import run_preprocess
from feature_engg import run_feature_engg
from model_training import run_model_training

with DAG("rt_dag", start_date= datetime(2023, 1, 1), schedule_interval="@daily", catchup = False) as dag:

    preprocess_task = PythonOperator(
        task_id = "preprocess_task",
        python_callable = run_preprocess
     )

    feature_engg_task = PythonOperator(
        task_id = "feature_engg_task",
        python_callable = run_feature_engg
     )

    model_training_task = PythonOperator(
        task_id = "model_training_task",
        python_callable = run_model_training
     )

    preprocess_task >> feature_engg_task >> model_training_task