FROM apache/airflow:2.6.0
COPY requirements.txt /requirements.txt
USER root
RUN sudo apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN sudo apt-get -y install curl
RUN sudo apt-get install libgomp1
USER airflow
RUN pip install --user --upgrade pip
RUN pip install -U --no-cache-dir --user -r /requirements.txt