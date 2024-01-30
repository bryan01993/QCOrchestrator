FROM apache/airflow:2.5.1

USER root
RUN apt-get update

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y


#COPY dags/afb-lz-dev-datalake-0.json .
#
#RUN gcloud auth activate-service-account --key-file afb-lz-dev-datalake-0.json

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt

