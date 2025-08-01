FROM apache/airflow:3.0.3

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
RUN python -m spacy download ru_core_news_sm
