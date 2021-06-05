from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
import json
import csv
import requests

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "nourhanabdelaziz97@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def download_rates():
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for row in reader:
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get('https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/' + ENDPOINTS[base]).json()
            print(indata)
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')


with DAG("forex_data_pipeline", start_date=datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available",
        http_conn_id="forex_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20
    )
    forex_currencies_file_available = FileSensor(
        task_id="is_forex_currencies_file_available",
        fs_conn_id="forex_path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        timeout=20
    )
    
    downloading_rates = PythonOperator(
        task_id="downloading_rates",
        python_callable = download_rates
    )
        
    saving_rates = BashOperator(
        task_id = "saving_rates",
        bash_command="""
            hdfs dfs -mkdir -p /forex &&\
            hdfs dfs -put -f /opt/airflow/dags/files/forex_rates.json /forex
        """

    )
    
    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )
    
    forex_processing = SparkSubmitOperator(
        task_id="forex_processing",
        conn_id="spark_conn",
        application="/opt/airflow/dags/scripts/forex_processing.py",
        verbose=False
    )

    sending_email_notification = EmailOperator(
            task_id="sending_email",
            to="nourhanabdelaziz97@gmail.com",
            subject="forex_data_pipeline",
            html_content="""
                <h3>forex_data_pipeline succeeded</h3>
            """
            )
    sending_slack_notification = SlackWebhookOperator(
	task_id="sending_slack",
	http_conn_id="slack_conn",
	webhook_token="T021QK75JS0/B021FDP3J3X/9MXPg1JZFLjBoIJ4uB8INvUO",
	message="DAG forex_data_pipeline: DONE",
	username="airflow"
)  
    
    is_forex_rates_available >>  forex_currencies_file_available >> downloading_rates >> saving_rates >> creating_forex_rates_table >> forex_processing >> sending_email_notification >> sending_slack_notification 
    
