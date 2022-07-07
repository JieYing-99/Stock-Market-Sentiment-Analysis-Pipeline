import sys
sys.path.append('/home/jieying')

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

SCHEDULE_INTERVAL_MINUTES = 10

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 5),
    'email': ['_@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='US Stock Market Sentiment Analysis',
    schedule_interval=timedelta(minutes=SCHEDULE_INTERVAL_MINUTES),
    catchup=False
)

t1 = BashOperator(
    task_id='twitter_scraping',
    bash_command='python3 /home/jieying/scripts/twitter_scraping.py',
    # bash_command='sleep 2',
    dag=dag
)

t2 = BashOperator(
    task_id='news_scraping',
    bash_command='python3 /home/jieying/scripts/news_scraping.py',
    # bash_command='sleep 3',
    dag=dag
)

t3 = BashOperator(
    task_id='spark_structured_streaming',
    bash_command='python3 /home/jieying/scripts/spark_structured_streaming.py',
    # bash_command='sleep 5',
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

t4 = BashOperator(
    task_id='druid_ingestion',
    bash_command='python3 /home/jieying/scripts/druid_ingestion.py',
    dag=dag
)

[t1, t2] >> t3 >> t4