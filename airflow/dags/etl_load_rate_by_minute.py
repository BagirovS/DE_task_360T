from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from pipelines.etl_load_rate_by_minute import main


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1
}

dag = DAG(
    dag_id='ANNA_DAG_from_PotgreSQL_2_JSON_file',
    default_args=default_args,
    schedule_interval='* * * * *'
)

start_task = DummyOperator(task_id='start_task', dag=dag)

run_etl = PythonOperator(
    task_id='complete_etl_load_by_hour',
    provide_context=True,
    python_callable=main(),
    dag=dag)

start_task >> run_etl 