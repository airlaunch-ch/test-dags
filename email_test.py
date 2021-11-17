from datetime import timedelta
import time

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

fail_email = Variable.get("FAIL_EMAIL")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [fail_email],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'email_on_failure',
    default_args=default_args,
    description='A Failing DAG that triggers an email',
    schedule_interval=None, #timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)

def failing_task():
    print("failing....")
    raise ValueError('Doing my job and failing...')

PythonOperator(
    dag=dag,
    task_id='failing-task',
    python_callable=failing_task,
    depends_on_past=False
)
