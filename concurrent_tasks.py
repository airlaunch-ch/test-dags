from datetime import timedelta
import time

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@airlaunch.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
    'concurrent_tasks',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=None, #timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)

def sleep_task():
    try:
        timeout = Variable.get("TIMEOUT")
    except KeyError:
        timeout = 2
    print("sleeping {} seconds".format(timeout))
    time.sleep(int(timeout))
    print("woke up")

try:
    n_tasks = Variable.get("NTASKS")
except KeyError:
    n_tasks = 10

for i in range(int(n_tasks)):
    PythonOperator(
        dag=dag,
        task_id='sleep_' + str(i),
        python_callable=sleep_task,
        depends_on_past=False
    )
