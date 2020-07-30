from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['sina@indivisible.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}

dag = DAG(
    'mobile_commons',
    default_args=default_args,
    description='mobile commons ETL',
    schedule_interval=timedelta(days=1),
)

# vars = BashOperator(
#     task_id='vars',
#     bash_command='source /src/vars.sh',
#     dag=dag
# )

cols = BashOperator(
    task_id='cols',
    bash_command='cp /src/columns.json .',
    dag=dag
)

incoming_messages = BashOperator(
    task_id='incoming_messages',
    bash_command='python /src/incoming_messages.py',
    dag=dag
)

outgoing_messages = BashOperator(
    task_id='outgoing_messages',
    bash_command='python /src/outgoing_messages.py',
    dag=dag
)

profiles = BashOperator(
    task_id='profiles',
    bash_command='python /src/profiles.py',
    dag=dag
)

broadcasts = BashOperator(
    task_id='broadcasts',
    bash_command='python /src/broadcasts.py',
    dag=dag
)

groups = BashOperator(
    task_id='groups',
    bash_command='python /src/groups.py',
    dag=dag
)

tags = BashOperator(
    task_id='tags',
    bash_command='python /src/tags.py',
    dag=dag
)

urls_clicks = BashOperator(
    task_id='urls_clicks',
    bash_command='python /src/urls_clicks.py',
    dag=dag
)

cols >> incoming_messages >> outgoing_messages >> profiles >> broadcasts >> groups >> tags >> urls_clicks
