"""
Data pipeline to monitor error in log server and send email if the amount of certain error exceed threshold
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from sql.create_ingestion_table import create_data_ingestion_table_operator
from ingest_user import parse_active_user_file_operator
import os


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(minutes=20),  #datetime(2020, 7, 24),
    "schedule_interval": None,
    # "start_date": None,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("monitor_errors", default_args=default_args, schedule_interval=timedelta(1))

table_name = f'data_ingest'
base_folder = f'{os.environ["AIRFLOW_HOME"]}/data/'

create_table = create_data_ingestion_table_operator(dag, table_name, base_folder)


parse_active_user = parse_active_user_file_operator(dag, table_name, base_folder)


# check_threshold = BranchPythonOperator(task_id='check_threshold', python_callable=check_error_threshold, provide_context=True, dag=dag)


# send_email = EmailOperator(task_id='send_email',
#         to='tony.xu@airflow.com',
#         subject='Daily report of error log generated',
#         html_content=""" <h1>Here is the daily report of error log for {{ ds }}</h1> """,
#         files=[f'{base_folder}/error_stats.csv', f'{base_folder}/error_logs.csv'],
#         dag=dag)


# dummy_op = DummyOperator(task_id='dummy_op', dag=dag)


# dl_tasks >> grep_exception >> create_table >> parse_log >> gen_reports >> check_threshold >> [send_email, dummy_op]
create_table >> parse_active_user
