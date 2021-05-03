"""
Data pipeline to monitor error in log server and send email if the amount of certain error exceed threshold
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.contrib.operators.sftp_operator import SFTPOperation
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from log_parser import parse_log_file, parse_active_user_file
from count_errors import gen_error_reports
import os


def check_error_threshold(**kwargs):
    threshold = int(Variable.get("error_threshold", default_var=3))
    ti = kwargs['ti']
    error_count = int(ti.xcom_pull(key='error_count', task_ids='gen_reports'))
    print(f'Error occurrencs: {error_count}, threshold: {threshold}')
    return 'send_email' if error_count >= threshold else 'dummy_op'


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(minutes=20),  #datetime(2020, 7, 24),
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

date_tag = date.today().strftime('%Y%m%d')    #'''{{ ds_nodash }}'''
table_name = f'data_ingest_{date_tag}'
base_folder = f'{os.environ["AIRFLOW_HOME"]}/data/{date_tag}'
remote_path = '<remote_path>'

# log_list = [
#             'securityApp.log',
#             'mainApp.log',
#             'extApp.log',
#             'timeApp.log',
#             'tokenApp.log',
#             'bridgeApp.log',
#             'daemonApp.log',
#             'notificationApp.log',
#             'messageApp.log']

# dl_tasks = []
# for file in log_list:
#     op = SFTPOperator(task_id=f"download_{file}",
#                 ssh_conn_id="log_server",
#                 local_filepath=f"{base_folder}/{file}",
#                 remote_filepath=f"{remote_path}/{file}",
#                 operation=SFTPOperation.GET,
#                 create_intermediate_dirs=True,
#                 dag=dag)
#     dl_tasks.append(op)


# bash_command = """
#     grep -E 'Exception' --include=\\*.log -rnw '{{ params.base_folder }}' > {{ params.base_folder }}/errors.txt
#     ls -l {{ params.base_folder }}/errors.txt && cat {{ params.base_folder }}/errors.txt
# """
# grep_exception = BashOperator(task_id="grep_exception",
#                         bash_command=bash_command,
#                         params={'base_folder': base_folder},
#                         dag=dag)


# creat postgres connection
create_table = PostgresOperator(task_id='create_table',
                        sql='''DROP TABLE IF EXISTS {0};
                                CREATE TABLE {0} (
                                SeqNumber SERIAL PRIMARY KEY,
                                FullName VARCHAR (100) NOT NULL,
                                Email VARCHAR (100) NULL,
                                UserName VARCHAR (100) NOT NULL,
                                JobTitle VARCHAR (100) NULL,
                                ScannedDate TIMESTAMP NOT NULL,
                                id uuid,
                                ManagerId uuid,
                                ProfileImageAddress VARCHAR(512),
                                State VARCHAR (20) NOT NULL,
                                UserType VARCHAR (20) NOT NULL
                            );'''.format(table_name),
                        dag=dag)


parse_active_user = PythonOperator(task_id='parse_active_user',
                        python_callable=parse_active_user_file,
                        op_kwargs={'filepath': f'{base_folder}/active_user.txt',
                                   'tablename': f'{table_name}'},
                        dag=dag)


# gen_reports = PythonOperator(task_id='gen_reports',
#                         python_callable=gen_error_reports,
#                         op_kwargs={'statfile': f'{base_folder}/error_stats.csv',
#                                    'logfile': f'{base_folder}/error_logs.csv',
#                                    'tablename': f'{table_name}'},
#                         provide_context=True,
#                         dag=dag)


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
