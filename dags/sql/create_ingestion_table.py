
from airflow.operators.postgres_operator import PostgresOperator

def create_data_ingestion_table_operator(dagObject, table_name, base_folder):
    # create postgres connection
    create_table = PostgresOperator(task_id='create_table',
                        sql='''
                                CREATE TABLE IF NOT EXISTS {0} (
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
                                UserType VARCHAR (20) NOT NULL,
                                ProcessDate VARCHAR (8) NOT NULL
                            );'''.format(table_name),
                        dag=dagObject)
    return create_table
