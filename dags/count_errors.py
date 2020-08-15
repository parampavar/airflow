from airflow.hooks.postgres_hook import PostgresHook


# select session, count(*) from timeout_err_20200726 where error = 'null' group by session
# select error, count(*) from timeout_err_20200726 group by error
def push_message(instance, message):
    print(f'Push message: error_count={message}')
    instance.xcom_push(key="error_count", value=message)

def gen_error_reports(statfile, logfile, tablename, **kwargs):
    # database hook
    db_hook = PostgresHook(postgres_conn_id='postgres_default', schema='airflow')
    db_conn = db_hook.get_conn()
    db_cursor = db_conn.cursor()

    #('extApp.log', '22995', '23 Jul 2020', '02:53:13,527', None, 'extApp', 'Unrecognized SSL message, plaintext connection?')
    sql = f"SELECT * FROM {tablename}"
    sql_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(sql)
    with open(logfile, 'w') as f_output:
        db_cursor.copy_expert(sql_output, f_output)

    sql = f"SELECT error, count(*) as occurrence FROM {tablename} group by error ORDER BY occurrence DESC"
    # db_cursor.execute(sql, group)
    # get the generated id back
    # Use the COPY function on the SQL we created above.
    sql_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(sql)

    # Set up a variable to store our file path and name.
    with open(statfile, 'w') as f_output:
        db_cursor.copy_expert(sql_output, f_output)


    db_cursor.execute(sql)
    print("The number of error type: ", db_cursor.rowcount)
    row = db_cursor.fetchone()
    print('first record: ', row)
    push_message(kwargs['ti'], row[1] if row else 0)

    db_cursor.close()
    db_conn.close()
    print(f"Two reports are generated from table: {tablename}\n1. {statfile}\n2. {logfile}")



if __name__ == "__main__":
    pass
