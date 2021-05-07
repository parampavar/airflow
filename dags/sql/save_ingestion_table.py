from airflow.hooks.postgres_hook import PostgresHook
import logging

def save_user_to_database(tablename, records):
    if not records:
        print("Empty records!")
        logging.debug("Empty records")
        return
    logging.info(f'Total number of records to insert: {len(records)}')
 
    # database hook
    db_hook = PostgresHook(postgres_conn_id='postgres_default', schema='airflow')
    db_conn = db_hook.get_conn()
    db_cursor = db_conn.cursor()

    for p in records:
        print("Inserting " + p['UserName'])
        logging.info(f'Inserting: {p["UserName"]}')

        sql = """INSERT INTO {} (FullName, Email, UserName, JobTitle, ScannedDate, id, ManagerId, ProfileImageAddress, State, UserType, ProcessDate)
                VALUES (%s, %s, %s, %s, to_timestamp(%s / 1000), %s, %s, %s, %s, %s, %s)""".format(tablename)
        db_cursor.execute(sql,(p['FullName'], p['Email'], p['UserName'], p["JobTitle"], p['ScannedDate'], p['id'], p["ManagerId"], p["ProfileImageAddress"], p["State"], p["UserType"], p["ProcessDate"]))
        logging.debug(f'Inserted: {p["UserName"]} successfully')

    db_conn.commit()
# select * from data_ingest_20210501 where username = 'Diane.B.Comer@kp.org'
    db_cursor.close()
    db_conn.close()
    print(f"  -> {len(records)} records are saved to table: {tablename}.")
    logging.info(f"  -> {len(records)} records are saved to table: {tablename}.")
