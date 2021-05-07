# from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import os
import json
import logging
import shutil
from datetime import datetime, timedelta, date
from sql.save_ingestion_table import save_user_to_database

def parse_active_user_file_operator(dagObject, table_name, base_folder):
    date_tag = date.today().strftime('%Y%m%d')    #'''{{ ds_nodash }}'''
    filepath = f'{base_folder}/input/{date_tag}/active_user.txt'
    parse_active_user = PythonOperator(task_id='parse_active_user',
                            python_callable=parse_active_user_file,
                            op_kwargs={'base_folder': f'{base_folder}',
                                    'filepath': f'{filepath}', 
                                    'table_name': f'{table_name}', 
                                    'date_tag': f'{date_tag}'},
                            dag=dagObject)
    return parse_active_user

def parse_active_user_file(table_name, base_folder, filepath, date_tag):
    print(f'Opening {filepath} ...')
    try:
        with open(filepath) as fp:
            records = []
            for line in fp:
                records.append(parse_active_user(line, date_tag))
            save_user_to_database(table_name, records)
    except:
        destfolder = os.path.join(base_folder, 'error', date_tag)
        destFile = os.path.join(destfolder, "active_user.txt")
        os.makedirs(destfolder, exist_ok=True)
        shutil.move(filepath, destFile)
    else:
        destfolder = os.path.join(base_folder, 'processed', date_tag)
        destFile = os.path.join(destfolder, "active_user.txt")
        os.makedirs(destfolder, exist_ok=True)
        shutil.move(filepath, destFile)

def parse_active_user(logString, date_tag):
    person = json.loads(logString)
    ManagerId = ""
    ProfileImageAddress = ""
    person["ProcessDate"] = date_tag
    person["State"] = "ACTIVE"
    person["UserType"] = "USER"
    if 'ManagerId' not in person.keys(): person['ManagerId'] = None
    if 'JobTitle' not in person.keys(): person['JobTitle'] = ""
    if 'ProfileImageAddress' not in person.keys(): person['ProfileImageAddress'] = None
    if person['Email'] is None: person['Email'] = person['UserName']
    if person['Email'].lower().startswith('svc') or person['UserName'].lower().startswith('svc'): person["UserType"] = "SERVICE"
    print(person)
    return person

if __name__ == "__main__":
    """
    {"FullName":"Diane Comer","Email":"Diane.B.Comer@kp.org","UserName":"Diane.B.Comer@kp.org","JobTitle":"EVP, Chief Info & Tech Officer","ScannedDate":1619717902511,"id":"e98be936-a1d8-4c01-8773-8e1ac7b9aee0"},
    {"FullName":"Lisa L Caplan","Email":"Lisa.Caplan@kp.org","UserName":"Lisa.Caplan@kp.org","JobTitle":"SVP, Care Delivery Technology Services","ProfileImageAddress":"https://sfnam.loki.delve.office.com/api/v2/personaphoto?aadObjectId=f11c16da-8185-4223-a228-61abb2bea0d6&clientType=DelveMiddleTier&clientFeature=LivePersonaCard&personaType=User","AadObjectId":"f11c16da-8185-4223-a228-61abb2bea0d6","ScannedDate":1619717902511,"ManagerId":"e98be936-a1d8-4c01-8773-8e1ac7b9aee0","id":"f11c16da-8185-4223-a228-61abb2bea0d6"},
    {"FullName":"Steven P Draeger","Email":"Steven.P.Draeger@kp.org","UserName":"Steven.P.Draeger@kp.org","JobTitle":"VP, CFO - IT","ProfileImageAddress":"https://sfnam.loki.delve.office.com/api/v2/personaphoto?aadObjectId=9a1bbc7b-47b4-4585-b886-08e32f9ab4ee&clientType=DelveMiddleTier&clientFeature=LivePersonaCard&personaType=User","AadObjectId":"9a1bbc7b-47b4-4585-b886-08e32f9ab4ee","ScannedDate":1619717902511,"ManagerId":"e98be936-a1d8-4c01-8773-8e1ac7b9aee0","id":"9a1bbc7b-47b4-4585-b886-08e32f9ab4ee"},
    """
    
    user0 = '{"FullName":"Diane Comer","Email":"Diane.B.Comer@kp.org","UserName":"Diane.B.Comer@kp.org","JobTitle":"EVP, Chief Info & Tech Officer","ScannedDate":1619717902511,"id":"e98be936-a1d8-4c01-8773-8e1ac7b9aee0"}'
    user1 = '{"FullName":"Lisa L Caplan","Email":"Lisa.Caplan@kp.org","UserName":"Lisa.Caplan@kp.org","JobTitle":"SVP, Care Delivery Technology Services","ProfileImageAddress":"https://sfnam.loki.delve.office.com/api/v2/personaphoto?aadObjectId=f11c16da-8185-4223-a228-61abb2bea0d6&clientType=DelveMiddleTier&clientFeature=LivePersonaCard&personaType=User","AadObjectId":"f11c16da-8185-4223-a228-61abb2bea0d6","ScannedDate":1619717902511,"ManagerId":"e98be936-a1d8-4c01-8773-8e1ac7b9aee0","id":"f11c16da-8185-4223-a228-61abb2bea0d6"}'
    tablename = "TESTING"
    date_tag = date.today().strftime('%Y%m%d')    #'''{{ ds_nodash }}'''
    print(user0)
    p = parse_active_user(user0,date_tag)
    sql = """INSERT INTO {} (FullName, Email, UserName, JobTitle, ScannedDate, id, ManagerId, ProfileImageAddress, State, UserType, ProcessDate)
            VALUES (%s, %s, %s, %s, to_timestamp(%s / 1000), %s, %s, %s, %s, %s, %s)""".format(tablename, (p['FullName'], p['Email'], p['UserName'], p["JobTitle"], p['ScannedDate'], p['id'], p["ManagerId"], p["ProfileImageAddress"], p["State"], p["UserType"], p["ProcessDate"]))
    print(sql)

    # print(user1)
    # parse_active_user(user1,date_tag)
 