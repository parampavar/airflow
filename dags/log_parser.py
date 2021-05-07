from airflow.hooks.postgres_hook import PostgresHook
import re
import json
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
        ManagerId = ""
        ProfileImageAddress = ""
        State = "ACTIVE"
        UserType = "USER"
        if 'ManagerId' in p.keys(): 
            ManagerId = p['ManagerId'] 
        else: 
            ManagerId = None
        if 'JobTitle' in p.keys(): 
            JobTitle = p['JobTitle'] 
        else: 
            JobTitle = ""
        if p['Email'] is None: p['Email'] = p['UserName']

        if p['Email'].lower().startswith('svc') or p['UserName'].lower().startswith('svc'):
            UserType = "SERVICE"
        
        print("Inserting " + p['UserName'])
        logging.info(f'Inserting: {p["UserName"]}')

        if 'ProfileImageAddress' in p.keys(): ProfileImageAddress = p['ProfileImageAddress']
        sql = """INSERT INTO {} (FullName, Email, UserName, JobTitle, ScannedDate, id, ManagerId, ProfileImageAddress, State, UserType)
                VALUES (%s, %s, %s, %s, to_timestamp(%s / 1000), %s, %s, %s, %s, %s)""".format(tablename)
        db_cursor.execute(sql,(p['FullName'], p['Email'], p['UserName'], JobTitle, p['ScannedDate'], p['id'], ManagerId, ProfileImageAddress, State, UserType))
        logging.debug(f'Inserted: {p["UserName"]} successfully')

    db_conn.commit()
# select * from data_ingest_20210501 where username = 'Diane.B.Comer@kp.org'
    db_cursor.close()
    db_conn.close()
    print(f"  -> {len(records)} records are saved to table: {tablename}.")
    logging.info(f"  -> {len(records)} records are saved to table: {tablename}.")

def parse_log(logString):
    # r = r".+\/(?P<file>.+):(?P<line>\d+):\[\[\]\] (?P<date>.+)/(?P<time>\d{2}:\d{2}:\d{2},\d{3}) ERROR ?(?:SessionId : )?(?P<session>.+)? \[(?P<app>\w+)\] dao\.AbstractSoapDao - (?P<module>(?=[\w]+ -)[\w]+)? - Service Exception: (?P<errMsg>.+)"
    r = r".+\/(?P<file>.+):(?P<line>\d+):\[\[\]\] (?P<date>.+)/(?P<time>\d{2}:\d{2}:\d{2},\d{3}) ERROR ?(?:SessionId : )?(?P<session>.+)? \[(?P<app>\w+)\] .+ (?:Error \:|service Exception) (?P<module>(?=[\w\.-]+ : )[\w\.-]+)?(?: \: )?(?P<errMsg>.+)"
    group = re.match(r, logString)
    # print(group.group('file'), group.group('session'), group.group('errMsg'))
    print(group.groups())
    return group.groups()

def parse_active_user(logString):
    # # r = r".+\/(?P<file>.+):(?P<line>\d+):\[\[\]\] (?P<date>.+)/(?P<time>\d{2}:\d{2}:\d{2},\d{3}) ERROR ?(?:SessionId : )?(?P<session>.+)? \[(?P<app>\w+)\] dao\.AbstractSoapDao - (?P<module>(?=[\w]+ -)[\w]+)? - Service Exception: (?P<errMsg>.+)"
    # r = r".+\/(?P<file>.+):(?P<line>\d+):\[\[\]\] (?P<date>.+)/(?P<time>\d{2}:\d{2}:\d{2},\d{3}) ERROR ?(?:SessionId : )?(?P<session>.+)? \[(?P<app>\w+)\] .+ (?:Error \:|service Exception) (?P<module>(?=[\w\.-]+ : )[\w\.-]+)?(?: \: )?(?P<errMsg>.+)"
    # group = re.match(r, logString)
    # # print(group.group('file'), group.group('session'), group.group('errMsg'))
    # print(group.groups())
    # return group.groups()
    person = json.loads(logString)
    print(person)
    return person

def parse_log_file(filepath, tablename):
    print(f'Opening {filepath} ...')
    with open(filepath) as fp:
        records = []
        for line in fp:
            records.append(parse_log(line))
        save_log_to_database(tablename, records)

def parse_active_user_file(filepath, tablename):
    print(f'Opening {filepath} ...')
    with open(filepath) as fp:
        records = []
        for line in fp:
            records.append(parse_active_user(line))
        save_user_to_database(tablename, records)



if __name__ == "__main__":
    """
    /usr/local/airflow/data/20200726/securityApp.log:817:[[]] 26 Jul 2020/02:00:39,055 ERROR SessionId : pULd6qoRgSJf9YME2Ij7G71+CgQ= [securityApp] dao.AbstractSoapDao - service Exception GetToken-V2 : java.net.SocketTimeoutException: Read timed out
    /usr/local/airflow/data/20200726/timeApp.log:1:[[]] 26 Jul 2020/03:10:53,463 ERROR [timeApp] exception.ExceptionHandler - Error : No message found under code 'GET=-SM-https%3a%2f%2fwww%2egoogle%2ecom%2fmytest%2fapi%2fsecureDomain%2fresource%2ftimeGet' for locale 'en_US'.
    /usr/local/airflow/data/20200724/extApp.log:140241:[[]] 24 Jul 2020/12:10:37,590 ERROR SessionId : Kwg8BIZESWeZ12/j3vefwPe2KGA= [extApp] dao.AbstractSoapDao - service Exception GetNotificationService : java.net.SocketTimeoutException: Read timed out
    /usr/local/airflow/data/20200723/extApp.log:140344:[[]] 23 Jul 2020/13:22:26,430 ERROR  [extApp] exception.ExceptionHandler - Error : Unrecognized SSL message, plaintext connection?
    /usr/local/airflow/data/20200723/extApp.log:140851:[[]] 23 Jul 2020/13:23:19,196 ERROR SessionId : u0UkvLFDNMsMIcbuOzo86Lq8OcU= [extApp] exception.ExceptionHandler - Error : Unrecognized SSL message, plaintext connection?
    /usr/local/airflow/data/20200726/loginApp.log:169:[[]] 26 Jul 2020/02:53:57,226 ERROR SessionId : ALQJ7pIq8FOrCWwCkHBQn/YP9q8= [loginApp] dao.AbstractSoapDao - service Exception LoginDao.SubmitForm : java.net.SocketTimeoutException: Read timed out
    """
    temp0 = "/usr/local/airflow/data/20200724/extApp.log:140241:[[]] 24 Jul 2020/12:10:37,590 ERROR SessionId : Kwg8BIZESWeZ12/j3vefwPe2KGA= [extApp] dao.AbstractSoapDao - service Exception GetNotificationService : java.net.SocketTimeoutException: Read timed out"
    # parse_log(temp0)

    """
    {"FullName":"Diane Comer","Email":"Diane.B.Comer@kp.org","UserName":"Diane.B.Comer@kp.org","JobTitle":"EVP, Chief Info & Tech Officer","ScannedDate":1619717902511,"id":"e98be936-a1d8-4c01-8773-8e1ac7b9aee0"},
    {"FullName":"Lisa L Caplan","Email":"Lisa.Caplan@kp.org","UserName":"Lisa.Caplan@kp.org","JobTitle":"SVP, Care Delivery Technology Services","ProfileImageAddress":"https://sfnam.loki.delve.office.com/api/v2/personaphoto?aadObjectId=f11c16da-8185-4223-a228-61abb2bea0d6&clientType=DelveMiddleTier&clientFeature=LivePersonaCard&personaType=User","AadObjectId":"f11c16da-8185-4223-a228-61abb2bea0d6","ScannedDate":1619717902511,"ManagerId":"e98be936-a1d8-4c01-8773-8e1ac7b9aee0","id":"f11c16da-8185-4223-a228-61abb2bea0d6"},
    {"FullName":"Steven P Draeger","Email":"Steven.P.Draeger@kp.org","UserName":"Steven.P.Draeger@kp.org","JobTitle":"VP, CFO - IT","ProfileImageAddress":"https://sfnam.loki.delve.office.com/api/v2/personaphoto?aadObjectId=9a1bbc7b-47b4-4585-b886-08e32f9ab4ee&clientType=DelveMiddleTier&clientFeature=LivePersonaCard&personaType=User","AadObjectId":"9a1bbc7b-47b4-4585-b886-08e32f9ab4ee","ScannedDate":1619717902511,"ManagerId":"e98be936-a1d8-4c01-8773-8e1ac7b9aee0","id":"9a1bbc7b-47b4-4585-b886-08e32f9ab4ee"},
    """
    
    user0 = '{"FullName":"Diane Comer","Email":"Diane.B.Comer@kp.org","UserName":"Diane.B.Comer@kp.org","JobTitle":"EVP, Chief Info & Tech Officer","ScannedDate":1619717902511,"id":"e98be936-a1d8-4c01-8773-8e1ac7b9aee0"}'
    user1 = '{"FullName":"Lisa L Caplan","Email":"Lisa.Caplan@kp.org","UserName":"Lisa.Caplan@kp.org","JobTitle":"SVP, Care Delivery Technology Services","ProfileImageAddress":"https://sfnam.loki.delve.office.com/api/v2/personaphoto?aadObjectId=f11c16da-8185-4223-a228-61abb2bea0d6&clientType=DelveMiddleTier&clientFeature=LivePersonaCard&personaType=User","AadObjectId":"f11c16da-8185-4223-a228-61abb2bea0d6","ScannedDate":1619717902511,"ManagerId":"e98be936-a1d8-4c01-8773-8e1ac7b9aee0","id":"f11c16da-8185-4223-a228-61abb2bea0d6"}'
    print(user0)
    parse_active_user(user0)
    print(user1)
    parse_active_user(user1)
 