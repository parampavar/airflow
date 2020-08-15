from airflow.hooks.postgres_hook import PostgresHook
import re

def save_to_database(tablename, records):
    if not records:
        print("Empty record!")
        return

    # database hook
    db_hook = PostgresHook(postgres_conn_id='postgres_default', schema='airflow')
    db_conn = db_hook.get_conn()
    db_cursor = db_conn.cursor()

    #('extApp.log', '22995', '23 Jul 2020', '02:53:13,527', None, 'extApp', 'Unrecognized SSL message, plaintext connection?')
    sql = """INSERT INTO {} (filename, line, date, time, session, app, module, error)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""".format(tablename)
    # db_cursor.execute(sql, group)
    # get the generated id back
    # vendor_id = db_cursor.fetchone()[0]
    # execute the INSERT statement
    db_cursor.executemany(sql, records)
    db_conn.commit()

    db_cursor.close()
    db_conn.close()
    print(f"  -> {len(records)} records are saved to table: {tablename}.")


def parse_log(logString):
    # r = r".+\/(?P<file>.+):(?P<line>\d+):\[\[\]\] (?P<date>.+)/(?P<time>\d{2}:\d{2}:\d{2},\d{3}) ERROR ?(?:SessionId : )?(?P<session>.+)? \[(?P<app>\w+)\] dao\.AbstractSoapDao - (?P<module>(?=[\w]+ -)[\w]+)? - Service Exception: (?P<errMsg>.+)"
    r = r".+\/(?P<file>.+):(?P<line>\d+):\[\[\]\] (?P<date>.+)/(?P<time>\d{2}:\d{2}:\d{2},\d{3}) ERROR ?(?:SessionId : )?(?P<session>.+)? \[(?P<app>\w+)\] .+ (?:Error \:|service Exception) (?P<module>(?=[\w\.-]+ : )[\w\.-]+)?(?: \: )?(?P<errMsg>.+)"
    group = re.match(r, logString)
    # print(group.group('file'), group.group('session'), group.group('errMsg'))
    print(group.groups())
    return group.groups()


def parse_log_file(filepath, tablename):
    print(f'Opening {filepath} ...')
    with open(filepath) as fp:
        records = []
        for line in fp:
            records.append(parse_log(line))
        save_to_database(tablename, records)



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
    parse_log(temp0)
 