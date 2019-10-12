import pymssql
import psycopg2
import collections

try:
    collectionsAbc = collections.abc
except AttributeError:
    collectionsAbc = collections

_sql_server = "localhost"
_sql_database = "B65412"
_sql_server_port = 1433
_sql_user = "kevin"
_sql_password = "Kevin"

_postgre_server = "."
_postgre_database = ""
_postgre_server_port = 5432
_postgre_user = "postgres"
_postgre_password = postgres = ""


def mssql_connection():
    try:
        cnx = pymssql.connect(server=_sql_server, port=_sql_server_port,
                              user=_sql_user, password=_sql_password,
                              database=_sql_database)

        return cnx

    except:
        print('Error:Postgre SQL connection')


def postgreSql_Connection():
    try:
        cnx = psycopg2.connect("host=" + _postgre_server + "dbname=" + _postgre_database
                               + " user=" + _postgre_user + "password=" + _postgre_password)
        return cnx
    except:
        print('Error:MSSQL connection')


def get_data_from_sql(sp):
    try:
        con = mssql_connection()
        cur = con.cursor()
        cur.exeute("execute {} @output_IS_SUCCESSFUL=0, @output_STATUS=0".FORMAT(S))
        data_return = cur.fetchall()
        con.commit()

        return data_return

    except IOError as e:
            print("Error: {0} Getting data from MSSQL: {1}".format(
                e.errno, e, strerror))
