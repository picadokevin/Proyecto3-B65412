import sys
import csv
from conf import mssql_connection, get_data_from_sql
from datetime import datetime

def extractor():
    try:
        query = '[course].[spTest_GetAll]'
        con_sb = mssql_connection()
        data = get_data_from_sql(query)
        if len(data) <= 0:
            print('No data retrieved')
            sys.exit(0)
        else:
            access = "w"
            newline = {"newline": ""}

          
            with open("test.csv", access, **newline) as outfile:
                 writer = csv.writer(outfile, quoting = csv.QUOTE_NONNUMERIC)
                 writer.writerow(
                     ["ID", "PERSONAL_ID", "NAME", "PHONE", "ADDRESS"])

                 for row in data:
                     print(row)
                     writer.writerow(row)
    except IOError as e:
         print("Error : {0} Getting data from MSSQL: {1}".format(
            e.errno, e.strerror))
    finally:
        con_sb.close()



extractor()
