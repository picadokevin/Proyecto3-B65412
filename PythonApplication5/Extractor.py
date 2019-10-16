import sys
import csv
from conf import mssql_connection, get_data_from_sql
from datetime import datetime

def extractor(tableName):
    try:
        query = '[spTest_GetAll]'
        con_sb = mssql_connection()
        data = get_data_from_sql(query,tableName)
        if len(data) <= 0:
            print('No data retrieved')
            sys.exit(0)
        else:
            access = "w"
            newline = {"newline": ""}

            now = datetime.now()
            timestamp = datetime.now()
            nombre= str(now.strftime('%Y-%m-%d') +'.csv')
            if tableName=="Cliente":

                with open(nombre , access, **newline) as outfile:
                     writer = csv.writer(outfile, quoting = csv.QUOTE_NONNUMERIC)
                     writer.writerow(
                        [ "id_client","name","birth_date"])

                for row in data:
                     print(row)
                     writer.writerow(row)

            elif tableName=="Correo":
                  with open(nombre , access, **newline) as outfile:
                        writer = csv.writer(outfile, quoting = csv.QUOTE_NONNUMERIC)
                        writer.writerow(
                           [ "mail_address","password"])

                  for row in data:
                     print(row)
                     writer.writerow(row)

            elif tableName=="Direccion":
                   with open(nombre , access, **newline) as outfile:
                         writer = csv.writer(outfile, quoting = csv.QUOTE_NONNUMERIC)
                         writer.writerow(
                             [ "id_address","cod_postal","description"])

                   for row in data:
                        print(row)
                        writer.writerow(row)  

            elif tableName=="direccionCliente":
                    with open(nombre , access, **newline) as outfile:
                           writer = csv.writer(outfile, quoting = csv.QUOTE_NONNUMERIC)
                           writer.writerow(
                               [ "id_address","id_client"])

                    for row in data:
                        print(row)
                        writer.writerow(row)
            elif tableName=="tarjetaCliente":
                    with open(nombre , access, **newline) as outfile:
                           writer = csv.writer(outfile, quoting = csv.QUOTE_NONNUMERIC)
                           writer.writerow(
                               [ "id_client","id_credit_card"])

                    for row in data:
                        print(row)
                        writer.writerow(row)
            elif tableName=="TarjetaCredito":
                     with open(nombre , access, **newline) as outfile:
                           writer = csv.writer(outfile, quoting = csv.QUOTE_NONNUMERIC)
                           writer.writerow(
                               [ "id_credit_card","expiration_date"])

                     for row in data:
                        print(row)
                        writer.writerow(row)
    except IOError as e:
         print("Error : {0} Getting data from MSSQL: {1}".format(
            e.errno, e.strerror))
    finally:
        con_sb.close()



extractor('Cliente')
