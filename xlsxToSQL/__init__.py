import logging
import azure.functions as func
from azure.storage.blob import ContainerClient
import pandas as pd
import pyodbc 
import re
import json

def main(myblob: func.InputStream):
    try:

        logging.info('Python blob trigger function processed a request.')

        account_name = "###" # confidential
        account_key = "###" # confidential
        top_level_container_name = "###" # confidential
        blob_service = ContainerClient(account_url=account_name, container_name=top_level_container_name, credential=account_key)

        # Make connection with Azure SQL database

        server = 'datatrust-ff.database.windows.net' 
        database = 'DataTrust' 
        username = 'lucas.vergeest' 
        password = 'DitIsDataTrust2020' 
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        cursor.execute("SELECT @@version;")
        row = cursor.fetchone() 
        while row:
            print(row[0])
            row = cursor.fetchone()

        # Download xlsx-files from Azure blob storage

        logging.info("\nList blobs in the container")
        generator = blob_service.list_blobs()
        for blob in generator:
            if blob.name.endswith('.xlsx'):
                logging.info("\t Blob name: " + blob.name)
                file_name = re.sub('.*/', '', blob.name)
                xlsx_file = open(file_name, 'wb')
                b = blob_service.download_blob(blob)
                b.readinto(xlsx_file)
                #xlsx_file.write(b)
                xlsx_file = open(file_name, 'rb')
                data = xlsx_file.read()
                data = pd.read_excel(data)
                headers = list(data.columns.values)
                list_values = data.values.tolist()
                blob_name = blob.name

                tableName = re.sub('.xlsx', '', blob_name)
                tableName = re.sub('^.*/', '', tableName)
                tableName = re.sub(' ', '_', tableName)

                # If table exists: remove and rewrite

                try:
                    cursor.execute("DROP TABLE dbo." + tableName)
                except:
                    print('Table does not exist yet')

                # Create new table

                query_string = 'CREATE TABLE dbo.' + tableName + ' ('

                # Add columns to table

                columns = ''
                for i in range(len(headers)):
                    headers[i] = re.sub('[ /-]', '_', str(headers[i]))
                    headers[i] = re.sub("[\(\)€'\.,]", '', str(headers[i]))
                    columns += headers[i] + ', '
                    if i == len(headers) - 1:
                        query_string += '\n' + headers[i] + ' VARCHAR(1000)'
                    else:
                        query_string += '\n' + headers[i] + ' VARCHAR(1000),'
                query_string += '\n);'
                query_string = re.sub('[/-]', '', query_string)
                cursor.execute(query_string)

                # Add rows to table

                query_string = "INSERT INTO dbo." + tableName + "(" + columns[:-2] +") VALUES "
                for row in range(len(list_values)):
                    list_values[row] = [str(i) for i in list_values[row]]
                    row_new = []
                    for item in list_values[row]:
                        item = re.sub('[\(\)\r\n\,\'\-]', '', item)
                        item = "'" + item + "'"
                        row_new.append(item)

                    row_new = ','.join(row_new)
                    if (row + 1) % 1000 == 0 or row + 1 == len(list_values):
                        query_string += '(' + row_new + ');'
                        print(query_string)
                        cursor.execute(query_string)
                        query_string = "INSERT INTO dbo." + tableName + "(" + columns[:-2] +") VALUES "
                    else:
                        query_string += '(' + row_new + '),'
                    
                cnxn.commit()

    except Exception as e:
        logging.exception(e)

