import psycopg2, os
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.core.exceptions import AzureError


class Dbo:
    def __init__(self):
        self.connection = None

    def connect(self):
        try:
            credential = ClientSecretCredential(os.environ['AZURE_TENANT_ID'], os.environ['AZURE_CLIENT_ID'],
                                                os.environ['AZURE_CLIENT_SECRET'])
            token = credential.get_token("https://ossrdbms-aad.database.windows.net/.default")
            access_token = token.token
            connection = psycopg2.connect(
                host=os.environ['DATABASE_HOST'],
                database=os.environ['DATABASE_NAME'],
                user=os.environ['DATABASE_USER'],
                password=access_token,
                port=os.environ['DATABASE_PORT'],
                sslmode='require'
            )
            print("Connection established successfully")
            self.connection = connection
        except AzureError as e:
            print(f"Error obtaining token: {e}")
            raise e
        except Exception as e:
            raise e

    def run_select_query(self, sql, params=None):
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            data_list = []
            column_names = [column[0] for column in cursor.description]
            for row in cursor.fetchall():
                temp = {}
                for i, value in enumerate(row):
                    temp[column_names[i]] = value
                data_list.append(temp)
            return data_list
        except Exception as e:
            raise e

    def run_insert_query(self, sql, params=None):
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            cursor.commit()
        except Exception as e:
            raise e

    def run_sql_query(self, sql, params=None):
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            cursor.commit()
        except Exception as e:
            raise e

    def connection_close(self):
        if self.connection:
            self.connection.cursor().close()
            self.connection.close()
