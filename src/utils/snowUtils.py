import snowflake.connector
from attr import dataclass


class SnowflakeConnector:
    def __init__(self, account, user, password, warehouse, database, schema, role):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role


    def connect(self):
        self.connection = snowflake.connector.connect(
            user = self.user,
            password = self.password,
            account = self.account,
            warehouse = self.warehouse,
            database = self.database,
            schema = self.schema
        )
        self.curser = self.connection.cursor()

    def execute_query(self, query):
        self.curser.execute(query)
        return self.curser.fetchall()

    def close_connection(self):
        self.curser.close()
        self.connection.close()
        


