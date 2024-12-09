import snowflake.connector



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
            schema = self.schema,
            role = self.role
        )
        self.curser = self.connection.cursor()

    def execute_query(self, query):
        self.curser.execute(query)
        return self.curser.fetchall()

    def close_connection(self):
        self.curser.close()
        self.connection.close()

user = "GAURI"
password = "Bhagya@1234"
account = "ISLXTGM-RM48767"
warehouse = "COMPUTE_WH"
database = "SNOWFLAKE_SAMPLE_DATA"
schema = "TPCH_SF1"
role = "SYSADMIN"


sf_connector = SnowflakeConnector(
                user = user,
                password = password,
                account =  account,
                warehouse = warehouse,
                database =  database,
                schema =  schema,
                role = role
                   )

sf_connector.connect()

query = "select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER limit 10"
result = sf_connector.execute_query(query)
print(query)

sf_connector.close_connection()
