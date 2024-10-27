import snowflake.connector

class CriticalServices:
    def __init__(self, user, password, account, warehouse, database, role, url):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.url = url
        self.conn = None
    
    def retrieve_gold_table_task(self):
        """Retrieve data from the Gold_MacroEconomic_Forecast table."""
        if_exist = self.check_table_exists('Gold', 'Gold_MacroEconomic_Forecast')
        if if_exist:
            self.connect()
            query = """
            SELECT 
                DateTime,
                GDP_Gold,
                Forecasted_GDP,
                Forecasted_Inflation,
                Unemployment_Rate_Gold
            FROM 
                Gold.Gold_MacroEconomic_Forecast;
            """
            cursor = self.conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            result_data = [dict(zip(column_names, row)) for row in result]
            cursor.close()
            self.close_connection()
            return result_data
        else:
            self.close_connection()
            return if_exist
    
    def connect(self):
        """Establish a connection to Snowflake."""
        self.conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            role= self.role,
            url=self.url
        )
        return self.conn

    def close_connection(self):
        """Close the connection to Snowflake."""
        if self.conn:
            self.conn.close()

    def execute_query(self, query):
        """Execute a given SQL query in Snowflake."""
        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()
    
    def check_table_exists(self, schema_name, table_name):
        """Check if a table exists in the specified schema."""
        self.connect()
        query = f"""
        SELECT COUNT(*) FROM Gold.Gold_MacroEconomic_Forecast;
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        return result