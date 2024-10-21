from dotenv import load_dotenv
import snowflake.connector
import os

airflow_home = os.getenv('AIRFLOW_HOME')
env_file_path = os.path.join(airflow_home, 'snowflake.env')
load_dotenv(env_file_path, override=True)

USER_SNOWFLAKE = os.getenv("USER_SNOWFLAKE")
PASSWORD_SNOWFLAKE = os.getenv("PASSWORD_SNOWFLAKE")
ACCOUNT_SNOWFLAKE = os.getenv("ACCOUNT_SNOWFLAKE")
WAREHOUSE_SNOWFLAKE = os.getenv("WAREHOUSE_SNOWFLAKE")
DATABASE_SNOWFLAKE = os.getenv("DATABASE_SNOWFLAKE")
ROLE_SNOWFLAKE = os.getenv("ROLE_SNOWFLAKE")
URL_SNOWFLAKE = os.getenv("URL_SNOWFLAKE")

# AirFlow task create Snowflake Layers Architecture
def create_snowflake_tables():
    manager = SnowflakeTableManager(
        user=USER_SNOWFLAKE,
        password=PASSWORD_SNOWFLAKE,
        account=ACCOUNT_SNOWFLAKE,
        warehouse=WAREHOUSE_SNOWFLAKE,
        database=DATABASE_SNOWFLAKE,
        role=ROLE_SNOWFLAKE,
        url=URL_SNOWFLAKE
    )
    manager.connect()
    manager.create_all_tables()
    manager.close_connection()

class SnowflakeTableManager:
    def __init__(self, user, password, account, warehouse, database, role, url):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.url = url
        self.conn = None

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
        query = f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}' 
        AND table_name = '{table_name}';
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        # Return True if the table exists
        return result[0] > 0  

    def create_schema(self, schema_name):
        """Create a schema if it does not exist."""
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
        self.execute_query(create_schema_query)

    def create_bronze_tables(self):
        """Create Bronze layer tables if they don't exist."""
        tables = {
            'Bronze_Consumption': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Consumption (
                    DateTime DATE,
                    Consumption FLOAT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_Investment': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Investment (
                    DateTime DATE,
                    Investment FLOAT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_Government_Spending': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Government_Spending (
                    DateTime DATE,
                    Government_Spending FLOAT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_Exports': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Exports (
                    DateTime DATE,
                    Exports FLOAT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_Imports': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Imports (
                    DateTime DATE,
                    Imports FLOAT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_Unemployed': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Unemployed (
                    DateTime DATE,
                    Unemployed INT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_Labor_Force': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Labor_Force (
                    DateTime DATE,
                    Labor_Force INT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_CPI': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_CPI (
                    DateTime DATE,
                    CPI FLOAT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_Current_Account_Balance': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Current_Account_Balance (
                    DateTime DATE,
                    Current_Account_Balance FLOAT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_Public_Debt': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Public_Debt (
                    DateTime DATE,
                    Public_Debt FLOAT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_Interest_Rate': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Interest_Rate (
                    DateTime DATE,
                    Interest_Rate FLOAT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_FDI': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_FDI (
                    DateTime DATE,
                    FDI FLOAT
                ) CLUSTER BY (DateTime);
            """,
            'Bronze_Labor_Force_Participation': """
                CREATE TABLE IF NOT EXISTS Bronze.Bronze_Labor_Force_Participation (
                    DateTime DATE,
                    Labor_Force_Participation FLOAT
                ) CLUSTER BY (DateTime);
            """,
        }

        for table_name, query in tables.items():
            if not self.check_table_exists('Bronze', table_name):
                self.execute_query(query)

    def create_silver_tables(self):
        """Create Silver layer table if it doesn't exist."""
        if not self.check_table_exists('Silver', 'Silver_MacroEconomic_Indicators'):
            silver_query = """
Create TABLE IF NOT EXISTS Silver.Silver_MacroEconomic_Indicators CLUSTER BY (DateTime) AS
WITH Generated_Data AS (
    SELECT 
        DATEADD(DAY, SEQ4(), '2023-01-01') AS DateTime,
        UNIFORM(500, 1000, RANDOM())::FLOAT AS Consumption,
        UNIFORM(300, 800, RANDOM())::FLOAT AS Investment,
        UNIFORM(200, 600, RANDOM())::FLOAT AS Government_Spending,
        UNIFORM(400, 900, RANDOM())::FLOAT AS Exports,
        UNIFORM(200, 700, RANDOM())::FLOAT AS Imports,
        UNIFORM(50, 200, RANDOM())::NUMBER(38,0) AS Unemployed,
        UNIFORM(200, 500, RANDOM())::NUMBER(38,0) AS Labor_Force,
        UNIFORM(50, 150, RANDOM())::FLOAT AS CPI,
        UNIFORM(-100, 100, RANDOM())::FLOAT AS Current_Account_Balance,
        UNIFORM(500, 2000, RANDOM())::FLOAT AS Public_Debt,
        UNIFORM(1, 10, RANDOM())::FLOAT AS Interest_Rate,
        UNIFORM(50, 300, RANDOM())::FLOAT AS FDI,
        UNIFORM(50, 100, RANDOM())::FLOAT AS Labor_Force_Participation
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 365))
)
SELECT 
    DateTime,
    Consumption,
    Investment,
    Government_Spending,
    Exports,
    Imports,
    Unemployed,
    Labor_Force,
    CPI,
    Current_Account_Balance,
    Public_Debt,
    Interest_Rate,
    FDI,
    Labor_Force_Participation,
    
    (Consumption + Investment + Government_Spending + (Exports - Imports)) AS GDP,
    (Consumption + Investment + Government_Spending + (Exports - Imports)) / 
        LAG((Consumption + Investment + Government_Spending + (Exports - Imports)), 1) 
        OVER (ORDER BY DateTime) - 1 AS GDP_Growth_Rate,
    CPI / LAG(CPI, 1) OVER (ORDER BY DateTime) - 1 AS Inflation_Rate,
    (Unemployed / Labor_Force) * 100 AS Unemployment_Rate
FROM 
    Generated_Data;
            """
            self.execute_query(silver_query)

    def create_gold_table(self):
        """Create Gold layer table if it doesn't exist."""
        if not self.check_table_exists('Gold', 'Gold_MacroEconomic_Forecast'):
            gold_query = """
CREATE TABLE IF NOT EXISTS Gold.Gold_MacroEconomic_Forecast 
CLUSTER BY (DateTime, GDP_Gold) AS
WITH Generated_Data AS (
    SELECT 
        DATEADD(DAY, SEQ4(), '2023-01-01') AS DateTime,
        UNIFORM(2000, 5000, RANDOM())::FLOAT AS GDP_Gold,
        UNIFORM(-0.05, 0.05, RANDOM())::FLOAT AS GDP_Growth_Rate_Gold,
        UNIFORM(-0.02, 0.02, RANDOM())::FLOAT AS Inflation_Rate_Gold,
        UNIFORM(4, 10, RANDOM())::FLOAT AS Unemployment_Rate_Gold,
        UNIFORM(100, 300, RANDOM())::FLOAT AS CPI
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 365))
),
GDP_Forecast AS (
    SELECT 
        DateTime,
        GDP_Gold,
        LAG(GDP_Gold, 1) OVER (ORDER BY DateTime) AS Previous_GDP,
        GDP_Gold * (1 + COALESCE(GDP_Growth_Rate_Gold, 0)) AS Forecasted_GDP
    FROM 
        Generated_Data
),
Inflation_Forecast AS (
    SELECT 
        DateTime,
        CPI,
        LAG(CPI, 1) OVER (ORDER BY DateTime) AS Previous_CPI,
        CPI * (1 + COALESCE(Inflation_Rate_Gold, 0)) AS Forecasted_Inflation
    FROM 
        Generated_Data
)
SELECT 
    GDPF.DateTime,
    GDPF.GDP_Gold,
    GDPF.Forecasted_GDP,
    IF.Forecasted_Inflation,
    GD.Unemployment_Rate_Gold
FROM 
    GDP_Forecast GDPF
JOIN 
    Inflation_Forecast IF ON GDPF.DateTime = IF.DateTime
JOIN 
    Generated_Data GD ON GDPF.DateTime = GD.DateTime;
            """
            self.execute_query(gold_query)

    def create_all_tables(self):
        """Create all schemas and tables."""
        self.create_schema('Bronze')
        self.create_schema('Silver')
        self.create_schema('Gold')
        self.create_bronze_tables()
        self.create_silver_tables()
        self.create_gold_table()
