from dotenv import load_dotenv
import snowflake.connector
from fredapi import Fred
import pandas as pd
import os

airflow_home = os.getenv('AIRFLOW_HOME')
env_file_path = os.path.join(airflow_home, 'snowflake.env')
load_dotenv(env_file_path, override=True)

FRED_API_KEY = os.getenv("FRED_API_KEY")
START_DATE = '2009-01-01'
END_DATE = pd.to_datetime('today').strftime('%Y-%m-%d')
#END_DATE = (pd.to_datetime('today') - pd.Timedelta(days=1500)).strftime('%Y-%m-%d')
LIST_BRONZE_TABLES = ['Bronze_Micro_Household_Spending','Bronze_Micro_Small_Business_Loans', 'Bronze_Micro_Retail_Sales', 'Bronze_Micro_Personal_Income']
Table_Series_Mapping  = {
    'Bronze_Micro_Household_Spending': 'DPCERG3M086SBEA',        # Household Spending
    'Bronze_Micro_Small_Business_Loans': 'BUSLOANS',             # Small Business Loans
    'Bronze_Micro_Retail_Sales': 'RSAFS',                        # Retail Sales
    'Bronze_Micro_Personal_Income': 'PI',                        # Personal Income
}

USER_SNOWFLAKE = os.getenv("USER_SNOWFLAKE")
PASSWORD_SNOWFLAKE = os.getenv("PASSWORD_SNOWFLAKE")
ACCOUNT_SNOWFLAKE = os.getenv("ACCOUNT_SNOWFLAKE")
WAREHOUSE_SNOWFLAKE = os.getenv("WAREHOUSE_SNOWFLAKE")
DATABASE_SNOWFLAKE = os.getenv("DATABASE_SNOWFLAKE")
ROLE_SNOWFLAKE = os.getenv("ROLE_SNOWFLAKE")
URL_SNOWFLAKE = os.getenv("URL_SNOWFLAKE")

# Airflow task to retrieve data from FRED
def retrieve_data(**kwargs):
    manager = MicroDataRetriever(
        api_key=FRED_API_KEY,
        user=USER_SNOWFLAKE,
        password=PASSWORD_SNOWFLAKE,
        account=ACCOUNT_SNOWFLAKE,
        warehouse=WAREHOUSE_SNOWFLAKE,
        database=DATABASE_SNOWFLAKE,
        role=ROLE_SNOWFLAKE,
        url=URL_SNOWFLAKE
    )
    df = manager.retrieve_data_fred()
    kwargs['ti'].xcom_push(key='retrieved_data', value=df.to_dict())
    return df

class MicroDataRetriever:
    def __init__(self, api_key, user, password, account, warehouse, database, role, url):
        """Constructor for the DataRetriever class

        Args:
            api_key (str): FRED API key
            start_date (str): Start date for the data retrieval
            end_date (str): end date for the data retrieval
            user (str): username for the Snowflake connection
            password (str): password for the Snowflake connection
            account (str): account for the Snowflake connection
            warehouse (str): warehouse for the Snowflake connection
            database (str): database for the Snowflake connection
            role (str): role for the Snowflake connection
            url (str): url for the Snowflake connection
        """
        self.api_key = api_key
        self.fred = Fred(api_key=self.api_key)
        # Snowflake connection parameters
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.url = url
        self.conn = self.connect_to_snowflake()
        self.table_date_retrieve_dictionary = self.check_db_data()

    def check_db_data(self):
        list_of_tables_exist_in_bronze_snowflake = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'Bronze'
                AND table_type = 'BASE TABLE';
        """
        cursor = self.conn.cursor()
        cursor.execute(list_of_tables_exist_in_bronze_snowflake)
        tables = cursor.fetchall()
        table_time_ranges = {}
        for table in tables:
            table_name = table[0]
            time_range_query = f"""
            SELECT MIN(DateTime) AS min_time, MAX(DateTime) AS max_time
            FROM Bronze.{table_name};
            """
            cursor.execute(time_range_query)
            time_range_result = cursor.fetchone()

            # Store the time range (min and max DateTime) in the dictionary
            if time_range_result:
                min_time, max_time = time_range_result
                table_time_ranges[table_name] = {
                    'min_time': min_time,
                    'max_time': max_time
                }
        
        return table_time_ranges
        
    def connect_to_snowflake(self):
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
    
    def retrieve_data_fred(self):
        fred = Fred(api_key=FRED_API_KEY)
        start_date = START_DATE
        end_date = END_DATE
        
        for table_name in LIST_BRONZE_TABLES:
            if table_name in self.table_date_retrieve_dictionary:
                start_date = self.table_date_retrieve_dictionary[table_name]['max_time'] + pd.Timedelta(days=1)
            else:
                # Default start date if the table is not in the dictionary
                start_date = START_DATE
                
            if start_date > END_DATE:
                start_date = (pd.to_datetime(END_DATE) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')

            # Get the corresponding FRED series based on the table name
            if table_name in Table_Series_Mapping:
                series_code = Table_Series_Mapping[table_name]
                # Fetch data from FRED using the appropriate series code and date range
                data = fred.get_series(series_code, observation_start=start_date, observation_end=end_date)
                Table_Series_Mapping[table_name] = data
            else:
                print(f"No matching FRED series found for table {table_name}")

        household_spending = Table_Series_Mapping['Bronze_Micro_Household_Spending'].resample('M').last()
        small_bussiness_loans = Table_Series_Mapping['Bronze_Micro_Small_Business_Loans'].resample('M').last()
        retails_sales = Table_Series_Mapping['Bronze_Micro_Retail_Sales'].resample('M').last()
        personal_income = Table_Series_Mapping['Bronze_Micro_Personal_Income'].resample('M').last()

        
        # Concatenate all series into a single DataFrame, aligning them by the common index (date)
        df = pd.concat([household_spending, small_bussiness_loans, retails_sales, personal_income], axis=1, join='inner')
        # Assign column names to the DataFrame
        df.columns = ['Bronze_Micro_Household_Spending', 'Bronze_Micro_Small_Business_Loans', 'Bronze_Micro_Retail_Sales', 'Bronze_Micro_Personal_Income']
        df.ffill(inplace=True)
        df['DateTime'] = df.index
        df['Date'] = pd.to_datetime(df['DateTime']).dt.date
        df = df.drop(columns=['DateTime'])
        df = df.rename(columns={'Date': 'DateTime'})
        # Fill any missing values with forward fill
        df.ffill(inplace=True)
        df = df.map(lambda x: str(x) if isinstance(x, pd.Timestamp) else x)
        return df