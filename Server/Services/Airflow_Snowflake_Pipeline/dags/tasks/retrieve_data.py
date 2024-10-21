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
LIST_BRONZE_TABLES = ['Bronze_Consumption', 'Bronze_Investment', 'Bronze_Government_Spending', 'Bronze_Exports', 'Bronze_Imports', 'Bronze_Unemployed', 'Bronze_Labor_Force', 'Bronze_CPI', 'Bronze_Current_Account_Balance', 'Bronze_Public_Debt', 'Bronze_Interest_Rate', 'Bronze_FDI', 'Bronze_Labor_Force_Participation']
Table_Series_Mapping  = {
    'Bronze_Consumption': 'PCE',                     # Personal Consumption Expenditures (C)
    'Bronze_Investment': 'GPDIC1',                   # Gross Private Domestic Investment (I)
    'Bronze_Government_Spending': 'GCEC1',           # Government Consumption Expenditures (G)
    'Bronze_Exports': 'EXPGSC1',                     # Exports of Goods and Services (X)
    'Bronze_Imports': 'IMPGSC1',                     # Imports of Goods and Services (M)
    'Bronze_Unemployed': 'UNEMPLOY',                 # Number of Unemployed
    'Bronze_Labor_Force': 'CLF16OV',                 # Civilian Labor Force
    'Bronze_CPI': 'CPIAUCSL',                        # CPI for All Urban Consumers
    'Bronze_Current_Account_Balance': 'IEABCA',      # Current Account Balance
    'Bronze_Public_Debt': 'GFDEBTN',                 # Federal Debt: Total Public Debt
    'Bronze_Interest_Rate': 'DGS10',                 # Treasury Constant Maturity Rate
    'Bronze_FDI': 'ROWFDIQ027S',                     # Net FDI Flows
    'Bronze_Labor_Force_Participation': 'CIVPART'    # Civilian Labor Force Participation Rate
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
    manager = DataRetriever(
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

class DataRetriever:
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

        # Fetch the components of GDP from FRED, specifying the time range
        # consumption = fred.get_series('PCE', observation_start=start_date, observation_end=end_date)                    # Personal Consumption Expenditures (C)
        # investment = fred.get_series('GPDIC1', observation_start=start_date, observation_end=end_date)                  # Gross Private Domestic Investment (I)
        # government_spending = fred.get_series('GCEC1', observation_start=start_date, observation_end=end_date)          # Government Consumption Expenditures (G)
        # exports = fred.get_series('EXPGSC1', observation_start=start_date, observation_end=end_date)                    # Exports of Goods and Services (X)
        # imports = fred.get_series('IMPGSC1', observation_start=start_date, observation_end=end_date)                    # Imports of Goods and Services (M)
        # unemployed = fred.get_series('UNEMPLOY', observation_start=start_date, observation_end=end_date)                # Number of Unemployed
        # labor_force = fred.get_series('CLF16OV', observation_start=start_date, observation_end=end_date)                # Civilian Labor Force
        # cpi_series  = fred.get_series('CPIAUCSL', observation_start=start_date, observation_end=end_date)               # CPI for All Urban Consumers
        # current_account_balance = fred.get_series('IEABCA', observation_start=start_date, observation_end=end_date)     # Current Account Balance
        # public_debt = fred.get_series('GFDEBTN', observation_start=start_date, observation_end=end_date)                # Federal Debt: Total Public Debt
        # interest_rate = fred.get_series('DGS10', observation_start=start_date, observation_end=end_date)                # Treasury Constant Maturity Rate
        # fdi = fred.get_series('ROWFDIQ027S', observation_start=start_date, observation_end=end_date)                    # Net FDI Flows
        # labor_force_participation = fred.get_series('CIVPART', observation_start=start_date, observation_end=end_date)  # Civilian Labor Force Participation Rate
        
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
        
        consumption = Table_Series_Mapping['Bronze_Consumption'].resample('M').last()
        investment = Table_Series_Mapping['Bronze_Investment'].resample('M').last()
        government_spending = Table_Series_Mapping['Bronze_Government_Spending'].resample('M').last()
        exports = Table_Series_Mapping['Bronze_Exports'].resample('M').last()
        imports = Table_Series_Mapping['Bronze_Imports'].resample('M').last()
        unemployed = Table_Series_Mapping['Bronze_Unemployed'].resample('M').last()
        labor_force = Table_Series_Mapping['Bronze_Labor_Force'].resample('M').last()
        cpi_series = Table_Series_Mapping['Bronze_CPI'].resample('M').last()
        current_account_balance = Table_Series_Mapping['Bronze_Current_Account_Balance'].resample('M').last()
        public_debt = Table_Series_Mapping['Bronze_Public_Debt'].resample('M').last()
        interest_rate = Table_Series_Mapping['Bronze_Interest_Rate'].resample('M').last()
        fdi = Table_Series_Mapping['Bronze_FDI'].resample('M').last()
        labor_force_participation = Table_Series_Mapping['Bronze_Labor_Force_Participation'].resample('M').last()
        
        # consumption = consumption.resample('M').last()
        # investment = investment.resample('M').last()
        # government_spending = government_spending.resample('M').last()
        # exports = exports.resample('M').last()
        # imports = imports.resample('M').last()
        # unemployed = unemployed.resample('M').last()
        # labor_force = labor_force.resample('M').last()
        # cpi_series = cpi_series.resample('M').last()
        # current_account_balance = current_account_balance.resample('M').last()
        # public_debt = public_debt.resample('M').last()
        # interest_rate = interest_rate.resample('M').last()
        # fdi = fdi.resample('M').last()
        # labor_force_participation = labor_force_participation.resample('M').last()
        
        # Concatenate all series into a single DataFrame, aligning them by the common index (date)
        df = pd.concat([consumption, investment, government_spending, exports, imports, unemployed, labor_force,
                        cpi_series, current_account_balance, public_debt, interest_rate, fdi, labor_force_participation], axis=1, 
                        join='inner')
        # Assign column names to the DataFrame
        df.columns = ['Consumption', 'Investment', 'Government_Spending', 'Exports', 'Imports', 
                    'Unemployed', 'Labor_Force', 'CPI', 'Current_Account_Balance', 'Public_Debt', 
                    'Interest_Rate', 'FDI', 'Labor_Force_Participation']
        
        # Calculate GDP using the formula GDP = C + I + G + (X - M)
        df['GDP'] = df['Consumption'] + df['Investment'] + df['Government_Spending'] + (df['Exports'] - df['Imports'])
        df.ffill(inplace=True)
        # Calculate the GDP Growth Rate
        df['GDP Growth Rate'] = df['GDP'].pct_change(fill_method=None) * 100
        # Calculate Inflation Rate using the CPI
        df['Inflation Rate'] = df['CPI'].pct_change(fill_method=None) * 100
        # Calculate Unemployment Rate
        df['Unemployment Rate'] = (df['Unemployed'] / df['Labor_Force']) * 100
        df['DateTime'] = df.index
        # Fill any missing values with forward fill
        df.ffill(inplace=True)
        df = df.map(lambda x: str(x) if isinstance(x, pd.Timestamp) else x)
        return df