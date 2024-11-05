from dotenv import load_dotenv
import snowflake.connector
from fredapi import Fred
import pandas as pd
import os

# Load environment variables for Snowflake and FRED API
load_dotenv("path_to_your_env_file")

FRED_API_KEY = os.getenv("FRED_API_KEY")
START_DATE = '2009-01-01'
END_DATE = pd.to_datetime('today').strftime('%Y-%m-%d')

# Microeconomic table mappings (example series codes)
MICROECONOMIC_TABLES = {
    'Micro_Household_Spending': 'DPCERG3M086SBEA',        # Household Spending
    'Micro_Housing_Prices': 'CSUSHPINSA',                 # Case-Shiller U.S. National Home Price Index
    'Micro_Small_Business_Loans': 'BUSLOANS',             # Small Business Loans
    'Micro_Retail_Sales': 'RSAFS',                        # Retail Sales
    'Micro_Personal_Income': 'PI',                        # Personal Income
    'Micro_Unemployment_Rate': 'UNRATE',                  # Unemployment Rate
    'Micro_Consumption': 'PCE',                           # Personal Consumption Expenditures (for GDP calc)
    'Micro_Investment': 'GPDIC1',                         # Gross Private Domestic Investment (for GDP calc)
    'Micro_Government_Spending': 'GCEC1',                 # Government Consumption Expenditures (for GDP calc)
    'Micro_Exports': 'EXPGSC1',                           # Exports of Goods and Services (for GDP calc)
    'Micro_Imports': 'IMPGSC1',                           # Imports of Goods and Services (for GDP calc)
    'Micro_Public_Debt': 'GFDEBTN',                       # Federal Debt: Total Public Debt
    'Micro_Interest_Rate': 'DGS10'                        # Treasury Constant Maturity Rate
}

USER_SNOWFLAKE = os.getenv("USER_SNOWFLAKE")
PASSWORD_SNOWFLAKE = os.getenv("PASSWORD_SNOWFLAKE")
ACCOUNT_SNOWFLAKE = os.getenv("ACCOUNT_SNOWFLAKE")
WAREHOUSE_SNOWFLAKE = os.getenv("WAREHOUSE_SNOWFLAKE")
DATABASE_SNOWFLAKE = os.getenv("DATABASE_SNOWFLAKE")
ROLE_SNOWFLAKE = os.getenv("ROLE_SNOWFLAKE")

class MicroeconomicDataRetriever:
    def __init__(self):
        """Initialize the MicroeconomicDataRetriever class."""
        self.fred = Fred(api_key=FRED_API_KEY)
        self.conn = self.connect_to_snowflake()

    def connect_to_snowflake(self):
        """Establish a connection to Snowflake."""
        return snowflake.connector.connect(
            user=USER_SNOWFLAKE,
            password=PASSWORD_SNOWFLAKE,
            account=ACCOUNT_SNOWFLAKE,
            warehouse=WAREHOUSE_SNOWFLAKE,
            database=DATABASE_SNOWFLAKE,
            role=ROLE_SNOWFLAKE
        )

    def retrieve_data(self):
        """Retrieve data from FRED for each microeconomic indicator."""
        micro_data = {}
        for table_name, series_code in MICROECONOMIC_TABLES.items():
            data = self.fred.get_series(series_code, observation_start=START_DATE, observation_end=END_DATE)
            micro_data[table_name] = data

        # Convert micro_data dictionary to DataFrame
        df = pd.DataFrame(micro_data)
        df.index.name = 'DateTime'
        df.reset_index(inplace=True)
        
        # Additional Calculations
        df = self.calculate_metrics(df)
        
        return df

    def calculate_metrics(self, df):
        """Perform additional calculations and add derived metrics to DataFrame."""
        # Calculate GDP using the formula GDP = C + I + G + (X - M)
        df['GDP'] = df['Micro_Consumption'] + df['Micro_Investment'] + df['Micro_Government_Spending'] + (df['Micro_Exports'] - df['Micro_Imports'])
        
        # GDP Growth Rate
        df['GDP_Growth_Rate'] = df['GDP'].pct_change(fill_method=None) * 100

        # Inflation Rate using CPI (if available in microeconomic data)
        if 'Micro_CPI' in df.columns:
            df['Inflation_Rate'] = df['Micro_CPI'].pct_change(fill_method=None) * 100

        # Unemployment Rate calculation
        df['Unemployment_Rate'] = df['Micro_Unemployment_Rate']  # Assuming this is already a percentage

        # Trade Balance calculation
        df['Trade_Balance'] = df['Micro_Exports'] - df['Micro_Imports']
        
        # Investment-to-GDP Ratio
        df['Investment_to_GDP_Ratio'] = (df['Micro_Investment'] / df['GDP']) * 100
        
        # Consumption Growth Rate
        df['Consumption_Growth_Rate'] = df['Micro_Consumption'].pct_change(fill_method=None) * 100
        
        # Interest Rate Changes
        df['Interest_Rate_Change'] = df['Micro_Interest_Rate'].diff()
        
        # Debt-to-GDP Ratio
        df['Debt_to_GDP_Ratio'] = (df['Micro_Public_Debt'] / df['GDP']) * 100

        # Forward fill any missing data
        df.ffill(inplace=True)
        
        return df

    def insert_data_to_snowflake(self, df, table_name):
        """Insert DataFrame data into Snowflake."""
        cursor = self.conn.cursor()
        
        # Create table if not exists (simple schema for demonstration)
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS Microeconomics.{table_name} (
            DateTime DATE,
            Household_Spending FLOAT,
            Housing_Prices FLOAT,
            Small_Business_Loans FLOAT,
            Retail_Sales FLOAT,
            Personal_Income FLOAT,
            Unemployment_Rate FLOAT,
            Consumption FLOAT,
            Investment FLOAT,
            Government_Spending FLOAT,
            Exports FLOAT,
            Imports FLOAT,
            Public_Debt FLOAT,
            Interest_Rate FLOAT,
            GDP FLOAT,
            GDP_Growth_Rate FLOAT,
            Inflation_Rate FLOAT,
            Trade_Balance FLOAT,
            Investment_to_GDP_Ratio FLOAT,
            Consumption_Growth_Rate FLOAT,
            Interest_Rate_Change FLOAT,
            Debt_to_GDP_Ratio FLOAT
        );
        """)

        # Insert data row by row
        for _, row in df.iterrows():
            cursor.execute(f"""
            INSERT INTO Microeconomics.{table_name} (DateTime, Household_Spending, Housing_Prices, Small_Business_Loans, Retail_Sales, Personal_Income, 
            Unemployment_Rate, Consumption, Investment, Government_Spending, Exports, Imports, Public_Debt, Interest_Rate, GDP, GDP_Growth_Rate, 
            Inflation_Rate, Trade_Balance, Investment_to_GDP_Ratio, Consumption_Growth_Rate, Interest_Rate_Change, Debt_to_GDP_Ratio)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['DateTime'], row['Micro_Household_Spending'], row['Micro_Housing_Prices'], row['Micro_Small_Business_Loans'], 
                row['Micro_Retail_Sales'], row['Micro_Personal_Income'], row['Unemployment_Rate'], row['Micro_Consumption'], 
                row['Micro_Investment'], row['Micro_Government_Spending'], row['Micro_Exports'], row['Micro_Imports'], 
                row['Micro_Public_Debt'], row['Micro_Interest_Rate'], row['GDP'], row['GDP_Growth_Rate'], row.get('Inflation_Rate', None), 
                row['Trade_Balance'], row['Investment_to_GDP_Ratio'], row['Consumption_Growth_Rate'], row['Interest_Rate_Change'], 
                row['Debt_to_GDP_Ratio']
            ))

        self.conn.commit()
        cursor.close()

# Airflow task function
def microeconomic_retrieve_task(**kwargs):
    retriever = MicroeconomicDataRetriever()
    df = retriever.retrieve_data()
    kwargs['ti'].xcom_push(key='micro_data', value=df.to_dict())
    retriever.insert_data_to_snowflake(df, "Microeconomic_Indicators")

# Run independently (for testing purposes)
if __name__ == "__main__":
    retriever = MicroeconomicDataRetriever()
    df = retriever.retrieve_data()
    retriever.insert_data_to_snowflake(df, "Microeconomic_Indicators")
