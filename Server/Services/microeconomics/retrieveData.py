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
            Unemployment_Rate FLOAT
        );
        """)

        # Insert data row by row
        for _, row in df.iterrows():
            cursor.execute(f"""
            INSERT INTO Microeconomics.{table_name} (DateTime, Household_Spending, Housing_Prices, Small_Business_Loans, Retail_Sales, Personal_Income, Unemployment_Rate)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row['DateTime'], row['Micro_Household_Spending'], row['Micro_Housing_Prices'], row['Micro_Small_Business_Loans'], 
                  row['Micro_Retail_Sales'], row['Micro_Personal_Income'], row['Micro_Unemployment_Rate']))

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
