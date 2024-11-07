# retrieveData.py

from dotenv import load_dotenv
import os
import snowflake.connector
import pandas as pd

# Load environment variables
load_dotenv("Server/Services/Airflow_Snowflake_Pipeline/snowflake.env")

# Retrieve environment variables
USER_SNOWFLAKE = os.getenv("USER_SNOWFLAKE")
PASSWORD_SNOWFLAKE = os.getenv("PASSWORD_SNOWFLAKE")
ACCOUNT_SNOWFLAKE = os.getenv("ACCOUNT_SNOWFLAKE")
WAREHOUSE_SNOWFLAKE = os.getenv("WAREHOUSE_SNOWFLAKE")
DATABASE_SNOWFLAKE = os.getenv("DATABASE_SNOWFLAKE")
ROLE_SNOWFLAKE = os.getenv("ROLE_SNOWFLAKE")

class MicroeconomicDataRetriever:
    def __init__(self):
        """Initialize the MicroeconomicDataRetriever class."""
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
        """Retrieve data from Snowflake for analysis."""
        query = """
        SELECT DateTime, Consumption, Investment, Government_Spending, Exports, Imports, 
               Unemployed, Labor_Force, CPI, Current_Account_Balance, Public_Debt, 
               Interest_Rate, FDI, Labor_Force_Participation
        FROM Silver_Macroeconomic_Indicators
        WHERE DATE_TRUNC('month', DateTime) BETWEEN DATE_TRUNC('month', TO_DATE(%s))
                                                  AND DATE_TRUNC('month', TO_DATE(%s))
        """
        
        # Define start and end dates
        start_date = '2009-01-01'
        end_date = pd.to_datetime('today').strftime('%Y-%m-%d')
        
        # Retrieve data into a DataFrame
        df = pd.read_sql(query, self.conn, params=[start_date, end_date])
        return self.calculate_metrics(df)
    
    def calculate_metrics(self, df):
        """Perform additional calculations and add derived metrics to DataFrame."""
        # Calculate GDP
        df['GDP'] = df['Consumption'] + df['Investment'] + df['Government_Spending'] + (df['Exports'] - df['Imports'])
        
        # GDP Growth Rate
        df['GDP_Growth_Rate'] = df['GDP'].pct_change() * 100
        
        # Inflation Rate using CPI
        df['Inflation_Rate'] = df['CPI'].pct_change() * 100
        
        # Unemployment Rate calculation
        df['Unemployment_Rate'] = (df['Unemployed'] / df['Labor_Force']) * 100
        
        # Trade Balance calculation
        df['Trade_Balance'] = df['Exports'] - df['Imports']
        
        # Investment-to-GDP Ratio
        df['Investment_to_GDP_Ratio'] = (df['Investment'] / df['GDP']) * 100
        
        # Consumption Growth Rate
        df['Consumption_Growth_Rate'] = df['Consumption'].pct_change() * 100
        
        # Interest Rate Changes
        df['Interest_Rate_Change'] = df['Interest_Rate'].diff()
        
        # Debt-to-GDP Ratio
        df['Debt_to_GDP_Ratio'] = (df['Public_Debt'] / df['GDP']) * 100
        
        # Forward fill any missing data
        df.ffill(inplace=True)
        return df
