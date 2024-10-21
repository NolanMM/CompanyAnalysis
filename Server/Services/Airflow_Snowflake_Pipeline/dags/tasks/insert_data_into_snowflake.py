from dotenv import load_dotenv
import snowflake.connector
import pandas as pd
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
SILVER_TABLE_NAME = "Silver_MacroEconomic_Indicators"
GOLD_TABLE_NAME = "Gold_MacroEconomic_Forecast"
BRONZE_SCHEMA = "BRONZE"
SILVER_SCHEMA = "SILVER"
GOLD_SCHEMA = "GOLD"

def insert_snowflake_tables(**kwargs):
    manager = SnowflakeDataManager(
        user=USER_SNOWFLAKE,
        password=PASSWORD_SNOWFLAKE,
        account=ACCOUNT_SNOWFLAKE,
        warehouse=WAREHOUSE_SNOWFLAKE,
        database=DATABASE_SNOWFLAKE,
        role=ROLE_SNOWFLAKE,
        url=URL_SNOWFLAKE
    )
    # For testing 
    # df = pd.read_csv('/mnt/c/Users/NolanM/Desktop/WithGautam/data_insert_3.csv')

    df_dict = kwargs['ti'].xcom_pull(key='retrieved_data', task_ids='retrieve_data')
    df = pd.DataFrame(df_dict)
    df = df.map(lambda x: pd.to_datetime(x) if isinstance(x, str) and x.isdigit() == False else x)
    
    bronze_result = manager.load_bronze_data(df)
    if bronze_result != True:
        return f"Error in Silver Table Processing: {bronze_result}"
    
    silver_result = manager.insert_or_update_silver(SILVER_TABLE_NAME, df)
    if silver_result != True:
        return f"Error in Silver Table Processing: {silver_result}"

    gold_result = manager.insert_or_update_gold(GOLD_TABLE_NAME, df)
    if gold_result != True:
        return f"Error in Gold Table Processing: {gold_result}"

    print("Data inserted successfully into Snowflake.")
    return True
    
    
class SnowflakeDataManager:
    def __init__(self, user, password, account, warehouse, database, role, url):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.url = url
        self.connection = self._connect()

    def _connect(self):
        # Connect to the Snowflake database
        conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            role= self.role,
            url=self.url
        )
        return conn

    def check_table_exists(self, schema,  table_name):
        # Check if table exists in the database
        try:
            query = f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {schema};"
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return len(result) > 0
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False

    def get_last_datetime(self,schema, table_name):
        # Get the last DateTime from the specified table
        try:
            query = f"SELECT MAX(DateTime) FROM {schema}.{table_name};"
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result[0] is not None else None
        except Exception as e:
            print(f"Error getting last DateTime: {e}")
            return None
    
    def insert_new_data(self, schema, table_name, df):
        # Insert new data into the specified table
        try:
            if df.empty:
                print("No data to insert; DataFrame is empty.")
                return

            # Handle NaN values
            df = df.where(pd.notnull(df), None)

            # Convert DataFrame to a list of tuples
            rows_to_insert = [tuple(x) for x in df.to_numpy()]

            # Prepare SQL query for batch insert
            columns = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_query = f'INSERT INTO "{schema}"."{table_name}" ({columns}) VALUES ({placeholders});'

            cursor = self.connection.cursor()
            cursor.executemany(insert_query, rows_to_insert)
            self.connection.commit()
            print(f"New data inserted successfully into {table_name}.")
        except Exception as e:
            print(f"Error inserting data: {e}")
    
    def load_bronze_data(self, df):
        # Insert or update data into individual tables in the Bronze schema
        try:
            self.insert_or_update_bronze('Bronze_Consumption', df[['DateTime', 'Consumption']])
            self.insert_or_update_bronze('Bronze_Investment', df[['DateTime', 'Investment']])
            self.insert_or_update_bronze('Bronze_Government_Spending', df[['DateTime', 'Government_Spending']])
            self.insert_or_update_bronze('Bronze_Exports', df[['DateTime', 'Exports']])
            self.insert_or_update_bronze('Bronze_Imports', df[['DateTime', 'Imports']])
            self.insert_or_update_bronze('Bronze_Unemployed', df[['DateTime', 'Unemployed']])
            self.insert_or_update_bronze('Bronze_Labor_Force', df[['DateTime', 'Labor_Force']])
            self.insert_or_update_bronze('Bronze_CPI', df[['DateTime', 'CPI']])
            self.insert_or_update_bronze('Bronze_Current_Account_Balance', df[['DateTime', 'Current_Account_Balance']])
            self.insert_or_update_bronze('Bronze_Public_Debt', df[['DateTime', 'Public_Debt']])
            self.insert_or_update_bronze('Bronze_Interest_Rate', df[['DateTime', 'Interest_Rate']])
            self.insert_or_update_bronze('Bronze_FDI', df[['DateTime', 'FDI']])
            self.insert_or_update_bronze('Bronze_Labor_Force_Participation', df[['DateTime', 'Labor_Force_Participation']])
            return True
        except Exception as e:
            return f"Error loading Bronze data: {e}"
            
    def insert_or_update_bronze(self, table_name, data_df):
        cursor = self.connection.cursor()
        # for index, row in data_df.iterrows():
        #     # Check if the record for the date already exists
        #     check_query = f"SELECT COUNT(1) FROM Bronze.{table_name} WHERE DateTime = '{row['DateTime']}'"
        #     cursor.execute(check_query)
        #     result = cursor.fetchone()

        #     if result[0] == 0:
        #         # If no data exists for the date, insert the row
        #         insert_query = f"""
        #         INSERT INTO Bronze.{table_name} (DateTime, {', '.join(data_df.columns[1:])})
        #         VALUES ('{row['DateTime']}', {', '.join([str(v) for v in row.values[1:]])});
        #         """
        #         cursor.execute(insert_query)
        #     else:
        #         # Update the row if it already exists
        #         update_query = f"""
        #         UPDATE Bronze.{table_name}
        #         SET {', '.join([f"{col} = {row[col]}" for col in data_df.columns[1:]])}
        #         WHERE DateTime = '{row['DateTime']}';
        #         """
        #         cursor.execute(update_query)

        values = ', '.join(
            [
                f"('{row['DateTime']}', {', '.join([str(v) for v in row.values[1:]])})"
                for index, row in data_df.iterrows()
            ]
        )

        # Construct the full INSERT INTO query without using quotes around column names
        insert_query = f"""
            INSERT INTO {self.database}.BRONZE.{table_name} (DateTime, {', '.join(data_df.columns[1:])})
            VALUES {values};
        """
        # print(insert_query)
        cursor.execute(insert_query)
        
        cursor.close()
        
    def insert_or_update_silver(self, table_name, df):
        if not self.check_table_exists("MACROECONOMIC.SILVER", table_name):
            print("Silver table does not exist.")
            return

        last_datetime = self.get_last_datetime("MACROECONOMIC.SILVER", table_name)

        if last_datetime is None:
            new_data = df
        else:
            # Filter the DataFrame to get rows with DateTime greater than the last DateTime
            new_data = df[df['DateTime'] > last_datetime]
        try:
            cursor = self.connection.cursor()

            # Convert DataFrame to list of tuples
            rows_to_insert = [tuple(x) for x in new_data.to_numpy()]

            # Prepare the insert query
            insert_query = """
            INSERT INTO Silver.Silver_MacroEconomic_Indicators (
                DateTime, Consumption, Investment, Government_Spending, Exports, Imports,
                Unemployed, Labor_Force, CPI, Current_Account_Balance, Public_Debt,
                Interest_Rate, FDI, Labor_Force_Participation, GDP, GDP_Growth_Rate,
                Inflation_Rate, Unemployment_Rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            # Execute the batch insert
            cursor.executemany(insert_query, rows_to_insert)
            self.connection.commit()
            print("Data inserted successfully into Silver.Silver_MacroEconomic_Indicators.")

        except Exception as e:
            print(f"Error inserting data: {e}")

        finally:
            cursor.close()

    def insert_or_update_gold(self, gold_table_name, df):
        if not self.check_table_exists("MACROECONOMIC.GOLD", gold_table_name):
            print("Gold table does not exist.")
            return

        last_datetime = self.get_last_datetime("MACROECONOMIC.GOLD", gold_table_name)

        if last_datetime is None:
            new_data = df
        else:
        # Filter the DataFrame to get rows with DateTime greater than the last DateTime
            new_data = df[df['DateTime'] > last_datetime]
        #new_data = df
        if new_data.empty:
            print("No new data to insert into the Gold table.")
            return

        # Calculate additional columns for the Gold table
        new_data['GDP_Gold'] = new_data['GDP']
        new_data['GDP_Growth_Rate_Gold'] = new_data['GDP_Growth_Rate']
        new_data['Inflation_Rate_Gold'] = new_data['Inflation_Rate']
        new_data['Unemployment_Rate_Gold'] = new_data['Unemployment_Rate']

        new_data['Previous_GDP'] = new_data['GDP_Gold'].shift(1)
        new_data['Forecasted_GDP'] = new_data['GDP_Gold'] * (1 + new_data['GDP_Growth_Rate_Gold'].fillna(0))

        new_data['Previous_CPI'] = new_data['CPI'].shift(1)
        new_data['Forecasted_Inflation'] = new_data['CPI'] * (1 + new_data['Inflation_Rate_Gold'].fillna(0))

        gold_data = new_data[['DateTime', 'GDP_Gold', 'Forecasted_GDP', 'Forecasted_Inflation', 'Unemployment_Rate_Gold']]

        try:
            cursor = self.connection.cursor()

            # Convert DataFrame to list of tuples
            rows_to_insert = [tuple(x) for x in gold_data.to_numpy()]

            # Prepare the insert query
            insert_query = """
            INSERT INTO Gold.Gold_MacroEconomic_Forecast (
                DateTime, GDP_Gold, Forecasted_GDP, Forecasted_Inflation, Unemployment_Rate_Gold
            ) VALUES (%s, %s, %s, %s, %s);
            """

            # Execute the batch insert
            cursor.executemany(insert_query, rows_to_insert)
            self.connection.commit()
            print("Data inserted successfully into Gold.Gold_MacroEconomic_Forecast.")

        except Exception as e:
            print(f"Error inserting data: {e}")

        finally:
            cursor.close()