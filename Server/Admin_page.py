from microservices.Helper import load_or_create_airflow_snowflake_env_file
from dotenv import set_key, load_dotenv
import streamlit as st
import pandas as pd
import os

env_file = './Services/Airflow_Snowflake_Pipeline/snowflake.env'

st.set_page_config(page_title="Admin Page", page_icon=":shield:", layout="wide")

load_or_create_airflow_snowflake_env_file(env_file)

class AdminPage:
    def __init__(self):
        # Initialize default values from .env
        self.fred_api_key = os.getenv("FRED_API_KEY", "")
        self.user_snowflake = os.getenv("USER_SNOWFLAKE", "")
        self.password_snowflake = os.getenv("PASSWORD_SNOWFLAKE", "")
        self.account_snowflake = os.getenv("ACCOUNT_SNOWFLAKE", "")
        self.warehouse_snowflake = os.getenv("WAREHOUSE_SNOWFLAKE", "")
        self.database_snowflake = os.getenv("DATABASE_SNOWFLAKE", "")
        self.role_snowflake = os.getenv("ROLE_SNOWFLAKE", "")
        self.url_snowflake = os.getenv("URL_SNOWFLAKE", "")
        
    def update_env_file(self):
        # Update each environment variable in the .env file
        set_key(env_file, "FRED_API_KEY", self.fred_api_key)
        set_key(env_file, "USER_SNOWFLAKE", self.user_snowflake)
        set_key(env_file, "PASSWORD_SNOWFLAKE", self.password_snowflake)
        set_key(env_file, "ACCOUNT_SNOWFLAKE", self.account_snowflake)
        set_key(env_file, "WAREHOUSE_SNOWFLAKE", self.warehouse_snowflake)
        set_key(env_file, "DATABASE_SNOWFLAKE", self.database_snowflake)
        set_key(env_file, "ROLE_SNOWFLAKE", self.role_snowflake)
        set_key(env_file, "URL_SNOWFLAKE", self.url_snowflake)

    def update_env_file_UI(self):
        # Input fields for attributes
        self.fred_api_key = st.text_input("FRED API Key", self.fred_api_key)
        self.user_snowflake = st.text_input("Snowflake User", self.user_snowflake)
        self.password_snowflake = st.text_input("Snowflake Password", self.password_snowflake, type="password")
        self.account_snowflake = st.text_input("Snowflake Account", self.account_snowflake)
        self.warehouse_snowflake = st.text_input("Snowflake Warehouse", self.warehouse_snowflake)
        self.database_snowflake = st.text_input("Snowflake Database", self.database_snowflake)
        self.role_snowflake = st.text_input("Snowflake Role", self.role_snowflake)
        self.url_snowflake = st.text_input("Snowflake URL", self.url_snowflake)

        # Save button
        if st.button("Save Configuration"):
            # Update .env file with new values
            self.update_env_file()
            st.toast("Configuration saved successfully!")
    
    def run(self):
        # Page Title
        st.title("Admin Configuration Page")
        st.write("Modify the settings below to update the .env file.")
        
        self.update_env_file_UI()
        

if __name__ == "__main__":
    app = AdminPage()
    app.run()