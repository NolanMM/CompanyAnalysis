from microservices.Helper import (
    load_or_create_airflow_snowflake_env_file,
    set_java_home_instructions,
    set_airflow_home_instructions,
    techstack_infor,
    test_snowflake_connection,
    set_scala_home_instructions,
    set_spark_home_instructions,
    check_postgresql_version,
    set_kafka_home_instructions
)
from dotenv import set_key, load_dotenv
import streamlit as st
import pandas as pd
import sys
import os

env_file = './Services/Airflow_Snowflake_Pipeline/snowflake.env'

st.set_page_config(page_title="Admin Page", page_icon=":shield:", layout="wide")

load_or_create_airflow_snowflake_env_file(env_file)

class AdminPage:
    def __init__(self):
        self.update_snowflake_env_file()
        self.update_spark_env_file()
        self.update_kafka_env_file()
    
    def update_snowflake_env_file(self):
        self.fred_api_key = os.getenv("FRED_API_KEY", "")
        self.user_snowflake = os.getenv("USER_SNOWFLAKE", "")
        self.password_snowflake = os.getenv("PASSWORD_SNOWFLAKE", "")
        self.account_snowflake = os.getenv("ACCOUNT_SNOWFLAKE", "")
        self.warehouse_snowflake = os.getenv("WAREHOUSE_SNOWFLAKE", "")
        self.database_snowflake = os.getenv("DATABASE_SNOWFLAKE", "")
        self.role_snowflake = os.getenv("ROLE_SNOWFLAKE", "")
        self.url_snowflake = os.getenv("URL_SNOWFLAKE", "")
    
    def update_spark_env_file(self):
        self.java_home = os.getenv("JAVA_HOME", "")
        self.scala_home = os.getenv("SCALA_HOME", "")
        self.spark_home = os.getenv("SPARK_HOME", "")
    
    def update_kafka_env_file(self):
        self.kafka_home = os.getenv("KAFKA_HOME", "")
        
    def update_env_file(self):
        set_key(env_file, "FRED_API_KEY", self.fred_api_key)
        set_key(env_file, "USER_SNOWFLAKE", self.user_snowflake)
        set_key(env_file, "PASSWORD_SNOWFLAKE", self.password_snowflake)
        set_key(env_file, "ACCOUNT_SNOWFLAKE", self.account_snowflake)
        set_key(env_file, "WAREHOUSE_SNOWFLAKE", self.warehouse_snowflake)
        set_key(env_file, "DATABASE_SNOWFLAKE", self.database_snowflake)
        set_key(env_file, "ROLE_SNOWFLAKE", self.role_snowflake)
        set_key(env_file, "URL_SNOWFLAKE", self.url_snowflake)
    
    def check_kafka_set_up_configurations(self):
        st.subheader("I. Prerequisites", divider=True)
        st.markdown("""
                    - **Java**: JDK 17
                    - **Scala**: 2.13.6
                    - **Kafka**: 3.8.0
                    - **JAVA_HOME**: Set the **JAVA_HOME** environment variable in the **WSL** environment.
                    - **SCALA_HOME**: Set the **SCALA_HOME** environment variable in the **WSL** environment.
                    - **KAFKA_HOME**: Set the **KAFKA_HOME** environment variable in the **WSL** environment.
                    
                    Set up Instructions: [Kafka Set Up Instructions](https://github.com/NolanMM/CompanyAnalysis/blob/main/Documents/Setup_Step.md#apache-kafka-380)
                    """)
        st.subheader("II. Check the Kafka Set Up Configurations.", divider=True)
        if st.button("Check Kafka Set Up Configurations"):
            st.markdown("#### 1. Checking the KAFKA_HOME Configurations...")
            if self.kafka_home == "":
                st.error("Please set the KAFKA_HOME environment variable by the instruction below in terminal and retry.")
                set_kafka_home_instructions()
            else:
                st.success("KAFKA_HOME is set.")
    
    def check_postgresql(self):
        st.subheader("I. Prerequisites", divider=True)
        st.markdown("""
                    - **PostgreSQL**: 16
                    
                    Set up Instructions: [PostgreSQL Set Up Instructions](https://github.com/NolanMM/CompanyAnalysis/blob/main/Documents/Setup_Step.md#postgresql)
                    """)
        st.subheader("II. Check the PostgreSQL Version.", divider=True)
        if st.button("Check PostgreSQL Version"):
            version = check_postgresql_version()
            st.write(f"PostgreSQL version: {version}")
        
    def check_spark_set_up_configurations(self):
        st.subheader("I. Prerequisites", divider=True)
        st.markdown("""
                    - **Java**: JDK 17
                    - **Scala**: Scala 2.13.6
                    - **Maven**: 3.9.9
                    - **Python**: 3.10, 3.11, 3.12
                    - **JAVA_HOME**: Set the **JAVA_HOME** environment variable in the **WSL** environment.
                    - **SCALA_HOME**: Set the **SCALA_HOME** environment variable in the **WSL** environment.
                    - **MAVEN_HOME**: Set the **MAVEN_HOME** environment variable in the **WSL** environment. (To build Spark from source) (Recommend 3.9.9)
                    - **SPARK_HOME**: Set the **SPARK_HOME** environment variable in the **WSL** environment.
                    
                    Set up Instructions: [Spark 3.5.3 Set Up Instructions](https://github.com/NolanMM/CompanyAnalysis/blob/main/Documents/Setup_Step.md#spark-353)
                    """, unsafe_allow_html=True)
        st.subheader("II. Check the Spark Set Up Configurations.", divider=True)
        if st.button("Check Spark Set Up Configurations"):
            st.markdown("#### 1. Checking the JAVA_HOME Configurations...")
            if self.java_home == "":
                st.error("Please set the JAVA_HOME environment variable by the instruction below in terminal and retry.")
                set_java_home_instructions()
            else:
                st.success("JAVA_HOME is set.")
            st.markdown("#### 2. Checking the SCALA_HOME Configurations...")
            if self.scala_home == "":
                st.error("Please set the SCALA_HOME environment variable by the instruction below in terminal and retry.")
                set_scala_home_instructions()
            else:
                st.success("SCALA_HOME is set.")
            st.markdown("#### 3. Checking the SPARK_HOME Configurations...")
            if self.spark_home == "":
                st.error("Please set the SPARK_HOME environment variable by the instruction below in terminal and retry.")
                set_spark_home_instructions()
            else:
                st.success("SPARK_HOME is set.")
        
    def check_airflow_path_configurations(self):
        st.subheader("I. Prerequisites", divider=True)
        st.markdown("""
                    - **Python**: 3.10, 3.11, 3.12
                    - **PostgreSQL**: 14, 15, 16
                    - **SQLite**: 3.15.0+ (Built-in Python)
                    - **AIRFLOW_HOME**: Set the **AIRFLOW_HOME** environment variable in the **WSL** environment.
                    
                    Set up Instructions: [Airflow Set Up Instructions](https://github.com/NolanMM/CompanyAnalysis/blob/main/Documents/Setup_Step.md#apache-airflow)
                    """)
        st.subheader("II. Check the Path Configurations for the Airflow-Snowflake pipeline.", divider=True)
        if st.button("Check Path Configuration"):
            # Check if AIRFLOW_HOME is set
            if sys.version_info >= (3, 10):
                st.success("You're using Python 3.10 or newer.")
                
            if os.getenv("AIRFLOW_HOME") is None:
                st.error("Please set the AIRFLOW_HOME environment variable by the instruction below in terminal and retry.")
                set_airflow_home_instructions()

    def check_snowflake_configurations(self):
        st.subheader("I. Prerequisites", divider=True)
        st.markdown("""
                        - **Create a Snowflake Account**: If you don’t have one, sign up for a Snowflake account. Choose the appropriate edition based on your needs.
                        - **Snowflake Set Up Instruction Details**: [Snowflake Set Up Instructions](https://github.com/NolanMM/CompanyAnalysis/blob/main/Documents/Create_Snowflake_Account_Connect_Python.md#how-to-create-a-snowflake-account-log-in-and-connect-using-python)
                        - **Snowflake API Configuration**: Set up the Snowflake API configurations in the APIs Configuration tab or in the snowflake.env file by route below.
                        <br> **```./Server/Services/Airflow_Snowflake_Pipeline/snowflake.env```**                   
                    """, unsafe_allow_html=True)
        st.subheader("II. Check the Snowflake Configurations.", divider=True)
        st.markdown("##### 1. Check the Snowflake Configurations.")
        if st.button("Check APIs Configuration"):
            if not all([self.user_snowflake, self.password_snowflake, self.account_snowflake,
                                self.warehouse_snowflake, self.database_snowflake, self.role_snowflake,
                                self.url_snowflake]):                
                st.error("Please set the Snowflake configurations in the APIs Configuration tab and retry.")
            else:
                st.success("Snowflake configurations are set")
        st.markdown("##### 2. Check the Snowflake Connection.")
        if st.button("Check Snowflake Connection"):
            connection_successful = test_snowflake_connection(self.user_snowflake, self.password_snowflake, self.account_snowflake, self.warehouse_snowflake, self.database_snowflake, self.role_snowflake, self.url_snowflake)
            if connection_successful:
                st.success("Snowflake connection successful!")
            else:
                st.error("Snowflake connection failed. Please check your apis in configurations tabs and retry.")

    def update_env_file_UI(self):
        st.subheader("II. Update the APIs configuration settings for the Airflow-Snowflake pipeline.")
        self.fred_api_key = st.text_input("FRED API Key", self.fred_api_key)
        self.user_snowflake = st.text_input("Snowflake User", self.user_snowflake)
        self.password_snowflake = st.text_input("Snowflake Password", self.password_snowflake, type="password")
        self.account_snowflake = st.text_input("Snowflake Account", self.account_snowflake)
        self.warehouse_snowflake = st.text_input("Snowflake Warehouse", self.warehouse_snowflake)
        self.database_snowflake = st.text_input("Snowflake Database", self.database_snowflake)
        self.role_snowflake = st.text_input("Snowflake Role", self.role_snowflake)
        self.url_snowflake = st.text_input("Snowflake URL", self.url_snowflake)
        if st.button("Save Configuration"):
            self.update_env_file()
            st.toast("Configuration saved successfully!")
    
    def render_project_introduction(self):
        read_me_path = os.getcwd()
        read_me_path = read_me_path.replace("\\Server", "\\README.md")
        read_me_path = read_me_path.replace("\\", "/")
        with open(read_me_path, 'r') as f:
            readme_content = f.read()
        st.markdown(readme_content, unsafe_allow_html=True)
    
    def run(self):
        # Page Title
        st.title("Admin Configuration Page")
        project_introduction_tab, project_path_configuration_tab, project_apis_configuration_tab = st.tabs(["Introduction", "Project Path Configuration", "Project APIs Configuration"])
        
        with project_introduction_tab:
            self.render_project_introduction()
        
        with project_path_configuration_tab:
            set_up_path_col_1, set_up_path_col_2 = st.columns([0.2, 1])
            with set_up_path_col_1:
                genre = st.radio(
                    "**Set Up Path Configuration for:**",
                    list(techstack_infor().values()),
                    format_func=lambda x: x.split("[")[1].split("]")[0] if "[" in x else x
                )
                selected_tech = [key for key, value in techstack_infor().items() if value == genre]
            with set_up_path_col_2:
                if selected_tech[0] == "Apache Airflow":
                    self.check_airflow_path_configurations()
                elif selected_tech[0] == "Snowflake":
                    self.check_snowflake_configurations()
                elif selected_tech[0] == "Apache Spark":
                    self.check_spark_set_up_configurations()
                elif selected_tech[0] == "PostgreSQL 16":
                    self.check_postgresql()
                elif selected_tech[0] == "Kafka":
                    self.check_kafka_set_up_configurations()
            
        st.divider()
        with project_apis_configuration_tab:
            self.update_snowflake_env_file()
            self.update_env_file_UI()
        

if __name__ == "__main__":
    app = AdminPage()
    app.run()