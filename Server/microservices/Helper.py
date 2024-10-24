from dotenv import set_key, load_dotenv
import snowflake.connector
import streamlit as st
import subprocess
import os

def load_or_create_airflow_snowflake_env_file(env_file):
    """
    Load the .env file if it exists, or create an empty one if it doesn't.
    """
    if os.path.exists(env_file):
        load_dotenv(env_file)
    else:
        with open(env_file, 'w') as f:
            f.write('')
            
def load_or_create_path_from_env_file(env_file):
    """
    Load the .env file if it exists, or create an empty one if it doesn't.
    """
    if os.path.exists(env_file):
        load_dotenv(env_file)
    else:
        with open(env_file, 'w') as f:
            f.write('')

def techstack_infor():
    return {
        "Apache Airflow": ":orange[Apache Airflow]",
        "Snowflake": ":blue[Snowflake]",
        "Apache Spark": ":red[Apache Spark]",
        "PostgreSQL 16": ":green[PostgreSQL 16]",
        "Kafka": ":purple[Kafka]",
    }

def test_snowflake_connection(user, password, account, warehouse, database, role, url):
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            role=role,
            url=url
        )
        conn.close()
        return True
    except Exception as e:
        return False
            
def set_airflow_home_instructions():
    try:
        current_directory = os.getcwd()
        pipeline_path = os.path.join(current_directory,'Services', 'Airflow_Snowflake_Pipeline')
        drive_letter  = pipeline_path[0]
        if drive_letter.isalpha() and pipeline_path[1:3] == ":\\":
            pipeline_path = pipeline_path.replace(f"{drive_letter}:\\", f"/mnt/{drive_letter.lower()}/")
        pipeline_path = pipeline_path.replace("\\", "/")
        airflow_home_cmd = (f"""
            export AIRFLOW_HOME="{pipeline_path}"
            export PATH=$PATH:$AIRFLOW_HOME
            source ~/.bashrc
            echo $AIRFLOW_HOME
            """
        )
        airflow_initiate_cmd = (f"""
            airflow db init
            airflow users create --username admin --firstname FIRST_NAME --lastname LAST_NAME --role Admin --email
            """)
        airflow_scheduler_cmd = (f"""
            airflow scheduler
            """)
        airflow_webserver_cmd = (f"""
                                 airflow webserver -p 8080
                                """)
        st.subheader("1. Run the following commands in your WSL terminal:", divider=True)
        st.code(airflow_home_cmd, language="bash")
        st.subheader("2. Airflow Initialize", divider=True)
        st.code(airflow_initiate_cmd, language="bash")
        st.subheader("3. Configure XCOM", divider=True)
        st.markdown(f"Open the **airflow.cfg** file in the path **{os.path.join(current_directory,'Services', 'Airflow_Snowflake_Pipeline')}\\airflow.cfg** directory **(line 212)** and set the **enable_xcom_pickling = True**")
        st.image("./microservices/docs/XComPicklingEnable.png", width=400)
        st.subheader("4. Airflow Scheduler", divider=True)
        st.markdown("Open a **NEW** WSL terminal, **re-setup** the **AIRFLOW_HOME** and run the following command:")
        st.code(airflow_scheduler_cmd, language="bash")
        st.subheader("5. Airflow Webserver", divider=True)
        st.markdown("Open **Double Check** the **AIRFLOW_HOME** and run the following command:")
        st.code(airflow_webserver_cmd, language="bash")
        st.markdown("Open web browser and go to http://localhost:8080/ and sign in with the credentials you set up.")
    except Exception as e:
        st.write(f"An error occurred: {e}")            
            
def set_java_home_instructions():
    try:
        java_home_cmd = ("""
            export JAVA_HOME=$(update-alternatives --query java | grep 'Value: ' | awk '{print $2}' | sed 's:/bin/java::')
            export PATH=$PATH:$JAVA_HOME/bin
            source ~/.bashrc
            echo $JAVA_HOME
            """
        )
        st.write("Run the following commands in your WSL terminal:")
        st.code(java_home_cmd, language="bash")
    except Exception as e:
        st.write(f"An error occurred: {e}")
        
def set_scala_home_instructions():
    try:
        java_home_cmd = ("""
            export SCALA_HOME=/usr/bin/scala
            export PATH=$SCALA_HOME/bin:$PATH
            source ~/.bashrc
            echo $SCALA_HOME
            """
        )
        st.write("Run the following commands in your WSL terminal:")
        st.code(java_home_cmd, language="bash")
    except Exception as e:
        st.write(f"An error occurred: {e}")
        
def set_spark_home_instructions():
    try:
        java_home_cmd = ("""
            export SPARK_HOME=$(ls -d ~/spark/spark-* | sort -V | tail -n 1)
            export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
            source ~/.bashrc
            echo $SPARK_HOME
            """
        )
        st.write("Run the following commands in your WSL terminal:")
        st.code(java_home_cmd, language="bash")
    except Exception as e:
        st.write(f"An error occurred: {e}")

def check_postgresql_version():
    try:
        result = subprocess.run(['wsl', 'psql', '--version'], capture_output=True, text=True, check=True)
        version = result.stdout.strip()
        return version
    except subprocess.CalledProcessError:
        return "PostgreSQL is not installed."
    except FileNotFoundError:
        return "PostgreSQL command not found. Make sure it's installed and in your WSL environment."
    
def set_kafka_home_instructions():
    try:
        kafka_home_cmd = ("""
            export KAFKA_HOME=/usr/bin/kafka
            export PATH=$PATH:$KAFKA_HOME/bin
            source ~/.bashrc
            echo $KAFKA_HOME
            """
        )
        st.write("Run the following commands in your WSL terminal:")
        st.code(kafka_home_cmd, language="bash")
    except Exception as e:
        st.write(f"An error occurred: {e}")

if __name__ == "__main__":
    set_java_home_instructions()
