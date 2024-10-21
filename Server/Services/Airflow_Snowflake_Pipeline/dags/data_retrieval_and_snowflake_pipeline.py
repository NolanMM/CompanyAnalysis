from tasks.insert_data_into_snowflake import insert_snowflake_tables
from tasks.create_snowflake_table import create_snowflake_tables
from airflow.operators.python import PythonOperator
from tasks.retrieve_data import retrieve_data
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG

class SnowflakePipeline:
    def __init__(self, dag_id, schedule, start_date, default_args):
        """
        Initialize the pipeline class with DAG parameters.
        :param dag_id: The DAG's ID or name
        :param schedule: The schedule for running the DAG (e.g., @monthly)
        :param start_date: The start date of the DAG execution
        :param default_args: Default arguments for the DAG
        """
        self.dag_id = dag_id
        self.schedule = schedule
        self.start_date = start_date
        self.default_args = default_args
        self.dag = None

    def create_dag(self):
        """
        Create the DAG object.
        """
        self.dag = DAG(
            self.dag_id,
            default_args=self.default_args,
            description='Retrieve data and store it in Snowflake, running monthly',
            schedule_interval=self.schedule,
            start_date=self.start_date,
            catchup=False
        )
        return self.dag

    def define_tasks(self):
        """
        Define the tasks that will run in the pipeline.
        """
        with self.dag:
            # Task 1: Retrieve data
            retrieve_data_task = PythonOperator(
                task_id='retrieve_data',
                python_callable=retrieve_data,
                provide_context=True,
                dag=dag
            )

            # Task 2: Create Snowflake table if it doesn't exist
            create_table_task = PythonOperator(
                task_id='create_snowflake_table',
                python_callable=create_snowflake_tables,
                dag=dag
            )

            # Task 3: Insert data into Snowflake
            insert_data_task = PythonOperator(
                task_id='insert_data_into_snowflake',
                python_callable=insert_snowflake_tables,
                provide_context=True,
                dag=dag
            )

            # Task flow dependencies
            retrieve_data_task >> create_table_task >> insert_data_task

# Default Arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# SnowflakePipeline with schedule to monthly with start date 1 day ago
snowflake_pipeline = SnowflakePipeline(
    dag_id='Data_Retrieval_And_Snowflake_Storage',
    schedule='@monthly',  
    start_date=days_ago(1),
    default_args=default_args
)
dag = snowflake_pipeline.create_dag()
snowflake_pipeline.define_tasks()