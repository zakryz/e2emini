from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define the DAG
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 5, 24),
    'retries': 3,
    'retry_delay': timedelta(minutes=3)}

dag = DAG(
    'Kakao',
    default_args=default_args,
    description='end2end',
    schedule_interval='@daily')

# Define the tasks
create_table_hive = BashOperator(
    task_id = 'create_table_hive',
    bash_command= '/bin/python3.7 /home/asd123/e2e/staging.py',
    dag=dag)

spark_transformation = BashOperator(
    task_id='spark_transformation',
    bash_command= '/bin/python3.7 /home/asd123/e2e/transform.py',
    dag=dag)

load = BashOperator(
    task_id='load',
    bash_command= '/bin/python3.7 /home/asd123/e2e/load.py',
    dag=dag)


# Set task dependencies
create_table_hive >> spark_transformation >> load