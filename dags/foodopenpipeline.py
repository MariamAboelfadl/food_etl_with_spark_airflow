from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def extract(**kwargs):
    file_path = '/opt/airflow/dags/en.openfoodfacts.org.products.tsv'
    
    # Explicitly read all columns as strings to avoid dtype issues
    data_food = pd.read_csv(file_path, sep='\t', dtype=str, low_memory=False)
    
    data_food.to_parquet('/opt/airflow/dags/raw_data.parquet')  # نخزنها مؤقتاً
    print("Data extracted and saved as raw_data.parquet")

def load(**kwargs):
    df = pd.read_parquet('/opt/airflow/dags/df_trans.parquet')
    df.to_csv('/opt/airflow/dags/loadData.csv', index=False)


with DAG(
    dag_id='food_data_dag',
    start_date=datetime(2025, 4, 25),
    schedule_interval='@weekly',
    catchup=False
) as dag:
    extract_tk = PythonOperator(
        task_id='extract',
        python_callable=extract
    )
    transform_tk = SparkSubmitOperator(
    task_id='transform_with_spark',
    application='/opt/airflow/dags/spark_jobs/transform_job.py',  # ← هذا المسار مهم
    conn_id='spark_default',
    dag=dag,
)
    load_tk = PythonOperator(
        task_id='load',
        python_callable=load
    )
    

    extract_tk >> transform_tk >> load_tk
