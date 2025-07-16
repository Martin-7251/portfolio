from airflow import DAG
import os
import sys
sys.path.insert (0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.wikipedia_pipeline import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data



dag = DAG(
    dag_id='wikipedia_flow',
    default_args={
        "owner": "Martin Kamau",
        "start_date":datetime(2025, 7, 11), 
    },
    schedule_interval=None,
    catchup=False
)

# Extraction
extract_data_from_wikipedia = PythonOperator(
    task_id = "extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data, 
    provide_context=True,
    op_kwargs={ "url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag 
)

# Transform
transform_wikipedia_data = PythonOperator(
    task_id='transform_wikipedia_data',
    python_callable=transform_wikipedia_data,
    provide_context=True,
    dag=dag
)

write_wikipedia_data = PythonOperator(
    task_id = "write_wikipedia_data",
    python_callable =write_wikipedia_data,
    provide_context=True,
    dag=dag
)

extract_data_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data