from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Ajout du dossier lib au PATH pour importer les scripts
sys.path.append(os.path.join(os.path.dirname(__file__), "../lib"))

from ingest_box_office import fetch_box_office
from ingest_movie_details import fetch_movie_details
from format_box_office import format_box_office
from format_movie_details import format_movie_details
from combine_data import combine_data
from index_combined import  index_combined_to_es


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "retries": 0,
}

with DAG(
    dag_id="cinema_pipeline",
    default_args=default_args,
    schedule_interval=None,  # manuel
    catchup=False,
    tags=["cinema", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="ingest_box_office",
        python_callable=fetch_box_office,
    )

    t2 = PythonOperator(
        task_id="ingest_movie_details",
        python_callable=fetch_movie_details,
    )

    t3 = PythonOperator(
        task_id="format_box_office",
        python_callable=format_box_office,
    )

    t4 = PythonOperator(
        task_id="format_movie_details",
        python_callable=format_movie_details,
    )

    t5 = PythonOperator(
        task_id="combine_data",
        python_callable=combine_data,
    )
    t6 = PythonOperator(
        task_id="index_combined_to_es",
        python_callable=index_combined_to_es,
    )

# Dépendances
t1 >> t3
t1 >> t4
t2 >> t3
t2 >> t4
t3 >> t5
t4 >> t5
t5 >> t6
