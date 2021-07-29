from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator


# Default arguments for defining the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime.now(),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Running DAG everyday at midnight
dag = DAG(
    dag_id="retail_data_pipeline",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
)

# Copies PostgresDB data into CSV file
extract_retail_data = PostgresOperator(
    dag=dag,
    task_id="extract_retail_data",
    sql="./scripts/sql/extract_retail_data.sql",
    postgres_conn_id="postgres_default",
    params={"to_temp": "/temp/retail_profiling.csv"},
    depends_on_past=True,
    wait_for_downstream=True,
)

end_of_data_pipeline = DummyOperator(dag=dag, task_id="end_of_data_pipeline")


extract_retail_data >> end_of_data_pipeline
