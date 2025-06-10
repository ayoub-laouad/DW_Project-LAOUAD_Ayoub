from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='Orchestration_Banque_Dag',
    default_args=default_args,
    description='Orchestrates the full ETL process: collecting, cleaning, modeling',
    schedule_interval='@weekly',
    start_date=days_ago(1),
    catchup=False,
    tags=['banque', 'orchestration'],
) as dag:

    trigger_collecting = TriggerDagRunOperator(
        task_id='trigger_Banque_Collecting_Dag',
        trigger_dag_id='Banque_Collecting_Dag',
        wait_for_completion=True
    )

    trigger_cleaning = TriggerDagRunOperator(
        task_id='trigger_Banque_Cleaning_Dag',
        trigger_dag_id='Banque_Cleaning_Dag',
        wait_for_completion=True
    )

    trigger_modeling = TriggerDagRunOperator(
        task_id='trigger_Banque_Modeling_Dag',
        trigger_dag_id='Banque_Modeling_Dag',
        wait_for_completion=True
    )

    trigger_collecting >> trigger_cleaning >> trigger_modeling
