from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Banque_Modeling_Dag',
    default_args=default_args,
    description='DBT Modeling for Data Warehouse (Star Schema)',
    schedule_interval='@weekly',
    catchup=False
)

# ➤ Étape 1: dbt run (modélisation)
run_dbt_modeling = BashOperator(
    task_id='run_dbt_modeling',
    bash_command="""
    source /home/ayoub/dbt-venv/bin/activate &&
    cd /home/ayoub/dbt_project/reviews_DW_project &&
    dbt run
    """,
    dag=dag,
)

# ➤ Étape 2: dbt test (tests sur la qualité des dimensions/faits)
run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command="""
    source /home/ayoub/dbt-venv/bin/activate &&
    cd /home/ayoub/dbt_project/reviews_DW_project &&
    dbt test
    """,
    dag=dag,
)

run_dbt_modeling >> run_dbt_tests
