from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='jira_dag',
    start_date=datetime(2024, 10, 15),
    schedule ='@daily'
) as dag:

    run_shell_script = BashOperator(
        task_id='jira_script',
        bash_command='execute.sh ' # input script name here, with a space after name
    )

    run_shell_script
    
