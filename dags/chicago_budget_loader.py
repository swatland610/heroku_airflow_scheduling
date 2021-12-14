from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['seanjwatland@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'load_chicago_budgets',
    default_args=default_args,
    description='Pull in data from Chicago City budgets from 2011-2021',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021,12,13),
    catchup=False,
    tags=['chicago_budgets'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='extract_budget_data',
        bash_command='${AIRFLOW_HOME}/run.sh ',
    )

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    This will describe the task of extracting the budget data!
    )
    """
    )
    dag.doc_md = """
    #### DAG Documentation
    This will descirbe the process it's running!
    """ 