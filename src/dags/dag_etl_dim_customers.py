#airflow imports
import airflow
from airflow import models
from airflow import DAG
#other packages
from datetime import datetime, timedelta
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.models import Variable

default_dag_args = {
    'start_date':airflow.utils.dates.days_ago(3),
    'email_on_failure':True,
    'email_on_retry':True,
    'project_id' : 'Bike_Store_BI',
    'retries': 1,
    'on_failure_callback': 'fldemarchi88@gmail.com',
    'retry_delay': timedelta(minutes=5)
}
dag = airflow.DAG(
    dag_id = 'dag_etl_dim_customers',
    schedule_interval = timedelta(days=1),
    catchup = True,
    default_args=default_dag_args,
    template_searchpath=Variable.get('sql_path'),
    max_active_runs=1
)
t0= MsSqlOperator(
    task_id = 'clear_customers_data',
    sql = 'DELETE FROM bi.dim_customers',
    mssql_conn_id = 'mssql_conn',
    dag = dag
)
t1=MsSqlOperator(
    task_id= 'insert_customers_data',
    sql = 'insert_dim_customers.sql',
    mssql_conn_id = 'mssql_conn',
    dag=dag
)
t1.set_upstream([t0])
t0 >> t1