from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hkumar@glacierinfogroup.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2019, 7, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='geocode_mls_sales',
          default_args=default_args,
          schedule_interval='30 5 * * *')

t1_bash = """
/usr/local/bin/dp/database_jobs/run_py.sh "geo_property_sales.py --modified_days 7"
"""

t1 = SSHOperator(
    ssh_conn_id='ssh_aws_ec2',
    task_id='geocode_mls_sales',
    command=t1_bash,
    dag=dag)
