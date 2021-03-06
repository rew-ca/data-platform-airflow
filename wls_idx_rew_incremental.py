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

dag = DAG(dag_id='wls_idx_rew_incremental',
          default_args=default_args,
          schedule_interval='50 */2 * * *')

t1_bash = """
/usr/local/bin/dp/database_jobs/run_py.sh "wls_idx_feed.py --incremental"
"""

t1 = SSHOperator(
    ssh_conn_id='ssh_aws_ec2',
    task_id='wls_idx_rew_incremental',
    command=t1_bash,
    dag=dag)
