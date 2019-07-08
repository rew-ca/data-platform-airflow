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

dag = DAG(dag_id='vreb_idx_full',
          default_args=default_args,
          schedule_interval='50 10 * * *',
          dagrun_timeout=timedelta(seconds=120))

t1_bash = """
/usr/local/bin/dp/database_jobs/run_py.sh "vreb_idx_feed.py --full"
"""

t1 = SSHOperator(
    ssh_conn_id='ssh_aws_ec2',
    task_id='vreb_idx_full',
    command=t1_bash,
    dag=dag)