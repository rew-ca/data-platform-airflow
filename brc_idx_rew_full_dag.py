from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hkumar@glacierinfogroup.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='brc_idx_rew_full',
          default_args=default_args,
          schedule_interval='30 9 * * *',
          dagrun_timeout=timedelta(seconds=120))

t1_bash = """
/usr/local/bin/dp/database_jobs/run_py.sh "brc_idx_feed.py --full --feed_code brc_idx_rew"
"""

t1 = SSHOperator(
    ssh_conn_id='ssh_aws_ec2',
    task_id='brc_idx_rew_full',
    command=t1_bash,
    dag=dag)
