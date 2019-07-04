from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hkumar@glacierinfogroup.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='join_listings_to_properties_incremental',
          default_args=default_args,
          schedule_interval='12,42 * * * *',
          dagrun_timeout=timedelta(seconds=120))

t1_bash = """
/usr/local/bin/dp/database_jobs/run_py.sh "join_property_to_listing.py --incremental"
"""

t1 = SSHOperator(
    ssh_conn_id='ssh_aws_ec2',
    task_id='join_listings_to_properties_incremental',
    command=t1_bash,
    dag=dag)
