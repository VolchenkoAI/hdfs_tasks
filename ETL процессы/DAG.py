from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

my_dag = DAG(
    dag_id="dag_user50_report_loading",
    start_date=datetime(2023,12,23),
    schedule_interval= "0 2 * * *",
    catchup=False,
    tags=["user50"],
    default_args={
        "owner": "user50",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "run_as_user": "user50"
        }
)

task1 = DummyOperator(
    task_id = 'start_task',
    dag = my_dag
)

task2a = BashOperator(
    task_id = "capture_files",
    bash_command = "hdfs dfs -put /mnt/trans/*.* /user/user50/trans_loading/ | spark3-submit --master local --deploy-mode client /home/user50/capture_files.py",
    dag = my_dag
)

task2b = BashOperator(
    task_id = "capture_source",
    bash_command = "spark3-submit --master local --deploy-mode client --jars /home/trainer/postgresql-42.6.0.jar /home/user50/capture_source.py",
    dag = my_dag
)

task3 = BashOperator(
    task_id = "report_build",
    bash_command = "spark3-submit --master local --deploy-mode client /home/user50/report_build.py",
    dag = my_dag
)

task4a = BashOperator(
    task_id = "unloading_hdfs",
    bash_command = "spark3-submit --master local --deploy-mode client /home/user50/unloading_hdfs.py; hdfs dfs -get /warehouse/user50.db/reports/*.csv /home/user50/reports/; mv /home/user50/reports/*.csv /home/user50/reports/report_turnover.csv",
    dag = my_dag
)

task4b = DummyOperator(
    task_id = "unloading_hbase",
    dag = my_dag
)

task5 = DummyOperator(
    task_id = 'end_task',
    dag = my_dag
)

task1 >> task2a
task1 >> task2b
task2a >> task3
task2b >> task3
task3 >> task4a
task3 >> task4b
task4a >> task5
task4b >> task5