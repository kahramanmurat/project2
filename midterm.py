import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
import os


# this is just an example of how to use SPARK_STEPS, you need to define your own steps

# SPARK_STEPS = [
#     {
#         "Name": "wcd_data_engineer",
#         "ActionOnFailure": "CONTINUE",
#         "HadoopJarStep": {
#             "Jar": "command-runner.jar",
#             "Args": [
#                 "spark-submit",
#                 "--deploy-mode",
#                 "cluster",
#                 "--master",
#                 "yarn",
#                 "s3://midtertm-airflow/sales.py",
#                 "--spark_name",
#                 "airflow_lab",
#                 "--input_file_url",
#                 "s3://mid-term-wh-dump/sales_20221016.csv.gz",
#                 "--output_file_url",
#                 "s3://midterm-parquet/sales/date=20221016/",
#                 ""
#                 # '--avg_order_amount', "{{ task_instance.xcom_pull('get_avg_order_amount', key='avg_order_amount') }}"
#             ],
#         },
#     }
# ]


SPARK_STEPS = [
    {
        "Name": "wcd_data_engineer",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                # '--class', 'Driver.MainApp',
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--num-executors",
                "2",
                "--driver-memory",
                "1g",
                "--executor-memory",
                "3g",
                "--executor-cores",
                "2",
                "s3://midtertm-airflow/sales.py",
                # "{{ macros.ds_format(ds, key='sales') }}",
                # "{{ task_instance.xcom_pull('add_steps', key='sales') }}",
                "s3://mid-term-wh-dump/sales_inv_store_wk_20221021.csv.gz"
                # "s3://mid-term-wh-dump/sales_inv_store_dy_20221021.csv.gz"
                # '{{ macros.ds_format(macros.ds_add(ds, -1),'%Y-%m-%d','%Y%m%d') }}'
                # '--spark_name', 'mid-term',
                # "--input_bucket",
                # "s3://mid-term-wh-dump/sales_20221016.csv.gz",
                # '--data', "{{ task_instance.xcom_pull('parse_request', key='input_paths') }}",
                # "--path_output",
                # "s3://midterm-parquet/sales/date=20221016/",
                # '-c', 'job',
                # '-m', 'append',
                # '--input-options', 'header=true'
            ],
        },
    }
]


CLUSTER_ID = "j-3DVBYJBY0YHMH"

# DEFAULT_ARGS = {
#     "depends_on_past": False,
#     "email": ["airflow@example.com"],
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=2),
#     "schedule_interval": "0 0 * * *",
#     "start_date": datetime(2022, 1, 1),
#     "catchup": False,
#     "backfill": False,
# }


DEFAULT_ARGS = {
    "owner": "wcd_data_engineer",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(0),
    "email": ["airflow_data_eng@wcd.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "concurrency": 4,
}


# def retrieve_s3_files(**kwargs):
#     data = kwargs["dag_run"].conf["sales"]
#     # data = kwargs["dag_run"].conf["inventory"]
#     # data = kwargs["dag_run"].conf["product"]
#     # data = kwargs["dag_run"].conf["calendar"]
#     # data = kwargs["dag_run"].conf["store"]
#     kwargs["ti"].xcom_push(key="sales", value=data)
#     # kwargs["ti"].xcom_push(key="inventory", value=data)
#     # kwargs["ti"].xcom_push(key="product", value=data)
#     # kwargs["ti"].xcom_push(key="calendar", value=data)
#     # kwargs["ti"].xcom_push(key="store", value=data)
def retrieve_s3_files(**kwargs):
    keys = ["calendar", "inventory", "product", "sales", "store"]
    for i in keys:
        data = kwargs["dag_run"].conf[i]
        kwargs["ti"].xcom_push(key=i, value=data)


dag = DAG(
    "a_midterm_dag",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None,
)

parse_request = PythonOperator(
    task_id="parse_request",
    provide_context=True,  # Airflow will pass a set of keyword arguments that can be used in your function
    python_callable=retrieve_s3_files,
    dag=dag,
)

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id=CLUSTER_ID,
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    dag=dag,
)

step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id=CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

step_adder.set_upstream(parse_request)
step_checker.set_upstream(step_adder)
