from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime
# EMR cluster configuration
JOB_FLOW_OVERRIDES = {
    "Name": "Airflow-EMR-Cluster",
    "ReleaseLabel": "emr-6.15.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "VisibleToAllUsers": True,
    "LogUri": "s3://j0c0vk5-streaming-demo/logs/",
}

# Spark step definition
SPARK_STEPS = [
    {
        "Name": "Run Spark Job",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "s3://j0c0vk5-streaming-demo/code/generate_tweets_2.py",
                # add script arguments here if needed
            ],
        },
    }
]

with DAG(
    dag_id="emr_spark_job_example",
    default_args={"owner": "airflow"},
    schedule="@daily",
    start_date=datetime(2025, 5, 7),
    catchup=False,
    tags=["example", "emr"],
) as dag:

    # 1. Create EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
    )

    # 2. Add Spark step
    add_spark_step = EmrAddStepsOperator(
        task_id="add_spark_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
    )

    # 3. Wait for Spark step to complete
    watch_spark_step = EmrStepSensor(
        task_id="watch_spark_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    # 4. Terminate EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done",  # Ensures cluster is terminated even if previous tasks fail
    )

    create_emr_cluster >> add_spark_step >> watch_spark_step >> terminate_emr_cluster
