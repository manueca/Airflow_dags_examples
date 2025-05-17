from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

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
            ],
        },
    }
]

with DAG(
    dag_id="emr_spark_job_no_step_function",
    start_date=datetime(2025, 5, 8),
    schedule=None,
    catchup=False,
    tags=["example", "emr"],
) as dag:

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
    )

    add_spark_step = EmrAddStepsOperator(
        task_id="add_spark_step",
        job_flow_id=create_emr_cluster.output,
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
    )

    watch_spark_step = EmrStepSensor(
        task_id="watch_spark_step",
        job_flow_id=create_emr_cluster.output,
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=create_emr_cluster.output,
        aws_conn_id="aws_default",
        trigger_rule="all_done",
    )

    create_emr_cluster >> add_spark_step >> watch_spark_step >> terminate_emr_cluster
