from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from operators.upload_s3 import UploadToS3
from operators.check_upload import DataLakeUpload
from helpers.emr_instance import InstanceEMR
from operators.clean_description import creating_label_df
from scripts.bucket import bucketname
import logging
import os 

from datetime import timedelta, datetime


Spark_Steps=[
    { 
        'Name':'Start',
        'ActionOnFailure':'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar':'command-runner.jar',
            'Args':['state-pusher-script','sudo','pip','install','pandas']
        }
    },
    {
        'Name':'Copy Scripts',
        'ActionOnFailure':'TERMINATE_CLUSTER',
        'HadoopJarStep':{
            'Jar':'command-runner.jar',
            'Args':['aws','s3','cp','s3://'+bucketname.bucket+'/scripts','/home/hadoop/','--recursive']
        }
    },
    {
        'Name':'Immigration-ETL',
        'ActionOnFailure':'CONTINUE',
        'HadoopJarStep':{
            'Jar':'command-runner.jar',
            'Args':['spark-submit','/home/hadoop/i94_etl.py','s3://'+bucketname.bucket+'/','s3://'+bucketname.bucket+'/']
        }

    }, 
    {
        'Name':'Airport-ETL',
        'ActionOnFailure':'CONTINUE',
        'HadoopJarStep':{
            'Jar':'command-runner.jar',
            'Args':['spark-submit','/home/hadoop/airport_etl.py','s3://'+bucketname.bucket+'/', 's3://'+bucketname.bucket+'/']
        }
    }, 
    {
        'Name':'Weather-ETL',
        'ActionOnFailure':'CONTINUE',
        'HadoopJarStep':{
            'Jar':'command-runner.jar',
            'Args':['spark-submit','/home/hadoop/weather_etl.py','s3://'+bucketname.bucket+'/', 's3://'+bucketname.bucket+'/']
        }
    },
    {
        'Name':'Demographic-ETL',
        'ActionOnFailure':'CONTINUE',
        'HadoopJarStep':{
            'Jar':'command-runner.jar',
            'Args':['spark-submit','/home/hadoop/demographic_etl.py','s3://'+bucketname.bucket, 's3://'+bucketname.bucket+'/']
    }
    }
]

# Defining Args 

default_args = {
    'owner':'Ashraf',
    'start_date':datetime.now(),
    'depends_on_past': False,
    'retries':1,
    'catchup': False
}

dag=DAG('capstone_dag', 
        default_args=default_args,
        description='DAG for my Capstone Project',
        schedule_interval='@monthly',
        max_active_runs=1
        )

# Defining Operator 

start_operator=DummyOperator(task_id='Begin_execution', dag=dag)


create_dicts=creating_label_df(
    task_id='Create_Labels_JSON',
    dag    = dag,
    file ='I94_SAS_Labels_Descriptions.SAS')

upload_files = UploadToS3(
    task_id ='Upload_files_to_s3',
    dag     =dag,
    aws_credentials_id='aws_credentials',
    s3_bucket =bucketname.bucket,
    files_path='/opt/airflow/data/',
    path='data/'
    )

upload_scripts = UploadToS3(
    task_id ='Upload_scripts_to_s3',
    dag     =dag,
    aws_credentials_id='aws_credentials',
    s3_bucket =bucketname.bucket,
    files_path='/opt/airflow/plugins/scripts/',
    path='scripts/'
    )

setup_emr = EmrCreateJobFlowOperator(
    task_id= 'Create_EMR_Instance',
    dag=dag,
    job_flow_overrides=InstanceEMR.JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    region_name='us-west-2'
)

config_emr=EmrAddStepsOperator(
    task_id='Configuration_of_EMR',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Instance', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=dag,
    steps=Spark_Steps
)

immigration_etl=EmrStepSensor(
    task_id='Immigration_ETL',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Instance', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Configuration_of_EMR', key='return_value')[2] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

airport_etl=EmrStepSensor(
    task_id='Airport_ETL',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Instance', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Configuration_of_EMR', key='return_value')[3] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

weather_etl=EmrStepSensor(
    task_id='Weather_ETL',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Instance', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Configuration_of_EMR', key='return_value')[4] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

demogrpahic_etl=EmrStepSensor(
    task_id='Demographic_ETL',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Instance', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Configuration_of_EMR', key='return_value')[5] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

step_check=EmrStepSensor(
    task_id='Check_Steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Instance', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Configuration_of_EMR', key='return_value')[-1] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

end_emr=EmrTerminateJobFlowOperator(
    task_id='Shutdown_EMR',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Instance', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

check_upload=DataLakeUpload(
    task_id='Checking_Upload',
    aws_credentials_id='aws_credentials',
    s3_bucket =bucketname.bucket,
    path      ='processed/',
    keys      =['weather.csv/','airport.csv/','demographics.csv/','cleaned_immigration/'],
    dag=dag
)


end_operator=DummyOperator(task_id='Stop_execution',dag=dag)


# Defining Graph
start_operator>>create_dicts
start_operator>>upload_scripts
start_operator>>setup_emr

create_dicts>>upload_files
setup_emr>>config_emr
upload_scripts>>config_emr
upload_files>>config_emr

config_emr>>[immigration_etl,airport_etl,demogrpahic_etl,weather_etl] >>step_check

step_check>>end_emr
end_emr>>check_upload
check_upload>>end_operator