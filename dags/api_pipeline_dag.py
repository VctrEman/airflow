import sys
#import load_s3_full
#Testing pipelines book api example on S3
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '0 * * * *', #timedelta(days=1),
}

dag = DAG(
    'elt_pipeline_sample_dag', #name on airflow
    description='A sample ELT pipeline',
    default_args=default_args
    )

def callable_virtualenv():
    """
    Example function that will be performed in a virtual environment.
    """
    import json
    import boto3
    import pandas as pd 
    
    def get_from_s3(origin_bucket_name, Key, access_key, secret_key, aws_endpoint = None):
        s3 = boto3.client('s3',
                    aws_access_key_id = access_key,
                    aws_secret_access_key = secret_key)
        obj = s3.get_object(Bucket= origin_bucket_name, Key = Key)
        print("Extracted from","s3a://{}/{}".format(origin_bucket_name, Key) ) 
        return obj

    def dump_to_s3( Body, destination_bucket_name, Key, access_key, secret_key, aws_endpoint = None):
        s3 = boto3.client('s3',
                            aws_access_key_id = access_key,
                            aws_secret_access_key = secret_key)
        s3.put_object(Body = Body, Bucket = destination_bucket_name, Key= Key)
        print("Put into","s3a://{}/{}".format(destination_bucket_name, Key) )

    def main():
        # load the aws_boto_credentials values
        base_path = "/opt/airflow/data"
        with open(base_path+"/config.json", "r") as read_file: #https://www.reddit.com/r/docker/comments/ow3j8l/airflow_filenotfounderror_errno_2_no_such_file_or/
            cfg = json.load(read_file)

        #setting configurations
        s3_config = cfg['aws_boto_credentials']
        access_key = s3_config['access_key']
        secret_key = s3_config['secret_key']
        origin_bucket_name = s3_config['landing_layer_bucket']
        destination_bucket_name = s3_config['raw_layer_bucket']
                                    
        s3 = boto3.client('s3',
                            aws_access_key_id = access_key,
                            aws_secret_access_key = secret_key)

        origin_s3_file = "order_extract.csv"

        obj = get_from_s3( access_key = access_key,
                            secret_key = secret_key,
                            origin_bucket_name = origin_bucket_name,
                            Key= origin_s3_file)

        df = pd.read_csv(obj['Body'], sep = '|')
        print(df.info())
        to_csv = df.to_csv(index=False, header=False)

        dump_to_s3( destination_bucket_name = destination_bucket_name, 
                    Body = to_csv, 
                    Key = "raw_"+origin_s3_file,
                    access_key=access_key, 
                    secret_key=secret_key)

        print('Finished!')
    main()

virtualenv_task = PythonVirtualenvOperator(
        task_id="virtualenv_python",
        python_callable=callable_virtualenv,
        requirements=["boto3","pandas==1.1.5"],
        system_site_packages=False,
        dag=dag,
    )

virtualenv_task #>> load_orders_task