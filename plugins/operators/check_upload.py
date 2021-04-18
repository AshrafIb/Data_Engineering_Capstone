from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3 
import os
import logging


class DataLakeUpload(BaseOperator):
    '''
    Checking Datalake Transformations
    '''
    @apply_defaults
    def __init__(self,aws_credentials_id,s3_bucket,path,keys,*args,**kwargs):
        ''' 
        Arguments: 
            aws_credentials_id = Credentials of IAM-Role
            s3_bucket       = Bucket Name 
            path            = Bucket Key/Subfolder
            keys            = Futher Subfolder
        '''
        super(DataLakeUpload, self).__init__(*args,**kwargs)
        self.aws_credentials_id      = aws_credentials_id
        self.s3_bucket               = s3_bucket
        self.path                    = path
        self.keys                    = keys

    def execute(self,context):
        ''' 
        Execute function
        '''
        aws_hook    = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        s3=boto3.client('s3',aws_access_key_id=credentials.access_key,aws_secret_access_key=credentials.secret_key)

        bucket_list=[]

        for key in self.keys:
            for upload in s3.list_objects(Bucket=self.s3_bucket,Delimiter='/', Prefix=self.path+key)['Contents']:
                bucket_list.append(upload['Key'])
            if len(bucket_list)>0:
                logging.info('{} Files processed and uploaded'.format(key))
            else:
                assert bucket_list, "No Files found for {}".format(key) 
        