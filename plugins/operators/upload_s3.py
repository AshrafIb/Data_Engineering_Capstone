from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3 
import os
import logging


class UploadToS3(BaseOperator):
    '''
    S3 Upload Operator
    '''
    @apply_defaults
    def __init__(self, aws_credentials_id='',s3_bucket='',path='',files_path='',*args, **kwargs):
        '''
        Arguments:
            aws_credentials  = Credentials of IAM-Role
            s3_bucket        = Bucket Name (for Data Upload)
            path             = Bucket Key/Subfolder
            files_path       = Docker-path of Files to Upload
        '''
        super(UploadToS3, self).__init__(*args,**kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket          = s3_bucket
        self.path               = path 
        self.files_path         = files_path

    def execute(self, context):
        '''
        Execute function 
        
        '''
        aws_hook    = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        s3=boto3.client('s3',aws_access_key_id=credentials.access_key,aws_secret_access_key=credentials.secret_key)
        
        bucket_list=[]

        try:
            for upload in s3.list_objects(Bucket=self.s3_bucket,Delimiter='/', Prefix=self.path)['Contents']:
                bucket_list.append(upload['Key'])
        except: 
            pass 

        logging.info(bucket_list)  
        for file in os.listdir(self.files_path):
            filepath='{}/{}'.format(self.files_path,file)
            if os.path.isdir(filepath):
                continue
            else:
                file_new='{}{}'.format(self.path,file)
                if file_new in bucket_list:
                    logging.info('{} exists; no upload initiated'.format(file))
                else:
                    s3.upload_file(self.files_path+file, self.s3_bucket,self.path+file)
                    logging.info('{} uploaded to {}'.format(file, self.s3_bucket))



