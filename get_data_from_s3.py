import boto3
import yaml
from io import BytesIO

import pandas as pd

with open('aws.yaml') as f:
    config = yaml.safe_load(f)

aws_access_key = config['config']['access_key_id']
aws_secret_key = config['config']['secret_access_key']

s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key,
                  aws_secret_access_key=aws_secret_key)


def read_data(bucket_name = 'kdh', prefix = 'tmp'):
    obj = s3.get_object(Bucket = bucket_name, Key = prefix)
    return obj['Body'].read()


def upload_data(local_file_path = 'kdh', bucket_name = 'kdh', prefix = 'tmp'):
    s3.upload_file(local_file_path, bucket_name, prefix)


def get_file_list(bucket_name = 'kdh', folder_name = 'folder_name'):
    tmp = s3.list_objects_v2(Bucket = bucket_name, Prefix = folder_name)
    if 'Contents' in tmp:
        file_list = [obj['Key'] for obj in tmp['Contents']]
        return file_list
    else:
        print('No Files Found In The Folder')