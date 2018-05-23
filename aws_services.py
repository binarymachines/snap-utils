#!/usr/bin/env python

# Service objects for Squid-ink service

import boto3
from boto3 import session
import uuid
from snap import common
import json
import os


class S3Key(object):
    def __init__(self, s3_key_string):
        self.folder_path = self.extract_folder_path(s3_key_string)
        self.object_name = self.extract_object_name(s3_key_string)
        self.full_name = s3_key_string

    def extract_folder_path(self, s3_key_string):
        if s3_key_string.find('/') == -1:
            return ''
        key_tokens = s3_key_string.split('/')
        return '/'.join(key_tokens[0:-1])

    def extract_object_name(self, s3_key_string):
        if s3_key_string.find('/') == -1:
            return s3_key_string
        return s3_key_string.split('/')[-1]


class S3ServiceObject():
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('local_temp_path')
        kwreader.read(**kwargs)

        self.local_tmp_path = kwargs['local_temp_path']
        self.s3session = None

        # we set this to True if we are initializing this object from inside an AWS Lambda,
        # because in that case we do not require the aws credential parameters to be set.
        # The default is False, which is what we want when we are creating this object
        # in a normal (non-AWS-Lambda) execution context: clients must pass in credentials.
        should_authenticate_via_iam = kwargs.get('auth_via_iam', False)

        if not should_authenticate_via_iam:
            key_id = kwargs.get('aws_key_id')
            secret_key = kwargs.get('aws_secret_key')
            if not key_id or not secret_key:
                raise Exception('''
                S3ServiceObject must pe passed the "aws_key_id" and "aws_secret_key"
                parameters if the "auth_via_iam" init param is not set (or is False).''')

            self.s3session = session.Session(aws_access_key_id=key_id,
                                             aws_secret_access_key=secret_key)
        self.s3client = boto3.client('s3')

    def upload_object(self, local_filename, bucket_name, bucket_path=None):
        s3_key = None
        with open(local_filename, 'rb') as data:
            base_filename = os.path.basename(local_filename)
            if bucket_path:
                s3_key = os.path.join(bucket_path, base_filename)
            else:
                s3_key = base_filename
            self.s3client.upload_fileobj(data, bucket_name, s3_key)
        return s3_key

    def download_object(self, bucket_name, s3_key_string):
        s3_object_key = S3Key(s3_key_string)
        local_filename = os.path.join(self.local_tmp_path, s3_object_key.object_name)
        with open(local_filename, "wb") as f:
            self.s3client.download_fileobj(bucket_name, s3_object_key.full_name, f)
        return local_filename


class KinesisServiceObject(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('stream',
                                           'region')
        kwreader.read(**kwargs)
        self.stream_name = kwreader.get_value('stream')
        self.region = kwreader.get_value('region')

        should_authenticate_via_iam = kwargs.get('auth_via_iam', False)

        if not should_authenticate_via_iam:
            key_id = kwargs.get('aws_key_id')
            secret_key = kwargs.get('aws_secret_key')
            if not key_id or not secret_key:
                raise Exception('''KinesisServiceObject must pe passed the "aws_key_id"
                and "aws_secret_key" parameters if the "auth_via_iam" init param is not
                set (or is False).''')

            self.kinesis_session = session.Session(aws_access_key_id=key_id,
                                                   aws_secret_access_key=secret_key)

        self.kinesis_client = boto3.client('kinesis', region_name=self.region)

    def generate_partition_key(self, record):
        return str(uuid.uuid4())

    def bulk_write(self, record_dict_array, stream_name):
        input_records = []
        for record in record_dict_array:
            pkey = self.generate_partition_key(record)
            data = json.dumps(record).encode()   # defaults to utf-8
            input_records.append({'Data': data, 'PartitionKey': pkey})
        return self.kinesis_client.put_records(Records=input_records, StreamName=stream_name)

    def write(self, record_dict, stream_name):
        pkey = self.generate_partition_key(record_dict)
        return self.kinesis_client.put_record(StreamName=stream_name,
                                              Data=json.dumps(record_dict).encode(),
                                              PartitionKey=pkey)
