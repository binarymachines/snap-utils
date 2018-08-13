#!/usr/bin/env python

# Service objects for Squid-ink service

import boto3
from boto3 import session
import uuid
from snap import common
import json
import os
from snap.loggers import transform_logger as log


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


conditional_auth_mesage = '''
S3ServiceObject must pe passed the "aws_key_id" and "aws_secret_key"
parameters if the "auth_via_iam" init param is not set (or is False).'''

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
            log.info("NOT authenticating via IAM. Setting credentials now.")
            key_id = kwargs.get('aws_key_id')
            secret_key = kwargs.get('aws_secret_key')
            if not key_id or not secret_key:
                raise Exception(conditional_auth_message)
        
            key_id = kwargs.get('aws_key_id')
            secret_key = kwargs.get('aws_secret_key')            
            self.s3client = boto3.client('s3',
                                         aws_access_key_id=key_id,
                                         aws_secret_access_key=secret_key)
        else:
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


class AWSEmailService(object):
    def __init__(self, **kwargs):
        self.region = kwargs.get('aws_region')
        self.charset = 'utf-8'
        self.ses_client = boto3.client('ses', region_name=self.region)


    def _create_message(self, sender, recipient_list, subject, body):
        # Create a multipart/alternative child container.
        email_message = MIMEMultipart('mixed')
        email_message['Subject'] = subject
        email_message['From'] = sender
        email_message['To'] = ', '.join(recipient_list)
        # Encode the text and HTML content and set the character encoding. This step is
        # necessary if you're sending a message with characters outside the ASCII range.
        textpart = MIMEText(body.encode(self.charset), 'plain', charset)
        htmlpart = MIMEText(body.encode(self.charset), 'html', charset)

        msg_body = MIMEMultipart('alternative')
        # Add the text and HTML parts to the child container.
        msg_body.attach(textpart)
        msg_body.attach(htmlpart)
        email_message.attach(msg_body)

        return email_message
        

    def _create_attachment(self, filename):
        with open(filename, 'rb') as f:
            att = MIMEApplication(f.read())
            att.add_header('Content-Disposition','attachment',filename=os.path.basename(filename))
        return att


    def send(self,
             sender_address,
             recipient_list,
             subject,
             body,
             attachment_filename=None):

        message = self.create_message(sender_address, recipient_list, subject, body)
        if attachment_filename:
            attachment = self._create_attachment(attachment_filename)
            message.attach(attachment)
        
        try:
            response = client.send_raw_email(
                Source=sender_address,
                Destinations=[r for r in recipient_list],
                RawMessage={
                    'Data':message.as_string(),
                },
                ConfigurationSetName=CONFIGURATION_SET
            )

            return response['MessageId']
        except ClientError as e:        
            raise e # or return an error code
