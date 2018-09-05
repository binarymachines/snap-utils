#!/usr/bin/env python

# Service objects for Squid-ink service


import json
import os
import hashlib
import hmac
import base64
from collections import namedtuple
import boto3
from boto3 import session
import uuid
from snap import common
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


s3_auth_error_mesage = '''
S3ServiceObject must pe passed the "aws_key_id" and "aws_secret_key"
parameters if the "auth_via_iam" init param is not set (or is False).'''

kinesis_auth_error_mesage = '''
KinesisServiceObject must pe passed the "aws_key_id" and "aws_secret_key"
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
                raise Exception(kinesis_auth_error_mesage)

            self.kinesis_client = boto3.client('kinesis',
                                               aws_access_key_id=key_id,
                                               aws_secret_access_key=secret_key,
                                               region_name=self.region)
        else:
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


CognitoUserAttribute = namedtuple('CognitoUserAttribute', 'name value')

class AWSCognitoService(object):
    def __init__(self, **kwargs):     
        kwreader = common.KeywordArgReader('user_pool_id', 'client_id', 'aws_region', 'aws_secret_key', 'aws_key_id')
        kwreader.read(**kwargs)
        self.user_pool_id = kwreader.get_value('user_pool_id')
        self.client_id = kwreader.get_value('client_id')
        self.aws_region = kwreader.get_value('aws_region')
        self.client_secret = kwargs.get('client_secret')
        
        should_authenticate_via_iam = kwargs.get('auth_via_iam', False)

        if not should_authenticate_via_iam:
            key_id = kwargs.get('aws_key_id')
            secret_key = kwargs.get('aws_secret_key')
            if not key_id or not secret_key:
                raise Exception(cognito_auth_error_mesage)
        
            self.cognito_client = boto3.client('cognito-idp',
                                               aws_access_key_id=key_id,
                                               aws_secret_access_key=secret_key,
                                               region_name=self.aws_region)
        else:
            self.cognito_client = boto3.client('cognito_idp', region_name=self.aws_region)


    def generate_secret_hash(self, username):
        if not self.client_secret:
            raise Exception('Cognito client was spun up without specifying a client secret. Cannot create a valid secret hash.')
        message = username + self.client_id
        digest = hmac.new(self.client_secret.encode(),
                          message.encode(),
                          digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest).decode()
        return signature

    
    def generate_temp_password(self):
        return 'foobarpassword'

            
    def change_initial_password(self, username, new_password, session_key):
        payload = {}
        payload['ChallengeName'] = 'NEW_PASSWORD_REQUIRED'
        payload['ClientId'] = self.client_id
        payload['UserPoolId'] = self.user_pool_id
        payload['ChallengeResponses'] = {
            'NEW_PASSWORD': new_password,
            'USERNAME': username,
            'SECRET_HASH': self.generate_secret_hash(username)
            }
        payload['Session'] = session_key
        return self.cognito_client.admin_respond_to_auth_challenge(**payload)


    def reset_password(self, username):
        payload = {
            'Username': username,
            'UserPoolId': self.user_pool_id
            }
        return self.cognito_client.admin_reset_user_password(**payload)

    def verify_named_attribute(self, attr_name, access_token, code):
        payload = {}
        payload['AccessToken'] = access_token
        payload['AttributeName'] = attr_name
        payload['Code'] = code
        return self.cognito_client.verify_user_attribute(**payload)

    
    def get_verification_code_for_named_attribute(self, attr_name, access_token):
        return self.cognito_client.get_user_attribute_verification_code(AccessToken=access_token,
                                                                        AttributeName=attr_name)

    
    def user_create(self, username, attribute_list=[], **kwargs):
        payload = {}
        payload['DesiredDeliveryMediums'] = ['EMAIL'] # how to send invitation message to new user
        payload['ForceAliasCreation'] = False
        payload['MessageAction'] = 'SUPPRESS' # re-send confirmation message if user already exists
        payload['TemporaryPassword'] = self.generate_temp_password()
        payload['UserAttributes'] = [{'Name': attr.name, 'Value': attr.value} for attr in attribute_list]
        payload['Username'] = username
        payload['UserPoolId'] = self.user_pool_id
        # skip ValidationData parameter for now; may be required later
        return self.cognito_client.admin_create_user(**payload)

    
    def user_login(self, username, password, **kwargs):
        payload = {}
        payload['UserPoolId'] = self.user_pool_id
        payload['ClientId'] = self.client_id
        payload['AuthFlow'] =  'ADMIN_NO_SRP_AUTH'
        payload['AuthParameters'] = {
            'USERNAME': username,
            'PASSWORD': password,
            'SECRET_HASH': self.generate_secret_hash(username)
        }

        return self.cognito_client.admin_initiate_auth(**payload)
        # TODO: status = CognitoAuthStatus(response) and return the status object
        



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
