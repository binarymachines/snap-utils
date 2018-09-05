#!/usr/bin/env python

'''Usage: 
        cogtest.py --login --username=<username> --password=<password> [--save-token]
        cogtest.py --newuser <username>
        cogtest.py --initpw --username=<username> --password=<new-password> [--sessionfile=<filename>]
        cogtest.py --reset --username=<username>
        cogtest.py --verification-code <attribute>
        cogtest.py --verify <attribute> <code>

'''


import os
import sys
import json
from snap import common
import aws_services as awss
import docopt


def main(args):
    aws_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    client_secret = os.getenv('COGNITO_CLIENT_SECRET')
    client_id = os.getenv('COGNITO_CLIENT_ID')
    pool_id = os.getenv('COGNITO_USER_POOL_ID')
    cognito_svc = awss.AWSCognitoService(aws_region='us-east-1',
                                       user_pool_id=pool_id,
                                       client_id=client_id,
                                       client_secret=client_secret, # optional, depending on client app setup
                                       aws_secret_key=aws_key,
                                       aws_key_id=aws_key_id)
    result = None

    if args['--verify']:
        attr_name = args['<attribute>']
        code = args['<code>']
        
        raw_line = sys.stdin.readline()
        line = raw_line.lstrip().rstrip()
        if line:
            access_token = line
        return cognito_svc.verify_named_attribute(attr_name, access_token, code)
    
    if args['--verification-code']:
        raw_line = sys.stdin.readline()
        line = raw_line.lstrip().rstrip()
        if line:
            access_token = line
            result = cognito_svc.get_verification_code_for_named_attribute(args['<attribute>'], access_token)
            print(result)
    if args['--reset']:
        username = args['--username']
        result = cognito_svc.reset_password(username)
    
    if args['--login']:        
        username = args['--username']
        password = args['--password']
        result = cognito_svc.user_login(username, password)
        if args['--save-token']:
            if result.get('ChallengeName'):
                print(result['Session'])
            elif result['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(result['AuthenticationResult']['AccessToken'])
        else:
            print(json.dumps(result))
            
    if args['--newuser']:        
        user_attrs = []
        username = args['<username>']
        user_attrs.append(awss.CognitoUserAttribute(name='email', value=username))
        result = cognito_svc.user_create(username, user_attrs)
        print('sent cognito request with result:', file=sys.stderr)
        print(result)
        
    if args['--initpw']:        
        username = args['--username']
        new_password = args['--password']
        session = None
        raw_line = sys.stdin.readline()
        line = raw_line.lstrip().rstrip()
        if line:
            session = line
        result = cognito_svc.change_initial_password(username, new_password, session)
        print('sent cognito request with result:', file=sys.stderr)
        print(json.dumps(result))

    


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
