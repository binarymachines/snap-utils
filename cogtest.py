#!/usr/bin/env python

'''Usage: 
        cogtest.py <configfile> --login --username=<username> --password=<password> [--save-token]
        cogtest.py <configfile> --newuser <username>
        cogtest.py <configfile> --initpw --username=<username> --password=<new-password> [--sessionfile=<filename>]
        cogtest.py <configfile> --reset --username=<username>
        cogtest.py <configfile> --verification-code <attribute>
        cogtest.py <configfile> --verify <attribute> <code>

'''


import os
import sys
import json
from snap import snap, common
import aws_services as awss
import docopt
import yaml


def read_stdin():
    raw_line = sys.stdin.readline()
    return raw_line.lstrip().rstrip()
    

def main(args):
    configfile = args['<configfile>']
    yaml_config = None
    with open(configfile) as f:
        yaml_config = yaml.load(f)
        
    services = common.ServiceObjectRegistry(snap.initialize_services(yaml_config))
    cognito_svc = services.lookup('cognito')
    result = None

    if args['--verify']:
        attr_name = args['<attribute>']
        code = args['<code>']        
        line = read_stdin()
        if line:
            access_token = line
        return cognito_svc.verify_named_attribute(attr_name, access_token, code)
    
    if args['--verification-code']:
        line = read_stdin()
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
        line = read_stdin()
        if line:
            session = line
        result = cognito_svc.change_initial_password(username, new_password, session)
        print('sent cognito request with result:', file=sys.stderr)
        print(json.dumps(result))

        
if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
