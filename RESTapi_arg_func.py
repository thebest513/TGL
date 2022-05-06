# -*- coding: utf-8 -*-
import argparse
import urllib3
import os.path
import pathlib
import requests
import json  
urllib3.disable_warnings() # disable warnings when creating unverified requests

def base_url(host, port):
    ctm_base_url = f'https://{host}:{port}/automation-api'
    return ctm_base_url

def login(u, p, base_url):
    tokenf = 'token.txt'
    
    jsond = {
      'username': f'{u}',
      'password': f'{p}'
    }
    
    r = requests.post(url= base_url + '/session/login',\
                  json=jsond,\
                  verify=False)
    r_json = r.json()
    
    if r_json.get('token'):
        token = r_json.get('token')
        with open(tokenf, 'w') as f:
            f.write(token)
    return r_json.get('token')

def postj(j, service, base_url, token):
    CTM_headers = {
        'Authorization':f'Bearer {token}',
        }
    
    CTMfile = [
            ('definitionsFile', (j, open(j, 'rb'), 'application/json'))
    ]
    
    r = requests.post(url= base_url + f'/{service}',\
                  headers = CTM_headers,\
                  files = CTMfile,\
                  verify=False)

    jsons = json.dumps(r.json(),indent=4,separators=(',',':'),ensure_ascii=False)
    print(jsons) # Control-M error msg in json format
    return jsons

def getj(folder, path, base_url, token):
    CTM_headers = {
        'Authorization':f'Bearer {token}',
        }
    
    folder = folder.replace('#', '%23') # error occurs if url contains "#"
    r = requests.get(url= base_url + f'/deploy/jobs?server=*&folder={folder}',\
                  headers = CTM_headers,\
                  verify=False)
        
    jsons = json.dumps(r.json(),indent=4,separators=(',',':'),ensure_ascii=False)
    
    if not r.json().get('errors'):
        with open(path, 'w', encoding='UTF-8') as f:
            f.write(jsons)
    else:
        print(jsons)
    return jsons

def report(report, t, path, base_url, token):
    CTM_headers = {
        'Authorization':f'Bearer {token}',
        }
    
    report = report.replace('#', '%23') #  error occurs if url contains "#"
    r = requests.get(url= ctm_base_url + f'/reporting/report/shared:{report}?format={t}',\
                  headers = CTM_headers,\
                  verify=False)

    if r.json().get('reportURL'):
        t = t.replace('excel', 'xlsx')
        with open(f'{path}.{t}', 'wb') as f:
            f.write(requests.get(r.json().get('reportURL')).content)
    else:
        jsons = json.dumps(r.json(),indent=4,separators=(',',':'),ensure_ascii=False)
        print(jsons)
    return r.json().get('reportURL')


if __name__=='__main__':
    parser = argparse.ArgumentParser(prog='Control-M RESTapi',\
                                     description='Validate or deploy json, pull json from \
                                         Control-M Server, or generate Control-M reports')
    
    # login = parser.add_argument_group('login', 'Login to Control-M ang received a token')
    parser.add_argument('-u', '--user', help='Control-M user name')
    parser.add_argument('-p', '--password', help='Control-M user password')
    parser.add_argument('-H', '--host', default='10.67.70.210', help='Control-M Server hostname or ip')
    parser.add_argument('-D', '--delete', action='store_true', help='Delete ./token.txt')
    
    subparsers = parser.add_subparsers(dest='cmd')
    Post_parser = subparsers.add_parser('Post', description='Validate or deploy json\
                                                to Contorl-M Server')
    Post_parser.add_argument('obj', help='POST <json file> to Control-M Server')
    Post_parser.add_argument('-s', '--service', choices=['build', 'deploy'], default='build',\
                      help='"build"(default) to validate json or "deploy" to validate and deploy json')
     
    Folder_parser = subparsers.add_parser('Folder')
    Folder_parser.add_argument('obj', help='GET <folder name> in json format from Control-M Server')
    Folder_parser.add_argument('path', help='Save Control-M folder.json in <path/filename>')   
        
    Report_parser = subparsers.add_parser('Report')
    Report_parser.add_argument('obj', help='Generate Control-M report <report template name>')
    Report_parser.add_argument('-t', '--type', choices=['pdf', 'csv', 'excel'],\
                               default='excel', help='Control-M report format, "excel" by default')
    Report_parser.add_argument('path', help='Save Control-M report in <path/filename>')   
    
    args = parser.parse_args()
    
    port = '8446'
    tokenf = 'token.txt'
    token = ''
    ctm_base_url = base_url(args.host, port)
    
    if args.user and args.password:
        token = login(args.user, args.password, ctm_base_url)
            
    elif os.path.isfile(tokenf):
        with open(tokenf, 'r') as f:
            token = f.read()
    
    if args.delete and os.path.isfile(tokenf):
        os.remove(tokenf)
        print('./token.txt deleted')
        
    if token:
        if args.cmd == 'Post':
            postj(args.obj, args.service, ctm_base_url, token)
            
        elif args.cmd == 'Folder':
            getj(args.obj, args.path, ctm_base_url, token)
                
        elif args.cmd == 'Report':
            report(args.obj, args.type, args.path, ctm_base_url, token)
            
        else:
            print('One positional argument:{Post,Folder,Report} required')
    else:
        print('No token obtained')