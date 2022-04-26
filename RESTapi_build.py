# -*- coding: utf-8 -*-
import pathlib
import sys
currentPath = f'{pathlib.Path().absolute()}'
file = sys.argv[1]
jsonPath = f'{currentPath}\\{file}'
hostname = 'ULRCTRLMS1'
port = '8446'

ctm_base_url = f'https://{hostname}:{port}/automation-api'
username = 'emuser'
password = 'emuser123456'
jsond = {
  'username': f'{username}',
  'password': f'{password}'
}

import requests
r = requests.post(url= ctm_base_url + '/session/login',\
                  json=jsond,\
                  verify=False)
r_json = r.json()
token = r_json['token']

CTM_headers = {
    'Authorization':f'Bearer {token}',
    }

CTMfile = [
        ('definitionsFile', (f'{file}', open(jsonPath, 'rb'), 'application/json'))
]

r = requests.post(url= ctm_base_url + '/build',\
                  headers = CTM_headers,\
                  files = CTMfile,\
                  verify=False)
print(r.json())  # Control-M error msg wrapped in one line

import json  
saveJsonPath = f'{currentPath}\\{file[:-5]}_buildResponse.json' 
f = open(saveJsonPath,'w+',encoding='utf-8')
jsons = json.dumps(r.json(),indent=4,separators=(',',':'),ensure_ascii=False)
f.write(jsons)
f.close()

print(jsons) # Control-M error msg in json format


#CTMfile = [
#        ('definitionsFile', (f'{file}.json', open(jsonPath, 'rb'), 'application/json'))
#]
#
#if jsonPath == f'{currentPath}\\{file}.json':
#    CTMfile = [
#        ('definitionsFile', ('Jobs_mini.json', open(jsonPath, 'rb'), 'application/json'))
#        ]
#    
#r = requests.post(url= ctm_base_url + '/deploy',\
#                  headers = CTM_headers,\
#                  files = CTMfile,\
#                  verify=False)
#print(r.json())