#!/usr/bin/env python3
"""Create a dedicated three-node benchmark configuration and R2 bucket.

Requires the CF_* variables from a protected credential file in the environment.
This creates billable resources; it never overwrites a retained run directory.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import time
import urllib.request

import boto3
from botocore.config import Config


def api(path, value=None):
    request = urllib.request.Request(
        f"https://api.cloudflare.com/client/v4/accounts/{os.environ['CF_ACCOUNT_ID']}/{path}",
        data=None if value is None else json.dumps(value).encode(),
        headers={'Authorization': 'Bearer '+os.environ['CF_API_TOKEN'], 'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.load(response)
    if not result.get('success'):
        raise RuntimeError('Cloudflare API request failed')
    return result['result']


def private(path, value):
    with path.open('x', opener=lambda p,f: os.open(p,f,0o600)) as stream:
        stream.write(json.dumps(value,indent=2)+'\n')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir',required=True,type=Path)
    parser.add_argument('--binary',required=True,type=Path)
    args=parser.parse_args()
    required=('CF_ACCOUNT_ID','CF_API_TOKEN','CF_ACCESS_KEY_ID','CF_SECRET_ACCESS_KEY','CF_S3_API_ENDPOINT')
    if any(not os.environ.get(k) for k in required):
        parser.error('Source the protected CF_* credential file first')
    binary=args.binary.read_bytes()
    if binary[:4] != b'\x7fELF' or int.from_bytes(binary[18:20],'little') != 62:
        parser.error('A Linux x86-64 release binary is required')
    root=args.run_dir.resolve()
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    if any(root.iterdir()):
        parser.error('Use a fresh empty run directory; retain old manifests, credentials and data')
    suffix=secrets.token_hex(3)
    name='beyonddb-perf-'+suffix
    subdomain=api('workers/subdomain')['subdomain']
    settings={'worker_url':f'https://{name}.{subdomain}.workers.dev',
              'tunnel_token':secrets.token_urlsafe(48),'admin_token':secrets.token_urlsafe(48),
              'access_key':'AKIABDBCFPERF00001','secret':secrets.token_urlsafe(40),
              'peer_key':secrets.token_bytes(32).hex(),'encryption_key':secrets.token_bytes(32).hex()}
    repo=Path(__file__).resolve().parents[2]
    meta={'worker':name,'bucket':name,'prefix':'ordered-v1','nodes':3,'cells_per_node':10,
          'source_revision':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
          'created_at':time.time(),'vcpu':1,'memory_mib':3072,'disk_mb':5000,
          'binary_sha256':hashlib.sha256(binary).hexdigest(),
          'lockfile_sha256':hashlib.sha256((repo/'Cargo.lock').read_bytes()).hexdigest(),
          'status':'configuration-created'}
    private(root/'deployment.json',meta)
    private(root/'secrets.json',settings)
    worker_secrets={'R2_ENDPOINT':os.environ['CF_S3_API_ENDPOINT'],'R2_BUCKET':name,
                    'R2_ACCESS_KEY_ID':os.environ['CF_ACCESS_KEY_ID'],
                    'R2_SECRET_ACCESS_KEY':os.environ['CF_SECRET_ACCESS_KEY'],
                    'PEER_KEY_HEX':settings['peer_key'],'ENCRYPTION_KEY_HEX':settings['encryption_key'],
                    'DDB_ACCESS_KEY':settings['access_key'],'DDB_SECRET_KEY':settings['secret'],
                    'TUNNEL_TOKEN':settings['tunnel_token'],'ADMIN_TOKEN':settings['admin_token']}
    private(root/'worker-secrets.json',worker_secrets)
    image=root/'image';image.mkdir()
    shutil.copyfile(args.binary,image/'beyonddb');(image/'beyonddb').chmod(0o755)
    for f in ('agent.py','Dockerfile'):shutil.copyfile(repo/'deploy/cloudflare'/f,image/f)
    shutil.copyfile(repo/'deploy/cloudflare/worker.js',root/'worker.js')
    config={'name':name,'main':'worker.js','compatibility_date':'2026-10-19',
            'account_id':os.environ['CF_ACCOUNT_ID'],'workers_dev':True,
            'containers':[{'class_name':'BeyondNode','scheduling_policy':'durable_object',
                           'images':{'base':{'dockerfile':'./image/Dockerfile','build_context':'./image'}}}],
            'durable_objects':{'bindings':[{'name':'NODES','class_name':'BeyondNode'}]},
            'exports':{'BeyondNode':{'type':'durable-object','storage':'sqlite'}},
            'observability':{'enabled':True,'head_sampling_rate':1},
            'vars':{'WORKER_URL':settings['worker_url']}}
    private(root/'wrangler.json',config)
    api('r2/buckets',{'name':name})
    s3=boto3.client('s3',region_name='auto',endpoint_url=os.environ['CF_S3_API_ENDPOINT'],
                    aws_access_key_id=os.environ['CF_ACCESS_KEY_ID'],
                    aws_secret_access_key=os.environ['CF_SECRET_ACCESS_KEY'],
                    config=Config(retries={'total_max_attempts':1},request_checksum_calculation='when_required'))
    key='preflight/conditional-write'
    first=s3.put_object(Bucket=name,Key=key,Body=b'first',IfNoneMatch='*')
    for condition in ({'IfNoneMatch':'*'},{'IfMatch':'"not-current"'}):
        try:
            s3.put_object(Bucket=name,Key=key,Body=b'invalid',**condition)
        except s3.exceptions.ClientError as error:
            if error.response['ResponseMetadata']['HTTPStatusCode'] != 412:raise
        else:raise RuntimeError('R2 accepted a stale conditional write')
    s3.put_object(Bucket=name,Key=key,Body=b'second',IfMatch=first['ETag'])
    if s3.get_object(Bucket=name,Key=key)['Body'].read()!=b'second':
        raise RuntimeError('R2 conditional update read-back mismatch')
    meta['r2_conditional_preflight']='passed'
    (root/'deployment.json').write_text(json.dumps(meta,indent=2)+'\n')
    print(f'Prepared {name}; conditional R2 writes verified. Private files: {root}')


if __name__=='__main__':main()
