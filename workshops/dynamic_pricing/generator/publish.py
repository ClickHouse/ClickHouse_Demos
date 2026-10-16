"""Publish verified immutable bytes; never guess an account or overwrite a key."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
from urllib.parse import quote
import boto3
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config
from botocore.exceptions import ClientError
from .manifest import read_manifest,checked_path

class PublishConflict(RuntimeError):
    pass


def destination(bucket,prefix):
    prefix=prefix.strip('/')
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]',bucket) or '..' in bucket:
        raise ValueError('Use an explicit general-purpose S3 bucket name.')
    if not prefix or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_./-]*',prefix) or '..' in prefix.split('/'):
        raise ValueError('Use an explicit dataset prefix without traversal or wildcards.')
    return prefix


def read_policy(bucket,prefix):
    prefix=destination(bucket,prefix)
    return {'Version':'2012-10-17','Statement':[{'Sid':'ReadSyntheticPricingDataset','Effect':'Allow','Principal':'*',
            'Action':['s3:GetObject'],'Resource':f'arn:aws:s3:::{bucket}/{prefix}/*'}]}


def _head(client,bucket,key):
    try: return client.head_object(Bucket=bucket,Key=key,ChecksumMode='ENABLED')
    except ClientError as error:
        if error.response['Error']['Code'] in ('404','NoSuchKey','NotFound'): return None
        raise


def _verify(head,file):
    expected=base64.b64encode(bytes.fromhex(file['sha256'])).decode()
    if not head or head.get('ContentLength')!=file['bytes'] or head.get('ChecksumSHA256')!=expected:
        raise PublishConflict('Existing or uploaded bytes do not match the service-validated checksum and size. Choose a new versioned prefix; no object was overwritten.')


def publish_dataset(manifest_path,bucket,prefix,client,*,workers=1,known_keys=None):
    if type(workers) is not int or not 1<=workers<=16:
        raise ValueError('Use between 1 and 16 upload workers.')
    known_keys=frozenset(known_keys) if known_keys is not None else None
    prefix=destination(bucket,prefix);path=Path(manifest_path)
    manifest=read_manifest(path)
    files=manifest['files']+manifest['companion_files']
    files=files+[{'key':'manifest.json','sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':path.stat().st_size}]
    region=client.meta.region_name
    base=f'https://s3.{region}.amazonaws.com/{bucket}/{quote(prefix,safe="/")}/'
    def upload(file):
        uploaded=0
        key=prefix+'/'+file['key']
        head=_head(client,bucket,key) if known_keys is None or key in known_keys else None
        if head is None:
            checksum=base64.b64encode(bytes.fromhex(file['sha256'])).decode()
            try:
                source=path if file['key']=='manifest.json' else checked_path(path.parent,file['key'])
                with source.open('rb') as body:
                    client.put_object(Bucket=bucket,Key=key,Body=body,ContentLength=file['bytes'],ChecksumSHA256=checksum,
                      Metadata={'sha256':file['sha256']},IfNoneMatch='*',
                      ContentType='application/json' if file['key']=='manifest.json' else 'application/vnd.apache.parquet')
                uploaded+=1
            except ClientError as error:
                if error.response['Error']['Code'] not in ('PreconditionFailed','ConditionalRequestConflict','412','409'): raise
            head=_head(client,bucket,key)
        _verify(head,file)
        return uploaded,base+quote(file['key'],safe='/')
    # All data objects must finish verification before touching the manifest.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        completed=list(pool.map(upload,files[:-1]))
    completed.append(upload(files[-1]))
    uploaded=sum(result[0] for result in completed)
    urls=[result[1] for result in completed]
    return {'manifest_verified':True,'learner_download_verified':False,'manifest_uri':urls[-1],
            'manifest_sha256':files[-1]['sha256'],'uploaded_objects':uploaded,'rows':manifest['rows'],
            'object_urls':urls,'bucket':bucket,'prefix':prefix,'region':region}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--manifest',type=Path,required=True)
    parser.add_argument('--bucket',default=os.environ.get('PRICING_DATASET_BUCKET',''))
    parser.add_argument('--prefix',default=os.environ.get('PRICING_DATASET_PREFIX',''))
    parser.add_argument('--profile',default=os.environ.get('AWS_PROFILE'))
    parser.add_argument('--region',default=os.environ.get('AWS_REGION'))
    parser.add_argument('--workers',type=int,default=8)
    parser.add_argument('--list-existing',action='store_true',help='Use a prefix listing to skip HEAD requests for absent keys; requires ListBucket.')
    parser.add_argument('--verify-anonymous',action='store_true');args=parser.parse_args()
    try:
        if not args.region: raise ValueError('Set the explicit AWS region.')
        session=boto3.Session(profile_name=args.profile,region_name=args.region)
        client=session.client('s3',config=Config(max_pool_connections=16,connect_timeout=10,read_timeout=60,retries={'mode':'standard','max_attempts':8},tcp_keepalive=True))
        prefix=destination(args.bucket,args.prefix)
        keys=None
        if args.list_existing:
            pages=client.get_paginator('list_objects_v2').paginate(Bucket=args.bucket,Prefix=prefix+'/')
            keys={row['Key'] for page in pages for row in page.get('Contents',[])}
        result=publish_dataset(args.manifest,args.bucket,prefix,client,workers=args.workers,known_keys=keys)
        if args.verify_anonymous:
            from pricing_lab.history import resolve_manifest
            local=read_manifest(args.manifest)
            remote,_=resolve_manifest(result['manifest_uri'],expected_version=local['dataset_version'])
            assert remote==local
            result['learner_download_verified']=True
        print(json.dumps(result,indent=2));return 0
    except Exception:
        print('publication_failed: Check the explicit bucket/profile, immutable checksums, and learner download access. Existing keys are preserved; raw credential-bearing errors are omitted.')
        return 2

if __name__=='__main__': raise SystemExit(main())
