import base64
import hashlib
from pathlib import Path
import boto3
from botocore.stub import Stubber,ANY
import pytest
from generator.generate import generate_dataset
from generator.manifest import read_manifest
from generator.publish import publish_dataset,PublishConflict,read_policy

BUCKET='synthetic-workshop-test'
PREFIX='dynamic-pricing/retail-v1/test'

@pytest.fixture
def dataset(tmp_path):
    path=generate_dataset(tmp_path,1001,17,'test')
    manifest=read_manifest(path)
    files=manifest['files']+manifest['companion_files']
    return path,files+[{'key':'manifest.json','sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':path.stat().st_size}]

@pytest.fixture
def s3():
    return boto3.client('s3',region_name='ap-southeast-1',aws_access_key_id='test',aws_secret_access_key='test')

def head(file):
    return {'ContentLength':file['bytes'],'ChecksumSHA256':base64.b64encode(bytes.fromhex(file['sha256'])).decode(),'Metadata':{'sha256':file['sha256']}}

def existing(stub,file,checksum=None):
    response=head(file)
    if checksum: response['ChecksumSHA256']=checksum
    stub.add_response('head_object',response,{'Bucket':BUCKET,'Key':PREFIX+'/'+file['key'],'ChecksumMode':'ENABLED'})

def test_identical_upload_reuses_verified_immutable_objects(dataset,s3):
    path,files=dataset
    with Stubber(s3) as stub:
        for file in files: existing(stub,file)
        result=publish_dataset(path,BUCKET,PREFIX,s3)
        assert result['manifest_verified'] and result['uploaded_objects']==0
        assert result['manifest_uri'].endswith('/manifest.json')
        stub.assert_no_pending_responses()

def test_existing_conflicting_checksum_is_never_overwritten(dataset,s3):
    path,files=dataset
    with Stubber(s3) as stub:
        existing(stub,files[0],base64.b64encode(b'x'*32).decode())
        with pytest.raises(PublishConflict): publish_dataset(path,BUCKET,PREFIX,s3)
        stub.assert_no_pending_responses()

def test_manifest_is_uploaded_only_after_all_object_verification(dataset,s3):
    path,files=dataset
    path=path.rename(path.with_name('reviewed-manifest.json'))
    with Stubber(s3) as stub:
        for file in files:
            params={'Bucket':BUCKET,'Key':PREFIX+'/'+file['key'],'ChecksumMode':'ENABLED'}
            stub.add_client_error('head_object','404',http_status_code=404,expected_params=params)
            stub.add_response('put_object',{}, {'Bucket':BUCKET,'Key':PREFIX+'/'+file['key'],'Body':ANY,'ContentLength':file['bytes'],'ChecksumSHA256':head(file)['ChecksumSHA256'],'Metadata':head(file)['Metadata'],'IfNoneMatch':'*','ContentType':'application/json' if file['key']=='manifest.json' else 'application/vnd.apache.parquet'})
            existing(stub,file)
        result=publish_dataset(path,BUCKET,PREFIX,s3)
        assert result['uploaded_objects']==len(files)
        stub.assert_no_pending_responses()

def test_read_policy_only_grants_object_reads_in_explicit_prefix():
    policy=read_policy(BUCKET,PREFIX)
    statement=policy['Statement'][0]
    assert statement['Action']==['s3:GetObject']
    assert statement['Resource']==f'arn:aws:s3:::{BUCKET}/{PREFIX}/*'
    for prefix in ('','/','../other','path/*'):
        with pytest.raises(ValueError): read_policy(BUCKET,prefix)


def test_parallel_publication_verifies_all_files_before_manifest(dataset):
    from types import SimpleNamespace
    from threading import Barrier,Lock
    path,files=dataset
    class Client:
        meta=SimpleNamespace(region_name='ap-southeast-1')
        def __init__(self): self.seen=set();self.lock=Lock();self.barrier=Barrier(2)
        def head_object(self,**params):
            key=params['Key'].removeprefix(PREFIX+'/')
            if key=='manifest.json': assert self.seen=={f['key'] for f in files[:-1]}
            else:
                if key in {f['key'] for f in files[:2]}: self.barrier.wait(timeout=5)
                with self.lock: self.seen.add(key)
            return head(next(f for f in files if f['key']==key))
    result=publish_dataset(path,BUCKET,PREFIX,Client(),workers=2)
    assert result['uploaded_objects']==0 and result['manifest_verified']


def test_parallel_failure_never_publishes_manifest(dataset):
    from types import SimpleNamespace
    path,files=dataset
    class Client:
        meta=SimpleNamespace(region_name='ap-southeast-1')
        manifest_seen=False
        def head_object(self,**params):
            key=params['Key'].removeprefix(PREFIX+'/')
            if key=='manifest.json': self.manifest_seen=True
            response=head(next(f for f in files if f['key']==key))
            if key==files[0]['key']: response['ChecksumSHA256']='wrong'
            return response
    client=Client()
    with pytest.raises(PublishConflict): publish_dataset(path,BUCKET,PREFIX,client,workers=2)
    assert not client.manifest_seen


@pytest.mark.parametrize('conflict',[False,True])
def test_stale_inventory_hint_still_verifies_conditional_write_races(dataset,conflict):
    from types import SimpleNamespace
    from botocore.exceptions import ClientError
    path,files=dataset
    class Client:
        meta=SimpleNamespace(region_name='ap-southeast-1')
        touched=[]
        def put_object(self,**params):
            assert params['IfNoneMatch']=='*'
            self.touched.append(params['Key'])
            raise ClientError({'Error':{'Code':'PreconditionFailed'},'ResponseMetadata':{'HTTPStatusCode':412}},'PutObject')
        def head_object(self,**params):
            file=next(f for f in files if PREFIX+'/'+f['key']==params['Key']);response=head(file)
            if conflict and file==files[0]:response['ChecksumSHA256']='wrong'
            return response
    client=Client()
    if conflict:
        with pytest.raises(PublishConflict):publish_dataset(path,BUCKET,PREFIX,client,known_keys=set())
        assert PREFIX+'/manifest.json' not in client.touched
    else:
        result=publish_dataset(path,BUCKET,PREFIX,client,known_keys=set())
        assert result['uploaded_objects']==0 and result['manifest_verified']
