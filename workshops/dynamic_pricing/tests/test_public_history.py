import pytest
from types import SimpleNamespace
from uuid import uuid4
from pricing_lab.history import HistoryLoader


def test_public_s3_loading_explicitly_ignores_server_credentials():
    calls=[]
    analytics=SimpleNamespace(database='pricing',command=lambda sql,params:calls.append((sql,params)))
    HistoryLoader(analytics,None)._insert_file('https://public.example.test/manifest.json',{'key':'facts.parquet'},{'raw_table':'hist_test','sandbox_id':'learner/grocery','manifest':{'dataset_version':'retail-v1'}},uuid4())
    sql,params=calls[0]
    assert "s3({url:String}, NOSIGN, 'Parquet')" in sql
    assert params['url']=='https://public.example.test/facts.parquet'


def test_public_reads_retry_a_reset_during_body_consumption(monkeypatch):
    import io,json
    import pricing_lab.history as history
    calls=[]
    class Response(io.BytesIO):
        def __init__(self,fail):super().__init__(b'{"ok":true}');self.fail=fail
        def read(self,*args):
            if self.fail: raise ConnectionResetError('test transport reset')
            return super().read(*args)
    def open_url(*args,**kwargs):
        calls.append(1);return Response(len(calls)==1)
    monkeypatch.setattr(history,'urlopen',open_url)
    monkeypatch.setattr(history.time,'sleep',lambda seconds:None)
    assert history.public_read('https://public.example.test/manifest.json',json.load)=={'ok':True}
    assert len(calls)==2


def test_public_read_retries_are_bounded_and_access_denial_is_immediate(monkeypatch):
    from urllib.error import HTTPError
    import pytest
    import pricing_lab.history as history
    calls=[]
    def broken(*args,**kwargs): calls.append(1);raise ConnectionResetError('test transport reset')
    monkeypatch.setattr(history,'urlopen',broken);monkeypatch.setattr(history.time,'sleep',lambda seconds:None)
    with pytest.raises(ConnectionResetError):history.public_read('https://public.example.test/file',lambda r:r.read())
    assert len(calls)==4
    calls.clear()
    def forbidden(*args,**kwargs):calls.append(1);raise HTTPError('https://public.example.test/file',403,'Forbidden',{},None)
    monkeypatch.setattr(history,'urlopen',forbidden)
    with pytest.raises(HTTPError):history.public_read('https://public.example.test/file',lambda r:r.read())
    assert len(calls)==1


@pytest.mark.parametrize('wrong_checksum',[False,True])
def test_remote_manifest_checks_run_concurrently_and_reject_bad_bytes(monkeypatch,wrong_checksum):
    import base64,hashlib,io,json
    from threading import Barrier
    from urllib.request import Request
    import pricing_lab.history as history
    digest=hashlib.sha256(b'a').hexdigest()
    manifest={'dataset_version':'retail-v1','source_namespace':'history-v1','currency':'SGD','rows':2,'files':[{'key':key,'rows':1,'bytes':1,'sha256':digest} for key in ('a.parquet','b.parquet')],'companion_files':[]}
    barrier=Barrier(2)
    def opened(request,**kwargs):
        if not isinstance(request,Request):return io.BytesIO(json.dumps(manifest).encode())
        barrier.wait(timeout=2)
        result=io.BytesIO(b'');result.headers={'Content-Length':'1','x-amz-checksum-sha256':base64.b64encode(bytes.fromhex(digest)).decode()}
        if wrong_checksum and request.full_url.endswith('b.parquet'):result.headers['x-amz-checksum-sha256']='wrong'
        return result
    monkeypatch.setattr(history,'urlopen',opened)
    if wrong_checksum:
        with pytest.raises(ValueError,match='checksum'):history.resolve_manifest('https://public.example.test/manifest.json')
    else:
        assert history.resolve_manifest('https://public.example.test/manifest.json')[0]==manifest
