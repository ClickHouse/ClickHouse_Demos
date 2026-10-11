"""Same-origin local owner sessions. Secrets stay in the backend."""
import hashlib
import hmac
import secrets
import time
from urllib.parse import urlsplit
from fastapi import HTTPException,Request
from starlette.responses import JSONResponse

COOKIE='pricing_session'


def check_origin(request,settings):
    origin=settings.allowed_origin
    if request.headers.get('host')!=urlsplit(origin).netloc or request.headers.get('origin')!=origin:
        raise HTTPException(403,detail={'code':'origin_forbidden','action':'Open the workbench at the configured localhost URL.'})


def issue_session(settings):
    secret=settings.session_secret.get_secret_value()
    if len(secret)<32: raise HTTPException(503,detail={'code':'session_not_configured','action':'Generate the local session secret during setup.'})
    value=f'{settings.owner_id}.{int(time.time())}.{secrets.token_hex(16)}'
    return value+'.'+hmac.new(secret.encode(),value.encode(),hashlib.sha256).hexdigest()


def require_session(request,settings):
    token=request.cookies.get(COOKIE,'')
    try:
        owner,issued,nonce,signature=token.split('.')
        secret=settings.session_secret.get_secret_value()
        value=f'{owner}.{issued}.{nonce}'
        expected=hmac.new(secret.encode(),value.encode(),hashlib.sha256).hexdigest()
        age=time.time()-int(issued)
        valid=len(secret)>=32 and owner==settings.owner_id and 0<=age<8*3600 and hmac.compare_digest(signature,expected)
    except (ValueError,TypeError): valid=False
    if not valid: raise HTTPException(401,detail={'code':'session_required','action':'Start a learner session in this workbench.'})


class BodyLimit:
    def __init__(self,app,max_bytes=16384): self.app,self.max_bytes=app,max_bytes
    async def __call__(self,scope,receive,send):
        if scope['type']!='http' or scope['method'] in ('GET','HEAD','OPTIONS'):
            return await self.app(scope,receive,send)
        messages=[];size=0
        while True:
            message=await receive()
            if message['type']=='http.disconnect': return
            size+=len(message.get('body',b''))
            if size>self.max_bytes:
                response=JSONResponse({'error':{'code':'request_too_large','action':'Send a workshop action smaller than 16 KB.'}},status_code=413)
                return await response(scope,receive,send)
            messages.append(message)
            if not message.get('more_body',False): break
        async def replay(): return messages.pop(0) if messages else await receive()
        await self.app(scope,replay,send)
