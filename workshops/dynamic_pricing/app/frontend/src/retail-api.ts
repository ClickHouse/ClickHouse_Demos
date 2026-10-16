import {request} from './api';
import type {RetailEnvelope,RetailContext,Overview,Opportunity,Comparison,Subject,RetailReceipt,Outcome} from './retail-contracts';
const get=<T,>(path:string,signal?:AbortSignal)=>request<RetailEnvelope<T>>(`/retail/${path}`,{signal});
const post=<T,>(path:string,body:unknown)=>request<RetailEnvelope<T>>(`/retail/${path}`,{method:'POST',body:JSON.stringify(body)});
export const retailApi={
 session:()=>request('/session',{method:'POST',body:'{}'}),
 context:(signal?:AbortSignal)=>get<RetailContext>('context',signal),
 overview:(district:number,signal?:AbortSignal)=>get<Overview>(`overview?district=${district}`,signal),
 opportunities:(district:number,cutoff:string,signal?:AbortSignal)=>get<Opportunity[]>(`opportunities?district=${district}&cutoff=${encodeURIComponent(cutoff)}`,signal),
 compare:(subject:Subject,cutoff:string)=>post<Comparison>('compare',{subject,cutoff}),
 propose:(subject:Subject,cutoff:string,price_minor:number)=>post<{proposal_id:string}>('proposals',{subject,cutoff,price_minor}),
 apply:(proposal_id:string,idempotency_key:string)=>post<RetailReceipt>('apply',{proposal_id,idempotency_key}),
 outcomes:(receipt:string,signal?:AbortSignal)=>get<Outcome[]>(`outcomes?receipt=${receipt}`,signal),
 stream:(action:'start'|'stop'|'slow_district',district:number,rate=.5)=>post<{status:string}>('stream',{action,district,rate}),
};
export const money=(value:number|string)=>new Intl.NumberFormat('en-SG',{style:'currency',currency:'SGD',currencyDisplay:'code'}).format(Number(value)/100);
