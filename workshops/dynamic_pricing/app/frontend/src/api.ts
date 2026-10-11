import type {Envelope,SessionData,BaselineData,FreshnessData,CandidateEnvelope,ApplicationEnvelope,DecisionData,TimelineData,DiagnosticsData} from './contracts';
export class ApiError extends Error {
  constructor(public code:string,public action:string,public reasonIds:string[]=[]){super(reasonIds.length ? `${action} Reasons: ${reasonIds.join(", ")}.` : action);}
}
export async function request<T>(path:string,options:RequestInit={},generation?:number):Promise<T>{
  const headers=new Headers(options.headers);
  if(options.body)headers.set('Content-Type','application/json');
  if(generation!==undefined)headers.set('X-Pricing-Generation',String(generation));
  let response:Response;
  try {response=await fetch(`/api${path}`,{...options,headers,credentials:'same-origin'});}catch{throw new ApiError('connection_unavailable','The application connection failed. Check the backend and retry.');}
  if(!response.headers.get('content-type')?.includes('application/json'))throw new ApiError('response_unavailable','The application response could not be read. Check the backend and retry.');
  const payload=await response.json();
  if(!response.ok)throw new ApiError(payload.error?.code??'request_rejected',payload.error?.action??'Refresh the current inputs and retry.',payload.error?.reason_ids??[]);
  return payload as T;
}
const post=<T,>(path:string,body:unknown,generation:number)=>request<T>(path,{method:'POST',body:JSON.stringify(body)},generation);
export const api={
  startSession:()=>request<Envelope<SessionData>>('/session',{method:'POST',body:'{}'}),
  getBaseline:()=>request<Envelope<BaselineData>>('/baseline'),
  getFreshness:()=>request<Envelope<FreshnessData>>('/freshness'),
  getTimeline:()=>request<Envelope<TimelineData>>('/timeline'),
  listCandidates:()=>request<CandidateEnvelope>('/candidates'),
  propose:(generation:number)=>post<CandidateEnvelope>('/candidates',{subject:{sku:101,store_id:1,channel:'store'}},generation),
  approve:(candidateId:string,idempotencyKey:string,generation:number)=>post<ApplicationEnvelope>(`/candidates/${candidateId}/approve`,{idempotency_key:idempotencyKey},generation),
  reject:(candidateId:string,note:string,generation:number)=>post<Envelope<Record<string,unknown>>>(`/candidates/${candidateId}/reject`,{note},generation),
  reset:(checkpoint:string,idempotencyKey:string,generation:number)=>post<Envelope<{checkpoint:string}>>('/reset',{checkpoint,idempotency_key:idempotencyKey},generation),
  trigger:(event:string,generation:number)=>post<Envelope<Record<string,unknown>>>('/scenarios',{event,event_id:crypto.randomUUID()},generation),
  emitSale:(occurredAt:string,version:number,generation:number,eventId:string)=>post<Envelope<{event_id:string;unit_price_minor:number;applied_price_version:number}>>('/sales',{event_id:eventId,subject:{sku:101,store_id:1,channel:'store'},quantity:1,occurred_at:occurredAt,expected_price_version:version},generation),
  getDecision:(decisionId:string)=>request<Envelope<DecisionData>>(`/decisions/${decisionId}`),
  exportReport:(decisionId:string,format:'json'|'markdown'='json')=>`/api/reports/${decisionId}?format=${format}`,
  runEpisode:(mode:'live'|'replay',generation:number)=>post<Envelope<Record<string,unknown>>>('/agent/episodes',{mode,subject:{sku:101,store_id:1,channel:'store'}},generation),
  getDiagnostics:()=>request<Envelope<DiagnosticsData>>('/readiness?module=diagnostics'),
};

export const startSession=api.startSession;
