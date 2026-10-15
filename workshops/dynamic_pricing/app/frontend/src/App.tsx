import {useCallback,useEffect,useRef,useState} from 'react';
import {api,ApiError} from './api';
import type {Scope,SessionData,FreshnessData,BaselineData,AssessedCandidate,Snapshot,Receipt,TimelineEvent,DecisionData,DiagnosticsData} from './contracts';
import Baseline from './pages/Baseline';
import Freshness from './pages/Freshness';
import Candidates from './pages/Candidates';
import Diagnostics from './pages/Diagnostics';
import DecisionReport from './pages/DecisionReport';
import PriceTimeline from './components/PriceTimeline';
import Money from './components/Money';
const views=['Baseline','Freshness','Candidates','Diagnostics','Decision report'] as const;
type View=typeof views[number];

export default function App(){
  const [view,setView]=useState<View>('Baseline');
  const [session,setSession]=useState<SessionData|null>(null);
  const [scope,setScope]=useState<Scope|null>(null);
  const [baseline,setBaseline]=useState<BaselineData|null>(null);
  const [freshness,setFreshness]=useState<FreshnessData|null>(null);
  const [items,setItems]=useState<AssessedCandidate[]>([]);
  const [selectedId,setSelectedId]=useState<string|null>(null);
  const [snapshot,setSnapshot]=useState<Snapshot|null>(null);
  const [previous,setPrevious]=useState<Snapshot|null>(null);
  const [applied,setApplied]=useState<{price:number;version:number}|null>(null);
  const [events,setEvents]=useState<TimelineEvent[]>([]);
  const [receipt,setReceipt]=useState<Receipt|null>(null);
  const [decision,setDecision]=useState<DecisionData|null>(null);
  const [episode,setEpisode]=useState<Record<string,unknown>|null>(null);
  const [diagnostics,setDiagnostics]=useState<DiagnosticsData|null>(null);
  const [pending,setPending]=useState<string|null>(null);
  const [phase,setPhase]=useState('pending_review');
  const [error,setError]=useState<ApiError|null>(null);
  const [notice,setNotice]=useState('');
  const [note,setNote]=useState('');
  const [checkpoint,setCheckpoint]=useState('lab3_policy');
  const [lastSale,setLastSale]=useState<string|null>(null);
  const [resetElapsed,setResetElapsed]=useState(0);
  const [resetConfirmed,setResetConfirmed]=useState(false);
  const [resetMessage,setResetMessage]=useState('');
  const viewPanel=useRef<HTMLElement|null>(null);
  const generation=useRef(0),owner=useRef(''),version=useRef(0),busy=useRef(false);
  const sequence=useRef(0),acceptedSequence=useRef<Record<string,number>>({});
  const approvalKeys=useRef(new Map<string,string>()),resetKeys=useRef(new Map<string,string>());
  const acknowledgedEvents=useRef<TimelineEvent[]>([]);
  const currentSnapshot=useRef<Snapshot|null>(null);
  const saleRequest=useRef<{id:string;cutoff:string;version:number;generation:number}|null>(null);
  const accept=useCallback((incoming:Scope|null)=>{
    if(!incoming||incoming.owner_id!==owner.current)return false;
    if(generation.current===0)generation.current=incoming.generation;
    if(incoming.generation!==generation.current)return false;
    setScope(incoming);return true;
  },[]);
  const fail=useCallback((cause:unknown)=>setError(cause instanceof ApiError?cause:new ApiError('operation_failed','The workshop action failed. Refresh evidence and retry.')),[]);
  const refresh=useCallback(async()=>{
    const seq=++sequence.current;
    const current=(kind:string,incoming:Scope|null)=>{
      if(!accept(incoming)||seq<(acceptedSequence.current[kind]??0))return false;
      acceptedSequence.current[kind]=seq;return true;
    };
    await Promise.allSettled([
      api.getFreshness().then(r=>{
        if(!current('freshness',r.scope))return;
        setFreshness(r.data);
        if(r.data.applied_price_version>=version.current){version.current=r.data.applied_price_version;setApplied({price:r.data.current_price_minor,version:r.data.applied_price_version});}
      }).catch(fail),
      api.getBaseline().then(r=>{if(current('baseline',r.scope))setBaseline(r.data);}).catch(fail),
      api.listCandidates().then(r=>{
        if(!current('candidates',r.scope))return;
        setItems(r.candidates);
        setSelectedId(old=>r.candidates.some(c=>c.candidate.candidate_id===old)?old:r.candidates[0]?.candidate.candidate_id??null);
        if(r.snapshot){currentSnapshot.current=r.snapshot;setSnapshot(r.snapshot);}
      }).catch(fail),
      api.getTimeline().then(r=>{
        if(!current('timeline',r.scope))return;
        const merged=new Map<string,TimelineEvent>();
        [...r.data.events,...acknowledgedEvents.current].forEach(e=>merged.set(`${e.kind}/${e.evidence_id}`,e));
        setEvents([...merged.values()]);
      }).catch(fail),
    ]);
  },[accept,fail]);
  useEffect(()=>{if(!session||pending==='reset')return;const timer=setInterval(()=>void refresh(),2000);return()=>clearInterval(timer);},[session,refresh,pending]);
  useEffect(()=>{
    if(!session)return;
    viewPanel.current?.focus({preventScroll:true});
    viewPanel.current?.querySelector('h2')?.scrollIntoView({block:'nearest'});
  },[view,session]);
  useEffect(()=>{
    if(pending!=='reset')return;
    const started=Date.now();setResetElapsed(0);
    const timer=setInterval(()=>setResetElapsed(Math.floor((Date.now()-started)/1000)),1000);
    return()=>clearInterval(timer);
  },[pending]);
  async function start(){
    try{const r=await api.startSession();owner.current=r.data.owner_id;setSession(r.data);setError(null);await refresh();}catch(cause){fail(cause);}
  }
  async function action(kind:string,run:()=>Promise<void>){
    if(busy.current)return;busy.current=true;setPending(kind);setError(null);setNotice('');
    try{await run();}catch(cause){setPhase('failed');fail(cause);}finally{busy.current=false;setPending(null);}
  }
  const selected=items.find(c=>c.candidate.candidate_id===selectedId);
  async function propose(){await action('propose',async()=>{
    const before=currentSnapshot.current;const r=await api.propose(generation.current);if(!accept(r.scope))return;
    setPrevious(before);currentSnapshot.current=r.snapshot;setSnapshot(r.snapshot);setItems(r.candidates);
    setSelectedId(r.candidates[0]?.candidate.candidate_id??null);setPhase('pending_review');setReceipt(null);setDecision(null);setNote('');await refresh();
  });}
  async function approve(){
    if(!selected)return;const id=selected.candidate.candidate_id;
    const key=approvalKeys.current.get(id)??crypto.randomUUID();approvalKeys.current.set(id,key);
    await action('apply',async()=>{
      const r=await api.approve(id,key,generation.current);if(!accept(r.scope))return;
      version.current=r.receipt.applied_price_version;setApplied({price:r.receipt.price_minor,version:r.receipt.applied_price_version});
      setReceipt(r.receipt);setPhase('acknowledged');
      const event:TimelineEvent={kind:'applied',evidence_id:r.receipt.receipt_id,at:r.receipt.acknowledged_at,price_minor:r.receipt.price_minor,applied_price_version:r.receipt.applied_price_version,receipt_id:r.receipt.receipt_id};
      acknowledgedEvents.current.push(event);setEvents(old=>[...old.filter(e=>e.evidence_id!==event.evidence_id),event]);
      setNotice('Mock price acknowledged. Create the next sale to verify its price version.');
      try{const report=await api.getDecision(r.receipt.decision_id);if(accept(report.scope)&&report.data.decision_id===r.receipt.decision_id)setDecision(report.data);}catch{ /* Receipt remains authoritative if report loading fails. */ }
    });
  }
  async function reject(){if(!selected)return;await action('reject',async()=>{
    const r=await api.reject(selected.candidate.candidate_id,note,generation.current);if(!accept(r.scope))return;
    setPhase('rejected');setItems(old=>old.map(item=>item.candidate.candidate_id===selectedId?{...item,policy:{status:'rejected',reason_ids:['rejected_by_learner']}}:item));
    if(typeof r.data.decision_id==='string'){const report=await api.getDecision(r.data.decision_id);if(accept(report.scope))setDecision(report.data);}
  });}
  async function reset(){
    if(busy.current)return;
    setResetConfirmed(false);setResetMessage('');setResetElapsed(0);
    const key=resetKeys.current.get(checkpoint)??crypto.randomUUID();resetKeys.current.set(checkpoint,key);
    await action('reset',async()=>{
      const r=await api.reset(checkpoint,key,generation.current);if(!r.scope||r.scope.owner_id!==owner.current||r.scope.generation<generation.current)return;
      setResetConfirmed(true);
      generation.current=r.scope.generation;setScope(r.scope);version.current=0;acceptedSequence.current={};acknowledgedEvents.current=[];
      setItems([]);setSelectedId(null);setSnapshot(null);currentSnapshot.current=null;setPrevious(null);setReceipt(null);setDecision(null);setEvents([]);setFreshness(null);setApplied(null);setLastSale(null);setEpisode(null);setPhase('pending_review');approvalKeys.current.clear();resetKeys.current.delete(checkpoint);
      saleRequest.current=null;
      setNotice('Checkpoint restored. Earlier proposals cannot be applied in this generation.');await refresh();
      setResetMessage(`Restored ${checkpoint.replaceAll('_',' ')}. Generation ${r.scope.generation}.`);
    });
  }
  async function sale(){if(!freshness||!applied)return;await action('sale',async()=>{
    const request=saleRequest.current??{id:crypto.randomUUID(),cutoff:freshness.scenario_cutoff,version:applied.version,generation:generation.current};saleRequest.current=request;
    let r;
    try{r=await api.emitSale(request.cutoff,request.version,request.generation,request.id);}
    catch(cause){if(cause instanceof ApiError&&['price_version_changed','generation_changed'].includes(cause.code)){saleRequest.current=null;await refresh();}throw cause;}
    if(!accept(r.scope))return;saleRequest.current=null;
    setLastSale(r.data.event_id);setNotice(`Sale committed at version ${r.data.applied_price_version}. Wait for its CDC evidence before generating a fresh proposal.`);await refresh();
  });}
  async function runEpisode(mode:'live'|'replay'){await action('agent',async()=>{const r=await api.runEpisode(mode,generation.current);if(!accept(r.scope))return;setEpisode(r.data);});}
  async function inspect(){await action('diagnostics',async()=>{const r=await api.getDiagnostics();if(r.scope&&!accept(r.scope))return;setDiagnostics(r.data);});}
  async function trigger(event:string){await action('scenario',async()=>{const r=await api.trigger(event,generation.current);if(!accept(r.scope))return;setNotice(`${event.replaceAll('_',' ')} scenario recorded. Inspect source revisions, then generate a fresh proposal.`);await refresh();});}
  const hardBlocked=phase==='rejected'||['price_version_changed','policy_blocked','generation_changed'].includes(error?.code??'');
  return <><a className="skip-link" href="#workspace">Skip to workshop evidence</a><header className="app-header"><h1 className="brand">ClickHouse <span>Dynamic pricing workbench</span></h1><div className="session-meta">{session?<><span>{session.owner_id}</span><span data-testid="generation">Generation {scope?.generation??'not loaded'}</span><button className="text-button" onClick={start}>Restart learner session</button></>:<span>Your own instance</span>}</div></header>
    {!session?<main className="welcome" id="workspace"><h2 className="welcome-title">Follow a price change through the data</h2><p>Load historical facts, inspect live source revisions, compare price candidates, and approve a mock change yourself.</p><button className="primary" onClick={start}>Start learner session</button><p>Database credentials stay in your local backend. The workshop uses synthetic grocery transactions.</p>{error&&<p role="alert">{error.message}</p>}</main>:<div className="app-layout"><aside className="sidebar"><nav aria-label="Workshop evidence views">{views.map(name=><button key={name} aria-current={view===name?'page':undefined} onClick={()=>setView(name)}>{name}</button>)}</nav><div className="checkpoint"><label htmlFor="checkpoint">Recovery checkpoint</label><select id="checkpoint" disabled={!!pending} value={checkpoint} onChange={e=>setCheckpoint(e.target.value)}>{['lab1_baseline','lab2_features','lab3_policy','lab4_exception','lab5_review'].map(value=><option key={value} value={value}>{value.replaceAll('_',' ')}</option>)}</select><button onClick={reset} disabled={!!pending}>{pending==='reset'?'Restoring checkpoint...':'Restore checkpoint'}</button>
      {pending==='reset'?<div className="checkpoint-progress" role="status"><strong>{resetConfirmed?'Checkpoint restored. Refreshing the workbench...':`Restoring ${checkpoint.replaceAll('_',' ')}...`}</strong>{!resetConfirmed&&<p>Waiting for source records to reach ClickHouse and preparing exercise inputs.</p>}<span aria-live="off">{resetElapsed}s elapsed</span></div>:resetMessage&&<p className="checkpoint-result" role="status">{resetMessage}</p>}<p>Restores scenario state in a new generation. Historical imports remain.</p></div></aside>
    <main id="workspace" className="workspace"><div className="context-strip"><span>SKU 101 / Store 1 / SGD</span><span>{session.ingestion_mode==='clickpipes'?'ClickPipes ingestion':`${session.ingestion_mode.replaceAll('_',' ')} ingestion`}</span><span>{session.model_distribution==='development'?'Development model':'Prepared model'}</span></div>
      <div className="price-strip"><div><span className="price-label">Acknowledged mock price</span><strong data-testid="applied-price">{applied?<Money value={applied.price}/>:'Awaiting source state'}</strong><span>Applied version {applied?.version??'—'}</span></div><div className="proposed-price"><span className="price-label">Selected proposal</span><strong>{selected?<Money value={selected.candidate.price_minor}/>:'No proposal selected'}</strong><span>Projected current-window price</span></div><button className="refresh-button" onClick={()=>void refresh()}>Refresh evidence</button></div>
      {error&&<div className="alert" role="alert">{error.message}</div>}{notice&&<p className="notice" role="status">{notice}</p>}
      <section className="view-content" aria-label={`${view} view`} tabIndex={-1} ref={viewPanel}>
      {view==='Baseline'&&<Baseline data={baseline}/>} {view==='Freshness'&&<Freshness data={freshness} onSale={sale} pending={pending==='sale'} lastSale={lastSale}/>}
      {view==='Candidates'&&<Candidates items={items} selectedId={selectedId} onSelect={id=>{setSelectedId(id);setPhase('pending_review');setError(null);}} onPropose={propose} onApprove={approve} onReject={reject} pending={pending} phase={phase} receipt={receipt} note={note} setNote={setNote} previous={previous} current={snapshot} blocked={hardBlocked}/>}
      {view==='Diagnostics'&&<Diagnostics data={diagnostics} episode={episode} onEpisode={runEpisode} onInspect={inspect} onTrigger={trigger} pending={!!pending}/>}
      {view==='Decision report'&&<DecisionReport decision={decision} receipt={receipt}/>}
      </section>
      <PriceTimeline events={events}/>
    </main></div>}<footer className="app-footer">Synthetic projections and mock prices. Only an acknowledged receipt establishes an applied price.</footer></>;
}
