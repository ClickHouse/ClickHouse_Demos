import {useEffect,useRef,useState} from 'react';
import {ApiError} from './api';
import {retailApi,money} from './retail-api';
import type {RetailContext,Overview,Opportunity,Subject,Comparison,PriceRow,RetailReceipt,Outcome} from './retail-contracts';
import RetailOverview from './pages/RetailOverview';
import RetailOpportunities from './pages/RetailOpportunities';
import RetailComparison from './pages/RetailComparison';
import RetailOutcomes from './pages/RetailOutcomes';
const views=['Overview','Opportunities','Compare prices','Observed results'] as const;
type View=typeof views[number];
export default function RetailApp(){
 const [started,setStarted]=useState(false),[view,setView]=useState<View>('Overview');
 const [district,setDistrict]=useState(1),[context,setContext]=useState<RetailContext|null>(null);
 const [overview,setOverview]=useState<Overview|null>(null),[opportunities,setOpportunities]=useState<Opportunity[]|null>(null);
 const [comparison,setComparison]=useState<Comparison|null>(null),[selected,setSelected]=useState<PriceRow|null>(null);
 const [proposal,setProposal]=useState<string|null>(null),[receipt,setReceipt]=useState<RetailReceipt|null>(null);
 const [outcomes,setOutcomes]=useState<Outcome[]|null>(null),[pending,setPending]=useState<string|null>(null);
 const [error,setError]=useState(''),[notice,setNotice]=useState(''),[refreshKey,setRefreshKey]=useState(0);
 const busy=useRef(false),keys=useRef(new Map<string,string>()),sequence=useRef(0);
 function failed(cause:unknown){setError(cause instanceof Error?cause.message:'The request failed. Retry this action.');if(cause instanceof ApiError&&cause.code==='proposal_changed'){setProposal(null);setSelected(null);}}
 async function action(name:string,fn:()=>Promise<void>){if(busy.current)return;busy.current=true;setPending(name);setError('');try{await fn();}catch(cause){failed(cause);}finally{busy.current=false;setPending(null);}}
 useEffect(()=>{
  if(!started)return;
  const controller=new AbortController(),token=++sequence.current;let loading=false;
  const active=()=>!controller.signal.aborted&&token===sequence.current;
  async function refresh(){
   if(loading)return;loading=true;
   try{
    const c=await retailApi.context(controller.signal);if(!active())return;setContext(c.data);
    if(view==='Overview'){const r=await retailApi.overview(0,controller.signal);if(active())setOverview(r.data);}
    if(view==='Opportunities'){const r=await retailApi.opportunities(district,c.data.cutoff,controller.signal);if(active())setOpportunities(r.data);}
    if(view==='Observed results'&&receipt){const r=await retailApi.outcomes(receipt.receipt_id,controller.signal);if(active())setOutcomes(r.data);}
   }catch(cause){if(active())failed(cause);}finally{loading=false;}
  }
  void refresh();const timer=setInterval(()=>void refresh(),3000);
  return()=>{controller.abort();clearInterval(timer);};
 },[started,view,district,receipt,refreshKey]);
 async function compare(row:Subject){if(!context)return;await action('compare',async()=>{setSelected(null);setProposal(null);const latest=await retailApi.context();setContext(latest.data);const r=await retailApi.compare({store_id:row.store_id,sku:row.sku},latest.data.cutoff);setComparison(r.data);setSelected(null);setProposal(null);setView('Compare prices');setNotice('Price comparison ready. Review a permitted price.');});}
 async function review(row:PriceRow){if(!comparison)return;setSelected(row);setProposal(null);await action('review',async()=>{const r=await retailApi.propose(comparison.inputs.subject,comparison.inputs.cutoff,row.price_minor);setProposal(r.data.proposal_id);setNotice(`Review ${money(row.price_minor)} before applying it.`);});}
 async function apply(){if(!proposal)return;await action('apply',async()=>{const key=keys.current.get(proposal)??crypto.randomUUID();keys.current.set(proposal,key);const r=await retailApi.apply(proposal,key);setReceipt(r.data);setProposal(null);setNotice(`Price applied: ${money(r.data.price_minor)}. Open Observed results to follow the next sales.`);});}
 async function control(command:'start'|'stop'|'slow_district'){await action('stream',async()=>{await retailApi.stream(command,district);setNotice(command==='slow_district'?`District ${district} demand reduced to 50%. Watch the next completed sales windows.`:`Sales stream ${command==='start'?'resume':'pause'} requested.`);setRefreshKey(v=>v+1);});}
 const progress:Record<string,string>={session:'Starting learner session…',compare:'Running the price comparison in ClickHouse…',review:'Preparing the selected price for review…',apply:'Applying the reviewed price…',stream:'Updating sales stream control…'};
 return <div className="retail-app"><header className="retail-header"><strong>ClickHouse</strong><span>Retail pricing lab</span><span className="retail-badge">Synthetic retail chain</span></header>
 {!started?<main className="retail-welcome"><p className="retail-eyebrow">REAL-TIME ANALYTICS → PRICING DECISIONS</p><h1>Find the opportunity.<br/>Measure the price change.</h1><p>Explore chain-wide sales, identify excess stock, compare prices with ClickHouse SQL, and approve a price yourself.</p><button className="primary" disabled={!!pending} onClick={()=>void action('session',async()=>{await retailApi.session();setStarted(true);})}>{pending?'Starting learner session…':'Start learner session'}</button>{error&&<p role="alert">{error}</p>}</main>:
 <div className="retail-layout"><nav aria-label="Retail workshop views">{views.map(name=><button key={name} disabled={!!pending} aria-current={view===name?'page':undefined} onClick={()=>{setView(name);setError('');}}>{name}</button>)}<a href="https://ai.clickhouse.cloud/" target="_blank" rel="noreferrer">Open ClickHouse Agents ↗</a><p>Launch your Retail Pricing Strategist from ClickHouse Cloud.</p></nav>
 <main className="retail-main"><div className="retail-toolbar"><label>Opportunity district<select value={district} disabled={!!pending} onChange={e=>{setDistrict(Number(e.target.value));setOpportunities(null);setComparison(null);setSelected(null);setProposal(null);if(view==='Compare prices')setView('Opportunities');}}>{Array.from({length:20},(_,i)=><option key={i+1} value={i+1}>District {i+1}</option>)}</select></label><div><span>Live business time (UTC)</span><strong>{context?.cutoff.replace('T',' ').replace('Z','')??'Reading context…'}</strong></div><div><span>Delivery age</span><strong>{context?.delivery_age_seconds==null?'No completed live batch':`${context.delivery_age_seconds} seconds`}</strong></div><button disabled={!!pending} onClick={()=>view==='Compare prices'&&comparison?void compare(comparison.inputs.subject):setRefreshKey(v=>v+1)}>Refresh evidence</button></div>
 <p className="retail-status" role="status" aria-live="polite">{pending?progress[pending]:notice||'Live evidence refreshes every 3 seconds for this view.'}</p>
 {error&&<div className="retail-error" role="alert"><p>{error}</p>{comparison&&<button disabled={!!pending} onClick={()=>void compare(comparison.inputs.subject)}>Refresh comparison</button>}</div>}
 {view==='Overview'&&<RetailOverview data={overview}/>}
 {view==='Opportunities'&&<><div className="retail-actions"><button disabled={!!pending||!context?.analysis_ready} onClick={()=>void control('slow_district')}>Reduce district demand by 50%</button><button disabled={!!pending||context?.writer_connected===false} onClick={()=>void control(context?.stream_state==='running'?'stop':'start')}>{context?.stream_state==='running'?'Pause sales stream':'Resume sales stream'}</button></div><RetailOpportunities rows={opportunities} onCompare={r=>void compare(r)} pending={!!pending}/></>}
 {view==='Compare prices'&&<RetailComparison data={comparison} selected={selected} reviewed={!!proposal} pending={pending} onReview={r=>void review(r)} onApply={()=>void apply()}/>}
 {view==='Observed results'&&<RetailOutcomes rows={outcomes} receipt={receipt}/>}
 </main></div>}
 </div>;
}
