import type {DemandWindow} from '../retail-contracts';
const utc=(value:string)=>new Date(value).toISOString().replace('T',' ').replace('.000Z',' UTC');
const duration=(seconds:number)=>`${Math.floor(seconds/60)}:${String(seconds%60).padStart(2,'0')}`;
export default function DemandWindowProgress({window,writerConnected,streamState}:{window:DemandWindow;writerConnected?:boolean;streamState:string}){
 return <section className="retail-demand-window" role="region" aria-label="Demand slowdown progress">
  <h2>{window.ready?'Ready to find pricing opportunities':'Building the reduced-demand window'}</h2>
  <div className="retail-demand-times">
   <div><span>Selected at (wall-clock UTC)</span><time dateTime={window.requested_at}>{utc(window.requested_at)}</time></div>
   <div><span>Slowdown starts (business UTC)</span><time dateTime={window.started_at}>{utc(window.started_at)}</time></div>
   <div><span>Full-window target (business UTC)</span><time dateTime={window.ready_at}>{utc(window.ready_at)}</time></div>
  </div>
  <progress aria-label="Reduced-demand window coverage" max={900} value={window.covered_seconds}/>
  <p role="status">{duration(window.covered_seconds)} of 15:00 covered · {duration(window.remaining_seconds)} business time remaining.</p>
  {window.ready?<p>The complete 15-minute window is visible in ClickHouse. Continue with Module 03, Step 2.</p>:
   writerConnected===false?<p>The sales writer is not connected. Start it again to continue; completed progress is retained.</p>:
   streamState!=='running'?<p>The sales stream is paused. Select <strong>Resume sales stream</strong> to continue; progress is retained.</p>:
   window.remaining_seconds===0?<p>The window is complete. Waiting for fresh ClickHouse evidence before marking it ready.</p>:
   <p>The writer temporarily targets <strong>20×</strong> business-clock speed: 15 business minutes target about 45 elapsed seconds of pacing. Generation and delivery can take longer. Progress advances only when complete data reaches ClickHouse.</p>}
 </section>;
}
