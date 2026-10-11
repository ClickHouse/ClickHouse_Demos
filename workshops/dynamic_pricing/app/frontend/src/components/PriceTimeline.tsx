import type {TimelineEvent} from '../contracts';
import Money from './Money';
export default function PriceTimeline({events}:{events:TimelineEvent[]}){
  const sorted=[...events].sort((a,b)=>a.at.localeCompare(b.at));
  const prices=sorted.map(e=>e.price_minor);
  const minimum=Math.min(...prices,500)-50,maximum=Math.max(...prices,800)+50;
  const dates=sorted.map(e=>Date.parse(e.at));
  const first=Math.min(...dates),last=Math.max(...dates);
  const x=(e:TimelineEvent)=>50+(Date.parse(e.at)-first)/Math.max(last-first,1000)*790;
  const y=(e:TimelineEvent)=>190-(e.price_minor-minimum)/(maximum-minimum)*135;
  const applied=sorted.filter(e=>e.kind==='applied');
  const series=applied.flatMap((e,i)=>i?[`${x(e)},${y(applied[i-1])}`,`${x(e)},${y(e)}`]:[`${x(e)},${y(e)}`]).join(' ');
  return <section className="timeline" aria-labelledby="timeline-title">
    <div className="section-heading"><h2 id="timeline-title">Price and evidence timeline</h2><span>Evidence time in UTC</span></div>
    <div className="legend"><span className="applied-key">Applied price</span><span className="proposed-key">Price proposals (alternatives)</span><span>Sales and CDC markers</span></div>
    {sorted.length?<svg viewBox="0 0 900 230" role="img" aria-label="Proposed and applied mock price timeline">
      <title>Proposed and applied mock price timeline</title>
      <desc>Solid teal points show acknowledged prices. Outlined plum points show alternative proposals at each snapshot. Sales and CDC markers link to evidence IDs in the table.</desc>
      {[0,1,2].map(i=><g key={i}><line x1="50" x2="840" y1={55+i*67.5} y2={55+i*67.5} className="chart-grid"/><text x="3" y={59+i*67.5}>{((maximum-(maximum-minimum)*i/2)/100).toFixed(2)}</text></g>)}
      <polyline points={series} className="applied-line"/>
      {sorted.map((e,index)=><g key={`${e.kind}-${e.evidence_id}`}>
        {e.kind==='applied'?<circle cx={x(e)} cy={y(e)} r="6" className="applied-point"/>:e.kind==='proposed'?<circle cx={x(e)} cy={y(e)} r="6" className="proposed-point"/>:<path d={`M ${x(e)} ${y(e)-6} l 6 6 -6 6 -6 -6 z`} className="evidence-point"/>}
        <title>{e.kind}: {e.evidence_id}, {e.price_minor} minor units, {e.at}</title>
        {index===0||index===sorted.length-1?<text x={x(e)} y="215" textAnchor={index===0?'start':'end'}>{e.at.slice(11,19)}</text>:null}
      </g>)}
    </svg>:<p>No price events yet. Generate a proposal to add evidence.</p>}
    <details><summary>Inspect timeline evidence IDs</summary><div className="table-scroll"><table><caption>Prices are in SGD. Proposal and second-window prices remain projected.</caption><thead><tr><th>Event</th><th>Price</th><th>Version</th><th>Evidence ID</th><th>UTC time</th></tr></thead><tbody>{sorted.map(e=><tr key={`${e.kind}-${e.evidence_id}`}><td>{e.kind}</td><td><Money value={e.price_minor}/></td><td>{e.applied_price_version??'—'}</td><td><code>{e.evidence_id}</code></td><td><time dateTime={e.at}>{e.at}</time></td></tr>)}</tbody></table></div></details>
  </section>;
}
