import type {Overview} from '../retail-contracts';
import RetailSeries from '../components/RetailSeries';
import {money} from '../retail-api';
export default function RetailOverview({data}:{data:Overview|null}){
 return <><h1>Retail chain overview</h1><p className="retail-lead">Explore sales across 5,000 stores. These totals cover the last 24 hours of business time.</p>
 {!data?<p>Reading sales from ClickHouse…</p>:<>
 <div className="retail-kpis"><article><span>Sales receipts</span><strong>{money(data.totals.receipts_minor)}</strong></article><article><span>Transactions</span><strong>{data.totals.transactions.toLocaleString()}</strong></article><article><span>Live transactions / second</span><strong>{data.transactions_per_second.toFixed(1)}</strong><small>Measured over the last 60 wall-clock seconds</small></article></div>
 <section className="retail-card"><h2>Sales receipts over time</h2><RetailSeries label="Retail chain sales receipts over business time" points={data.series.map(r=>({x:r.minute,y:r.receipts_minor/100}))}/></section>
 <aside className="retail-query"><strong>{data.engine==='aggregate'?'Minute aggregate':'Raw sales'} query</strong><span>{(data.query_seconds*1000).toFixed(0)} ms client duration</span><span>{data.read_rows?.toLocaleString()??'Unavailable'} rows read</span><details><summary>Query evidence</summary><code>{data.query_id}</code><p>Query duration includes network time. Compare server timings in the performance exercise.</p></details></aside>
 <section className="retail-card"><h2>District sales</h2><div className="retail-districts">{data.districts.map(d=><article key={d.district_id}><strong>District {d.district_id}</strong><span>{money(d.receipts_minor)}</span><small>{d.transactions.toLocaleString()} transactions</small></article>)}</div></section>
 <details><summary>View sales data</summary><table><thead><tr><th>Business time (UTC)</th><th>Transactions</th><th>Receipts</th></tr></thead><tbody>{data.series.map(r=><tr key={r.minute}><td>{r.minute}</td><td>{r.transactions}</td><td>{money(r.receipts_minor)}</td></tr>)}</tbody></table></details>
 </> }</>;
}
