import type {Outcome,RetailReceipt} from '../retail-contracts';
import RetailSeries from '../components/RetailSeries';
import {money} from '../retail-api';
export default function RetailOutcomes({rows,receipt}:{rows:Outcome[]|null;receipt:RetailReceipt|null}){
 return <><h1>Observed results</h1><p className="retail-lead">Compare recorded synthetic sales after your approval with the unchanged-price branch. Both use the same random draws and separate stock balances.</p>
 {receipt&&<p>Receipt ID: <code>{receipt.receipt_id}</code>{receipt.subject&&<> · Store {receipt.subject.store_id} / SKU {receipt.subject.sku}</>}</p>}
 {!receipt?<p>Review and approve a price to start an observed comparison.</p>:!rows?.length?<p>Price applied at {money(receipt.price_minor)}. Waiting for sales and the approval receipt to arrive in ClickHouse.</p>:<>
 <section className="retail-card"><h2>Observed sales receipts</h2><RetailSeries label="Observed sales revenue by price branch" points={rows.map(r=>({x:r.minute,y:r.receipts_minor/100,branch:r.branch}))}/></section>
 <div className="retail-kpis">{(['applied','comparison'] as const).map(branch=>{const r=rows.filter(r=>r.branch===branch).at(-1);return r&&<article key={branch}><span>{branch==='applied'?'Applied price':'Unchanged price'}</span><strong>{money(r.price_minor)}</strong><p>{r.units} units sold · {money(r.receipts_minor)} receipts</p><p>{r.expired?`${r.waste_units} units of waste`:`${r.remaining_stock} units remaining`}</p><small>Contribution: {money(r.contribution_minor)}</small></article>})}</div>
 <p>Remaining stock becomes waste only at expiry. Contribution subtracts selling costs, the cost of all starting stock, and disposal costs after expiry.</p>
 <details><summary>View observed sales data</summary><table><thead><tr><th>Business time (UTC)</th><th>Branch</th><th>Units sold</th><th>Receipts</th><th>Remaining stock</th></tr></thead><tbody>{rows.map(r=><tr key={`${r.minute}/${r.branch}`}><td>{r.minute}</td><td>{r.branch==='applied'?'Applied price':'Unchanged price'}</td><td>{r.units}</td><td>{money(r.receipts_minor)}</td><td>{r.remaining_stock}</td></tr>)}</tbody></table></details>
 </>}</>;
}
