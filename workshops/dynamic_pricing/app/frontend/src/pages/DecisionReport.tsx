import type {DecisionData,Receipt} from '../contracts';
import {api} from '../api';
import Money from '../components/Money';
export default function DecisionReport({decision,receipt}:{decision:DecisionData|null;receipt:Receipt|null}){
  const id=decision?.decision_id??receipt?.decision_id;
  return <section><h2 className="page-title">Decision evidence</h2><p className="lede">The report joins snapshot versions, projected economics, policy, learner review, and the acknowledgement receipt.</p>{id?<><dl className="evidence-list"><dt>Decision</dt><dd><code>{id}</code></dd><dt>Learner review</dt><dd>{decision?.reviewer_id??receipt?.scope.owner_id}</dd><dt>Review state</dt><dd>{decision?.status??'approved'}</dd><dt>Application</dt><dd>{receipt?'Acknowledged mock price':'No acknowledgement receipt'}</dd>{receipt&&<><dt>Applied price</dt><dd><Money value={receipt.price_minor}/></dd><dt>Applied version</dt><dd>{receipt.applied_price_version}</dd><dt>Receipt</dt><dd><code>{receipt.receipt_id}</code></dd><dt>Acknowledged at</dt><dd>{receipt.acknowledged_at}</dd></>}</dl><div className="actions"><a className="button primary" href={api.exportReport(id)} download>Export decision JSON</a><a className="button" href={api.exportReport(id,'markdown')} download>Export decision Markdown</a></div></>:<p>No reviewed decision yet. Review a proposal, then export its evidence.</p>}
    <aside className="learning-note"><h3>Frame a data pilot</h3><p>Test whether fresher sales and stock inputs change feasible price choices. Verify convergence, policy failures, review identity, and sale prices before assessing a real business outcome.</p></aside>
  </section>;
}
