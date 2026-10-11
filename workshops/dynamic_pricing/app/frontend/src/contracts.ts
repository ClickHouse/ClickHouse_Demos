export interface Scope { owner_id:string;scenario_id:string;generation:number }
export interface Subject { sku:number;store_id:number;channel:'store' }
export interface Snapshot {
  snapshot_id:string;scope:Scope;subject:Subject;scenario_cutoff:string;captured_at:string;
  source_versions:Record<string,string>;source_ages_seconds:Record<string,number>;
  stock_units:number;inventory_expires_at:string;expires_at:string;current_price_minor:number;
  reference_price_minor:number;unit_cost_minor:number;selling_cost_minor:number;disposal_cost_minor:number;
  rule_version:number;applied_price_version:number;
}
export interface Candidate { candidate_id:string;snapshot:Snapshot;model_version:string;price_minor:number;created_at:string;expires_at:string;projection:Record<string,string> }
export interface Policy {status:'eligible'|'requires_review'|'rejected'|'held';reason_ids:string[]}
export interface AssessedCandidate {candidate:Candidate;policy:Policy}
export interface Envelope<T> {scope:Scope|null;mode:'live'|'recorded';data:T}
export interface CandidateEnvelope {scope:Scope;mode:'live'|'recorded';snapshot:Snapshot|null;candidates:AssessedCandidate[]}
export interface Receipt {receipt_id:string;decision_id:string;scope:Scope;price_minor:number;applied_price_version:number;acknowledged_at:string}
export interface ApplicationEnvelope {scope:Scope;mode:'live'|'recorded';receipt:Receipt}
export interface FreshnessData {stock_units:number;inventory_expires_at:string;current_price_minor:number;applied_price_version:number;source_versions:Record<string,string>;source_ages_seconds:Record<string,number|null>;scenario_cutoff:string;selected_versions?:Record<string,string|null>}
export interface BaselineData {reconciliation:{raw:Record<string,number>;rollup:Record<string,number>;history_run_id:string;complete_hour_cutoff:string}}
export interface TimelineEvent {kind:'proposed'|'applied'|'approval'|'rejection'|'sale'|'cdc';evidence_id:string;at:string;price_minor:number;applied_price_version?:number;receipt_id?:string|null;snapshot_id?:string;scenario_time?:string}
export interface TimelineData {events:TimelineEvent[];cdc_status:string;ingestion_mode?:string}
export interface DecisionData {decision_id:string;candidate_id:string;reviewer_id:string;reviewed_at:string;status:string;review:Record<string,unknown>;receipt:Receipt|null}
export interface SessionData {owner_id:string;scenario_id:string;model_distribution:string;ingestion_mode:string;model_version?:string}
export interface DiagnosticsData {ready:boolean;checks:{name:string;passed:boolean;action:string|null}[];label?:string;module?:string}
