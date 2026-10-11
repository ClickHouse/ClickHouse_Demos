"""Independent validation against authoritative state, never agent assertions."""
from .observability import traced
from .contracts import PolicyResult,Scope,Subject


@traced('pricing.policy')
def validate_candidate(candidate,trusted,rules,now):
    s=candidate.snapshot;rejected=[];held=[];review=[]
    def same(key,value,reason):
        if trusted.get(key)!=value: rejected.append(reason)
    scope=trusted.get('scope');subject=trusted.get('subject')
    if isinstance(scope,dict): scope=Scope.model_validate(scope)
    if isinstance(subject,dict): subject=Subject.model_validate(subject)
    if scope!=s.scope: rejected.append('scope_changed')
    if subject!=s.subject: rejected.append('subject_changed')
    if candidate.candidate_id not in trusted.get('candidate_ids',[]): rejected.append('candidate_unknown')
    if candidate.model_version not in trusted.get('model_versions',[]): held.append('model_unsupported')
    same('stock_units',s.stock_units,'stock_changed')
    same('inventory_expires_at',s.inventory_expires_at,'inventory_changed')
    same('source_versions',s.source_versions,'source_versions_changed')
    same('applied_price_version',s.applied_price_version,'price_version_changed')
    same('rule_version',s.rule_version,'rules_changed')
    if rules.get('version')!=s.rule_version: rejected.append('rules_changed')
    if now>=min(candidate.expires_at,s.expires_at): held.append('lease_expired')
    if not s.stock_units: held.append('no_stock')
    if s.inventory_expires_at<=s.scenario_cutoff: held.append('stock_expired')
    if any(age>rules['max_source_age_seconds'] for age in s.source_ages_seconds.values()) or not {'inventory','price','cost'}<=s.source_ages_seconds.keys(): held.append('stale_source')
    price=candidate.price_minor
    if price<rules['floor_minor']: rejected.append('below_floor')
    if price%rules['price_step_minor']: rejected.append('invalid_price_step')
    change=abs(price-s.current_price_minor)*10000
    if change>rules['max_change_bps']*s.current_price_minor: rejected.append('change_limit')
    ratio=price/s.reference_price_minor
    if not rules['allowed_price_ratios'][0]<=ratio<=rules['allowed_price_ratios'][1]: rejected.append('unsupported_price_ratio')
    if change>rules['review_change_bps']*s.current_price_minor: review.append('large_price_change')
    low=candidate.projection.get('parameter_advantage_min_minor')
    high=candidate.projection.get('parameter_advantage_max_minor')
    if low is None or high is None: held.append('uncertainty_missing')
    elif low<0<high: review.append('unstable_advantage')
    if rejected: return PolicyResult(status='rejected',reason_ids=sorted(set(rejected+held)))
    if held: return PolicyResult(status='held',reason_ids=sorted(set(held)))
    return PolicyResult(status='requires_review' if review else 'eligible',reason_ids=review)
