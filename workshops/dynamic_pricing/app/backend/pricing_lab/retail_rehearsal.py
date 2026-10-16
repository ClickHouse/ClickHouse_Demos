"""Conservative status aggregation for reproducible rehearsal evidence."""
def summarize(checks):
    statuses=[c['status'] for c in checks.values()]
    if 'failed' in statuses:return 'failed'
    return 'passed' if statuses and all(s=='passed' for s in statuses) else 'incomplete'


def stage_status(stage, evidence):
    if stage == 'benchmark' and not (evidence.get('equal') and evidence.get('performance_complete')):
        return 'failed'
    return 'passed'
