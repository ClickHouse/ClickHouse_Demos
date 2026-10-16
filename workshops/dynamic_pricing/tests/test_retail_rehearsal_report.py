from pricing_lab.retail_rehearsal import summarize


def test_required_unrun_check_prevents_pass():
    assert summarize({'sql':{'status':'passed'},'managed_agent':{'status':'not_run'}})=='incomplete'
    assert summarize({'sql':{'status':'failed'},'managed_agent':{'status':'not_run'}})=='failed'
    assert summarize({'sql':{'status':'passed'}})=='passed'


def test_benchmark_requires_complete_server_metrics():
    from pricing_lab.retail_rehearsal import stage_status
    assert stage_status('benchmark',{'equal':True,'performance_complete':False})=='failed'
    assert stage_status('benchmark',{'equal':True,'performance_complete':True})=='passed'
    assert stage_status('benchmark',{'equal':False,'performance_complete':True})=='failed'
    assert stage_status('initialize',{'ready':True})=='passed'
