from uuid import uuid4
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from pricing_lab.settings import LAB_ROOT
from test_retail_analysis import opportunity_case

pytestmark=pytest.mark.integration

@pytest.fixture
def agent_client(analytics,opportunity_case):
    import clickhouse_connect
    role='retail_agent_'+uuid4().hex
    user='retail_reader_'+uuid4().hex
    profile=role+'_limits'
    sql=(LAB_ROOT/'sql/clickhouse/011_retail_agent_access.sql').read_text()
    for old,new in [('__DATABASE__',analytics.database),('__ROLE__',role),('__PROFILE__',profile)]:sql=sql.replace(old,new)
    for statement in sql.split(';'):
        if statement.strip():analytics.command(statement)
    analytics.command(f"CREATE USER {user} IDENTIFIED BY 'integration-only-password'")
    analytics.command(f'GRANT {role} TO {user}')
    client=clickhouse_connect.get_client(host='127.0.0.1',port=analytics.settings.clickhouse_port,username=user,password='integration-only-password',database=analytics.database)
    try:yield client,opportunity_case
    finally:
        client.close();analytics.command(f'DROP USER {user}')
        analytics.command(f'DROP SETTINGS PROFILE {profile}');analytics.command(f'DROP ROLE {role}')


def test_agent_can_compare_but_cannot_mutate_or_escape(agent_client,analytics):
    client,(_,run,cutoff)=agent_client
    assert client.query('SELECT count() FROM retail_analysis_context').first_row[0]==1
    rows=client.query("SELECT price_minor,recommended FROM retail_price_comparison(run={run:UUID},store=1,sku=101,cutoff={cutoff:DateTime64(3,'UTC')},model='fixture') LIMIT 10",parameters={'run':run,'cutoff':cutoff}).result_rows
    assert (700,True) in rows
    schema=client.query(f"DESCRIBE TABLE (SELECT * FROM {analytics.database}.retail_price_comparison(run={{run:UUID}},store=1,sku=101,cutoff={{cutoff:DateTime64(3,'UTC')}},model='fixture'))",parameters={'run':run,'cutoff':cutoff}).result_rows
    assert 'incremental_profit_minor' in {r[0] for r in schema}
    limits=dict(client.query("SELECT name,value FROM system.settings WHERE name IN ('readonly','max_result_rows','max_execution_time')").result_rows)
    assert limits=={'readonly':'1','max_result_rows':'10000','max_execution_time':'30'}
    for statement in ['TRUNCATE TABLE retail_sales','INSERT INTO retail_sales (quantity) VALUES (1)','CREATE TABLE forbidden(x UInt8) ENGINE=Memory',"ALTER TABLE retail_price_history_cdc DELETE WHERE 1",'SET readonly=0','SELECT * FROM system.query_log LIMIT 1']:
        with pytest.raises(DatabaseError):client.command(statement)
    other='unrelated_'+uuid4().hex
    analytics.command(f'CREATE DATABASE {other}')
    try:
        analytics.command(f'CREATE TABLE {other}.private(x UInt8) ENGINE=Memory')
        with pytest.raises(DatabaseError):client.query(f'SELECT * FROM {other}.private')
    finally:analytics.command(f'DROP DATABASE {other}')
