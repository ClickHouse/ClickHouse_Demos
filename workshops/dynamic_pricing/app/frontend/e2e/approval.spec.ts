import {test,expect} from '@playwright/test';
import {setup,fixture} from './support';

test.beforeEach(async({page})=>setup(page));

test('failed application retains acknowledged price',async({page})=>{
  await page.route('**/api/candidates/*/approve',route=>route.fulfill({status:409,json:{error:{code:'price_version_changed',action:'Create a fresh proposal.'}}}));
  await page.getByRole('button',{name:'Approve and apply mock price'}).click();
  await expect(page.getByTestId('applied-price')).toHaveText('SGD 8.00');
  await expect(page.getByText('Create a fresh proposal.',{exact:true})).toBeVisible();
});

test('delayed acknowledgement and double clicks apply one receipt',async({page})=>{
  let release!:()=>void;const pending=new Promise<void>(resolve=>{release=resolve});let count=0;
  await page.route('**/api/candidates/*/approve',async route=>{count++;await pending;await route.fulfill({json:fixture.receipt});});
  await page.getByRole('button',{name:'Approve and apply mock price'}).dblclick();
  await expect(page.getByText('Applying mock price...',{exact:true})).toBeVisible();
  await expect(page.getByTestId('applied-price')).toHaveText('SGD 8.00');
  release();await expect(page.getByTestId('applied-price')).toHaveText('SGD 6.50');expect(count).toBe(1);
  await page.clock.runFor(2200);
  await expect(page.getByTestId('applied-price')).toHaveText('SGD 6.50');
});

test('retry keeps approval idempotency key',async({page})=>{
  const keys:string[]=[];
  await page.route('**/api/candidates/*/approve',route=>{keys.push(route.request().postDataJSON().idempotency_key);return route.fulfill(keys.length===1?{status:503,json:{error:{code:'application_failed',action:'Retry the mock application.'}}}:{json:fixture.receipt});});
  await page.getByRole('button',{name:'Approve and apply mock price'}).click();
  await expect(page.getByText('Retry the mock application.',{exact:true})).toBeVisible();
  await page.getByRole('button',{name:'Approve and apply mock price'}).click();
  await expect(page.getByTestId('applied-price')).toHaveText('SGD 6.50');expect(keys[0]).toBe(keys[1]);
});

test('held and expired proposals cannot be approved',async({page})=>{
  const held=structuredClone(fixture.candidates);held.candidates[0].policy={status:'held',reason_ids:['stale_source']};
  await page.route('**/api/candidates',route=>route.fulfill({json:held}));
  await page.getByRole('button',{name:'Refresh evidence'}).click();
  await expect(page.getByRole('button',{name:'Approve and apply mock price'})).toBeDisabled();
  await expect(page.getByText('stale_source',{exact:true})).toBeVisible();
  held.candidates[0].policy={status:'eligible',reason_ids:[]};held.candidates[0].candidate.expires_at='2029-01-01T00:00:00Z';
  await page.getByRole('button',{name:'Refresh evidence'}).click();
  await expect(page.getByRole('button',{name:'Approve and apply mock price'})).toBeDisabled();
});

test('keyboard controls and narrow screens retain price evidence',async({page})=>{
  await page.setViewportSize({width:390,height:844});
  await page.getByRole('button',{name:'Candidates',exact:true}).focus();await page.keyboard.press('Enter');
  await expect(page.getByRole('img',{name:'Proposed and applied mock price timeline'})).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
});

test('fractional monetary projections preserve decimal rounding and zero',async({page})=>{
  const values=structuredClone(fixture.candidates);
  values.candidates[0].candidate.projection.gross_margin_minor='-150.5';
  values.candidates[0].candidate.projection.full_contribution_minor='-0.25';
  await page.route('**/api/candidates',route=>route.fulfill({json:values}));
  await page.getByRole('button',{name:'Refresh evidence'}).click();
  await expect(page.getByRole('row',{name:'Sold-unit gross margin SGD -1.51'})).toBeVisible();
  await expect(page.getByRole('row',{name:'Full contribution SGD 0.00'})).toBeVisible();
});

test('source version conflict refreshes before a new sale request',async({page})=>{
  const requests:{event_id:string;expected_price_version:number}[]=[];
  await page.route('**/api/sales',route=>{
    requests.push(route.request().postDataJSON());
    return route.fulfill(requests.length===1?{status:409,json:{error:{code:'price_version_changed',action:'Refresh the applied price before creating a sale.'}}}:{json:{scope:fixture.freshness.scope,mode:'recorded',data:{event_id:requests[1].event_id,applied_price_version:2}}});
  });
  await page.getByRole('button',{name:'Freshness',exact:true}).click();
  await page.getByRole('button',{name:'Create one synthetic sale'}).click();
  await expect(page.getByRole('alert')).toHaveText('Refresh the applied price before creating a sale.');
  const fresh=structuredClone(fixture.freshness);fresh.data.applied_price_version=2;fresh.data.current_price_minor=650;
  await page.route('**/api/freshness',route=>route.fulfill({json:fresh}));
  await page.getByRole('button',{name:'Refresh evidence'}).click();
  await expect(page.getByTestId('applied-price')).toHaveText('SGD 6.50');
  await page.getByRole('button',{name:'Create one synthetic sale'}).click();
  await expect(page.getByRole('status')).toContainText('Sale committed at version 2');
  expect(requests[1].event_id).not.toBe(requests[0].event_id);expect(requests[1].expected_price_version).toBe(2);
});

test('snapshot hold displays actionable source reasons',async({page})=>{
  await page.route('**/api/candidates',route=>route.request().method()==='POST'
    ?route.fulfill({status:409,json:{error:{code:'snapshot_held',action:'Restore the checkpoint for stale data.',reason_ids:['inventory_stale']}}})
    :route.fulfill({json:fixture.candidates}));
  await page.getByRole('button',{name:'Generate fresh proposal'}).click();
  await expect(page.getByRole('alert')).toHaveText('Restore the checkpoint for stale data. Reasons: inventory_stale.');
  await expect(page.getByTestId('applied-price')).toHaveText('SGD 8.00');
});
