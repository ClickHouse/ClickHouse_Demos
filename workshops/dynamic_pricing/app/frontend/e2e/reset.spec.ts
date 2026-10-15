import {test,expect} from '@playwright/test';
import {setup,fixture} from './support';
test('old poll arriving after reset cannot restore old price or candidate',async({page})=>{
  await setup(page);let release!:()=>void;let calls=0;
  const pending=new Promise<void>(resolve=>{release=resolve});
  await page.route('**/api/freshness',async route=>{calls++;if(calls===1){await pending;return route.fulfill({json:fixture.freshness});}const fresh=structuredClone(fixture.freshness);fresh.scope.generation=2;return route.fulfill({json:fresh});});
  await page.clock.runFor(2100);
  await page.route('**/api/reset',route=>route.fulfill({json:{scope:{...fixture.freshness.scope,generation:2},mode:'recorded',data:{checkpoint:'lab3_policy'}}}));
  await page.getByRole('button',{name:'Restore checkpoint'}).click();
  await expect(page.getByTestId('generation')).toHaveText('Generation 2');release();
  await expect(page.getByText('No proposals yet. Generate a proposal from current inputs.')).toBeVisible();
  await expect(page.getByTestId('applied-price')).toHaveText('SGD 8.00');
});

test('reset retry reuses its request key until acknowledgement',async({page})=>{
  await setup(page);const keys:string[]=[];
  await page.route('**/api/reset',route=>{keys.push(route.request().postDataJSON().idempotency_key);return route.fulfill(keys.length===1?{status:503,json:{error:{code:'dependency_unavailable',action:'Retry checkpoint restoration.'}}}:{json:{scope:{...fixture.freshness.scope,generation:2},mode:'recorded',data:{checkpoint:'lab3_policy'}}});});
  await page.getByRole('button',{name:'Restore checkpoint'}).click();
  await expect(page.getByText('Retry checkpoint restoration.',{exact:true})).toBeVisible();
  await page.getByRole('button',{name:'Restore checkpoint'}).click();
  await expect(page.getByTestId('generation')).toHaveText('Generation 2');expect(keys[0]).toBe(keys[1]);
});

test('checkpoint restore gives immediate feedback throughout a slow request',async({page})=>{
  await setup(page);
  await page.getByLabel('Recovery checkpoint').selectOption('lab2_features');
  let release!:()=>void;const pending=new Promise<void>(resolve=>{release=resolve});
  await page.route('**/api/reset',async route=>{
    await pending;
    await route.fulfill({json:{scope:{...fixture.freshness.scope,generation:2},mode:'recorded',data:{checkpoint:'lab2_features'}}});
  });
  await page.getByRole('button',{name:'Restore checkpoint',exact:true}).click();
  await expect(page.getByRole('button',{name:'Restoring checkpoint...'})).toBeDisabled();
  await expect(page.getByLabel('Recovery checkpoint')).toBeDisabled();
  const status=page.locator('.checkpoint').getByRole('status');
  await expect(status).toContainText('Restoring lab2 features');
  await page.clock.runFor(3100);
  await expect(status).toContainText('3s elapsed');
  release();
  await expect(status).toContainText('Restored lab2 features. Generation 2');
  await expect(page.getByRole('button',{name:'Restore checkpoint',exact:true})).toBeEnabled();
});

test('restore feedback distinguishes acknowledgement from refreshing views',async({page})=>{
  await setup(page);
  let release!:()=>void;const pending=new Promise<void>(resolve=>{release=resolve});
  await page.route('**/api/baseline',async route=>{await pending;await route.fulfill({json:{...fixture.baseline,scope:{...fixture.freshness.scope,generation:2}}});});
  await page.route('**/api/reset',route=>route.fulfill({json:{scope:{...fixture.freshness.scope,generation:2},mode:'recorded',data:{checkpoint:'lab3_policy'}}}));
  await page.getByRole('button',{name:'Restore checkpoint',exact:true}).click();
  await expect(page.locator('.checkpoint').getByRole('status')).toContainText('Checkpoint restored. Refreshing the workbench...');
  await expect(page.getByRole('button',{name:'Restoring checkpoint...'})).toBeDisabled();
  release();
  await expect(page.locator('.checkpoint').getByRole('status')).toContainText('Restored lab3 policy. Generation 2');
});
