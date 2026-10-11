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
