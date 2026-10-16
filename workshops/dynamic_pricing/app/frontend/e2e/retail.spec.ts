import {test,expect,type Page} from '@playwright/test';
const run='11111111-1111-4111-8111-111111111111',receipt='22222222-2222-4222-8222-222222222222';
const cutoff='2026-01-30T08:00:00Z';
const inputs={subject:{store_id:5000,sku:101},cutoff,expires_at:'2026-01-30T10:00:00Z',stock_units:20,current_price_minor:800,model_version:'fixture',base_demand_units:16,demand_multipliers:[.5,1,1.5],scenario_weights:[.25,.5,.25]};
async function setup(page:Page){
 await page.route('**/api/**',async route=>{
  const path=new URL(route.request().url()).pathname;
  let data:unknown={};
  if(path.endsWith('/context'))data={cutoff,stream_state:'running',model_version:'fixture',delivery_age_seconds:2,aggregation_ready:true};
  if(path.endsWith('/overview'))data={totals:{units:10000,receipts_minor:8000000,transactions:9500},series:[{minute:cutoff,transactions:1000,receipts_minor:800000}],districts:[],query_seconds:.012,read_rows:12000,query_id:'test-query',engine:'aggregate',transactions_per_second:1000};
  if(path.endsWith('/opportunities'))data=[{store_id:5000,sku:101,stock_units:20,base_demand_units:16,excess_units:4,eligibility:'ready',current_price_minor:800}];
  if(path.endsWith('/compare'))data={inputs,rows:[{price_minor:800,units:16,receipts_minor:12800,waste_units:4,net_recovery_minor:12280,contribution_minor:4280,incremental_profit_minor:0,recommended:false},{price_minor:700,units:20,receipts_minor:14000,waste_units:0,net_recovery_minor:13600,contribution_minor:5600,incremental_profit_minor:1320,recommended:true}],query_seconds:.015};
  if(path.endsWith('/proposals'))data={proposal_id:receipt};
  if(path.endsWith('/apply'))data={receipt_id:receipt,price_minor:700,subject:inputs.subject};
  if(path.endsWith('/outcomes'))data=[{minute:cutoff,branch:'applied',price_minor:700,units:2,receipts_minor:1400,remaining_stock:18,contribution_minor:-6640,expired:false,waste_units:0},{minute:cutoff,branch:'comparison',price_minor:800,units:1,receipts_minor:800,remaining_stock:19,contribution_minor:-7220,expired:false,waste_units:0}];
  await route.fulfill({json:{run_id:run,mode:'synthetic',data,generated_at:'2026-10-16T00:00:00Z'}});
 });
 await page.goto('/'); await page.getByRole('button',{name:'Start learner session'}).click();
}

test('opportunity opens comparison and observed results',async({page})=>{
 await setup(page);
 await page.getByRole('button',{name:'Opportunities',exact:true}).click();
 await page.getByRole('button',{name:'Compare prices for Store 5000 / SKU 101'}).click();
 await expect(page.getByRole('heading',{name:'Compare prices',exact:true})).toBeVisible();
 await page.getByRole('button',{name:'Review SGD 7.00'}).click();
 await page.getByRole('button',{name:'Approve and apply price'}).click();
 await expect(page.getByRole('status')).toContainText('Price applied');
 await page.getByRole('button',{name:'Observed results',exact:true}).click();
 await expect(page.getByText(receipt,{exact:true})).toBeVisible();
 await expect(page.getByRole('img',{name:'Observed sales revenue by price branch'})).toBeVisible();
 await expect(page.getByText('Unchanged price', {exact:true}).first()).toBeVisible();
 await page.screenshot({path:'../../.run/retail-workbench-preview.png',fullPage:true});
});

test('pending approval is visible and duplicate clicks are disabled',async({page})=>{
 await setup(page);
 await page.getByRole('button',{name:'Opportunities',exact:true}).click();
 await page.getByRole('button',{name:'Compare prices for Store 5000 / SKU 101'}).click();
 await page.getByRole('button',{name:'Review SGD 7.00'}).click();
 let release:()=>void=()=>{};
 await page.route('**/api/retail/apply',async route=>{await new Promise<void>(resolve=>{release=resolve});await route.fulfill({json:{run_id:run,mode:'synthetic',data:{receipt_id:receipt,price_minor:700},generated_at:cutoff}})});
 await page.getByRole('button',{name:'Approve and apply price'}).click();
 await expect(page.getByRole('button',{name:'Applying price…'})).toBeDisabled();
 release();
 await expect(page.getByRole('status')).toContainText('Price applied');
});

test('late district responses cannot replace the current selection',async({page})=>{
 await setup(page);
 let release:()=>void=()=>{};let firstRequested=false;
 await page.route('**/api/retail/opportunities?**',async route=>{
  const district=new URL(route.request().url()).searchParams.get('district');
  if(district==='1'){firstRequested=true;await new Promise<void>(resolve=>{release=resolve});}
  await route.fulfill({json:{run_id:run,mode:'synthetic',data:[{store_id:district==='1'?1:251,sku:101,stock_units:20,base_demand_units:16,excess_units:4,eligibility:'ready',current_price_minor:800}],generated_at:cutoff}}).catch(()=>{});
 });
 await page.getByRole('button',{name:'Opportunities',exact:true}).click();
 await expect.poll(()=>firstRequested).toBe(true);
 await page.getByRole('combobox').selectOption('2');
 await expect(page.getByRole('button',{name:'Compare prices for Store 251 / SKU 101'})).toBeVisible();
 release();
 await expect(page.getByRole('button',{name:'Compare prices for Store 1 / SKU 101'})).toHaveCount(0);
});

test('comparison keeps its captured cutoff and refresh recomputes it',async({page})=>{
 await setup(page);
 await page.getByRole('button',{name:'Opportunities',exact:true}).click();
 await page.getByRole('button',{name:'Compare prices for Store 5000 / SKU 101'}).click();
 await page.getByRole('button',{name:'Review SGD 7.00'}).click();
 const newer='2026-01-30T08:01:00Z';
 await page.route('**/api/retail/context',route=>route.fulfill({json:{run_id:run,data:{cutoff:newer,delivery_age_seconds:1,model_version:'fixture',stream_state:'running',aggregation_ready:true}}}));
 await expect(page.locator('.retail-toolbar')).toContainText('08:01:00',{timeout:6000});
 await expect(page.getByLabel('Captured comparison evidence')).toContainText(cutoff);
 let comparedCutoff='';
 await page.route('**/api/retail/compare',async route=>{
  comparedCutoff=route.request().postDataJSON().cutoff;
  await route.fulfill({json:{run_id:run,data:{inputs:{...inputs,cutoff:comparedCutoff},rows:[],query_seconds:.01}}});
 });
 await page.getByRole('button',{name:'Refresh evidence'}).click();
 await expect.poll(()=>comparedCutoff).toBe(newer);
 await expect(page.getByLabel('Captured comparison evidence')).toContainText(newer);
 await expect(page.getByRole('button',{name:'Approve and apply price'})).toHaveCount(0);
});
