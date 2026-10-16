import {chromium} from '@playwright/test';
import {spawnSync} from 'node:child_process';
import assert from 'node:assert/strict';
const browser=await chromium.launch({headless:true});
try {
 const page=await browser.newPage();
 await page.goto(process.env.PRICING_ALLOWED_ORIGIN);
 await page.getByRole('button',{name:'Start learner session'}).click();
 await page.getByRole('button',{name:'Opportunities',exact:true}).click();
 await page.getByRole('button',{name:'Compare prices for Store 1 / SKU 101'}).click();
 await page.getByRole('button',{name:'Review SGD 7.00'}).click();
 await page.getByRole('button',{name:'Approve and apply price'}).click();
 await page.getByRole('status').filter({hasText:'Price applied'}).waitFor();
 const generated=spawnSync('.venv/bin/python',['tests/prepare_retail_edge.py','outcomes'],{stdio:'inherit',env:process.env});
 assert.equal(generated.status,0);
 await page.getByRole('button',{name:'Observed results',exact:true}).click();
 await page.getByRole('img',{name:'Observed sales revenue by price branch'}).waitFor();
 assert.ok(await page.locator('svg circle').count()>0);
 await page.screenshot({path:'.run/retail-real-edge.png',fullPage:true});
 console.log('Real retail edge passed: browser → API → ClickHouse comparison → Postgres approval → delivered paired events → observed chart. CDC replay is test-only.');
} finally {await browser.close();}
