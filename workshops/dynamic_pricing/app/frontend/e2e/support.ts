import { expect, type Page } from '@playwright/test';
import fixture from './fixtures/golden.json' with {type:'json'};
export {fixture};
export async function setup(page:Page){
  await page.clock.install({time:new Date('2030-01-30T12:00:00Z')});
  await page.route('**/api/**',async route=>{
    const path=new URL(route.request().url()).pathname;
    const key=path.split('/')[2] as keyof typeof fixture;
    if(key==='session'||key==='baseline'||key==='freshness'||key==='candidates'||key==='timeline')return route.fulfill({json:fixture[key]});
    return route.fulfill({json:{scope:fixture.freshness.scope,mode:'recorded',data:{checks:[],ready:false,label:'Recorded test fixture'}}});
  });
  await page.goto('/');await page.getByRole('button',{name:'Start learner session'}).click();
  await expect(page.getByTestId('applied-price')).toHaveText('SGD 8.00');
  await page.getByRole('button',{name:'Candidates',exact:true}).click();
}
