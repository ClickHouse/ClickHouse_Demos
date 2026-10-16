import {test,expect} from '@playwright/test';
import {setup} from './support';

for(const viewport of [{width:1440,height:800},{width:390,height:844}]){
  test(`selected page content is visible above the timeline at ${viewport.width}px`,async({page})=>{
    await page.setViewportSize(viewport);
    await setup(page);
    await page.getByRole('button',{name:'Freshness',exact:true}).click();
    await expect(page.getByRole('heading',{name:'Source freshness',exact:true})).toBeInViewport();
    const heading=await page.getByRole('heading',{name:'Source freshness',exact:true}).boundingBox();
    const timeline=await page.getByRole('heading',{name:'Price and evidence timeline',exact:true}).boundingBox();
    expect(heading!.y).toBeLessThan(timeline!.y);
    await page.getByRole('button',{name:'Baseline',exact:true}).click();
    await expect(page.getByRole('heading',{name:'Historical baseline',exact:true})).toBeInViewport();
  });
}
