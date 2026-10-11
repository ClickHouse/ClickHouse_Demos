import { defineConfig } from '@playwright/test';
export default defineConfig({testDir:'./e2e',fullyParallel:true,use:{baseURL:'http://127.0.0.1:4173',viewport:{width:1440,height:1000},trace:'retain-on-failure'},webServer:{command:'npm run dev -- --port 4173 --strictPort',url:'http://127.0.0.1:4173',reuseExistingServer:false},reporter:'list'});
