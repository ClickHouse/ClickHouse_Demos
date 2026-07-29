export const appName = 'Build Series: Solutions on ClickHouse Cloud';
export const docsRoute = '/docs';
export const docsImageRoute = '/og/docs';
export const docsContentRoute = '/llms.mdx/docs';

// GitHub info for the "Edit on GitHub" links and the nav GitHub button.
// The playbook lives in the ClickHouse_Demos monorepo under
// workshops/build_workshop/playbook on the production workshop branch.
export const gitConfig = {
  user: 'ClickHouse',
  repo: 'ClickHouse_Demos',
  branch: process.env.NEXT_PUBLIC_WORKSHOP_BRANCH ?? 'build-workshop-v1',
};

// The repo participants clone; the app lives at workshops/build_workshop/app.
export const appRepoUrl = 'https://github.com/ClickHouse/ClickHouse_Demos';
