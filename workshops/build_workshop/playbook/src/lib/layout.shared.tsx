import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { appName, gitConfig } from './shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: appName,
    },
    links: [
      {
        text: 'Overview',
        url: '/docs',
        active: 'nested-url',
      },
      {
        text: 'Learner track',
        url: '/docs/learner',
      },
      {
        text: 'Instructor track',
        url: '/docs/instructor',
      },
    ],
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
