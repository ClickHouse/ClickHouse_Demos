import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { gitConfig } from './shared';
import { Logo } from '@/components/logo';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: <Logo />,
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
