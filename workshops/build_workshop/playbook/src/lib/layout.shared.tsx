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
        text: 'Use cases',
        url: '/docs',
      },
      {
        text: 'AI SRE',
        url: '/docs/ai-sre',
      },
      {
        text: 'Polymarket',
        url: '/docs/polymarket',
      },
    ],
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
