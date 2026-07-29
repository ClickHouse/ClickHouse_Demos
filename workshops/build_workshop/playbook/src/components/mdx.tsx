import defaultMdxComponents from 'fumadocs-ui/mdx';
import { Tab, Tabs } from 'fumadocs-ui/components/tabs';
import type { MDXComponents } from 'mdx/types';
import { PlatformOnly } from '@/components/platform';

function DevOnly({ children }: { children: React.ReactNode }) {
  return process.env.NEXT_PUBLIC_WORKSHOP_ENV === 'dev' ? children : null;
}

export function getMDXComponents(components?: MDXComponents) {
  return {
    ...defaultMdxComponents,
    Tab,
    Tabs,
    PlatformOnly,
    DevOnly,
    ...components,
  } satisfies MDXComponents;
}

export const useMDXComponents = getMDXComponents;

declare global {
  type MDXProvidedComponents = ReturnType<typeof getMDXComponents>;
}
