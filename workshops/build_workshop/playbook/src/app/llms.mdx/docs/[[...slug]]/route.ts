import { getLLMText, getPageMarkdownUrl, isHiddenRehearsal, source } from '@/lib/source';
import { notFound } from 'next/navigation';

export const revalidate = false;

export async function GET(_req: Request, { params }: RouteContext<'/llms.mdx/docs/[[...slug]]'>) {
  const { slug } = await params;
  const pageSlugs = slug?.slice(0, -1);
  if (isHiddenRehearsal(pageSlugs)) notFound();
  const page = source.getPage(pageSlugs);
  if (!page) notFound();

  return new Response(await getLLMText(page), {
    headers: {
      'Content-Type': 'text/markdown',
    },
  });
}

export function generateStaticParams() {
  return source.getPages().filter((page) => !isHiddenRehearsal(page.slugs)).map((page) => ({
    lang: page.locale,
    slug: getPageMarkdownUrl(page).segments,
  }));
}
