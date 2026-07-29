import { getPageImage, isHiddenRehearsal, source } from '@/lib/source';
import { notFound } from 'next/navigation';
import { ImageResponse } from 'next/og';
import { generate as DefaultImage } from 'fumadocs-ui/og';
import { appName } from '@/lib/shared';

export const revalidate = false;

export async function GET(_req: Request, { params }: RouteContext<'/og/docs/[...slug]'>) {
  const { slug } = await params;
  const pageSlugs = slug.slice(0, -1);
  if (isHiddenRehearsal(pageSlugs)) notFound();
  const page = source.getPage(pageSlugs);
  if (!page) notFound();

  return new ImageResponse(
    <DefaultImage title={page.data.title} description={page.data.description} site={appName} />,
    {
      width: 1200,
      height: 630,
    },
  );
}

export function generateStaticParams() {
  return source.getPages().filter((page) => !isHiddenRehearsal(page.slugs)).map((page) => ({
    lang: page.locale,
    slug: getPageImage(page).segments,
  }));
}
