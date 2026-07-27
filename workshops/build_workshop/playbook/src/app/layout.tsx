import { RootProvider } from 'fumadocs-ui/provider/next';
import './global.css';
import { Inter, Inconsolata } from 'next/font/google';
import type { Metadata } from 'next';
import { PlatformProvider } from '@/components/platform';

const inter = Inter({
  subsets: ['latin'],
});

const inconsolata = Inconsolata({
  subsets: ['latin'],
  variable: '--font-inconsolata',
});

// Absolute base for Open Graph / social image URLs. Set NEXT_PUBLIC_SITE_URL to the
// deployed origin (for example https://demohouse.cloud) in production.
export const metadata: Metadata = {
  metadataBase: new URL(
    process.env.NEXT_PUBLIC_SITE_URL ?? 'http://localhost:3000',
  ),
};

export default function Layout({ children }: LayoutProps<'/'>) {
  return (
    <html
      lang="en"
      className={`${inter.className} ${inconsolata.variable}`}
      suppressHydrationWarning
    >
      <body className="flex flex-col min-h-screen">
        <RootProvider theme={{ defaultTheme: 'dark', enableSystem: false }}>
          <PlatformProvider>{children}</PlatformProvider>
        </RootProvider>
      </body>
    </html>
  );
}
