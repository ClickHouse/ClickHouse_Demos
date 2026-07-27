import { createMDX } from 'fumadocs-mdx/next';

const withMDX = createMDX();

// Production and dev each use a dedicated hostname and serve from `/`. Keep
// basePath configurable for local experiments, but deployments leave it empty.
const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? '';

/** @type {import('next').NextConfig} */
const config = {
  reactStrictMode: true,
  basePath,
  async redirects() {
    return [
      {
        source: '/docs/learner/07-chat-langfuse',
        destination: '/docs/learner/08-chat-langfuse',
        permanent: true,
      },
      {
        source: '/docs/instructor/07-chat-langfuse',
        destination: '/docs/instructor/08-chat-langfuse',
        permanent: true,
      },
      {
        source: '/docs/learner/08-break-and-fix',
        destination: '/docs/learner/07-break-and-fix',
        permanent: true,
      },
      {
        source: '/docs/instructor/08-break-and-fix',
        destination: '/docs/instructor/07-break-and-fix',
        permanent: true,
      },
    ];
  },
  // Emit a self-contained server bundle (.next/standalone) so the Docker image
  // can run `node server.js` without the full node_modules tree. See Dockerfile.
  output: 'standalone',
};

export default withMDX(config);
