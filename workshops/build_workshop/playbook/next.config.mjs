import { createMDX } from 'fumadocs-mdx/next';

const withMDX = createMDX();

// The playbook is published under demohouse.cloud/workshop, so it is served from a
// sub-path. Set NEXT_PUBLIC_BASE_PATH=/workshop at build time to prefix every route and
// asset. Leave it empty for local development (served at the domain root).
// See README.md ("Deployment") for the full deployment matrix.
const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? '';

/** @type {import('next').NextConfig} */
const config = {
  reactStrictMode: true,
  basePath,
  // Emit a self-contained server bundle (.next/standalone) so the Docker image
  // can run `node server.js` without the full node_modules tree. See Dockerfile.
  output: 'standalone',
};

export default withMDX(config);
