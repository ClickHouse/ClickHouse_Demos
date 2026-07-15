# =============================================================================
# BUILD workshop playbook -- multi-stage Next.js (Fumadocs) container
# =============================================================================
# Serves the docs site under a sub-path (default /workshop) behind the
# demohouse nginx/ALB. basePath is baked at build time via NEXT_PUBLIC_BASE_PATH
# (see next.config.mjs), so pass it as a build-arg -- it cannot be changed at
# runtime once the bundle is compiled.
#
#   docker build --build-arg NEXT_PUBLIC_BASE_PATH=/workshop \
#                --build-arg NEXT_PUBLIC_SITE_URL=https://dev.demohouse.cloud \
#                -t posthouse-demo-workshop:dev .
#
# Node >= 22.12 is required (package.json engines); the base image is 22-slim.
# =============================================================================

FROM node:22-slim AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM node:22-slim AS builder
WORKDIR /app
ARG NEXT_PUBLIC_BASE_PATH=/workshop
ARG NEXT_PUBLIC_SITE_URL=https://dev.demohouse.cloud
ENV NEXT_PUBLIC_BASE_PATH=$NEXT_PUBLIC_BASE_PATH
ENV NEXT_PUBLIC_SITE_URL=$NEXT_PUBLIC_SITE_URL
ENV NEXT_TELEMETRY_DISABLED=1
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM node:22-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
RUN groupadd --system --gid 1001 nodejs \
  && useradd --system --uid 1001 --gid nodejs nextjs

# Standalone output ships its own minimal node_modules + server.js.
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

USER nextjs
EXPOSE 3000
CMD ["node", "server.js"]
