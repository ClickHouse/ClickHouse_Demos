FROM node:20-alpine AS build

WORKDIR /app

ARG VITE_API_BASE_URL=/api
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL

# ClickStack browser observability (module 05). Left empty in the base build so
# the frontend emits no browser telemetry; docker-compose.otel.yml supplies these
# so the SDK initializes only when the observability overlay builds the image.
ARG VITE_HYPERDX_URL=
ENV VITE_HYPERDX_URL=$VITE_HYPERDX_URL
ARG VITE_HYPERDX_API_KEY=
ENV VITE_HYPERDX_API_KEY=$VITE_HYPERDX_API_KEY
ARG VITE_HYPERDX_SERVICE=nyc-taxi-frontend
ENV VITE_HYPERDX_SERVICE=$VITE_HYPERDX_SERVICE

COPY package.json /app/package.json
RUN npm install

COPY index.html tsconfig.json vite.config.ts /app/
COPY public /app/public
COPY src /app/src

RUN npm run build

FROM nginx:1.27-alpine

WORKDIR /usr/share/nginx/html

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 8080

CMD ["nginx", "-g", "daemon off;"]

