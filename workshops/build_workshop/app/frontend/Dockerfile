FROM node:20-alpine AS build

WORKDIR /app

ARG VITE_API_BASE_URL=/api
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL

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

