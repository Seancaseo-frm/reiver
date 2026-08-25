FROM node:20-alpine AS builder

WORKDIR /app
COPY docs/package.json docs/package-lock.json* ./
RUN npm install
COPY docs/ ./
RUN npm run build

FROM nginx:alpine

COPY --from=builder /app/.vitepress/dist /usr/share/nginx/html
COPY deploy/docker/nginx-docs.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
