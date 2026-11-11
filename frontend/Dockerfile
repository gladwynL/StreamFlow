# syntax=docker/dockerfile:1
ARG NODE_IMAGE=node:24.22.2-alpine
ARG NGINX_IMAGE=nginxinc/nginx-unprivileged:1.30.3-alpine

FROM ${NODE_IMAGE} AS build
WORKDIR /app
COPY package.json package-lock.json ./
# No dependency install scripts run; none are needed for the build.
RUN --mount=type=cache,target=/root/.npm npm ci --ignore-scripts --no-audit --no-fund
COPY . .
RUN npm run build

FROM ${NGINX_IMAGE}
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
