# syntax=docker/dockerfile:1

# Build web UI (Next.js export) so static assets are embedded into the Go binary.
FROM node:20-bookworm AS web
WORKDIR /src/web

COPY web/package.json web/pnpm-lock.yaml ./
COPY web/tsconfig.json web/next.config.ts web/postcss.config.mjs web/components.json web/eslint.config.mjs ./
COPY web/public ./public
COPY web/src ./src

RUN corepack enable && pnpm --version
ARG NEXT_PUBLIC_APP_VERSION=dev
ENV NEXT_PUBLIC_APP_VERSION=${NEXT_PUBLIC_APP_VERSION}
RUN pnpm install --frozen-lockfile
RUN pnpm run build

# Build Go binary (embeds static/out via //go:embed).
FROM golang:1.24.4-bookworm AS go
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
COPY --from=web /src/web/out ./static/out

RUN CGO_ENABLED=0 go build -tags=jsoniter -o /out/octopus .

# Runtime image
FROM debian:bookworm-slim
ENV TZ=Asia/Shanghai

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata gosu && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /app

WORKDIR /app
COPY --from=go /out/octopus /app/octopus
COPY scripts/dockerfiles/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080
VOLUME ["/app/data"]

CMD ["/entrypoint.sh"]

