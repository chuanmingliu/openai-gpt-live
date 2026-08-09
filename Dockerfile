ARG GO_IMAGE=golang:1.26.0-alpine3.23@sha256:d4c4845f5d60c6a974c6000ce58ae079328d03ab7f721a0734277e69905473e5
ARG RUNTIME_IMAGE=alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

FROM ${GO_IMAGE} AS certs

FROM ${GO_IMAGE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
COPY deploy/prestop ./deploy/prestop
COPY test/internal/chaosprovider ./test/internal/chaosprovider
ARG TARGETOS=linux
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/live-gateway ./cmd/live-gateway && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/live-worker ./cmd/live-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/live-migrate ./cmd/live-migrate && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/live-load ./cmd/live-load && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/live-chaos-provider ./test/internal/chaosprovider && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/live-prestop ./deploy/prestop

FROM ${RUNTIME_IMAGE} AS runtime
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN addgroup -S -g 65532 live && adduser -S -D -H -u 65532 -G live live
WORKDIR /var/empty
USER 65532:65532

FROM runtime AS gateway
COPY --from=build --chown=65532:65532 /out/live-gateway /usr/local/bin/live-gateway
COPY --from=build --chown=65532:65532 /out/live-prestop /usr/local/bin/live-prestop
COPY --chown=65532:65532 deploy/config/local.yaml /etc/live/config.yaml
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/live-gateway"]
CMD ["-config", "/etc/live/config.yaml"]

FROM runtime AS worker
COPY --from=build --chown=65532:65532 /out/live-worker /usr/local/bin/live-worker
COPY --from=build --chown=65532:65532 /out/live-prestop /usr/local/bin/live-prestop
COPY --chown=65532:65532 deploy/config/local.yaml /etc/live/config.yaml
ENTRYPOINT ["/usr/local/bin/live-worker"]
CMD ["-config", "/etc/live/config.yaml"]

FROM runtime AS migrate
COPY --from=build --chown=65532:65532 /out/live-migrate /usr/local/bin/live-migrate
ENTRYPOINT ["/usr/local/bin/live-migrate"]
CMD ["-direction", "up"]

FROM runtime AS load
COPY --from=build --chown=65532:65532 /out/live-load /usr/local/bin/live-load
COPY --from=build --chown=65532:65532 /out/live-chaos-provider /usr/local/bin/live-chaos-provider
ENTRYPOINT ["/usr/local/bin/live-load"]
