# A minimal, non-root container for simdlogs.
#
# Two stages: a builder with the toolchain, and a scratch image holding one
# static binary. Nothing else ships -- no shell, no package manager, no libc --
# because every one of those is attack surface in an image whose only job is to
# run one process, and because a scratch image's contents are exactly its
# provenance.

FROM golang:1.26.5-alpine AS build
WORKDIR /src

# Dependencies first, so a source change does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Reproducible: no cgo (so the binary is genuinely static), trimmed paths (so
# the build directory does not end up in the binary), and the version stamped
# from the tag rather than guessed at runtime.
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/simdlogs ./cmd/simdlogs

FROM scratch

# CA certificates, for a deployment that forwards to an HTTPS endpoint. Copied
# from the builder rather than installed, so the final image still has no
# package manager.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/simdlogs /simdlogs

# Non-root, by a numeric id. A name would need /etc/passwd, which a scratch
# image does not have -- and a numeric id is what a Kubernetes
# runAsNonRoot check reads anyway.
USER 65532:65532

# The data directory is a MOUNT, not a layer. Written into the image it would
# be lost on every redeploy, and a container that silently loses a log store on
# upgrade is worse than one that refuses to start.
VOLUME ["/data"]

EXPOSE 9428

ENTRYPOINT ["/simdlogs"]
CMD ["-storage", "/data", "-addr", ":9428"]
