# Build the manager binary
FROM golang:1.17-alpine3.15 as builder

WORKDIR /workspace

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN set -ex && \
    apk add --no-progress --no-cache \
      gcc \
      musl-dev

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

# Copy the go source
COPY main.go main.go

# Build
#RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o manager main.go
RUN GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -tags musl -installsuffix cgo -ldflags '-extldflags "-static"' -o kafka-tx main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM alpine:3.15
WORKDIR /
COPY --from=builder /workspace/kafka-tx .
USER 65532:65532

ENTRYPOINT ["/kafka-tx"]