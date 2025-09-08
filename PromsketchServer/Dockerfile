# syntax=docker/dockerfile:1
ARG GO_VERSION=1.25.1
FROM golang:${GO_VERSION} AS builder
WORKDIR /app

# Cache deps first
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest
COPY . .

# Build static binary
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -trimpath -tags netgo -ldflags='-s -w -extldflags "-static"' -o promsketch-server ./main.go

# Minimal runtime
FROM gcr.io/distroless/static-debian12
WORKDIR /app
COPY --from=builder /app/promsketch-server /usr/local/bin/promsketch-server

# Optional: document ports
EXPOSE 7000 6060
# EXPOSE 7100-7124  # (only if you want to document partition ports)

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/promsketch-server"]
