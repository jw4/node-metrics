FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /node-metrics-collector ./cmd/collector

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /node-metrics-collector /usr/local/bin/node-metrics-collector
USER nobody:nobody
ENTRYPOINT ["/usr/local/bin/node-metrics-collector"]
