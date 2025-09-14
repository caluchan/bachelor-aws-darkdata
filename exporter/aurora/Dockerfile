# App Bauen
FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o aurora_exporter .

# Schlankes image nur mit gebauter Binary
FROM alpine:latest
WORKDIR /app

# nur binary von builder übernehmen
COPY --from=builder /app/aurora_exporter .

# Expose port 8080
EXPOSE 8080
CMD ["./aurora_exporter"]
