FROM golang:1.24-alpine as build_env

# Copy go mod and sum files
COPY go.mod go.sum ./

# Install SSL ca certificates.
# Ca-certificates is required to call HTTPS endpoints.
RUN apk update && apk add --no-cache \
    ca-certificates \
    tzdata && \
    update-ca-certificates

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

FROM build_env as builder
# Copy the source from the current directory to the Working Directory inside the container
COPY layer.go ./
COPY cmd ./cmd

# Build the app binaries
RUN go vet ./...
RUN CGO_ENABLED=0 GOOS=linux go build -a -o /sparql-datalayer ./cmd/main.go

FROM scratch
# Copy the Pre-built binary file from the previous stage
COPY --from=builder /sparql-datalayer .
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Expose port 8090 to the outside world
EXPOSE 8090

# Command to run the executable
CMD ["./sparql-datalayer"]