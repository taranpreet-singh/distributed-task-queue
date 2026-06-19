FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/app ./cmd/${CMD}

FROM alpine:3.21
COPY --from=builder /bin/app /bin/app
ENTRYPOINT ["/bin/app"]
