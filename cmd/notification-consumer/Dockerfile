FROM golang:1.24-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/notification-consumer ./cmd/notification-consumer

FROM alpine:3.21

RUN adduser -D -H appuser
USER appuser

COPY --from=build /out/notification-consumer /app

ENTRYPOINT ["/app"]
