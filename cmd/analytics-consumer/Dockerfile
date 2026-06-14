FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/analytics-consumer ./cmd/analytics-consumer

FROM alpine:3.21

RUN adduser -D -H appuser
USER appuser

COPY --from=build /out/analytics-consumer /app

ENTRYPOINT ["/app"]
