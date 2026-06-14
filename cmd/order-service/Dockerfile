FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/order-service ./cmd/order-service

FROM alpine:3.21

RUN adduser -D -H appuser
USER appuser

COPY --from=build /out/order-service /app

EXPOSE 8080

ENTRYPOINT ["/app"]
