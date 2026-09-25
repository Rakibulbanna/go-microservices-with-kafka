FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /payment-service ./services/payment-service/cmd/

FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app

COPY --from=builder /payment-service .

EXPOSE 8082

CMD ["./payment-service"]
