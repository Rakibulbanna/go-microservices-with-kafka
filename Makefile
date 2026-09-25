.PHONY: up down logs build test lint topics clean restart \
       order payment notification \
       cluster-up cluster-down \
       order-test payment-test notification-test

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

build:
	cd services/order-service && go build -o ../../bin/order-service ./cmd/
	cd services/payment-service && go build -o ../../bin/payment-service ./cmd/
	cd services/notification-service && go build -o ../../bin/notification-service ./cmd/

test:
	cd services/order-service && go test ./...
	cd services/payment-service && go test ./...
	cd services/notification-service && go test ./...

lint:
	cd services/order-service && golangci-lint run ./...
	cd services/payment-service && golangci-lint run ./...
	cd services/notification-service && golangci-lint run ./...

topics:
	docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

topic-describe:
	docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $(TOPIC)

consumer-groups:
	docker exec kafka-broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

consumer-group-describe:
	docker exec kafka-broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $(GROUP)

clean:
	docker compose down -v
	rm -rf bin/

restart:
	docker compose restart

order:
	cd services/order-service && ~/go/bin/air

payment:
	cd services/payment-service && ~/go/bin/air

notification:
	cd services/notification-service && ~/go/bin/air

cluster-up:
	docker compose -f docker-compose.cluster.yml up -d

cluster-down:
	docker compose -f docker-compose.cluster.yml down

order-test:
	cd services/order-service && go test -v ./...

payment-test:
	cd services/payment-service && go test -v ./...

notification-test:
	cd services/notification-service && go test -v ./...
