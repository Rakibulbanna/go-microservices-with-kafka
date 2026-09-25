#!/bin/bash
set -e

BOOTSTRAP_SERVER="kafka:9092"
KAFKA_BIN="/opt/kafka/bin"

echo "Waiting for Kafka to be ready..."
until $KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list >/dev/null 2>&1; do
  sleep 2
done

echo "Creating topics..."

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic orders.v1 --partitions 3 --replication-factor 1

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic payments.v1 --partitions 3 --replication-factor 1

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic notifications.v1 --partitions 3 --replication-factor 1

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic orders.retry.v1 --partitions 3 --replication-factor 1

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic orders.dlt.v1 --partitions 3 --replication-factor 1

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic payments.retry.v1 --partitions 3 --replication-factor 1

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic payments.dlt.v1 --partitions 3 --replication-factor 1

echo "Topics created successfully!"
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list
