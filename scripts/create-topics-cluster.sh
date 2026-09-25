#!/bin/bash
set -e

BOOTSTRAP_SERVER="kafka-1:9092"

echo "Waiting for Kafka cluster to be ready..."
until kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list >/dev/null 2>&1; do
  sleep 2
done

echo "Creating topics with replication factor 3..."

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic orders.v1 --partitions 3 --replication-factor 3

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic payments.v1 --partitions 3 --replication-factor 3

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic notifications.v1 --partitions 3 --replication-factor 3

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic orders.retry.v1 --partitions 3 --replication-factor 3

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic orders.dlt.v1 --partitions 3 --replication-factor 3

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic payments.retry.v1 --partitions 3 --replication-factor 3

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
  --topic payments.dlt.v1 --partitions 3 --replication-factor 3

echo "Topics created successfully!"
kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list
