#!/bin/bash
# Toy producer: emits one JSON order event to the "demo" topic every 2 seconds.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-demo}"

echo "producing one event every 2s to topic '${TOPIC}' via ${BOOTSTRAP}"

i=0
while true; do
  i=$((i + 1))
  printf '{"order_id":%d,"status":"created","amount_cents":%d,"created_at":"%s"}\n' \
    "$i" "$((RANDOM % 9900 + 100))" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sleep 2
done | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"
