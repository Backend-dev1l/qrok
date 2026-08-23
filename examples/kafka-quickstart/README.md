# Kafka quickstart

End-to-end demo on a single machine: a test Kafka with a toy producer, the qrok server, an agent, and your "local service" — a tiny receiver that prints every event it gets.

```text
producer ──▶ Kafka (topic demo) ──▶ qrok agent ──▶ gateway ──▶ qrok listen ──▶ receiver :8888
                                                      │
                                                 event store ──▶ dashboard + replay
```

Prerequisites: Docker (Compose v2.20+), Go 1.26+. All commands run from the repo root.

## 1. Start Kafka + the producer

```bash
docker compose -f examples/kafka-quickstart/docker-compose.yml up -d --wait
```

This brings up the dev stack (Kafka, Postgres, MinIO, Redis) plus a producer that pushes one JSON order to the `demo` topic every 2 seconds. Check it:

```bash
docker compose -f examples/kafka-quickstart/docker-compose.yml logs -f producer
```

## 2. Prepare the control plane

```bash
go install github.com/pressly/goose/v3/cmd/goose@latest   # once, if you don't have goose
make migrate-up
make seed-dev     # prints agent token and project_id — keep both
```

## 3. Run the server

```bash
cp deploy/server.example.yaml qrok.yaml   # once
make run-server                            # HTTP :8080, gRPC :9090
```

## 4. Run the agent (new terminal)

Put the token from `seed-dev` into `deploy/agent.example.yaml` (`agent.token`), then:

```bash
make run-agent
```

The agent joins Kafka with its own consumer group and starts streaming the `demo` topic.

## 5. Run your "local service" (new terminal)

```bash
go run ./examples/kafka-quickstart/receiver
```

## 6. Connect the tunnel (new terminal)

```bash
go run ./cmd/qrok login --api http://127.0.0.1:8080 --project <project_id from seed-dev>
make run-listen
```

`login` opens an OAuth device flow: open the printed URL, enter the code, approve. Then `listen` connects and events start flowing — the receiver terminal prints a new order every 2 seconds:

```text
event id=01JD... topic=demo key="" offset=17 replay=false payload={"order_id":18,"status":"created",...}
```

## Replay

Open the dashboard at http://127.0.0.1:8080/dashboard/, pick any event, and hit Replay — it is re-delivered to the receiver with `X-Qrok-Replay: true`, without touching Kafka. Same thing from the CLI:

```bash
go run ./cmd/qrok replay <event-id> --api http://127.0.0.1:8080
```

## Cleanup

```bash
docker compose -f examples/kafka-quickstart/docker-compose.yml down -v
```

## Troubleshooting

- **WSL2 + Windows browser**: if `127.0.0.1:8080` does not open, use the IP from `hostname -I` (the server log prints it as `dashboard_wsl`).
- **No events in the receiver**: check the producer logs (step 1) and the agent terminal; the agent config reads from the oldest offset on first run (`kafka.start_from_oldest: true`).
- Already ran `make compose-up`? No problem — this compose file extends the same project and only adds the producer.
