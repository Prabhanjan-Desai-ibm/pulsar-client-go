# koddi-pulsar-client

**This is the official Apache Pulsar Go client — with one focused addition: structured JSON logging for disconnection visibility.**

The underlying client is [`apache/pulsar-client-go v0.21.1`](https://github.com/apache/pulsar-client-go) — unchanged. All Pulsar features, APIs, and behaviour are identical. The only difference is that disconnection events, reconnect timing, and publish latency are now surfaced as structured JSON to stdout, so they flow into Splunk for alerting.

> **If you already use `apache/pulsar-client-go` directly, switching to this client requires no code changes beyond the import path and config struct.**

---

## Why this exists

The upstream Pulsar Go client gives you no visibility when a connection drops. You don't know:
- Why it dropped (network cut? broker restart? auth expiry?)
- How long it was down
- Whether messages were lost
- What the publish latency looked like before and after

This client adds exactly that — nothing more.

---

## What is and isn't changed

| | Changed? |
|---|---|
| Pulsar protocol behaviour | ❌ No |
| Producer / Consumer / Admin APIs | ❌ No |
| Message delivery guarantees | ❌ No |
| Reconnect logic | ❌ No |
| Performance | ❌ No — logging is suppressed on the hot path |
| Disconnection log output | ✅ Yes — structured JSON added |
| Publish latency tracking | ✅ Yes — p50/p99/max/avg snapshot every 60s |
| Internal library noise | ✅ Yes — suppressed (was ~40 lines/min, now ~1/min) |

---

## Installation

Add to your `go.mod`:

```go
require github.com/Prabhanjan-Desai-ibm/pulsar-client-go/koddi-pulsar-client v1.0.7

replace github.com/apache/pulsar-client-go => github.com/Prabhanjan-Desai-ibm/pulsar-client-go v0.21.1
```

Then run:

```bash
go get github.com/Prabhanjan-Desai-ibm/pulsar-client-go/koddi-pulsar-client@v1.0.7
go mod tidy
```

> The `replace` directive is required. It tells Go to use the Koddi fork of the Pulsar client
> (which contains the improved disconnection logging) instead of the upstream Apache version.

---

## Usage

```go
package main

import (
    "context"
    "fmt"
    "os"

    koddi "github.com/Prabhanjan-Desai-ibm/pulsar-client-go/koddi-pulsar-client"
    "github.com/apache/pulsar-client-go/pulsar"
)

func main() {
    client, err := koddi.NewClient(koddi.Config{
        BrokerURL:             "pulsar+ssl://your-proxy:6651",
        ClusterName:           "aws.eu-west-2.prod-koddi",
        TLSTrustCertsFilePath: "/etc/ssl/certs/ca-certificates.crt",
        JWTToken:              os.Getenv("PULSAR_TOKEN"),
    })
    if err != nil {
        panic(err)
    }
    defer client.Close()

    producer, err := client.NewProducer(koddi.ProducerConfig{
        Topic: "persistent://tenant/namespace/my-topic",
    })
    if err != nil {
        panic(err)
    }
    defer producer.Close()

    msgID, err := koddi.Send(context.Background(), producer, []byte("hello"))
    if err != nil {
        panic(err)
    }
    fmt.Println("sent:", msgID)

    consumer, err := client.NewConsumer(koddi.ConsumerConfig{
        Topic:            "persistent://tenant/namespace/my-topic",
        SubscriptionName: "koddi-sub",
        SubscriptionType: pulsar.Shared,
    })
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    for {
        msg, err := consumer.Receive(context.Background())
        if err != nil {
            break
        }
        fmt.Println("received:", string(msg.Payload()))
        consumer.Ack(msg)
    }
}
```

---

## Config reference

| Field | Default | Description |
|---|---|---|
| `BrokerURL` | required | Pulsar service URL e.g. `pulsar+ssl://proxy:6651` |
| `ClusterName` | `""` | Stamped on every log line — use for Splunk filtering |
| `TLSTrustCertsFilePath` | `""` | Path to CA bundle for TLS verification |
| `JWTToken` | `""` | Bearer token for authentication |
| `KeepAliveInterval` | `10s` | How often PING is sent. Proxy idle timeout is typically 30s — 10s gives 3× margin |
| `ConnectionTimeout` | `15s` | TCP dial timeout per reconnect attempt |
| `MaxConnectionsPerBroker` | `3` | TCP connections per broker — reduces PING contention with message sends |
| `TLSAllowInsecureConnection` | `false` | Skip TLS cert verification. **Dev/test only — never use in production** |

---

## Log volume

Every log line is a JSON object with `source: "koddi-pulsar-client"` and `cluster: "<ClusterName>"` fields.

In normal healthy operation the client emits approximately **1 line per minute** (the latency snapshot). All internal Pulsar library noise (connection setup, handshake, PING/PONG) is suppressed. Only meaningful signals are surfaced.

| Scenario | Lines emitted |
|---|---|
| Healthy operation | ~1/min (latency snapshot) |
| Disconnect detected | 1 line immediately |
| Reconnect retry loop | 1 line per 30s retry |
| Recovery confirmed | 2 lines (producer + consumer) |
| Send failure | 2 lines (error + latency snapshot) |
| Message redelivery | 1 line per redelivery |

---

## Disconnection and reconnection logs

### Who caused the disconnect — the `side` field

Every disconnect is tagged so you immediately know which team to investigate:

| `side` value | Log message | Cause | Investigate |
|---|---|---|---|
| `side=client` | `Error reading from connection` | TCP dropped — network cut, proxy crash, pod restart | Infrastructure / k8s |
| `side=broker` | `Broker closed producer/consumer` | Broker sent explicit CLOSE — rebalance, topic deleted, auth expired | Pulsar ops |
| _(no side)_ | `Detected stale connection to broker` | Silent packet drop — PINGs unanswered for 20s | Network / firewall |

### Full disconnection lifecycle

| # | `msg` | `level` | Key fields | When it fires |
|---|---|---|---|---|
| 1 | `Error reading from connection` | **warn** | `error`, `side=client` | Network drop, TCP reset, EOF |
| 1 | `Broker closed producer: <id>` | **warn** | `side=broker` | Broker deliberately closed the producer |
| 1 | `Broker closed consumer: <id>` | **warn** | `side=broker` | Broker deliberately closed the consumer |
| 1 | `Detected stale connection to broker` | **warn** | `silent_for_seconds`, `threshold_seconds` | No PONG received for 2× keepAliveInterval |
| 2 | `Connection closing — notifying producers and consumers` | **warn** | `producers_affected`, `consumers_affected`, `write_queue_depth`, `write_queue_cap`, `last_ping_sent_ago_s`, `last_pong_received_ago_s` | Connection teardown |
| 3 | `Failed to reconnect to broker, will retry later.` | **warn** | `error`, `downtime_seconds`, `pending_messages` | Each failed reconnect attempt |
| 4 | `Reconnected producer to broker` | info | `downtime_seconds`, `pending_messages` | Producer recovery confirmed |
| 4 | `Reconnected consumer to broker` | info | `downtime_seconds`, `subscription` | Consumer recovery confirmed |

### Key fields on the disconnect log

| Field | What it tells you |
|---|---|
| `write_queue_depth` | Buffered sends waiting to go out at disconnect — if high, write loop was congested |
| `write_queue_cap` | Total channel capacity (256) — compare to depth for % full |
| `last_ping_sent_ago_s` | Seconds since last PING was sent — if large, PING loop was stalled |
| `last_pong_received_ago_s` | Seconds since broker last replied — if ≈ 2× ping value, broker went silent before disconnect |
| `downtime_seconds` | Exact recovery time — measurable SLA |
| `pending_messages` | Messages buffered during outage — confirms zero message loss on reconnect |

### Splunk searches

```
# Any disconnection
source="koddi-pulsar-client" level=warn

# Network/infra issue — page infrastructure team
source="koddi-pulsar-client" side=client

# Broker kicked us out — page Pulsar ops
source="koddi-pulsar-client" side=broker

# Silent network drop — broker went silent
source="koddi-pulsar-client" msg="Detected stale connection to broker"

# SLA breach — downtime over 30 seconds
source="koddi-pulsar-client" downtime_seconds>30

# Queue was congested at disconnect
source="koddi-pulsar-client" write_queue_depth>50

# Recoveries — check downtime_seconds for SLA
source="koddi-pulsar-client" msg="Reconnected producer to broker"
source="koddi-pulsar-client" msg="Reconnected consumer to broker"

# Consumer gave up reconnecting — needs immediate alert
source="koddi-pulsar-client" msg="KODDI consumer CLOSED by internal error"
```

### Example log output during a proxy restart

```json
{"level":"warn","msg":"Connection closing — notifying producers and consumers","producers_affected":1,"consumers_affected":1,"write_queue_depth":0,"write_queue_cap":256,"last_ping_sent_ago_s":9.57,"last_pong_received_ago_s":9.57,"cluster":"astradev-aws","source":"koddi-pulsar-client"}
{"level":"warn","msg":"Failed to reconnect to broker, will retry later.","downtime_seconds":30.0,"pending_messages":4,"cluster":"astradev-aws","source":"koddi-pulsar-client"}
{"level":"info","msg":"Reconnected producer to broker","downtime_seconds":68.6,"pending_messages":4,"cluster":"astradev-aws","source":"koddi-pulsar-client"}
{"level":"info","msg":"Reconnected consumer to broker","downtime_seconds":68.9,"cluster":"astradev-aws","source":"koddi-pulsar-client"}
```

### Example log output during a silent network drop (stale PING detection)

```json
{"level":"warn","msg":"Detected stale connection to broker","silent_for_seconds":20.1,"threshold_seconds":20.0,"broker":"pulsar+ssl://proxy:6651","cluster":"astradev-aws","source":"koddi-pulsar-client"}
{"level":"warn","msg":"Connection closing — notifying producers and consumers","producers_affected":1,"consumers_affected":1,"write_queue_depth":2,"write_queue_cap":256,"last_ping_sent_ago_s":10.1,"last_pong_received_ago_s":20.4,"cluster":"astradev-aws","source":"koddi-pulsar-client"}
```

> `last_pong_received_ago_s` ≈ 2× `last_ping_sent_ago_s` means two PINGs were sent with no PONG reply — the broker went silent before the connection was torn down.

---

## Publish latency tracking

Every producer created via `NewProducer` has an automatic latency tracker that measures the round-trip time between `Send()` and the broker ACK. No per-message logs are emitted — only two occasions produce output:

| When | Log line |
|---|---|
| Every **60 seconds** | Periodic snapshot over the last ≤ 1000 messages |
| Immediately on **send failure** | Snapshot dumped alongside the error for context |

### Example latency snapshot log

```json
{"level":"info","msg":"KODDI publish latency periodic | topic=persistent://... count=32 p50=7.02ms p99=19.69ms max=19.69ms avg=7.58ms","cluster":"astradev-aws","source":"koddi-pulsar-client"}
```

```json
{"level":"error","msg":"KODDI producer send FAILED | topic=persistent://... payload_bytes=64","cluster":"astradev-aws","source":"koddi-pulsar-client"}
{"level":"info","msg":"KODDI publish latency at send failure | topic=persistent://... count=32 p50=7.02ms p99=19.69ms max=19.69ms avg=7.58ms","cluster":"astradev-aws","source":"koddi-pulsar-client"}
```

### Splunk queries for latency

```
# All latency snapshots
source="koddi-pulsar-client" msg="KODDI publish latency*"

# Snapshots triggered by a send failure (correlation with disconnect)
source="koddi-pulsar-client" msg="KODDI publish latency at send failure"

# Periodic snapshots only (steady-state monitoring)
source="koddi-pulsar-client" msg="KODDI publish latency periodic"

# High p99 alert (adjust threshold as needed)
source="koddi-pulsar-client" msg="KODDI publish latency*" | rex "p99=(?<p99_ms>[0-9.]+)ms" | where p99_ms > 100
```

---

## File structure

```
koddi-pulsar-client/
├── go.mod          — module definition, replace directive for the fork
├── go.sum
├── client.go       — Config, NewClient, NewProducer, NewConsumer, Send
├── logger.go       — JSON stdout logger implementing pulsar/log.Logger
├── interceptors.go — producer latency stamping + consumer redelivery/close hooks
├── stats.go        — latencyTracker ring buffer, percentile computation, fmtDur
└── README.md
```
