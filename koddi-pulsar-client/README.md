# koddi-pulsar-client

A pre-configured Pulsar Go client for Koddi's production clusters.  
Wraps [`apache/pulsar-client-go`](https://github.com/apache/pulsar-client-go) with structured JSON logging, debug interceptors, and tuned keepalive defaults — all logs go to stdout as JSON for Splunk ingestion.

---

## Installation

Add to your `go.mod`:

```go
require github.com/Prabhanjan-Desai-ibm/pulsar-client-go/koddi-pulsar-client v1.0.3

replace github.com/apache/pulsar-client-go => github.com/Prabhanjan-Desai-ibm/pulsar-client-go v0.21.1
```

Then run:

```bash
go get github.com/Prabhanjan-Desai-ibm/pulsar-client-go/koddi-pulsar-client@v1.0.3
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

## Disconnection and reconnection logs

Every log line is a JSON object with `source: "koddi-pulsar-client"` and `cluster: "<ClusterName>"` fields.

The full disconnection lifecycle produces these logs in order:

| # | `msg` | `level` | Key fields | When it fires |
|---|---|---|---|---|
| 1 | `Error reading from connection` | **warn** | `error`, `side=client` | Network drop, TCP reset, EOF |
| 1 | `Broker closed producer: <id>` | **warn** | `side=broker` | Broker deliberately closed the producer |
| 1 | `Broker closed consumer: <id>` | **warn** | `side=broker` | Broker deliberately closed the consumer |
| 2 | `Connection closing — notifying producers and consumers` | **warn** | `producers_affected`, `consumers_affected` | Connection teardown |
| 3 | `Failed to reconnect to broker, will retry later.` | **warn** | `error`, `downtime_seconds`, `pending_messages` | Each failed reconnect attempt |
| 4 | `Reconnected producer to broker` | info | `downtime_seconds`, `pending_messages` | Producer recovery confirmed |
| 4 | `Reconnected consumer to broker` | info | `downtime_seconds`, `subscription` | Consumer recovery confirmed |

### Splunk searches

```
# Any disconnection
source="koddi-pulsar-client" level=warn

# Network-side drop (EOF, connection reset, connection refused)
source="koddi-pulsar-client" msg="Error reading from connection" side=client

# Broker deliberately closed a producer or consumer
source="koddi-pulsar-client" side=broker

# Recoveries — check downtime_seconds for SLA
source="koddi-pulsar-client" msg="Reconnected producer to broker"
source="koddi-pulsar-client" msg="Reconnected consumer to broker"

# Consumer gave up reconnecting (needs alerting)
source="koddi-pulsar-client" msg="KODDI consumer CLOSED by internal error"
```

### Example log output during a proxy restart

```json
{"level":"warn","side":"client","error":"dial tcp 10.100.91.147:6651: connect: connection refused","msg":"Error reading from connection","cluster":"astradev-aws","source":"koddi-pulsar-client"}
{"level":"warn","msg":"Connection closing — notifying producers and consumers","producers_affected":1,"consumers_affected":1,"cluster":"astradev-aws","source":"koddi-pulsar-client"}
{"level":"info","msg":"Reconnected producer to broker","downtime_seconds":3.2,"pending_messages":0,"cluster":"astradev-aws","source":"koddi-pulsar-client"}
{"level":"info","msg":"Reconnected consumer to broker","downtime_seconds":3.2,"subscription":"koddi-sub","cluster":"astradev-aws","source":"koddi-pulsar-client"}
```

---

## File structure

```
koddi-pulsar-client/
├── go.mod          — module definition, replace directive for the fork
├── go.sum
├── client.go       — Config, NewClient, NewProducer, NewConsumer, Send
├── logger.go       — JSON stdout logger implementing pulsar/log.Logger
├── interceptors.go — per-message producer and consumer debug interceptors
└── README.md
```
