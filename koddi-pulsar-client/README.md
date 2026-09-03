# koddi-pulsar-client

A pre-configured Pulsar Go client for Koddi's production clusters.  
Drop-in wrapper around `apache/pulsar-client-go` with:

- **Structured JSON logs** — every internal client event (reconnect, stale connection, send failure) goes to stdout as JSON, ready for Splunk
- **Debug interceptors** — per-message hooks on send latency, ack failures, nack spikes, and consumer close cause
- **Tuned defaults** — `KeepAliveInterval=10s`, `ConnectionTimeout=15s`, `MaxConnectionsPerBroker=3`

---

## Installation

```bash
go get github.com/your-org/koddi-pulsar-client
```

---

## Usage

```go
package main

import (
    "context"
    "fmt"
    "os"

    koddi "github.com/your-org/koddi-pulsar-client"
)

func main() {
    // 1. Create client
    client, err := koddi.NewClient(koddi.Config{
        BrokerURL:             "pulsar+ssl://pulsar-kd-prod-aws-euwest2-proxy:6651",
        ClusterName:           "aws.eu-west-2.prod-koddi-aws-euwest2",
        TLSTrustCertsFilePath: "/etc/ssl/certs/ca-certificates.crt",
        JWTToken:              os.Getenv("PULSAR_TOKEN"),
    })
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // 2. Create producer
    producer, err := client.NewProducer(koddi.ProducerConfig{
        Topic: "persistent://public/default/my-topic",
    })
    if err != nil {
        panic(err)
    }
    defer producer.Close()

    // 3. Send a message
    msgID, err := koddi.Send(context.Background(), producer, []byte("hello"))
    if err != nil {
        panic(err)
    }
    fmt.Println("sent:", msgID)

    // 4. Create consumer
    consumer, err := client.NewConsumer(koddi.ConsumerConfig{
        Topic:            "persistent://public/default/my-topic",
        SubscriptionName: "koddi-sub",
        SubscriptionType: pulsar.Shared,
    })
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    // 5. Receive messages
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

## What you will see in Splunk

Every log line is a JSON object. Examples:

**Client created:**
```json
{"ts":"2024-01-15T10:30:00Z","level":"info","source":"koddi-pulsar-client","cluster":"aws.eu-west-2.prod","msg":"KODDI pulsar client created"}
```

**Reconnection attempt (from internal client logs):**
```json
{"ts":"2024-01-15T10:30:05Z","level":"info","source":"koddi-pulsar-client","cluster":"aws.eu-west-2.prod","remote_addr":"10.14.12.15:6651","msg":"Reconnecting to broker","delayReconnectTime":"200ms"}
```

**Reconnection succeeded:**
```json
{"ts":"2024-01-15T10:30:06Z","level":"info","source":"koddi-pulsar-client","cluster":"aws.eu-west-2.prod","msg":"Reconnected consumer to broker"}
```

**Consumer closed due to error (the most important one):**
```json
{"ts":"2024-01-15T10:35:00Z","level":"error","source":"koddi-pulsar-client","cluster":"aws.eu-west-2.prod","msg":"KODDI consumer CLOSED by internal error | subscription=koddi-sub cause=max retry attempts reached for reconnecting to broker"}
```

**Stale connection detected:**
```json
{"ts":"2024-01-15T10:35:00Z","level":"warn","source":"koddi-pulsar-client","cluster":"aws.eu-west-2.prod","remote_addr":"10.14.12.15:6651","msg":"Detected stale connection to broker"}
```

---

## Config reference

| Field | Default | What it does |
|---|---|---|
| `BrokerURL` | required | Pulsar service URL |
| `ClusterName` | `""` | Label on every log line for Splunk filtering |
| `TLSTrustCertsFilePath` | `""` | CA bundle path for TLS |
| `JWTToken` | `""` | Bearer token for authentication |
| `KeepAliveInterval` | `10s` | How often PING is sent (proxy timeout is 30s, so 10s gives 3× margin) |
| `ConnectionTimeout` | `15s` | TCP dial timeout during reconnect attempts |
| `MaxConnectionsPerBroker` | `3` | TCP connections per broker (reduces PING delay from write contention) |

---

## File structure

```
koddi-pulsar-client/
├── go.mod           — module definition, points at apache/pulsar-client-go
├── client.go        — Client, NewClient, NewProducer, NewConsumer, Config
├── logger.go        — JSON logger implementing pulsar/log.Logger
├── interceptors.go  — Producer and Consumer debug interceptors
└── README.md
```

---

## Switching to your fork

Once you have forked `apache/pulsar-client-go` and added the deep debug logs
inside `connection.go` and `producer_partition.go`, update `go.mod`:

```
require github.com/your-org/pulsar-client-go v0.0.1

replace github.com/apache/pulsar-client-go => github.com/your-org/pulsar-client-go v0.0.1
```

No changes needed in `client.go`, `logger.go`, or `interceptors.go`.
The fork and the official upstream have the same public API.
