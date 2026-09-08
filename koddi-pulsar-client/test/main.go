// This is your LOCAL TEST program.
// Run this against a real Pulsar broker to verify the custom client works
// and all debug logs appear correctly before handing it to Koddi.
//
// How to run:
//   cd koddi-pulsar-client/test
//   PULSAR_URL=pulsar://localhost:6650 go run main.go
//
// Disconnection simulation:
//   While this is running, stop the broker (Ctrl+C in Docker terminal),
//   wait a few seconds, then restart it. Watch the reconnect logs fire.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	koddi "github.com/Prabhanjan-Desai-ibm/pulsar-client-go/koddi-pulsar-client"
	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	// ── 1. Read broker URL from env or use localhost default ──────────────────
	brokerURL := os.Getenv("PULSAR_URL")
	if brokerURL == "" {
		brokerURL = "pulsar://localhost:6650"
	}
	jwtToken := os.Getenv("PULSAR_TOKEN") // leave empty if no auth needed

	fmt.Println("=== KODDI PULSAR CLIENT DISCONNECTION TEST ===")
	fmt.Println("Broker:", brokerURL)
	fmt.Println(">> Kill the broker now to see disconnection logs, then restart it.")
	fmt.Println(">> Ctrl+C to stop the test.")
	fmt.Println("==============================================")

	// ── 2. Create the Koddi client ────────────────────────────────────────────
	client, err := koddi.NewClient(koddi.Config{
		BrokerURL:   brokerURL,
		ClusterName: "local-test",
		JWTToken:    jwtToken,
	})
	if err != nil {
		fmt.Println("FAILED to create client:", err)
		os.Exit(1)
	}
	defer client.Close()

	topic := "persistent://public/default/koddi-test-topic"

	// ── 3. Create consumer first ──────────────────────────────────────────────
	consumer, err := client.NewConsumer(koddi.ConsumerConfig{
		Topic:            topic,
		SubscriptionName: "koddi-test-sub",
		SubscriptionType: pulsar.Shared,
	})
	if err != nil {
		fmt.Println("FAILED to create consumer:", err)
		os.Exit(1)
	}
	defer consumer.Close()

	// ── 4. Create producer ────────────────────────────────────────────────────
	producer, err := client.NewProducer(koddi.ProducerConfig{
		Topic: topic,
	})
	if err != nil {
		fmt.Println("FAILED to create producer:", err)
		os.Exit(1)
	}
	defer producer.Close()

	fmt.Println("\n[RUNNING] Sending one message every 3 seconds. Kill the broker to simulate disconnection.\n")

	ctx := context.Background()
	i := 0
	for {
		i++
		payload := fmt.Sprintf("koddi-msg-%d", i)

		// Send — will fail during disconnection, succeed after reconnect
		sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msgID, err := koddi.Send(sendCtx, producer, []byte(payload))
		cancel()
		if err != nil {
			fmt.Printf("[%s] ✗ Send #%d FAILED: %v\n", timestamp(), i, err)
		} else {
			fmt.Printf("[%s] ✓ Send #%d OK — msgID: %s\n", timestamp(), i, msgID.String())
		}

		// Receive — drain whatever arrived
		for {
			recvCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			msg, err := consumer.Receive(recvCtx)
			cancel()
			if err != nil {
				break // nothing waiting, move on
			}
			fmt.Printf("[%s] ✓ Recv: %s\n", timestamp(), string(msg.Payload()))
			consumer.Ack(msg)
		}

		time.Sleep(3 * time.Second)
	}
}

func timestamp() string {
	return time.Now().Format("15:04:05")
}
