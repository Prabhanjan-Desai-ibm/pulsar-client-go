// This is your LOCAL TEST program.
// Run this against a real Pulsar broker to verify the custom client works
// and all debug logs appear correctly before handing it to Koddi.
//
// How to run:
//   cd koddi-pulsar-client/test
//   PULSAR_URL=pulsar://localhost:6650 go run main.go
//
// What it does:
//   1. Creates a Koddi client
//   2. Creates a producer on "test-topic"
//   3. Sends 5 messages and checks they are acked
//   4. Creates a consumer on "test-topic"
//   5. Receives 5 messages and acks them
//   6. Prints all JSON logs to stdout — this is what Splunk will see
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	koddi "github.com/Prabhanjan-Desai-ibm/koddi-pulsar-client"
	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	// ── 1. Read broker URL from env or use localhost default ──────────────────
	brokerURL := os.Getenv("PULSAR_URL")
	if brokerURL == "" {
		brokerURL = "pulsar://localhost:6650"
	}
	jwtToken := os.Getenv("PULSAR_TOKEN") // leave empty if no auth needed

	fmt.Println("=== KODDI PULSAR CLIENT TEST ===")
	fmt.Println("Broker:", brokerURL)
	fmt.Println("Watch the JSON lines below — these are what Splunk will see")
	fmt.Println("=================================")

	// ── 2. Create the Koddi client ────────────────────────────────────────────
	client, err := koddi.NewClient(koddi.Config{
		BrokerURL:   brokerURL,
		ClusterName: "local-test",                      // shows up in every log line
		JWTToken:    jwtToken,
		// These are the tuned defaults baked in:
		// KeepAliveInterval:       10s
		// ConnectionTimeout:       15s
		// MaxConnectionsPerBroker: 3
	})
	if err != nil {
		fmt.Println("FAILED to create client:", err)
		os.Exit(1)
	}
	defer client.Close()

	topic := "persistent://public/default/koddi-test-topic"

	// ── 3. Create producer and send 5 messages ────────────────────────────────
	fmt.Println("\n--- PRODUCER TEST ---")

	producer, err := client.NewProducer(koddi.ProducerConfig{
		Topic: topic,
	})
	if err != nil {
		fmt.Println("FAILED to create producer:", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		payload := fmt.Sprintf("koddi-test-message-%d", i)
		msgID, err := koddi.Send(ctx, producer, []byte(payload))
		if err != nil {
			fmt.Printf("FAILED to send message %d: %v\n", i, err)
			os.Exit(1)
		}
		fmt.Printf("✓ Sent message %d — msgID: %s\n", i, msgID.String())
	}

	// ── 4. Create consumer and receive 5 messages ─────────────────────────────
	fmt.Println("\n--- CONSUMER TEST ---")

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

	for i := 1; i <= 5; i++ {
		// Use a timeout so the test doesn't hang if messages weren't produced
		receiveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		msg, err := consumer.Receive(receiveCtx)
		cancel()

		if err != nil {
			fmt.Printf("FAILED to receive message %d: %v\n", i, err)
			os.Exit(1)
		}

		fmt.Printf("✓ Received message %d — payload: %s\n", i, string(msg.Payload()))
		consumer.Ack(msg)
	}

	fmt.Println("\n=== ALL TESTS PASSED ===")
	fmt.Println("Check the JSON lines above — every line with 'source':'koddi-pulsar-client'")
	fmt.Println("is what will appear in Splunk when Koddi runs this client.")
}
