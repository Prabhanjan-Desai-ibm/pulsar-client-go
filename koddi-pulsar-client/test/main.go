// Disconnection demo — run this binary on the bastion pod against the dev cluster.
//
// Build for Linux (from your Mac):
//
//	cd koddi-pulsar-client/test
//	GOOS=linux GOARCH=amd64 go build -o koddi-test .
//
// Copy to bastion:
//
//	kubectl cp koddi-test pulsar/<bastion-pod>:/tmp/koddi-test
//	kubectl exec -n pulsar <bastion-pod> -- chmod +x /tmp/koddi-test
//
// Run on bastion:
//
//	export PULSAR_URL=pulsar+ssl://astradev-aws-proxy.pulsar.svc.cluster.local:6651
//	export PULSAR_CLUSTER=astradev-aws
//	export PULSAR_TOKEN=$(cat /pulsar/token-superuser-stripped.jwt)
//	export PULSAR_TLS_SKIP_VERIFY=true
//	export PULSAR_TOPIC=persistent://koddi-dev/debug/koddi-test-topic
//	/tmp/koddi-test
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
	brokerURL := env("PULSAR_URL", "")
	clusterName := env("PULSAR_CLUSTER", "")
	jwtToken := env("PULSAR_TOKEN", "")
	tlsCert := env("PULSAR_TLS_CERT", "")
	tlsSkipVerify := os.Getenv("PULSAR_TLS_SKIP_VERIFY") == "true"
	topic := env("PULSAR_TOPIC", "persistent://public/default/koddi-test-topic")

	if brokerURL == "" {
		fmt.Println("ERROR: PULSAR_URL is not set")
		fmt.Println("  export PULSAR_URL=pulsar+ssl://<proxy>:6651")
		os.Exit(1)
	}
	if clusterName == "" {
		fmt.Println("ERROR: PULSAR_CLUSTER is not set")
		fmt.Println("  export PULSAR_CLUSTER=astradev-aws")
		os.Exit(1)
	}

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║     KODDI PULSAR CLIENT — DISCONNECTION DEMO     ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Printf("Broker  : %s\n", brokerURL)
	fmt.Printf("Cluster : %s\n", clusterName)
	fmt.Printf("Topic   : %s\n", topic)
	fmt.Printf("TLS skip: %v\n\n", tlsSkipVerify)

	client, err := koddi.NewClient(koddi.Config{
		BrokerURL:                  brokerURL,
		ClusterName:                clusterName,
		JWTToken:                   jwtToken,
		TLSTrustCertsFilePath:      tlsCert,
		TLSAllowInsecureConnection: tlsSkipVerify,
	})
	if err != nil {
		fmt.Println("FAILED to create client:", err)
		os.Exit(1)
	}
	defer client.Close()

	consumer, err := client.NewConsumer(koddi.ConsumerConfig{
		Topic:            topic,
		SubscriptionName: "koddi-demo-sub",
		SubscriptionType: pulsar.Shared,
	})
	if err != nil {
		fmt.Println("FAILED to create consumer:", err)
		os.Exit(1)
	}
	defer consumer.Close()

	producer, err := client.NewProducer(koddi.ProducerConfig{
		Topic: topic,
	})
	if err != nil {
		fmt.Println("FAILED to create producer:", err)
		os.Exit(1)
	}
	defer producer.Close()

	fmt.Println("[RUNNING] Sending every 3s. Ctrl+C to stop.")
	fmt.Println("──────────────────────────────────────────────────")

	ctx := context.Background()
	i := 0
	for {
		i++
		sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msgID, err := koddi.Send(sendCtx, producer, []byte(fmt.Sprintf("koddi-demo-msg-%d", i)))
		cancel()

		ts := time.Now().Format("15:04:05")
		if err != nil {
			fmt.Printf("[%s] ✗ Send #%d FAILED: %v\n", ts, i, err)
		} else {
			fmt.Printf("[%s] ✓ Send #%d OK  msgID=%s\n", ts, i, msgID.String())
		}

		for {
			recvCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			msg, err := consumer.Receive(recvCtx)
			cancel()
			if err != nil {
				break
			}
			fmt.Printf("[%s] ✓ Recv: %s\n", time.Now().Format("15:04:05"), string(msg.Payload()))
			consumer.Ack(msg)
		}

		time.Sleep(3 * time.Second)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
