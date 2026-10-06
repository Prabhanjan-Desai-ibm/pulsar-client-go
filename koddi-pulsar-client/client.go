// Package koddi provides a pre-configured Pulsar client tuned for Koddi's
// production environment. It wraps github.com/apache/pulsar-client-go with:
//
//   - Structured JSON logging (every internal client log → stdout → Splunk)
//   - Debug interceptors on producers and consumers
//   - Tuned keepalive / connection settings
//   - Disconnection events surfaced through OnConsumerClose / log output
package koddi

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/backoff"
)

// ── Config ────────────────────────────────────────────────────────────────────

// Config holds all settings for the Koddi Pulsar client.
// Sane defaults are applied for anything you leave at zero value.
type Config struct {
	// BrokerURL is the Pulsar service URL.
	// Example: "pulsar+ssl://pulsar-kd-prod-aws-euwest2-proxy:6651"
	BrokerURL string

	// ClusterName is a label added to every log line so you can filter
	// in Splunk by cluster.
	// Example: "aws.eu-west-2.prod-koddi-aws-euwest2"
	ClusterName string

	// TLSTrustCertsFilePath is the path to your CA bundle.
	// Leave empty if not using TLS.
	TLSTrustCertsFilePath string

	// JWTToken is the bearer token for authentication.
	// Leave empty if not using token auth.
	JWTToken string

	// KeepAliveInterval controls how often PING is sent to the proxy/broker.
	// Default: 10s  (sends PING every 10s, well within the proxy's 30s window)
	KeepAliveInterval time.Duration

	// ConnectionTimeout is how long a TCP dial attempt may take.
	// Default: 15s
	ConnectionTimeout time.Duration

	// MaxConnectionsPerBroker is how many TCP connections to maintain per broker.
	// More connections = PING contends less with message sends.
	// Default: 3
	MaxConnectionsPerBroker int

	// TLSAllowInsecureConnection disables TLS certificate verification.
	// Only use this for internal cluster testing where the cert CN does not
	// match the broker hostname (e.g. connecting via internal k8s DNS).
	// Never set this in production.
	TLSAllowInsecureConnection bool
}

func (c *Config) applyDefaults() {
	if c.KeepAliveInterval == 0 {
		c.KeepAliveInterval = 10 * time.Second
	}
	if c.ConnectionTimeout == 0 {
		c.ConnectionTimeout = 15 * time.Second
	}
	if c.MaxConnectionsPerBroker == 0 {
		c.MaxConnectionsPerBroker = 3
	}
}

// ── Client ────────────────────────────────────────────────────────────────────

// Client is the Koddi wrapper around pulsar.Client.
// Create one per application process and reuse it.
type Client struct {
	inner  pulsar.Client
	log    *debugLogger
	stopCh chan struct{} // closed by Close() to stop background goroutines
}

// NewClient creates a new Koddi Pulsar client with all debug settings applied.
//
// Example:
//
//	client, err := koddi.NewClient(koddi.Config{
//	    BrokerURL:             "pulsar+ssl://proxy:6651",
//	    ClusterName:           "aws.eu-west-2.prod",
//	    TLSTrustCertsFilePath: "/etc/ssl/certs/ca-certificates.crt",
//	    JWTToken:              os.Getenv("PULSAR_TOKEN"),
//	})
func NewClient(cfg Config) (*Client, error) {
	cfg.applyDefaults()

	log := newDebugLogger(cfg.ClusterName).(*debugLogger)

	opts := pulsar.ClientOptions{
		URL:                     cfg.BrokerURL,
		KeepAliveInterval:       cfg.KeepAliveInterval,
		ConnectionTimeout:       cfg.ConnectionTimeout,
		MaxConnectionsPerBroker: cfg.MaxConnectionsPerBroker,
		Logger:                  log,
		// Tag this client in broker stats so you can tell it apart
		// from the standard go client
		Description: "koddi-debug-client",
	}

	if cfg.TLSTrustCertsFilePath != "" {
		opts.TLSTrustCertsFilePath = cfg.TLSTrustCertsFilePath
	}
	if cfg.TLSAllowInsecureConnection {
		opts.TLSAllowInsecureConnection = true
	}

	if cfg.JWTToken != "" {
		opts.Authentication = pulsar.NewAuthenticationToken(cfg.JWTToken)
	}

	inner, err := pulsar.NewClient(opts)
	if err != nil {
		log.write("error", "KODDI failed to create pulsar client: "+err.Error())
		return nil, err
	}

	log.write("info", "KODDI pulsar client created")
	return &Client{inner: inner, log: log, stopCh: make(chan struct{})}, nil
}

// Close shuts down the client and releases all resources.
func (c *Client) Close() {
	c.log.write("info", "KODDI pulsar client closing")
	close(c.stopCh)
	c.inner.Close()
}

// ── Producer ──────────────────────────────────────────────────────────────────

// ProducerConfig holds producer-specific settings.
type ProducerConfig struct {
	// Topic is required.
	Topic string

	// Name is optional. If empty the broker assigns one.
	Name string

	// SendTimeout overrides the default 30s send timeout.
	// Set to -1 to disable.
	SendTimeout time.Duration
}

// latencyFlushInterval is how often the background goroutine emits a latency
// snapshot. One line per minute — minimal log volume.
const latencyFlushInterval = 60 * time.Second

// NewProducer creates a producer with the latency-tracking interceptor and
// tuned backoff attached. A background goroutine emits a latency snapshot
// (p50/p99/max/avg) every 60s — stopped when the client is closed.
//
// Example:
//
//	producer, err := client.NewProducer(koddi.ProducerConfig{Topic: "persistent://tenant/ns/events"})
func (c *Client) NewProducer(cfg ProducerConfig) (pulsar.Producer, error) {
	c.log.write("info", "KODDI creating producer | topic="+cfg.Topic)

	interceptor := newProducerInterceptor(c.log, cfg.Topic)

	opts := pulsar.ProducerOptions{
		Topic: cfg.Topic,

		// Latency-tracking interceptor — records RTT per message, logs snapshot
		// every 60s and immediately on any send failure.
		Interceptors: pulsar.ProducerInterceptors{interceptor},

		// Faster initial reconnect attempts.
		BackOffPolicyFunc: func() backoff.Policy {
			return backoff.NewDefaultBackoffWithInitialBackOff(200 * time.Millisecond)
		},
	}

	if cfg.Name != "" {
		opts.Name = cfg.Name
	}
	if cfg.SendTimeout != 0 {
		opts.SendTimeout = cfg.SendTimeout
	}

	producer, err := c.inner.CreateProducer(opts)
	if err != nil {
		c.log.write("error", "KODDI failed to create producer | topic="+cfg.Topic+" error="+err.Error())
		return nil, err
	}

	c.log.write("info", "KODDI producer created | topic="+cfg.Topic+" name="+producer.Name())

	// Background goroutine: flush latency snapshot every 60s.
	go c.runLatencyFlusher(interceptor, cfg.Topic)

	return producer, nil
}

// runLatencyFlusher periodically logs a latency snapshot for the given producer.
// Exits cleanly when the client is closed.
func (c *Client) runLatencyFlusher(interceptor *debugProducerInterceptor, topic string) {
	ticker := time.NewTicker(latencyFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			if s := interceptor.latency.snapshot(); s != nil {
				s.log(c.log, topic, "periodic")
			}
		}
	}
}

// ── Consumer ──────────────────────────────────────────────────────────────────

// ConsumerConfig holds consumer-specific settings.
type ConsumerConfig struct {
	// Topic is required (single topic).
	Topic string

	// SubscriptionName is required.
	SubscriptionName string

	// SubscriptionType defaults to pulsar.Shared if not set.
	SubscriptionType pulsar.SubscriptionType

	// ReceiverQueueSize defaults to 1000 if not set.
	ReceiverQueueSize int
}

// NewConsumer creates a consumer with the debug interceptor attached.
// The OnConsumerClose hook will log the exact reason if the consumer
// is ever closed internally (e.g. reconnect exhausted, auth error).
//
// Example:
//
//	consumer, err := client.NewConsumer(koddi.ConsumerConfig{
//	    Topic:            "persistent://tenant/ns/events",
//	    SubscriptionName: "koddi-sub",
//	})
func (c *Client) NewConsumer(cfg ConsumerConfig) (pulsar.Consumer, error) {
	c.log.write("info", "KODDI creating consumer | topic="+cfg.Topic+" subscription="+cfg.SubscriptionName)

	subType := cfg.SubscriptionType // zero value = Exclusive, which is pulsar default

	opts := pulsar.ConsumerOptions{
		Topic:            cfg.Topic,
		SubscriptionName: cfg.SubscriptionName,
		Type:             subType,

		// Debug interceptor — logs BeforeConsume, OnAcknowledge,
		// OnNegativeAcksSend and crucially OnConsumerClose with the error cause
		Interceptors: pulsar.ConsumerInterceptors{
			newConsumerInterceptor(c.log),
		},

		// Faster initial reconnect (same policy as producer)
		BackOffPolicyFunc: func() backoff.Policy {
			return backoff.NewDefaultBackoffWithInitialBackOff(200 * time.Millisecond)
		},
	}

	if cfg.ReceiverQueueSize > 0 {
		opts.ReceiverQueueSize = cfg.ReceiverQueueSize
	}

	consumer, err := c.inner.Subscribe(opts)
	if err != nil {
		c.log.write("error", "KODDI failed to create consumer | topic="+cfg.Topic+" error="+err.Error())
		return nil, err
	}

	c.log.write("info", "KODDI consumer created | topic="+cfg.Topic+" subscription="+cfg.SubscriptionName)
	return consumer, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// Send is a convenience wrapper around producer.Send that logs send errors.
func Send(ctx context.Context, producer pulsar.Producer, payload []byte) (pulsar.MessageID, error) {
	msgID, err := producer.Send(ctx, &pulsar.ProducerMessage{Payload: payload})
	if err != nil {
		return nil, err
	}
	return msgID, nil
}
