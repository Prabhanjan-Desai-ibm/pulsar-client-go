package koddi

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/pulsar-client-go/pulsar"
)

// ── Producer interceptor ──────────────────────────────────────────────────────
//
// Tracks publish round-trip latency (BeforeSend → OnSendAcknowledgement).
// Per-message debug logs are intentionally omitted to avoid log volume overhead.
// A latency snapshot (p50/p99/max/avg) is emitted:
//   - every 60s via a background goroutine in client.go
//   - immediately on any send failure

type debugProducerInterceptor struct {
	log     *debugLogger
	topic   string
	latency *latencyTracker

	// sentAt maps *ProducerMessage pointer → send time.
	// The Pulsar library passes the same pointer to both BeforeSend and
	// OnSendAcknowledgement so the pointer is stable for this pair.
	sentAtMu sync.Mutex
	sentAt   map[uintptr]time.Time
}

func newProducerInterceptor(log *debugLogger, topic string) *debugProducerInterceptor {
	return &debugProducerInterceptor{
		log:     log,
		topic:   topic,
		latency: &latencyTracker{},
		sentAt:  make(map[uintptr]time.Time),
	}
}

// BeforeSend stamps the send time — no log emitted (avoids per-message noise).
func (p *debugProducerInterceptor) BeforeSend(_ pulsar.Producer, msg *pulsar.ProducerMessage) {
	key := uintptr(unsafe.Pointer(msg)) //nolint:unsafeptr
	p.sentAtMu.Lock()
	p.sentAt[key] = time.Now()
	p.sentAtMu.Unlock()
}

// OnSendAcknowledgement records RTT on success; logs error + latency snapshot on failure.
func (p *debugProducerInterceptor) OnSendAcknowledgement(
	producer pulsar.Producer,
	msg *pulsar.ProducerMessage,
	msgID pulsar.MessageID,
) {
	key := uintptr(unsafe.Pointer(msg)) //nolint:unsafeptr
	p.sentAtMu.Lock()
	sentAt, ok := p.sentAt[key]
	delete(p.sentAt, key)
	p.sentAtMu.Unlock()

	if msgID == nil {
		// Send failed — log error and dump latency snapshot immediately so the
		// disconnect context is visible in the same log stream.
		p.log.write("error", fmt.Sprintf(
			"KODDI producer send FAILED | topic=%s payload_bytes=%d",
			producer.Topic(), len(msg.Payload),
		))
		if s := p.latency.snapshot(); s != nil {
			s.log(p.log, p.topic, "at send failure")
		}
		return
	}

	if ok {
		p.latency.record(time.Since(sentAt))
	}
}

// ── Consumer interceptor ──────────────────────────────────────────────────────
//
// Keeps all existing warn/error hooks — nacks and consumer close.
// Per-message debug logs (BeforeConsume, OnAcknowledge) are removed to avoid
// log volume overhead; the important signal is nacks and close events.

type debugConsumerInterceptor struct {
	log *debugLogger
}

func newConsumerInterceptor(log *debugLogger) pulsar.ConsumerInterceptor {
	return &debugConsumerInterceptor{log: log}
}

// BeforeConsume — intentionally silent. Only redeliveries at warn level matter;
// logging every receive would flood the output.
func (c *debugConsumerInterceptor) BeforeConsume(msg pulsar.ConsumerMessage) {
	// Log only if this is a redelivery — means something was nacked/reconnected.
	if msg.RedeliveryCount() > 0 {
		c.log.write("warn", fmt.Sprintf(
			"KODDI consumer redelivery | topic=%s msgID=%s redelivery_count=%d",
			msg.Topic(), msg.ID().String(), msg.RedeliveryCount(),
		))
	}
}

// OnAcknowledge — silent. Acks are the happy path; no value in logging every one.
func (c *debugConsumerInterceptor) OnAcknowledge(_ pulsar.Consumer, _ pulsar.MessageID) {}

// OnNegativeAcksSend — warn. A spike here during reconnect means message redelivery.
func (c *debugConsumerInterceptor) OnNegativeAcksSend(consumer pulsar.Consumer, msgIDs []pulsar.MessageID) {
	c.log.write("warn", fmt.Sprintf(
		"KODDI consumer nacked %d messages | subscription=%s",
		len(msgIDs), consumer.Subscription(),
	))
}

// OnConsumerClose — error if closed internally, info if closed by application.
// This is the key log for Koddi — tells you exactly why the consumer stopped.
func (c *debugConsumerInterceptor) OnConsumerClose(consumer pulsar.Consumer, err error) {
	if err != nil {
		c.log.write("error", fmt.Sprintf(
			"KODDI consumer CLOSED by internal error | subscription=%s cause=%v",
			consumer.Subscription(), err,
		))
		return
	}
	c.log.write("info", fmt.Sprintf(
		"KODDI consumer closed by application | subscription=%s",
		consumer.Subscription(),
	))
}
