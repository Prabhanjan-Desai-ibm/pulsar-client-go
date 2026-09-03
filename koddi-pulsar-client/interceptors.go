package koddi

import (
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// ── Producer interceptor ──────────────────────────────────────────────────────
//
// Hooks into every send and every ack/failure.
// Tracks send latency so you can see in Splunk if sends start getting
// slow RIGHT BEFORE a disconnection — this is the signal that the
// write loop is congested and PINGs are being delayed.

type debugProducerInterceptor struct {
	log     *debugLogger
	sentAt  map[string]time.Time // msgID string → time sent (approximate)
}

func newProducerInterceptor(log *debugLogger) pulsar.ProducerInterceptor {
	return &debugProducerInterceptor{
		log:    log,
		sentAt: make(map[string]time.Time),
	}
}

// BeforeSend is called just before the message leaves the client.
// We log the topic and payload size so you can see message volume over time.
func (p *debugProducerInterceptor) BeforeSend(producer pulsar.Producer, msg *pulsar.ProducerMessage) {
	p.log.write("debug", fmt.Sprintf(
		"KODDI producer sending | topic=%s payload_bytes=%d properties=%v",
		producer.Topic(),
		len(msg.Payload),
		msg.Properties,
	))
}

// OnSendAcknowledgement is called when broker acks OR when send fails.
// err == nil  → success, log latency
// err != nil  → failure, log the error — this is the most important one
//               because send failures happen DURING reconnection
func (p *debugProducerInterceptor) OnSendAcknowledgement(
	producer pulsar.Producer,
	msg *pulsar.ProducerMessage,
	msgID pulsar.MessageID,
) {
	// msgID is nil when the send failed
	if msgID == nil {
		p.log.write("error", fmt.Sprintf(
			"KODDI producer send FAILED | topic=%s payload_bytes=%d",
			producer.Topic(),
			len(msg.Payload),
		))
		return
	}

	p.log.write("debug", fmt.Sprintf(
		"KODDI producer send ACKed | topic=%s msgID=%s",
		producer.Topic(),
		msgID.String(),
	))
}

// ── Consumer interceptor ──────────────────────────────────────────────────────
//
// Hooks into every message delivery, every ack, every nack, and crucially
// into the consumer CLOSE event which tells you WHY the consumer died.
// OnConsumerClose is the most important one for Koddi's disconnection issue.

type debugConsumerInterceptor struct {
	log *debugLogger
}

func newConsumerInterceptor(log *debugLogger) pulsar.ConsumerInterceptor {
	return &debugConsumerInterceptor{log: log}
}

// BeforeConsume is called just before the message is handed to your app.
// RedeliveryCount > 0 means this message was nacked before — useful to
// see if reconnection caused duplicate redeliveries.
func (c *debugConsumerInterceptor) BeforeConsume(msg pulsar.ConsumerMessage) {
	c.log.write("debug", fmt.Sprintf(
		"KODDI consumer received | topic=%s msgID=%s redelivery_count=%d",
		msg.Topic(),
		msg.ID().String(),
		msg.RedeliveryCount(),
	))
}

// OnAcknowledge is called when your app acks a message.
func (c *debugConsumerInterceptor) OnAcknowledge(consumer pulsar.Consumer, msgID pulsar.MessageID) {
	c.log.write("debug", fmt.Sprintf(
		"KODDI consumer acked | subscription=%s msgID=%s",
		consumer.Subscription(),
		msgID.String(),
	))
}

// OnNegativeAcksSend is called when messages are nacked (your app failed to process them).
// A spike here during reconnection means messages are being redelivered.
func (c *debugConsumerInterceptor) OnNegativeAcksSend(consumer pulsar.Consumer, msgIDs []pulsar.MessageID) {
	c.log.write("warn", fmt.Sprintf(
		"KODDI consumer nacked %d messages | subscription=%s",
		len(msgIDs),
		consumer.Subscription(),
	))
}

// OnConsumerClose is called when the consumer is permanently closed.
// err == nil  → your app called consumer.Close() intentionally
// err != nil  → the client closed it internally, err explains WHY:
//               - "max retry attempts reached for reconnecting to broker"
//               - "TopicNotFound"
//               - "AuthorizationError"
//               etc.
// This is the KEY log for Koddi — it tells you exactly why the consumer
// stopped working, with the full cause attached.
func (c *debugConsumerInterceptor) OnConsumerClose(consumer pulsar.Consumer, err error) {
	if err != nil {
		c.log.write("error", fmt.Sprintf(
			"KODDI consumer CLOSED by internal error | subscription=%s cause=%v",
			consumer.Subscription(),
			err,
		))
		return
	}
	c.log.write("info", fmt.Sprintf(
		"KODDI consumer closed by application | subscription=%s",
		consumer.Subscription(),
	))
}
