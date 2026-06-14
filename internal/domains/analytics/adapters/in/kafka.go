package in

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/fastprodman/consistent-store/internal/domains/analytics/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/analytics/ports/in"
	"github.com/fastprodman/consistent-store/internal/shared/events"
	"github.com/segmentio/kafka-go"
)

const (
	orderEventsTopic      = "order.events"
	orderCreatedEventType = "OrderCreated"
)

type KafkaConsumer struct {
	reader  *kafka.Reader
	handler portsin.OrderCreatedHandler
}

func NewKafkaConsumer(broker string, handler portsin.OrderCreatedHandler) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokersFromString(broker),
			Topic:          orderEventsTopic,
			GroupID:        "analytics-consumer",
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
		}),
		handler: handler,
	}
}

func brokersFromString(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))

	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	return brokers
}

func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}

func (c *KafkaConsumer) Run(ctx context.Context) {
	log.Println("analytics consumer started")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("analytics consumer stopped")
				return
			}

			log.Println("fetch message:", err)
			continue
		}

		if err := c.handleMessage(ctx, msg); err != nil {
			log.Println("handle message:", err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Println("commit message:", err)
			continue
		}
	}
}

func (c *KafkaConsumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	eventType := messageEventType(msg.Headers)
	if eventType != orderCreatedEventType {
		log.Printf("analytics consumer ignored event: topic=%s event_type=%s", msg.Topic, eventType)
		return nil
	}

	var payload events.OrderCreated

	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return err
	}

	event, err := entities.NewOrderCreatedEvent(
		payload.EventID,
		payload.OrderID,
		payload.TotalCents,
		payload.CreatedAt,
	)
	if err != nil {
		return err
	}

	applied, err := c.handler.HandleOrderCreated(ctx, event)
	if err != nil {
		return err
	}

	if applied {
		log.Printf(
			"analytics updated: topic=%s partition=%d offset=%d event_id=%s total_cents=%d",
			msg.Topic,
			msg.Partition,
			msg.Offset,
			event.EventID(),
			event.TotalCents(),
		)
	} else {
		log.Printf("duplicate analytics event ignored: event_id=%s", event.EventID())
	}

	return nil
}

func messageEventType(headers []kafka.Header) string {
	for _, header := range headers {
		if header.Key == "event_type" {
			return string(header.Value)
		}
	}

	return ""
}
