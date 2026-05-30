package in

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/fastprodman/consistent-store/internal/domains/analytics/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/analytics/ports/in"
	"github.com/fastprodman/consistent-store/internal/shared/events"
	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader  *kafka.Reader
	handler portsin.OrderCreatedHandler
}

func NewKafkaConsumer(broker string, handler portsin.OrderCreatedHandler) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			Topic:          "order.events",
			GroupID:        "analytics-consumer",
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
		}),
		handler: handler,
	}
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
