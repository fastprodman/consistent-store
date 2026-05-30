package in

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/fastprodman/consistent-store/internal/domains/ordernotifier/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/ordernotifier/ports/in"
	"github.com/fastprodman/consistent-store/internal/events"
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
			GroupID:        "order-notifier",
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
	log.Println("order notifier started")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("order notifier stopped")
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
		payload.CustomerID,
	)
	if err != nil {
		return err
	}

	notified, err := c.handler.HandleOrderCreated(ctx, event)
	if err != nil {
		return err
	}

	if notified {
		log.Printf(
			"order notification sent: topic=%s partition=%d offset=%d event_id=%s order_id=%s customer_id=%s",
			msg.Topic,
			msg.Partition,
			msg.Offset,
			event.EventID(),
			event.OrderID(),
			event.CustomerID(),
		)
	} else {
		log.Printf("duplicate order notification ignored: event_id=%s", event.EventID())
	}

	return nil
}
