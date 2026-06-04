package in

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/fastprodman/consistent-store/internal/domains/notification/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/notification/ports/in"
	"github.com/fastprodman/consistent-store/internal/shared/events"
	"github.com/segmentio/kafka-go"
)

const (
	orderEventsTopic    = "order.events"
	customerEventsTopic = "customer.events"

	orderCreatedEventType          = "OrderCreated"
	orderNotificationSentEventType = "OrderNotificationSent"
	customerCreatedEventType       = "CustomerCreated"
)

type eventHandler interface {
	portsin.OrderCreatedHandler
	portsin.CustomerCreatedHandler
}

type KafkaConsumer struct {
	reader  *kafka.Reader
	handler eventHandler
}

func NewKafkaConsumer(broker string, handler eventHandler) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokersFromString(broker),
			GroupTopics:    []string{orderEventsTopic, customerEventsTopic},
			GroupID:        "notification-service",
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
	log.Println("notification service started")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("notification service stopped")
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
	switch msg.Topic {
	case orderEventsTopic:
		return c.handleOrderEvent(ctx, msg)
	case customerEventsTopic:
		return c.handleCustomerEvent(ctx, msg)
	default:
		log.Printf("notification service ignored message from unexpected topic: topic=%s", msg.Topic)
		return nil
	}
}

func (c *KafkaConsumer) handleOrderEvent(ctx context.Context, msg kafka.Message) error {
	eventType, err := messageEventType(msg.Value)
	if err != nil {
		return err
	}

	switch eventType {
	case orderCreatedEventType:
		return c.handleOrderCreated(ctx, msg)
	case orderNotificationSentEventType:
		log.Printf("notification service ignored event: topic=%s event_type=%s", msg.Topic, eventType)
		return nil
	default:
		log.Printf("notification service ignored unknown order event: topic=%s event_type=%s", msg.Topic, eventType)
		return nil
	}
}

func (c *KafkaConsumer) handleCustomerEvent(ctx context.Context, msg kafka.Message) error {
	eventType, err := messageEventType(msg.Value)
	if err != nil {
		return err
	}

	switch eventType {
	case customerCreatedEventType:
		return c.handleCustomerCreated(ctx, msg)
	default:
		log.Printf("notification service ignored unknown customer event: topic=%s event_type=%s", msg.Topic, eventType)
		return nil
	}
}

func messageEventType(value []byte) (string, error) {
	var envelope struct {
		EventType string `json:"event_type"`
	}

	if err := json.Unmarshal(value, &envelope); err != nil {
		return "", err
	}

	return envelope.EventType, nil
}

func (c *KafkaConsumer) handleOrderCreated(ctx context.Context, msg kafka.Message) error {
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

func (c *KafkaConsumer) handleCustomerCreated(ctx context.Context, msg kafka.Message) error {
	var payload events.CustomerCreated

	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return err
	}

	event, err := entities.NewCustomerCreatedEvent(
		payload.EventID,
		payload.CustomerID,
	)
	if err != nil {
		return err
	}

	if err := c.handler.HandleCustomerCreated(ctx, event); err != nil {
		return err
	}

	return nil
}
