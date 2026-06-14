package in

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"

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
	orderReader    *kafka.Reader
	customerReader *kafka.Reader
	handler        eventHandler
}

func NewKafkaConsumer(broker string, handler eventHandler) *KafkaConsumer {
	brokers := brokersFromString(broker)

	return &KafkaConsumer{
		orderReader:    newReader(brokers, orderEventsTopic, "notification-service-order"),
		customerReader: newReader(brokers, customerEventsTopic, "notification-service-customer"),
		handler:        handler,
	}
}

func newReader(brokers []string, topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:               brokers,
		Topic:                 topic,
		GroupID:               groupID,
		MinBytes:              1,
		MaxBytes:              10e6,
		CommitInterval:        0,
		StartOffset:           kafka.FirstOffset,
		WatchPartitionChanges: true,
	})
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
	orderErr := c.orderReader.Close()
	customerErr := c.customerReader.Close()

	if orderErr != nil {
		return orderErr
	}

	return customerErr
}

func (c *KafkaConsumer) Run(ctx context.Context) {
	log.Println("notification service started")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		c.runReader(ctx, c.orderReader, c.handleOrderEvent)
	}()

	go func() {
		defer wg.Done()
		c.runReader(ctx, c.customerReader, c.handleCustomerEvent)
	}()

	wg.Wait()
	log.Println("notification service stopped")
}

func (c *KafkaConsumer) runReader(ctx context.Context, reader *kafka.Reader, handle func(context.Context, kafka.Message) error) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			log.Println("fetch message:", err)
			continue
		}

		if err := handle(ctx, msg); err != nil {
			log.Println("handle message:", err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Println("commit message:", err)
			continue
		}
	}
}

func (c *KafkaConsumer) handleOrderEvent(ctx context.Context, msg kafka.Message) error {
	eventType := messageEventType(msg.Headers)

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
	eventType := messageEventType(msg.Headers)

	switch eventType {
	case customerCreatedEventType:
		return c.handleCustomerCreated(ctx, msg)
	default:
		log.Printf("notification service ignored unknown customer event: topic=%s event_type=%s", msg.Topic, eventType)
		return nil
	}
}

func messageEventType(headers []kafka.Header) string {
	for _, header := range headers {
		if header.Key == "event_type" {
			return string(header.Value)
		}
	}

	return ""
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
