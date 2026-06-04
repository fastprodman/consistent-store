package out

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/notification/entities"
)

type EventPublisher interface {
	PublishOrderNotificationSent(ctx context.Context, event entities.OrderNotificationSentEvent) error
}
