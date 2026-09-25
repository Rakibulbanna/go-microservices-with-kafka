package events

const (
	EventTypeOrderCreated   = "order.created"
	EventTypeOrderCancelled = "order.cancelled"
	EventTypePaymentCompleted = "payment.completed"
	EventTypePaymentFailed  = "payment.failed"
	EventTypeNotificationSent = "notification.sent"
)

const (
	ProducerOrderService      = "order-service"
	ProducerPaymentService    = "payment-service"
	ProducerNotificationService = "notification-service"
)

const (
	EventVersion1 = 1
	EventVersion2 = 2
)
