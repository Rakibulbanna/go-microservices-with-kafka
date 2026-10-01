package events

const (
	EventTypeOrderCreated   = "order.created"
	EventTypeOrderCancelled = "order.cancelled"
	EventTypePaymentCompleted = "payment.completed"
	EventTypePaymentFailed  = "payment.failed"
	EventTypeNotificationSent = "notification.sent"
	EventTypeOrderAnalytics = "order.analytics"
)

const (
	ProducerOrderService      = "order-service"
	ProducerPaymentService    = "payment-service"
	ProducerNotificationService = "notification-service"
	ProducerAnalyticsService  = "analytics-service"
)

const (
	EventVersion1 = 1
	EventVersion2 = 2
)
