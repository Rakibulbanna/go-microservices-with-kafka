package kafka

const (
	TopicOrders         = "orders.v1"
	TopicPayments       = "payments.v1"
	TopicNotifications  = "notifications.v1"
	TopicOrdersRetry    = "orders.retry.v1"
	TopicOrdersDLT      = "orders.dlt.v1"
	TopicPaymentsRetry  = "payments.retry.v1"
	TopicPaymentsDLT    = "payments.dlt.v1"
)

const (
	ConsumerGroupPayment      = "payment-service"
	ConsumerGroupNotification = "notification-service"
)
