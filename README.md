# rabbitmq

Usage see:

/rabbitmq_test.go

```
type RabbitMQ interface {
	// dial rabbit and init queue with exchange and queuebind.
	Dial() error
	// publish message.
	// header spec for queue with header exchange, default x-match: all.
	Publish(body []byte, exchangeName, routingKey string, header ...map[string]interface{}) error
	// subscribe operation occur error will finish
	// and
	// will block until ack & nack occured error or context was canceled
	Consume(ctx context.Context, queueName, consumerName string, consumer ConsumeFunc)
	// return the subscribe operation occured error or ack & nack error.
	ConsumeDone() <-chan error
}
```