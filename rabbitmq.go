package rabbitmq

import (
	"context"
	"fmt"
)

type ConsumeFunc func(ctx context.Context, data []byte) (requeue bool)

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

func NewRabbitMQ(username, password, ip string, port uint, vhost string, opts ...Option) RabbitMQ {
	c := &rabbitmq{
		link:          fmt.Sprintf(`amqp://%s:%s@%s:%d/%s`, username, password, ip, port, vhost),
		queueConfigs:  make([]queueConfig, 0),
		consumeDoneCh: make(chan error, 1),
	}

	for _, opt := range opts {
		opt.apply(c)
	}
	return c
}
