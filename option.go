package rabbitmq

import (
	"time"
)

type Option interface {
	apply(c *rabbitmq)
}

type optionFunc func(c *rabbitmq)

func (fn optionFunc) apply(c *rabbitmq) {
	fn(c)
}

func InitQueueOption(exchangeName string, exchangeType ExchangeType, bindingKey, queueName string, header map[string]interface{}) Option {
	return optionFunc(func(c *rabbitmq) {
		cfg := queueConfig{
			exchangeName: exchangeName,
			exchangeType: exchangeType,
			bindingKey:   bindingKey,
			queueName:    queueName,
			header:       header,
		}
		c.appendQueue(cfg)
	})
}

type queueConfig struct {
	exchangeName string
	exchangeType ExchangeType

	queueName string

	bindingKey string
	header     map[string]interface{}
}

func KeepliveOption(duration time.Duration) Option {
	return optionFunc(func(c *rabbitmq) {
		c.keepliveCfg = &keepliveConfig{
			duration: duration,
		}
	})
}

type keepliveConfig struct {
	duration time.Duration
}
