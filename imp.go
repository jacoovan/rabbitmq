package rabbitmq

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

type rabbitmq struct {
	link         string
	conn         *amqp.Connection
	ch           *amqp.Channel
	queueConfigs []queueConfig

	keepliveCfg *keepliveConfig

	consumeDoneCh chan error
	consumeDone   atomic.Value
	consumeMutex  sync.Mutex
}

func (c *rabbitmq) Dial() error {
	var err error
	if c.conn, err = amqp.Dial(c.link); err != nil {
		return err
	}

	if c.ch, err = c.conn.Channel(); err != nil {
		return nil
	}

	if err := c.init(); err != nil {
		return err
	}

	if c.keepliveCfg != nil && c.keepliveCfg.duration > 0 {
		go c.keeplive(c.keepliveCfg.duration)
	}
	return nil
}

func (c *rabbitmq) keeplive(duration time.Duration) {
	ticker := time.NewTicker(duration)
	for {
		select {
		case <-ticker.C:
			if !c.conn.IsClosed() {
				continue
			}

			conn, err := amqp.Dial(c.link)
			if err != nil {
				continue
			}
			c.conn = conn

			ch, err := conn.Channel()
			if err != nil {
				continue
			}
			c.ch = ch
		}
	}
}

func (c *rabbitmq) init() error {
	// 1. init queues
	for _, q := range c.queueConfigs {
		if err := c.initExchange(q.exchangeName, string(q.exchangeType)); err != nil {
			return err
		}
		if err := c.initQueue(q.queueName); err != nil {
			return err
		}
		if err := c.initQueueBind(q.queueName, q.bindingKey, q.exchangeName, q.header); err != nil {
			return err
		}
	}
	return nil
}

func (c *rabbitmq) Publish(body []byte, exchangeName, routingKey string, header ...map[string]interface{}) error {
	msg := amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	}
	if len(header) > 0 {
		msg.Headers = amqp.Table(header[0])
	}
	mandatory := false
	immediate := false
	return c.ch.Publish(exchangeName, routingKey, mandatory, immediate, msg)
}

func (c *rabbitmq) ConsumeDone() <-chan error {
	if d := c.consumeDone.Load(); d != nil {
		return d.(chan error)
	}

	c.consumeMutex.Lock()
	defer c.consumeMutex.Unlock()
	d := c.consumeDone.Load()
	if d == nil {
		d = c.consumeDoneCh
		c.consumeDone.Store(d)
	}
	return d.(chan error)
}

func (c *rabbitmq) Consume(ctx context.Context, queueName, consumerName string, consumer ConsumeFunc) {
	autoAck := false
	exclusive := false
	noLocal := false
	noWait := false
	args := amqp.Table{}
	ch, err := c.ch.Consume(queueName, consumerName, autoAck, exclusive, noLocal, noWait, args)
	defer func() {
		c.consumeMutex.Lock()
		defer c.consumeMutex.Unlock()
		c.consumeDone.Store(c.consumeDoneCh)
	}()
	if err != nil {
		c.consumeDoneCh <- err
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			requeue := consumer(ctx, msg.Body)
			if requeue {
				err = msg.Nack(false, requeue)
			} else {
				err = msg.Ack(false)
			}
			if err != nil {
				c.consumeDoneCh <- err
				return
			}
		}
	}
}

func (c *rabbitmq) initExchange(name, kind string) error {
	durable := true
	autoDelete := false
	internal := false
	noWait := false
	args := amqp.Table{}
	err := c.ch.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
	if err != nil {
		return err
	}
	return nil
}

func (c *rabbitmq) initQueue(name string) error {
	durable := true
	autoDelete := false
	exclusive := false
	noWait := false
	args := amqp.Table{}
	_, err := c.ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		return err
	}
	return nil
}

func (c *rabbitmq) initQueueBind(queueName, bindingKey, exchangeName string, header map[string]interface{}) error {
	noWait := false
	args := amqp.Table(header)
	err := c.ch.QueueBind(queueName, bindingKey, exchangeName, noWait, args)
	if err != nil {
		return err
	}
	return nil
}

func (c *rabbitmq) appendQueue(cfg queueConfig) {
	c.queueConfigs = append(c.queueConfigs, cfg)
}
