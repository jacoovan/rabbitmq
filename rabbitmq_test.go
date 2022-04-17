package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	rabbitmq "github.com/jacoovan/rabbitmq"
	"github.com/smartystreets/goconvey/convey"
)

func TestValidateExchangeType(t *testing.T) {
	convey.Convey("TestValidateExchangeType", t, func() {
		convey.Convey("TestValidateExchangeType Ok", func() {
			var err error
			var et string

			et = "topic"
			_, err = rabbitmq.ValidateExchangeType(et)
			convey.So(err, convey.ShouldBeNil)

			et = "direct"
			_, err = rabbitmq.ValidateExchangeType(et)
			convey.So(err, convey.ShouldBeNil)

			et = "fanout"
			_, err = rabbitmq.ValidateExchangeType(et)
			convey.So(err, convey.ShouldBeNil)

			et = "direct"
			_, err = rabbitmq.ValidateExchangeType(et)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("TestValidateExchangeType Fail", func() {
			var err error
			var et string

			et = "topic1"
			_, err = rabbitmq.ValidateExchangeType(et)
			convey.So(err, convey.ShouldNotBeNil)

			et = "direct1"
			_, err = rabbitmq.ValidateExchangeType(et)
			convey.So(err, convey.ShouldNotBeNil)

			et = "fanout1"
			_, err = rabbitmq.ValidateExchangeType(et)
			convey.So(err, convey.ShouldNotBeNil)

			et = "direct1"
			_, err = rabbitmq.ValidateExchangeType(et)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestPublishTopic(t *testing.T) {
	convey.Convey("TestPublishTopic", t, func() {
		ctx := context.Background()
		username := `test`
		password := `123456`
		ip := `127.0.0.1`
		var port uint = 5672
		vhost := `test-vhost`

		opts := []rabbitmq.Option{
			rabbitmq.InitQueueOption(
				`exchange-topic`,
				rabbitmq.ExchangeType_Topic,
				`bindingKey-topic.*`,
				`queue-topic`,
				nil,
			),
		}
		rabbit := rabbitmq.NewRabbitMQ(username, password, ip, port, vhost, opts...)

		err := rabbit.Dial()
		convey.Convey("Dial OK", func() {
			convey.So(err, convey.ShouldBeNil)
		})

		err = rabbit.Publish(
			[]byte("topic-data"),
			`exchange-topic`,
			`bindingKey-topic.1`,
		)
		convey.Convey("Publish OK", func() {
			convey.So(err, convey.ShouldBeNil)
		})

		var consumerFunc = func(ctx context.Context, data []byte) (requeue bool) {
			t.Log("data:", string(data))
			return false
		}
		ctx, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Second))
		defer cancel()
		go rabbit.Consume(ctx, `queue-topic`, ``, consumerFunc)
		select {
		case <-ctx.Done():
			err = ctx.Err()
			convey.Convey("Context Deadline", func() {
				convey.So(err, convey.ShouldResemble, context.DeadlineExceeded)
			})
		case err = <-rabbit.ConsumeDone():
			t.Fatal("ConsumeDone will not receive error")
		}
	})
}

func TestPublishFanout(t *testing.T) {
	convey.Convey("TestPublishFanout", t, func() {
		ctx := context.Background()
		username := `test`
		password := `123456`
		ip := `127.0.0.1`
		var port uint = 5672
		vhost := `test-vhost`

		opts := []rabbitmq.Option{
			rabbitmq.InitQueueOption(
				`exchange-fanout`,
				rabbitmq.ExchangeType_Fanout,
				`bindingKey-fanout`,
				`queue-fanout`,
				nil,
			),
		}
		rabbit := rabbitmq.NewRabbitMQ(username, password, ip, port, vhost, opts...)

		err := rabbit.Dial()
		convey.Convey("Dial OK", func() {
			convey.So(err, convey.ShouldBeNil)
		})

		err = rabbit.Publish(
			[]byte("fanout-data"),
			`exchange-fanout`,
			`bindingKey-fanout01`,
		)
		convey.Convey("Publish OK", func() {
			convey.So(err, convey.ShouldBeNil)
		})

		var consumerFunc = func(ctx context.Context, data []byte) (requeue bool) {
			t.Log("data:", string(data))
			return false
		}
		ctx, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Second))
		defer cancel()
		go rabbit.Consume(ctx, `queue-fanout`, ``, consumerFunc)
		select {
		case <-ctx.Done():
			err = ctx.Err()
			convey.Convey("Context Deadline", func() {
				convey.So(err, convey.ShouldResemble, context.DeadlineExceeded)
			})
		case err = <-rabbit.ConsumeDone():
			t.Fatal("ConsumeDone will not receive error")
		}
	})
}

func TestPublishDirect(t *testing.T) {
	convey.Convey("TestPublishDirect", t, func() {
		ctx := context.Background()
		username := `test`
		password := `123456`
		ip := `127.0.0.1`
		var port uint = 5672
		vhost := `test-vhost`

		opts := []rabbitmq.Option{
			rabbitmq.InitQueueOption(
				`exchange-direct`,
				rabbitmq.ExchangeType_Direct,
				`bindingKey-direct`,
				`queue-direct`,
				nil,
			),
		}

		rabbit := rabbitmq.NewRabbitMQ(username, password, ip, port, vhost, opts...)

		err := rabbit.Dial()
		convey.Convey("Dial OK", func() {
			convey.So(err, convey.ShouldBeNil)
		})

		err = rabbit.Publish(
			[]byte("direct-data"),
			`exchange-direct`,
			`bindingKey-direct`,
		)
		convey.Convey("Publish OK", func() {
			convey.So(err, convey.ShouldBeNil)
		})

		var consumerFunc = func(ctx context.Context, data []byte) (requeue bool) {
			t.Log("data:", string(data))
			return false
		}
		ctx, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Second))
		defer cancel()
		go rabbit.Consume(ctx, `queue-direct`, ``, consumerFunc)
		select {
		case <-ctx.Done():
			err = ctx.Err()
			convey.Convey("Context Deadline", func() {
				convey.So(err, convey.ShouldResemble, context.DeadlineExceeded)
			})
		case err = <-rabbit.ConsumeDone():
			t.Fatal("ConsumeDone will not receive error")
		}
	})
}

func TestPublishHeader(t *testing.T) {
	convey.Convey("TestPublishHeader", t, func() {
		ctx := context.Background()
		username := `test`
		password := `123456`
		ip := `127.0.0.1`
		var port uint = 5672
		vhost := `test-vhost`

		opts := []rabbitmq.Option{
			rabbitmq.InitQueueOption(
				`exchange-headers`,
				rabbitmq.ExchangeType_Header,
				`bindingKey-headers`,
				`queue-headers`,
				map[string]interface{}{
					"type":    "log",
					"name":    "name",
					"x-match": "any",
				},
			),
		}

		rabbit := rabbitmq.NewRabbitMQ(username, password, ip, port, vhost, opts...)

		err := rabbit.Dial()
		convey.Convey("Dial OK", func() {
			convey.So(err, convey.ShouldBeNil)
		})

		err = rabbit.Publish(
			[]byte("headers-data"),
			`exchange-headers`,
			`bindingKey-headers`,
			map[string]interface{}{
				"type": "log",
			},
		)
		convey.Convey("Publish OK", func() {
			convey.So(err, convey.ShouldBeNil)
		})

		var consumerFunc = func(ctx context.Context, data []byte) (requeue bool) {
			t.Log("data:", string(data))
			return false
		}
		ctx, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Second))
		defer cancel()
		go rabbit.Consume(ctx, `queue-headers`, ``, consumerFunc)
		select {
		case <-ctx.Done():
			err = ctx.Err()
			convey.Convey("Context Deadline", func() {
				convey.So(err, convey.ShouldResemble, context.DeadlineExceeded)
			})
		case err = <-rabbit.ConsumeDone():
			t.Fatal("ConsumeDone will not receive error")
		}
	})
}
