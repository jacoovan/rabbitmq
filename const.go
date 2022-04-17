package rabbitmq

import (
	"errors"
	"strings"

	"github.com/streadway/amqp"
)

type ExchangeType string

const (
	ExchangeType_Topic  ExchangeType = amqp.ExchangeTopic
	ExchangeType_Direct ExchangeType = amqp.ExchangeDirect
	ExchangeType_Fanout ExchangeType = amqp.ExchangeFanout
	ExchangeType_Header ExchangeType = amqp.ExchangeHeaders
)

var (
	SupportExchangeTypeList = []ExchangeType{
		ExchangeType_Topic,
		ExchangeType_Direct,
		ExchangeType_Fanout,
		ExchangeType_Header,
	}
)

func ValidateExchangeType(exchangeType string) (ExchangeType, error) {
	exchangeType = strings.ToLower(exchangeType)
	support := false
	for _, t := range SupportExchangeTypeList {
		if !strings.EqualFold(exchangeType, string(t)) {
			continue
		}
		support = true
	}
	var err error
	if !support {
		err = errors.New("unsupport exchangeType:" + exchangeType)
	}
	return ExchangeType(exchangeType), err
}
