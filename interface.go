package rabbitmq_go

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// Producer 生产者
type Producer interface {
	Publish(msg interface{}, headers amqp.Table, routingKey ...string) error // 发送消息
	GetExchange() string
}

// Consumer 消费者
type Consumer interface {
	Consume(f func(d Delivery), autoAck bool, prefetchCount int) error // 接收信息
	GetQueue() string
}
