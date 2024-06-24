package rabbitmq_go

import amqp "github.com/rabbitmq/amqp091-go"

/*
路由模式, 生产者通过指定路由，发送给指定的消费者
*/

// 路由模式生产者
type DirectProducer struct {
	producer Producer
}

// NewDirectProducer 路由模式生产者
func (p *Conn) NewDirectProducer(name string, exchange string) (*DirectProducer, error) {
	producer, err := p.NewProducer(name, exchange, amqp.ExchangeDirect)
	if err != nil {
		return nil, err
	}
	return &DirectProducer{producer: producer}, nil
}

func (d *DirectProducer) Publish(msg interface{}, headers amqp.Table, routingKey []string) error {
	return d.producer.Publish(msg, headers, routingKey...)
}

type DirectConsumer Consumer

// NewDirectConsumer 路由模式消费者
func (p *Conn) NewDirectConsumer(name, queue string, routingKey []string, exchange string) (DirectConsumer, error) {
	return p.NewConsumer(name, queue, routingKey, exchange, amqp.ExchangeDirect)
}
