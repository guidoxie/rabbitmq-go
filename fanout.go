package rabbitmq_go

import amqp "github.com/rabbitmq/amqp091-go"

/*
发布订阅模式,生产者发布的消息将广播给所有消费者
*/

type FanoutProducer struct {
	producer Producer
}

// NewFanoutProducer 发布订阅模式生产者
func (p *Conn) NewFanoutProducer(name, exchange string) (*FanoutProducer, error) {
	producer, err := p.NewProducer(name, exchange, amqp.ExchangeFanout)
	if err != nil {
		return nil, err
	}
	return &FanoutProducer{producer: producer}, nil
}

func (f *FanoutProducer) Publish(msg interface{}, headers amqp.Table) error {
	return f.producer.Publish(msg, headers)
}

func (f *FanoutProducer) GetExchange() string {
	return f.producer.GetExchange()
}

type FanoutConsumer Consumer

// NewFanoutConsumer 发布订阅模式消费者
func (p *Conn) NewFanoutConsumer(name, queue, exchange string) (FanoutConsumer, error) {
	return p.NewConsumer(name, queue, nil, exchange, amqp.ExchangeFanout)
}
