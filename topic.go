package rabbitmq_go

import amqp "github.com/rabbitmq/amqp091-go"

/*
主题模式,生产者指定具体的路由，而消费者指定通配符的路由
*/

type TopicProducer struct {
	producer Producer
}

// NewTopicProducer 主题模式生产者
func (p *Conn) NewTopicProducer(name, exchange string) (*TopicProducer, error) {
	producer, err := p.NewProducer(name, exchange, amqp.ExchangeTopic)
	if err != nil {
		return nil, err
	}
	return &TopicProducer{producer: producer}, nil
}

func (t *TopicProducer) Publish(msg interface{}, headers amqp.Table, routingKey []string) error {
	return t.producer.Publish(msg, headers, routingKey...)
}

type TopicConsumer Consumer

// NewTopicConsumer 主题模式消费者
func (p *Conn) NewTopicConsumer(name, queue string, routingKey []string, exchange string) (TopicConsumer, error) {
	return p.NewConsumer(name, queue, routingKey, exchange, amqp.ExchangeTopic)
}
