package rabbitmq_go

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"math"
	"sync"
	"time"
)

type Conn struct {
	connect         *amqp.Connection
	notifyConnClose chan *amqp.Error
	connectMux      *sync.RWMutex
	ConnOption
}

type ConnOption struct {
	Url               string
	Prefix            string
	Log               Logger
	ReconnectInterval time.Duration // connection重连频率
	MaxReconnect      int           // 最大重连次数
}

func NewConn(option ConnOption) (*Conn, error) {
	connect, err := amqp.Dial(option.Url) // 创建连接
	if err != nil {
		return nil, err
	}
	if option.Log == nil {
		option.Log = StdLog{}
	}
	conn := &Conn{
		connect:         connect,
		notifyConnClose: connect.NotifyClose(make(chan *amqp.Error)),
		connectMux:      &sync.RWMutex{},
	}
	conn.ConnOption = option
	// 启动重连
	go conn.reconnect()
	return conn, err
}

func (p *Conn) NewProducer(name string, exchange, kind string) (Producer, error) {
	ch, err := p.connect.Channel()
	if err != nil {
		return nil, err
	}

	ex, err := p.exchangeDeclare(ch, exchange, kind)
	if err != nil {
		return nil, err
	}
	w := &worker{
		conn:              p,
		ch:                ch,
		notifyChanClose:   ch.NotifyClose(make(chan *amqp.Error)),
		name:              name,
		exchange:          ex,
		queue:             amqp.Queue{},
		log:               p.Log,
		channelMux:        &sync.RWMutex{},
		reconnectInterval: p.ReconnectInterval,
	}
	// 尝试重连
	go w.reconnect()
	return w, nil
}

func (p *Conn) NewConsumer(name, queue string, routingKey []string, exchange, kind string) (Consumer, error) {
	ch, err := p.connect.Channel()
	if err != nil {
		return nil, err
	}
	if _, err := p.exchangeDeclare(ch, exchange, kind); err != nil {
		return nil, err
	}
	// 队列声明，默认持久化
	ex, q, err := p.queueDeclareAndBind(ch, queue, routingKey, exchange)
	if err != nil {
		return nil, err
	}
	w := &worker{
		conn:              p,
		ch:                ch,
		notifyChanClose:   ch.NotifyClose(make(chan *amqp.Error)),
		name:              name,
		exchange:          ex,
		routingKey:        routingKey,
		queue:             q,
		log:               p.Log,
		channelMux:        &sync.RWMutex{},
		reconnectInterval: p.ReconnectInterval,
	}
	// 尝试重连
	go w.reconnect()
	return w, nil
}

// 交换机声明，默认持久化
func (p *Conn) exchangeDeclare(ch *amqp.Channel, exchange string, kind string) (string, error) {
	exchange = p.addPrefix(exchange)
	err := ch.ExchangeDeclare(exchange, kind, true, false, false, false, nil)
	if err != nil {
		return "", err
	}
	return exchange, err
}

// 队列声明,默认持久化
func (p *Conn) queueDeclareAndBind(ch *amqp.Channel, queue string, keys []string, exchange string) (string, amqp.Queue, error) {
	queue = p.addPrefix(queue)
	exchange = p.addPrefix(exchange)
	q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return "", amqp.Queue{}, err
	}
	if len(keys) == 0 {
		keys = []string{""}
	}
	for _, key := range keys {
		err = ch.QueueBind(queue, key, exchange, false, nil)
		if err != nil {
			return "", amqp.Queue{}, err
		}
	}
	return exchange, q, nil
}

// addPrefix 添加前缀
func (p *Conn) addPrefix(str string) string {
	if len(str) == 0 || len(p.Prefix) == 0 {
		return str
	}
	return fmt.Sprintf("%s.%s", p.Prefix, str)
}

func (p *Conn) checkoutConnection() *amqp.Connection {
	p.connectMux.RLock()
	return p.connect
}

func (p *Conn) checkinConnection() {
	p.connectMux.RUnlock()
}

func (p *Conn) reconnect() {
	for {
		select {
		case err := <-p.notifyConnClose:
			if err != nil {
				p.Log.Errorf("connection closed: %v", err.Error())
			}
			p.reconnectLoop()
			if p.connect.IsClosed() {
				p.Log.Errorf("exceeded max RabbitMQ reconnect attempts, exiting") // 超出重连的次数，退出重连
				return
			}
		}
	}
}

func (p *Conn) reconnectLoop() {
	for i := 0; i < p.MaxReconnect && p.connect.IsClosed(); i++ {
		time.Sleep(time.Duration(math.Pow(2, float64(i))) * p.ReconnectInterval)
		newConnect, err := amqp.Dial(p.Url) // 创建新连接
		if err != nil {
			p.Log.Errorf("failed to reconnect to RabbitMQ:%v", err.Error())
			continue
		}
		// 清空
		for err = range p.notifyConnClose {
			p.Log.Errorf("connection closed: %v", err.Error())
		}
		p.Log.Infof("successfully reconnected to RabbitMQ")
		// 重新赋值
		p.connectMux.Lock()
		p.connect = newConnect
		p.connectMux.Unlock()
		p.notifyConnClose = p.connect.NotifyClose(make(chan *amqp.Error))
	}
}
