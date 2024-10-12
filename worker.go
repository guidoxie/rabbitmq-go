package rabbitmq_go

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type worker struct {
	conn              *Conn
	ch                *amqp.Channel
	notifyChanClose   chan *amqp.Error
	name              string
	exchange          string
	routingKey        []string
	queue             amqp.Queue
	consumeHandler    *consumeHandler
	log               Logger
	channelMux        *sync.RWMutex
	reconnectInterval time.Duration // channel重连频率
}

type consumeHandler struct {
	prefetchCount int
	autoAck       bool
	handler       func(d amqp.Delivery)
}

func (w *worker) GetExchange() string {
	return w.exchange
}

func (w *worker) GetQueue() string {
	return w.queue.Name
}

func (w *worker) Publish(msg interface{}, headers amqp.Table, routingKey ...string) error {
	channel := w.checkoutChannel()
	defer w.checkinChannel()
	// 设置5秒超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body, err := w.getBytes(msg)
	if err != nil {
		return err
	}
	// 默认消息持久化
	if len(routingKey) > 0 {
		for _, key := range routingKey {
			err = channel.PublishWithContext(ctx, w.exchange, key, false, false, amqp.Publishing{
				Body:         body,
				DeliveryMode: amqp.Persistent,
				Headers:      headers,
			})
			if err != nil {
				return err
			}
		}
	} else {
		err = channel.PublishWithContext(ctx, w.exchange, "", false, false, amqp.Publishing{
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Headers:      headers,
		})
		if err != nil {
			return err
		}
	}
	return err
}

type Delivery struct {
	amqp.Delivery
}

// prefactchCount: 控制channel上未进行ack点最大消息数量上限制,autoAck 为false时生效
func (w *worker) Consume(handler func(d Delivery), autoAck bool, prefetchCount int) error {
	w.consumeHandler = &consumeHandler{
		prefetchCount: prefetchCount,
		autoAck:       autoAck,
		handler: func(d amqp.Delivery) {
			handler(Delivery{d})
		},
	}
	return w.registerConsume()
}

func (w *worker) registerConsume() error {
	channel := w.checkoutChannel()
	defer w.checkinChannel()

	if w.consumeHandler.prefetchCount > 0 {
		if err := channel.Qos(w.consumeHandler.prefetchCount, 0, false); err != nil {
			channel.Close()
			return err
		}
	}
	msg, err := channel.Consume(w.queue.Name, w.name, w.consumeHandler.autoAck, false, false, false, nil)
	if err != nil {
		channel.Close()
		return err
	}
	go func() {
		// 捕获panic
		defer func() {
			if r := recover(); r != nil {
				w.log.Errorf("panic %v", err)
			}
		}()
		defer func() {
			fmt.Println("exist")
			c := w.checkoutChannel()
			if !c.IsClosed() {
				c.Close()
			}
			w.checkinChannel()
		}()
		for m := range msg {
			w.consumeHandler.handler(m)
		}
	}()
	return nil
}

func (w *worker) getBytes(msg interface{}) ([]byte, error) {
	v, ok := msg.([]byte)
	if ok {
		return v, nil
	}
	str, err := ToStringE(msg)
	if err == nil {
		return []byte(str), nil
	}
	return json.Marshal(msg)
}

func (w *worker) checkoutChannel() *amqp.Channel {
	w.channelMux.RLock()
	return w.ch
}

func (w *worker) checkinChannel() {
	w.channelMux.RUnlock()
}

func (w *worker) reconnect() {
	for {
		select {
		case err := <-w.notifyChanClose:
			if err != nil {
				w.log.Errorf("channel closed: %v", err.Error())
			}
			w.reconnectLoop()
			if w.ch.IsClosed() {
				w.log.Errorf("exceeded max channel reconnect attempts, exiting") // 超出重连的次数，退出重连
				return
			}
		}
	}
}

func (w *worker) reconnectLoop() {
	for w.ch.IsClosed() {
		time.Sleep(w.reconnectInterval)
		// 获取连接
		connect := w.conn.checkoutConnection()
		if connect.IsClosed() {
			// 释放连接
			w.conn.checkinConnection()
			w.log.Errorf("failed to recreate channel: connection closed")
			continue
		}
		// 创建新channel
		newChannel, err := connect.Channel()
		// 释放连接
		w.conn.checkinConnection()
		if err != nil {
			w.log.Errorf("failed to recreate channel: %v connection status:%v", err.Error(), connect.IsClosed())
			continue
		}
		// 清空
		for err = range w.notifyChanClose {
			w.log.Errorf("channel closed: %v", err.Error())
		}
		w.log.Infof("successfully recreated channel")
		// 重新赋值
		w.channelMux.Lock()
		w.ch = newChannel
		w.channelMux.Unlock()
		w.notifyChanClose = w.ch.NotifyClose(make(chan *amqp.Error))
		if w.consumeHandler != nil { // 重新注册消费函数
			if err := w.registerConsume(); err != nil {
				w.log.Errorf("re register consume err: %v", err.Error())
			}
		}
	}
}
