package main

import (
	"fmt"
	rabbitmq "github.com/guidoxie/rabbitmq-go"
	"time"
)

func main() {
	fanoutExample()
	directExample()
	topicExample()
	select {}
}

func directExample() {
	conn, err := rabbitmq.NewConn(rabbitmq.ConnOption{
		Url:               "amqp://guest:guest@127.0.0.1:5672/",
		Prefix:            "test",
		ReconnectInterval: 2 * time.Second,
		MaxReconnect:      10,
	})
	if err != nil {
		panic(err)
	}
	producer, err := conn.NewDirectProducer("directProducer", "direct.exchange")
	if err != nil {
		panic(err)
	}
	_ = producer
	go func() {
		for {
			if err := producer.Publish("hello,direct consumer1", nil, []string{"direct.consumer1"}); err != nil {
				fmt.Println(err)
			}
			if err := producer.Publish("hello,direct consumer2", nil, []string{"direct.consumer2"}); err != nil {
				fmt.Println(err)
			}
			time.Sleep(2 * time.Second)
		}
	}()
	consumer, err := conn.NewDirectConsumer("directConsumer1", "queue.direct.consumer1", []string{"direct.consumer1"}, "direct.exchange")
	if err != nil {
		panic(err)
	}
	if err := consumer.Consume(func(d rabbitmq.Delivery) {
		fmt.Println("Direct consumer1:", string(d.Body))
	}, true, 0); err != nil {
		panic(err)
	}
	consumer2, err := conn.NewDirectConsumer("directConsumer2", "queue.direct.consumer2", []string{"direct.consumer2"}, "direct.exchange")
	if err != nil {
		panic(err)
	}
	if err := consumer2.Consume(func(d rabbitmq.Delivery) {
		fmt.Println("Direct consumer2:", string(d.Body))
		d.Ack(true)
	}, false, 0); err != nil {
		panic(err)
	}
}

func fanoutExample() {
	conn, err := rabbitmq.NewConn(rabbitmq.ConnOption{
		Url:               "amqp://guest:guest@127.0.0.1:5672/",
		Prefix:            "test",
		ReconnectInterval: 2 * time.Second,
		MaxReconnect:      10,
	})
	if err != nil {
		panic(err)
	}
	producer, err := conn.NewFanoutProducer("fanoutProducer", "fanout.exchange")
	if err != nil {
		panic(err)
	}
	_ = producer
	go func() {
		for {
			if err := producer.Publish("hello", nil); err != nil {
				fmt.Println(err)
			}
			time.Sleep(2 * time.Second)
		}
	}()
	consumer, err := conn.NewFanoutConsumer("fanoutConsumer1", "queue.fanout.consumer1", "fanout.exchange")
	if err != nil {
		panic(err)
	}
	if err := consumer.Consume(func(d rabbitmq.Delivery) {
		fmt.Println("Fanout consumer1:", string(d.Body))
	}, true, 0); err != nil {
		panic(err)
	}
	consumer2, err := conn.NewFanoutConsumer("fanoutConsumer2", "queue.fanout.consumer2", "fanout.exchange")
	if err != nil {
		panic(err)
	}
	if err := consumer2.Consume(func(d rabbitmq.Delivery) {
		fmt.Println("Fanout consumer2:", string(d.Body))
		d.Ack(true)
	}, false, 0); err != nil {
		panic(err)
	}
}

func topicExample() {
	conn, err := rabbitmq.NewConn(rabbitmq.ConnOption{
		Url:               "amqp://guest:guest@127.0.0.1:5672/",
		Prefix:            "test",
		ReconnectInterval: 2 * time.Second,
		MaxReconnect:      10,
	})
	if err != nil {
		panic(err)
	}
	producer, err := conn.NewTopicProducer("topicProducer", "topic.exchange")
	if err != nil {
		panic(err)
	}
	_ = producer
	go func() {
		for {
			if err := producer.Publish("hello,topic consumer1", nil, []string{"topic.consumer1.say"}); err != nil {
				fmt.Println(err)
			}
			if err := producer.Publish("hello,topic consumer2", nil, []string{"topic.consumer2.say"}); err != nil {
				fmt.Println(err)
			}
			time.Sleep(2 * time.Second)
		}
	}()
	consumer, err := conn.NewTopicConsumer("topicConsumer1", "queue.topic.consumer1", []string{"topic.consumer1.*"}, "topic.exchange")
	if err != nil {
		panic(err)
	}
	if err := consumer.Consume(func(d rabbitmq.Delivery) {
		fmt.Println("Topic consumer1:", string(d.Body))
	}, true, 0); err != nil {
		panic(err)
	}
	consumer2, err := conn.NewTopicConsumer("topicConsumer2", "queue.topic.consumer2", []string{"topic.consumer2.*"}, "topic.exchange")
	if err != nil {
		panic(err)
	}
	if err := consumer2.Consume(func(d rabbitmq.Delivery) {
		fmt.Println("Topic consumer2:", string(d.Body))

	}, false, 0); err != nil {
		panic(err)
	}
}
