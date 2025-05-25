package main

import (
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func main() {

	// connect to RMQ
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln("Failed to open a channel:", err)
	}
	defer ch.Close()

	exchangeName := "x.topic"

	err = ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare exchange:", err)
	}

	q, err := ch.QueueDeclare("queue1", true, false, false, false, amqp091.Table{
		"x-queue-type": "quorum",
	})
	if err != nil {
		log.Fatalln("Failed to declare queue:", err)
	}

	q2, err := ch.QueueDeclare("queue2", true, false, false, false, amqp091.Table{
		"x-queue-type": "quorum",
	})
	if err != nil {
		log.Fatalln("Failed to declare queue:", err)
	}

	err = ch.QueueBind(q.Name, "*.error", exchangeName, false, nil)
	if err != nil {
		log.Fatalln("Failed to bind queue:", err)
	}

	err = ch.QueueBind(q2.Name, "kern.*", exchangeName, false, nil)
	if err != nil {
		log.Fatalln("Failed to bind queue:", err)
	}

	routingKeys := []string{"kern.info", "kern.error", "user.error", "app.debug"}
	for _, key := range routingKeys {
		message := "Log Message with key:" + key
		err = ch.Publish(exchangeName, key, false, false, amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
		if err != nil {
			log.Fatalln("Failed to publish message:", err)
		}
		log.Println("Sent:", message)
		time.Sleep(time.Second)
	}

}
