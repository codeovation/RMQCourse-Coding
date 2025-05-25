package main

import (
	"log"

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

	exchangeName := "x.auto.delete"
	err = ch.ExchangeDeclare(exchangeName, "fanout", true, true, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare exchange:", err)
	}

	q, err := ch.QueueDeclare("", true, true, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare queue:", err)
	}

	err = ch.QueueBind(q.Name, "", exchangeName, false, nil)
	if err != nil {
		log.Fatalln("Failed to bind queue:", err)
	}

	msgs, err := ch.Consume(q.Name, "ashish", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to register consumer:", err)
	}

	log.Println("Waiting for messages")

	// Process messages
	for msg := range msgs {
		log.Println("Received:", string(msg.Body))
	}
}
