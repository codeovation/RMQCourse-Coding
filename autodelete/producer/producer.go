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

	msg := "Hello Auto Delete!"
	err = ch.Publish(exchangeName, "", false, false, amqp091.Publishing{
		ContentType: "text/plain",
		Body:        []byte(msg),
	})
	if err != nil {
		log.Fatalln("Failed to publish message:", err)
	}

	log.Println("Sent:", msg)
}
