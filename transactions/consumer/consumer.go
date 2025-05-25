package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Step 1: Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	// Step 2: Create a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Failed to open a channel:", err)
	}
	defer ch.Close()

	// Step 3: Declare the same queue (it must be same as producer)
	queueName := "q.transaction"
	_, err = ch.QueueDeclare(
		queueName,
		false, // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatal("Failed to declare queue:", err)
	}

	// Step 4: Consume messages
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer tag
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		log.Fatal("Failed to register consumer:", err)
	}

	log.Println("Waiting for transactional messages...")

	// Step 5: Receive and print messages
	for msg := range msgs {
		log.Printf("Received message: %s", msg.Body)
	}
}
