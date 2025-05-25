// Order Service with RabbitMQ integration
package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func checkError(err error) {
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}

func consumeLoginEvent(ch *amqp.Channel) {
	// Declare a fanout exchange
	err := ch.ExchangeDeclare(
		"logs",   // Exchange name
		"fanout", // Exchange type
		true,     // Durable
		false,    // Auto-delete
		false,    // Internal
		false,    // No-wait
		nil,      // Arguments
	)
	checkError(err)

	// Declare a queue for this service
	q, err := ch.QueueDeclare(
		"order-queue", // Queue name
		false,         // Durable
		false,         // Delete when unused
		false,         // Exclusive
		false,         // No-wait
		nil,           // Arguments
	)
	checkError(err)

	// Bind the queue to the fanout exchange
	err = ch.QueueBind(
		q.Name, // Queue name
		"",     // Routing key (not used in fanout)
		"logs", // Exchange name
		false,  // No-wait
		nil,    // Arguments
	)
	checkError(err)

	// Consume messages from the queue
	msgs, err := ch.Consume(
		q.Name, // Queue name
		"",     // Consumer name
		true,   // Auto-acknowledge
		false,  // Exclusive
		false,  // No-local
		false,  // No-wait
		nil,    // Arguments
	)
	checkError(err)

	for msg := range msgs {
		println(msg.Body)
		// When a user logs in, simulate order processing
		fmt.Println("Order Service: User logged in, processing order")
	}
}

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	checkError(err)
	defer conn.Close()

	ch, err := conn.Channel()
	checkError(err)
	defer ch.Close()

	log.Println("Order Service running...")
	consumeLoginEvent(ch) // Start consuming user login events
}
