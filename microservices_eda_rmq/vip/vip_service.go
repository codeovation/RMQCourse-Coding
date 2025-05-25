// Payment Service with RabbitMQ integration
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
	// Declare the queue to consume from
	q, err := ch.QueueDeclare(
		"vip-area", // Queue name
		false,      // Durable
		false,      // Delete when unused
		false,      // Exclusive
		false,      // No-wait
		nil,        // Arguments
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

	for range msgs {
		// Simulate processing and send a response back
		fmt.Println("VIP Service: Processing login event")

		// Declare a response queue for the user service
		respQueue, err := ch.QueueDeclare(
			"vip-response", // Queue name for response
			false,          // Durable
			false,          // Delete when unused
			false,          // Exclusive
			false,          // No-wait
			nil,            // Arguments
		)
		checkError(err)

		// Send a response to the user service
		err = ch.Publish(
			"",             // Exchange
			respQueue.Name, // Routing key (queue name)
			false,          // Mandatory
			false,          // Immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte("Welcome to the VIP area123456!"), // Response message
			},
		)
		checkError(err)
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

	log.Println("vip Service running...")
	consumeLoginEvent(ch) // Start consuming user login events
}
