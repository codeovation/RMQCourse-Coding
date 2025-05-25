package main

import (
	"log"
	"strings"

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

	// Declare an RPC queue
	q, err := ch.QueueDeclare("q.rpc", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare queue:", err)
	}

	// Consume messages from queue
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to register consumer:", err)
	}

	log.Println("RPC server is waiting for requests...")

	for msg := range msgs {
		// Process the request (convert text to uppercase)
		processedText := strings.ToUpper(string(msg.Body))

		log.Printf("Received: %s | responding: %s", msg.Body, processedText)

		// Publish response to the reply queue
		err = ch.Publish("", msg.ReplyTo, false, false, amqp091.Publishing{
			ContentType:   "text/plain",
			CorrelationId: msg.CorrelationId,
			Body:          []byte(processedText),
		})
		if err != nil {
			log.Fatalln("Failed to send response:", err)
		}

		// Acknowledge the message
		msg.Ack(false)
	}
}
