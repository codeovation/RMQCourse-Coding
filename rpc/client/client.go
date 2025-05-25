package main

import (
	"bufio"
	"log"
	"math/rand"
	"os"
	"strconv"
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

	// Declare a temporary queue
	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true, // Auto delete when connection closes
		false,
		nil,
	)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}

	// Generate a unique correlation ID
	corrID := strconv.Itoa(rand.Int())
	println(corrID)

	reader := bufio.NewReader(os.Stdin)
	log.Println("Type a message to send to RabbitMQ(type 'quit' to exit)")

	for {
		log.Println("Enter message:")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Unable to read string:", err)
		}

		input = strings.TrimSpace(input)

		if strings.ToLower(input) == "quit" {
			log.Println("Exiting client...")
			break
		}

		err = ch.Publish("", "q.rpc", false, false, amqp091.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrID,
			ReplyTo:       q.Name,
			Body:          []byte(input),
		})
		if err != nil {
			log.Fatalln("Unable to publish message:", err)
		}

		log.Println("Sent:", input)

		msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
		if err != nil {
			log.Fatalln("Failed to register consumer:", err)
		}

		for msg := range msgs {
			if msg.CorrelationId == corrID {
				log.Println("Received Response:", string(msg.Body))
				break
			}
		}
	}
}
