package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

func main() {

	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln("Failed to establish a connection:", err)
	}
	defer ch.Close()

	queues := []string{"q.user", "q.admin"}

	for _, queue := range queues {

		// Declare a queue
		_, err := ch.QueueDeclare(
			queue, // queue name
			true,  // durability
			false, // delete when unused
			false, // exclusive
			false, // no wait
			amqp091.Table{
				"x-queue-type": "quorum",
			}, // arguments
			// nil,
		)
		if err != nil {
			log.Fatalln("Failed to declare a queue:", err)
		}
	}

	reader := bufio.NewReader(os.Stdin)
	var selectedQueue string

	for {
		fmt.Printf("Enter the queue name to consume from (options: %v/%v):", queues[0], queues[1])
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read string:", err)
		}
		input = strings.TrimSpace(input)

		if input == queues[0] || input == queues[1] {
			selectedQueue = input
			break
		}
		fmt.Println("Invalid queue name, Please enter either:", queues[0], "or", queues[1])
	}

	fmt.Println("Listening on queue:", selectedQueue)

	msgs, err := ch.Consume(selectedQueue, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to register the consumer:", err)
	}

	for msg := range msgs {
		log.Printf("Received: %s", msg.Body)
		msg.Ack(false) // false means "ack this single message only", 'true' in case of batch processing
	}
}
