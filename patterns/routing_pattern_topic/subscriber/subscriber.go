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

	exchangeName := "x.topic.logs"
	err = ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare exchange:", err)
	}

	queues := []struct {
		name    string
		pattern string
	}{
		{name: "q.error", pattern: "error.#"},
		{name: "q.info", pattern: "*.database"},
	}

	for _, queue := range queues {

		// Declare a queue
		q, err := ch.QueueDeclare(
			queue.name, // queue name
			true,       // durability
			false,      // delete when unused
			false,      // exclusive
			false,      // no wait
			amqp091.Table{
				"x-queue-type": "quorum",
			}, // arguments
			// nil,
		)
		if err != nil {
			log.Fatalln("Failed to declare a queue:", err)
		}

		err = ch.QueueBind(q.Name, queue.pattern, exchangeName, false, nil)
		if err != nil {
			log.Fatalln("Failed to bind the queue:", err)
		}
	}

	reader := bufio.NewReader(os.Stdin)
	var selectedQueue string

	for {
		fmt.Printf("Enter the queue name to consume from (options: %v/%v):", queues[0].name, queues[1].name)
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read string:", err)
		}
		input = strings.TrimSpace(input)

		if input == queues[0].name || input == queues[1].name {
			selectedQueue = input
			break
		}
		fmt.Println("Invalid queue name, Please enter either:", queues[0].name, "or", queues[1].name)
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
