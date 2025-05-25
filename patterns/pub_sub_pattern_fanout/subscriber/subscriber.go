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

	queueNames := []string{"queue1", "queue2"}

	for _, name := range queueNames {

		_, err := ch.QueueDeclare(name, true, false, false, false, amqp091.Table{
			"x-queue-type": "quorum",
		})
		if err != nil {
			log.Fatalln("Failed to declare a queue:", err)
		}

		err = ch.QueueBind(name, "", "x.logs", false, nil)
		if err != nil {
			log.Fatalln("Failed to bind queue:", err)
		}
	}

	reader := bufio.NewReader(os.Stdin)
	var selectedQueue string

	for {
		fmt.Print("Enter the queue name to consume from (options: queue1/queue2):")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read string:", err)
		}
		input = strings.TrimSpace(input)

		if input == queueNames[0] || input == queueNames[1] {
			selectedQueue = input
			break
		}
		fmt.Println("Invalid queue name, Please enter either:", queueNames[0], "or", queueNames[1])
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
