package main

import (
	"bufio"
	"log"
	"os"
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

	exchangeName := "x.headers.log"
	err = ch.ExchangeDeclare(exchangeName, "headers", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare exchange:", err)
	}

	queues := []struct {
		name    string
		headers amqp091.Table
	}{
		{name: "q.admin", headers: amqp091.Table{
			"user_type": "admin",
			"x-match":   "all",
		}},
		{name: "q.user", headers: amqp091.Table{
			"user_type": "user",
			"x-match":   "all",
		}},
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

		err = ch.QueueBind(q.Name, "", exchangeName, false, queue.headers)
		if err != nil {
			log.Fatalln("Failed to bind the queue:", err)
		}
	}

	log.Println("Attempting to publish message...")

	reader := bufio.NewReader(os.Stdin)
	log.Println("Type a message to send to RabbitMQ (type 'quit' to exit)")
	for {

		log.Printf("Enter user type [%v/%v]:", queues[0].headers["user_type"], queues[1].headers["user_type"])
		userType, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Unable to read user type:", err)
		}

		userType = strings.TrimSpace(strings.ToLower(userType))

		if userType == "quit" {
			log.Println("Exiting publisher...")
			break
		}

		if userType != queues[0].headers["user_type"] && userType != queues[1].headers["user_type"] {
			log.Println("Invalid user type. Please enter either 'admin' or 'user'")
			continue
		}

		log.Print("Enter message:")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Unable to read string:", err)
		}
		input = strings.TrimSpace(input)

		if strings.ToLower(input) == "quit" {
			log.Println("Exiting producer.")
			break
		}
		err = ch.Publish(exchangeName, "", false, false, amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(input),
			Headers: amqp091.Table{
				"user_type": userType,
			},
		})
		if err != nil {
			log.Fatalln("Failed to publish message:", err)
		}

		log.Println("Sent:", input)
	}
}
