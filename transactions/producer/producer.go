package main

import (
	"bufio"
	"log"
	"os"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	// Connect to RabbitMQ
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	// Open a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Failed to open a channel:", err)
	}
	defer ch.Close()

	// Declare a queue
	q, err := ch.QueueDeclare(
		"q.transaction", // Queue name
		false,           // Durable
		false,           // Auto-delete
		false,           // Exclusive
		false,           // No-wait
		nil,             // Arguments
	)
	if err != nil {
		log.Fatal("Failed to declare a queue:", err)
	}

	// Start Transaction
	err = ch.Tx()
	if err != nil {
		log.Fatal("Failed to start transaction:", err)
	}
	log.Println("Transaction started")

	reader := bufio.NewReader(os.Stdin)
	log.Println("Type a message to send to RabbitMQ (type 'quit' to exit)")
	log.Println("Messages will be sent as transaction (type 'commit' to send messages in a batch and 'rollback' to rollback the batch of all messages)")
	for {
		log.Print("Enter message:")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Unable to read string:", err)
		}
		input = strings.ToLower(strings.TrimSpace(input))

		if input == "quit" {
			log.Println("Exiting producer.")
			break
		}

		// Simple validation - if message is "rollback", we will rollback
		if input == "rollback" {
			log.Println("Message is 'rollback', rolling back transaction...")
			err = ch.TxRollback()
			if err != nil {
				log.Fatal("Failed to rollback transaction:", err)
			}
			log.Println("Transaction rolled back. Message not published.")
			return
		}

		// Simple validation - if message is "commit", we will commit
		if input == "commit" {
			log.Println("Message is 'commit', committing transaction...")
			// Commit Transaction
			err = ch.TxCommit()
			if err != nil {
				log.Fatal("Failed to commit transaction:", err)
			}
			log.Println("Transaction committed. Messages published.")
			return
		}

		err = ch.Publish("", q.Name, false, false, amqp091.Publishing{
			DeliveryMode: amqp091.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(input),
		})
		if err != nil {
			log.Println("Failed to publish message, rolling back...")
			ch.TxRollback()
			log.Fatal("Transaction rolled back because publishing failed")
		}
	}
}
