package main

import (
	"log"
	"time"

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

	q, err := ch.QueueDeclare("task_queue", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare queue:", err)
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalln("Failed to set QoS:", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to register a consumer:", err)
	}

	log.Println("Worker is waiting for messages...")

	for msg := range msgs {
		log.Printf("Processing: %s", msg.Body)
		time.Sleep(time.Duration(len(msg.Body)) * time.Millisecond * 300) // simulate work
		log.Println("Done:", string(msg.Body))

		// Acknowledge message after processing
		msg.Ack(false)
	}

}
