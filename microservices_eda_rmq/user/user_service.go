// User Service with RabbitMQ integration
package main

import (
	"log"
	"net/http"

	amqp "github.com/rabbitmq/amqp091-go"
)

func checkError(err error) {
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}

func publishLoginEvent(ch *amqp.Channel) {
	// Declare a fanout exchange
	err := ch.ExchangeDeclare(
		"logs",   // Exchange name
		"fanout", // Exchange type (fanout will broadcast to all queues)
		true,     // Durable (survive broker restarts)
		false,    // Auto-delete (delete when no consumers)
		false,    // Internal (used by the broker internally)
		false,    // No-wait
		nil,      // Arguments
	)
	checkError(err)

	// Publish a login event to the fanout exchange
	message := "User logged in"
	err = ch.Publish(
		"logs", // Exchange name
		"",     // Routing key (not used in fanout)
		false,  // Mandatory
		false,  // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
	checkError(err)

	log.Println("Published user login event to fanout exchange")
}

func publishVipEvent(ch *amqp.Channel) {
	// Declare a queue to which the login event will be sent
	q, err := ch.QueueDeclare(
		"vip-area", // Queue name
		false,      // Durable
		false,      // Delete when unused
		false,      // Exclusive
		false,      // No-wait
		nil,        // Arguments
	)
	checkError(err)

	// Publish login event
	message := "User logged in and accessing VIP area"
	err = ch.Publish(
		"",     // Exchange
		q.Name, // Routing key (queue name)
		false,  // Mandatory
		false,  // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
	checkError(err)

	log.Println("Published user login event")
}

// Consume VIP response from VIP service
func consumeVipResponse(ch *amqp.Channel) string {
	// Declare the response queue from VIP service
	q, err := ch.QueueDeclare(
		"vip-response", // Queue name
		false,          // Durable
		false,          // Delete when unused
		false,          // Exclusive
		false,          // No-wait
		nil,            // Arguments
	)
	checkError(err)

	// Consume messages from the response queue
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

	// Get the message from the queue
	for msg := range msgs {
		log.Println("Received from VIP service:", string(msg.Body))
		return string(msg.Body)
	}
	return ""
}

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	checkError(err)
	defer conn.Close()

	ch, err := conn.Channel()
	checkError(err)
	defer ch.Close()

	log.Println("User Service running on port 8081...")
	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		// Simulate user login
		log.Println("User logged in")
		publishLoginEvent(ch) // Publish login event to RabbitMQ
		w.Write([]byte("User logged in"))
	})
	http.HandleFunc("/vipaccess", func(w http.ResponseWriter, r *http.Request) {
		// Simulate vipaccess
		log.Println("User accessing vip area")
		publishVipEvent(ch) // Publish login event to RabbitMQ

		// Consume the response from VIP service
		vipResponse := consumeVipResponse(ch)

		// Respond to the client with the VIP service message
		w.Write([]byte(vipResponse)) // The message received from VIP service
	})
	http.ListenAndServe(":8081", nil)
}
