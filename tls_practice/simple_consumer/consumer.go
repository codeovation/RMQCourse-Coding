package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

func main() {

	// conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672")
	// conn, err := amqp091.DialConfig("amqp://guest:guest@localhost:5672", amqp091.Config{Heartbeat: 3 * time.Second}) // Heartbeat interval of 3 seconds
	crt, err := tls.LoadX509KeyPair("client_venom_certificate.pem", "client_venom_key.pem")
	if err != nil {
		log.Fatalln("Failed to load client cert/key:", err)
	}

	//Load CA Cert
	caCert, err := os.ReadFile("ca_certificate.pem")
	if err != nil {
		log.Fatalln("Failed to load CA cert:", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{crt},
		RootCAs:      caCertPool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
	}

	//Dial using TLS
	conn, err := amqp091.DialTLS("amqps://guest:guest@localhost:5671", tlsConfig)
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

		// Simulate message processing
		// If processing successful
		msg.Ack(false) // false means "ack this single message only", 'true' in case of batch processing

		// If processing failed and you want to requeue:
		// msg.Nack(false, true) // requeue = true

		// If processing failed and you don’t want to requeue (e.g., send to DLX):
		// msg.Reject(false)
	}
}
