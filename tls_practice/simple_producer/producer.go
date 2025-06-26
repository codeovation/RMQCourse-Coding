package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

func main() {

	// connect to RMQ
	// conn, err := amqp091.Dial("amqp://guest:guest@localhost:5677/")
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
		log.Fatalln("Failed to open a channel:", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare("x.logs", "fanout", true, false, false, false, nil)
	if err != nil {
		log.Fatalln("Failed to declare exchange:", err)
	}

	// Declare a queue
	q, err := ch.QueueDeclare(
		"queue1", // queue name
		true,     // durability
		false,    // delete when unused
		false,    // exclusive
		false,    // no wait
		amqp091.Table{
			"x-queue-type": "quorum",
		}, // arguments
		// nil,
	)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}
	// log.Println("Connected and queue declared")

	// Declare a queue
	q2, err := ch.QueueDeclare(
		"queue2", // queue name
		true,     // durability
		false,    // delete when unused
		false,    // exclusive
		false,    // no wait
		amqp091.Table{
			"x-queue-type": "quorum",
		}, // arguments
		// nil,
	)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}

	err = ch.QueueBind(q.Name, "", "x.logs", false, nil)
	if err != nil {
		log.Fatalln("Failed to bind the queue:", err)
	}

	err = ch.QueueBind(q2.Name, "", "x.logs", false, nil)
	if err != nil {
		log.Fatalln("Failed to bind the queue:", err)
	}

	// log.Println("Stopping RabbitMQ container")
	// cmd := exec.Command("docker", "stop", "rabbitmq")
	// err = cmd.Run()
	// if err != nil {
	// 	log.Fatalln("Failed to stop the container:", err)
	// }

	// time.Sleep(time.Second)

	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()
	// msg := "Hello, RabbitMQ!"
	log.Println("Attempting to publish message...")

	reader := bufio.NewReader(os.Stdin)
	log.Println("Type a message to send to RabbitMQ (type 'quit' to exit)")
	for {
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
		err = ch.Publish("x.logs", "", false, false, amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(input),
		})
		if err != nil {
			log.Fatalln("Failed to publish message:", err)
		}

		log.Println("Sent:", input)
	}
}
