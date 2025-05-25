package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	exchangeName := "x.amqp1.fanout"
	ctx := context.Background()
	stateChanged := make(chan *rmq.StateChanged, 1)
	var wg sync.WaitGroup

	rmq.Info("Starting Producer...")

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672", nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		rmq.Error("Error establishing connection with RabbitMQ...", err)
		return
	}
	conn.NotifyStatusChange(stateChanged)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for statusChanged := range stateChanged {
			fmt.Println("Status changed:", statusChanged)
			if statusChanged.String() == "From: open, To: closed, Error: %!s(<nil>)" {
				fmt.Println("Exiting stateChanged monitoring goroutine")
				return
			}
		}
	}()

	management := conn.Management()

	_, err = management.DeclareExchange(ctx, &rmq.FanOutExchangeSpecification{
		Name: exchangeName,
	})
	if err != nil {
		rmq.Error("Error declaring exchange", err)
		return
	}

	publisher, err := conn.NewPublisher(ctx, &rmq.ExchangeAddress{ // url or address : "exchange/routing_key"
		Exchange: exchangeName,
	}, nil)
	if err != nil {
		rmq.Error("Error creating publisher...", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)
	log.Println("Type a message to send to RabbitMQ AMQP 1.0 (type 'quit' to exit)")

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

		publishResult, err := publisher.Publish(ctx, rmq.NewMessage([]byte(input)))
		if err != nil {
			rmq.Error("Error publishing message...", err)
			continue
		}
		// fmt.Println("Publish result:", publishResult)

		switch publishResult.Outcome.(type) {
		case *rmq.StateAccepted:
			fmt.Println("Message accepted")
		case *rmq.StateRejected:
			fmt.Println("Message rejected")
		case *rmq.StateReleased:
			fmt.Println("Message released")
		case *rmq.StateModified:
			fmt.Println("Message modified")
		}
	}

	err = publisher.Close(ctx)
	if err != nil {
		rmq.Error("Error closing publisher...", err)
		return
	}

	// err = conn.Close(ctx) // handle the error

	err = env.CloseConnections(ctx)
	if err != nil {
		rmq.Error("Error closing connection...", err)
		return
	}

	wg.Wait()
	close(stateChanged)
}
