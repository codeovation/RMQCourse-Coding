package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {

	queueName := "q.amqp1"
	ctxB := context.Background()
	var wg sync.WaitGroup
	stateChanged := make(chan *rmq.StateChanged, 1)

	rmq.Info("Starting Consumer...")

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672", nil)

	conn, err := env.NewConnection(ctxB)
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

	consumer, err := conn.NewConsumer(ctxB, queueName, nil)
	if err != nil {
		rmq.Error("Error creating consumer...", err)
		return
	}

	ctxC, cancel := context.WithCancel(ctxB)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			deliveryContext, err := consumer.Receive(ctxC)
			if errors.Is(err, context.Canceled) {
				fmt.Println("Consumer Closed")
				return
			} else if err != nil {
				rmq.Error("Error creating consumer...", err)
				return
			}

			log.Println("Received:", fmt.Sprintf("%s", deliveryContext.Message().Data))
			err = deliveryContext.Accept(ctxB)
			if err != nil {
				rmq.Error("Error accepting message...", err)
				return
			}
		}
	}()

	fmt.Println("Consumer is running. Press enter to exit...")
	fmt.Scanln()

	cancel()
	err = consumer.Close(ctxB)
	if err != nil {
		rmq.Error("Error closing consumer...", err)
		return
	}

	err = env.CloseConnections(ctxB)
	if err != nil {
		rmq.Error("Error closing connection...", err)
		return
	}

	wg.Wait()
	close(stateChanged)

}
