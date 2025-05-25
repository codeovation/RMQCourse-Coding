package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	queueName := "q.amqp1.reliable"
	var received int32
	ctxB := context.Background()
	var wg sync.WaitGroup
	stateChanged := make(chan *rmq.StateChanged, 1)

	startTime := time.Now()

	rmq.Info("Starting Reliable Consumer...")

	conn, err := rmq.Dial(ctxB, "amqp://", &rmq.AmqpConnOptions{
		ContainerID: "reliable-producer",
		RecoveryConfiguration: &rmq.RecoveryConfiguration{
			ActiveRecovery:           true,
			BackOffReconnectInterval: 2 * time.Second,
			MaxReconnectAttempts:     5,
		},
	})
	if err != nil {
		rmq.Error("Error opening connection...", err)
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

	q, err := management.DeclareQueue(ctxB, &rmq.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		rmq.Error("Error declaring queue", err)
		return
	}

	consumer, err := conn.NewConsumer(ctxB, q.Name(), nil)
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
			atomic.AddInt32(&received, 1)
			// log.Println("Received:", fmt.Sprintf("%s", deliveryContext.Message().Data))
			err = deliveryContext.Accept(ctxB)
			if err != nil {
				rmq.Error("Error accepting message...", err)
				return
			}
		}
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Stats goroutine exiting...")
				return
			case <-ticker.C:
				mps := float64(received) / float64(time.Since(startTime).Seconds())
				log.Println("[Stats]", "Received:", received, "Messages Per Second", mps)
			}
		}
	}(ctxC)

	fmt.Println("Consumer is running. Press enter to exit...")
	_, err = fmt.Scanln()
	if err != nil {
		rmq.Error("Error reading input...", err)
		return
	}

	cancel()
	err = consumer.Close(ctxB)
	if err != nil {
		rmq.Error("Error closing consumer...", err)
		return
	}

	err = conn.Close(ctxB)
	// err = env.CloseConnections(ctxB)
	if err != nil {
		rmq.Error("Error closing connection...", err)
		return
	}

	wg.Wait()
	close(stateChanged)

}
