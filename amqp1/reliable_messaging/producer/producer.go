package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	queueName := "q.amqp1.reliable"
	ctx := context.Background()
	var stateAccepted int32
	var stateReleased int32
	var stateRejected int32
	var stateModified int32
	var failed int32

	startTime := time.Now()

	stateChanged := make(chan *rmq.StateChanged, 1)
	var wg sync.WaitGroup

	rmq.Info("Starting Reliable Producer...")

	conn, err := rmq.Dial(ctx, "amqp://", &rmq.AmqpConnOptions{
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

	publisher, err := conn.NewPublisher(ctx, &rmq.QueueAddress{
		Queue: queueName,
	}, nil)
	if err != nil {
		rmq.Error("Error creating publisher...", err)
		return
	}

	for i := 0; i < 100_000; i++ {

		publishResult, err := publisher.Publish(ctx, rmq.NewMessage([]byte("Hello, AMQP1.0! "+fmt.Sprintf("%d", i))))
		if err != nil {
			atomic.AddInt32(&failed, 1)
			rmq.Error("Error publishing message...", err)
			continue
		}

		switch publishResult.Outcome.(type) {
		case *rmq.StateAccepted:
			atomic.AddInt32(&stateAccepted, 1)
			// fmt.Println("Message accepted")
		case *rmq.StateRejected:
			atomic.AddInt32(&stateRejected, 1)
			// fmt.Println("Message rejected")
		case *rmq.StateReleased:
			atomic.AddInt32(&stateReleased, 1)
			// fmt.Println("Message released")
		case *rmq.StateModified:
			atomic.AddInt32(&stateModified, 1)
			// fmt.Println("Message modified")
		}
	}

	err = publisher.Close(ctx)
	if err != nil {
		rmq.Error("Error closing publisher...", err)
		return
	}

	mps := float64(stateAccepted+stateModified+stateRejected+stateReleased) / float64(time.Since(startTime).Seconds())
	fmt.Println("[*Stats*]", "sent:", stateAccepted+stateModified+stateRejected+stateReleased, "failed:", failed, "Message Rate:", mps)
	fmt.Println("[*Stats*]", "Accepted:", stateAccepted, "Modified:", stateModified, "Rejected:", stateRejected, "Unrouted/Lost:", stateReleased)

	// err = env.CloseConnections(ctx)
	err = conn.Close(ctx)
	if err != nil {
		rmq.Error("Error closing connection...", err)
		return
	}

	wg.Wait()
	close(stateChanged)
}
