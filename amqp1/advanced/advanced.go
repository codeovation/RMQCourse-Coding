package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {

	fmt.Println("Go AMQP 1.0 Advanced connection settings")

	env := rmq.NewClusterEnvironmentWithStrategy([]rmq.Endpoint{
		{
			Address: "amqp://localhost:5674",
			Options: &rmq.AmqpConnOptions{
				Id: "wrong container config",
				RecoveryConfiguration: &rmq.RecoveryConfiguration{
					ActiveRecovery:           true,
					BackOffReconnectInterval: 2 * time.Second,
					MaxReconnectAttempts:     5,
				},
				SASLType: amqp.SASLTypePlain("guest", "guest"),
			},
		},

		{
			Address: "amqp://localhost:5673",
			Options: &rmq.AmqpConnOptions{
				SASLType: amqp.SASLTypeAnonymous(),
				RecoveryConfiguration: &rmq.RecoveryConfiguration{
					ActiveRecovery: false,
				},
				Id: "container one",
			},
		},

		{
			Address: "amqp://localhost:5672",
			Options: &rmq.AmqpConnOptions{
				SASLType: amqp.SASLTypePlain("guest", "guest"),
				RecoveryConfiguration: &rmq.RecoveryConfiguration{
					ActiveRecovery:           true,
					BackOffReconnectInterval: 2 * time.Second,
					MaxReconnectAttempts:     5,
				},
				Id: "container two",
			},
		},
	}, rmq.StrategyRandom)

	for range 5 {
		conn, err := env.NewConnection(context.Background())
		if err != nil {
			rmq.Error("Error opening a connection", err)
			return
		}

		fmt.Println("Connection Properties", conn.Properties())
		fmt.Println("Connection Cluster Name", conn.Properties()["cluster_name"])
		fmt.Println("Connection ID", conn.Id())
		fmt.Println("Connection State", conn.State())
		time.Sleep(time.Millisecond * 200)
	}

	fmt.Println("Press any key to exit...")
	fmt.Scanln()

}
