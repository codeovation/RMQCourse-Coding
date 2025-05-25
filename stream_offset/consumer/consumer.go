// docker run -d --name rabbitmq-streams -p 5552:5552 -p 5676:5672 -p 15676:15672 rabbitmq:management
// docker exec -it rabbitmq-streams rabbitmq-plugins enable rabbitmq_stream_management
package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func main() {

	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetHost("localhost").SetPort(5552).SetUser("guest").SetPassword("guest"))
	if err != nil {
		log.Fatalln("Failed to create stream environment:", err)
	}
	defer env.Close()

	streamName := "stream.hello"
	consumerName := "ashish"
	err = env.DeclareStream(streamName, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.GB(2),
	})
	if err != nil {
		log.Fatalln("Failed to declare stream:", err)
	}

	messagesHandler := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("Received message: %s\n", message.Data)
		err = consumerContext.Consumer.StoreOffset()
		if err != nil {
			log.Fatalln("Failed to store offset:", err)
		}
		log.Println("Consumer name:", consumerContext.Consumer.GetName())
	}

	reader := bufio.NewReader(os.Stdin)
	var consumerOptions *stream.ConsumerOptions

	for {
		fmt.Println("Where do you want to start reading messages from?")
		fmt.Println("Options: first / last / last read message")
		fmt.Print("Enter choice:")

		offsetInput, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read user input:", err)
		}
		offsetInput = strings.ToLower(strings.TrimSpace(offsetInput))

		switch offsetInput {
		case "first":
			consumerOptions = stream.NewConsumerOptions().SetManualCommit().SetConsumerName(consumerName).SetOffset(stream.OffsetSpecification{}.First())
			// consumerOptions := stream.NewConsumerOptions().SetOffset(stream.OffsetSpecification{}.First())
		case "last":
			consumerOptions = stream.NewConsumerOptions().SetManualCommit().SetConsumerName(consumerName).SetOffset(stream.OffsetSpecification{}.Last())
		default:
			// case "last read message":
			var offsetSpecification stream.OffsetSpecification
			storedOffset, err := env.QueryOffset(consumerName, streamName)
			if errors.Is(err, stream.OffsetNotFoundError) || err != nil {
				fmt.Println("Last stored offset not found...")
				offsetSpecification = stream.OffsetSpecification{}.First()
			} else {
				fmt.Println("Last stored offset found")
				offsetSpecification = stream.OffsetSpecification{}.Offset(storedOffset + 1)
			}
			consumerOptions = stream.NewConsumerOptions().SetManualCommit().SetConsumerName(consumerName).SetOffset(offsetSpecification)
		}
		break
	}

	consumer, err := env.NewConsumer(streamName, messagesHandler, consumerOptions)
	if err != nil {
		log.Fatalln("Failed to declare consumer:", err)
	}

	log.Println("Consumer is ready and listening...")

	fmt.Println("Waiting for incoming messages, press enter to close the consumer")
	_, err = reader.ReadString('\n')
	if err != nil {
		log.Fatalln("Failed to read user input:", err)
	}

	err = consumer.Close()
	if err != nil {
		log.Fatalln("Failed to close consumer:", err)
	}
}
