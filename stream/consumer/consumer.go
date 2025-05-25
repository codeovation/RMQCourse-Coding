// docker run -d --name rabbitmq-streams -p 5552:5552 -p 5676:5672 -p 15676:15672 rabbitmq:management
// docker exec -it rabbitmq-streams rabbitmq-plugins enable rabbitmq_stream_management
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

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
	err = env.DeclareStream(streamName, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.GB(2),
	})
	if err != nil {
		log.Fatalln("Failed to declare stream:", err)
	}

	messagesHandler := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("Stream: %s - Received message: %s\n", consumerContext.Consumer.GetStreamName(), message.Data)
	}

	consumerOptions := stream.NewConsumerOptions().SetOffset(stream.OffsetSpecification{}.First())

	consumer, err := env.NewConsumer(streamName, messagesHandler, consumerOptions)
	if err != nil {
		log.Fatalln("Failed to declare consumer:", err)
	}

	log.Println("Consumer is ready and listening...")

	reader := bufio.NewReader(os.Stdin)
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
