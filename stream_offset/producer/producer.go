// docker run -d --name rabbitmq-streams -p 5552:5552 -p 5676:5672 -p 15676:15672 rabbitmq:management
// docker exec -it rabbitmq-streams rabbitmq-plugins enable rabbitmq_stream_management
package main

import (
	"bufio"
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
	err = env.DeclareStream(streamName, &stream.StreamOptions{
		MaxLengthBytes: stream.ByteCapacity{}.GB(2),
	})
	if err != nil {
		log.Fatalln("Failed to declare stream:", err)
	}

	producer, err := env.NewProducer(streamName, stream.NewProducerOptions())
	if err != nil {
		log.Fatalln("Failed to declare producer:", err)
	}
	defer producer.Close()

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

		err = producer.Send(amqp.NewMessage([]byte(input)))
		if err != nil {
			log.Fatalln("Failed to publish message:", err)
		}
		log.Println("Sent:", input)
	}
}
