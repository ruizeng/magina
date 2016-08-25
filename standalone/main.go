// a standalone mqtt broker implementation with default behaviour.

package main

import (
	"flag"
	"github.com/ruizeng/magina"
)

func authenticate(client *magina.Client, username string, password string) bool {
	return true
}
func authorizePublish(client *magina.Client, topic string) bool {
	return true
}
func authorizeSubscribe(client *magina.Client, topic string) bool {
	return true
}

var rabbitURI = flag.String("rabbit", "amqp://guest:guest@localhost:5672/", "rabbitmq uri")

func main() {
	server := &magina.Broker{
		// server address to listen
		Addr:      ":1883",
		RabbitURI: *rabbitURI,
		// callbacks
		Authenticate:       authenticate,
		AuthorizePublish:   authorizePublish,
		AuthorizeSubscribe: authorizeSubscribe,
	}

	server.ListenAndServe()

	return
}
