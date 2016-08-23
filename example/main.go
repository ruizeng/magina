package main

import (
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

func main() {
	server := &magina.Broker{
		// server address to listen
		Addr: ":1883",
		// callbacks
		Authenticate:       authenticate,
		AuthorizePublish:   authorizePublish,
		AuthorizeSubscribe: authorizeSubscribe,
	}

	server.ListenAndServe()

	return
}
