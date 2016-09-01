# magina
An [MQTT](http://mqtt.org) broker library for Golang, using RabbitMQ as backend.

## WARNING
This project is under development and not production ready. Issues and contributions are wellcomed.

## Features

* support running standalone or being embed in your own application as golang library.
* based on [RabbitMQ](http://www.rabbitmq.com)'s stable, reliable and efficient message service.
* suport QoS 0 and 1.
* easy to scale up because of the share nothing implementation. 
* extend topic schema to support RPC(non-mqtt-official).

## Features in the near future.

* suport QoS 2.
* suport websocket proxy.
* suport TLS/SSL.

## How to use 

(package management depends on [glide](https://github.com/Masterminds/glide), install it first and run `glide install` in the projectv folder)

### standalone

the standalone borker is in ```./standalone```， you can build and run it as you like.

###### options:
* `-rabbit=xxx`: follw [rabbitmq uri schema](http://www.rabbitmq.com/uri-spec.html). example: `-rabbit=amqp://user:pass@host:10000/vhost`

### As GoLang library

###### example: 

``` Go
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
		// rabbit uri
		RabbitURI: "amqp://guest:guest@localhost:5672/",
		// callbacks
		Authenticate:       authenticate,
		AuthorizePublish:   authorizePublish,
		AuthorizeSubscribe: authorizeSubscribe,
	}

	server.ListenAndServe()

	return
}


```

