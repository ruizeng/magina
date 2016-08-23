package magina

import (
	"github.com/streadway/amqp"
	"log"
	"net"
)

// fail fast if critical error happened.
func failOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// callback functions for authentication and authorization
type AuthenticateFunc func(client *Client, username string, password string) bool
type AuthorizePublishFunc func(client *Client, topic string) bool
type AuthorizeSubscribeFunc func(client *Client, topic string) bool

type Broker struct {
	// server address to listen
	Addr string
	// extend the broker to suport RPC. (WARNNING: NOT standard MQTT feature)
	SuportRPC bool
	// rabbitmq connection
	RabbitConnection *amqp.Connection
	// callbacks
	Authenticate       AuthenticateFunc
	AuthorizePublish   AuthorizePublishFunc
	AuthorizeSubscribe AuthorizeSubscribeFunc
}

func (b *Broker) InitRabbitConn() {
	if b.RabbitConnection == nil {
		conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
		failOnError(err)
		b.RabbitConnection = conn
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
	client := &Client{
		Conn:   conn,
		Broker: b,
	}
	client.Serve()
}

func (b *Broker) ListenAndServe() {
	b.InitRabbitConn()
	log.Println("listen and serve mqtt broker on " + b.Addr)
	listener, err := net.Listen("tcp", b.Addr)
	failOnError(err)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("error accepting new connection: " + err.Error())
		}
		go b.handleConnection(conn)
	}
}
