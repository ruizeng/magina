package magina

import (
	"fmt"
	"github.com/ruizeng/magina/packets"
	"github.com/streadway/amqp"
	"log"
	"net"
)

const (
	defaultPubsubExchange = "pubsub"
	defaultCrpcExchange   = "crpc"
	defaultDrpcExchange   = "drpc"
)

type Client struct {
	Conn       net.Conn
	Broker     *Broker
	Identifier string
	Channel    *amqp.Channel
}

func (c *Client) initRabbit() error {
	var err error
	if c.Channel == nil {
		c.Channel, err = c.Broker.RabbitConnection.Channel()
		if err != nil {
			return err
		}

		err = c.Channel.ExchangeDeclare(
			defaultPubsubExchange, // name
			"topic",               // type
			true,                  // durable
			false,                 // auto-deleted
			false,                 // internal
			false,                 // no-wait
			nil,                   // arguments
		)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) trySendPacket(packet packets.ControlPacket) {
	log.Printf("try send packet******** \n%v\n**********\n", packet)
	err := packet.Write(c.Conn)
	if err != nil {
		log.Println(err)
	}
}

func (c *Client) handlePublish(pub *packets.PublishPacket) error {
	if c.Channel == nil {
		return fmt.Errorf("client channel not ready")
	}
	err := c.Channel.Publish(defaultPubsubExchange,
		pub.TopicName, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        pub.Payload,
		})
	return err
}

func (c *Client) handleSubscribe(sub *packets.SubscribePacket) error {
	if c.Channel == nil {
		return fmt.Errorf("client channel not ready")
	}
	q, err := c.Channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	err = c.Channel.QueueBind(
		q.Name,                // queue name
		sub.Topics[0],         // routing key
		defaultPubsubExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	msgs, err := c.Channel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			pub.Payload = d.Body
			pub.TopicName = d.RoutingKey
			c.trySendPacket(pub)
		}
	}()

	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = sub.MessageID
	c.trySendPacket(suback)
	return nil
}

func (c *Client) Serve() {
	defer func() {
		if c.Channel != nil {
			c.Channel.Close()
		}
		if c.Conn != nil {
			c.Conn.Close()
		}
	}()
	needDisconnect := false
	for {
		if needDisconnect {
			break
		}
		packet, err := packets.ReadPacket(c.Conn)
		if err != nil {
			log.Printf("reading packets from connection error: %v", err)
			break
		}
		log.Printf("packet received =========\n%v\n===============\n", packet)
		switch packet.(type) {
		case *packets.ConnectPacket:
			conn := packet.(*packets.ConnectPacket)
			ca := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
			ca.ReturnCode = conn.Validate()
			err = c.initRabbit()
			if err != nil {
				log.Printf("init rabbitmq for client failed: %v\n", err)
				ca.ReturnCode = packets.ErrRefusedServerUnavailable
			}
			c.trySendPacket(ca)
		case *packets.DisconnectPacket:
			log.Println("disconnecting client...")
			needDisconnect = true
		case *packets.PingreqPacket:
			pres := packets.NewControlPacket(packets.Pingresp)
			log.Println("ping back to cliend...")
			c.trySendPacket(pres)
		case *packets.PublishPacket:
			pub := packet.(*packets.PublishPacket)
			c.handlePublish(pub)
		case *packets.SubscribePacket:
			sub := packet.(*packets.SubscribePacket)
			c.handleSubscribe(sub)
		default:
			log.Println("unknown packet: ", packet)
		}
	}
}
