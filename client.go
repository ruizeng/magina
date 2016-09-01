package magina

import (
	"github.com/ruizeng/magina/packets"
	"github.com/streadway/amqp"
	"log"
	"net"
	"strings"
)

type Client struct {
	Conn       net.Conn
	Broker     *Broker
	Identifier string
	Channel    *amqp.Channel
	Exchangers map[string]Exchanger
}

func (c *Client) initRabbit() error {
	var err error
	if c.Channel == nil {
		c.Channel, err = c.Broker.RabbitConnection.Channel()
		if err != nil {
			return err
		}

		err = c.initExchangers()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) initExchangers() error {
	c.Exchangers = make(map[string]Exchanger)
	c.Exchangers[""] = NewPubSubExchanger(c.Channel)
	c.Exchangers["rpc"] = NewPRCExchanger(c.Channel)
	for exc := range c.Exchangers {
		err := c.Exchangers[exc].Init()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) getExchanger(topic string) Exchanger {
	if strings.HasPrefix(topic, "rpc://") {
		return c.Exchangers["rpc"]
	} else {
		return c.Exchangers[""]
	}
}

func (c *Client) trySendPacket(packet packets.ControlPacket) error {
	log.Printf("try send packet******** \n%v\n**********\n", packet)
	return packet.Write(c.Conn)
}

func (c *Client) handlePublish(pub *packets.PublishPacket) error {
	exchanger := c.getExchanger(pub.TopicName)
	err := exchanger.Publish(ExchangeMessage{pub.TopicName, pub.Payload})
	if pub.Qos == 1 {
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = pub.MessageID
		puback.Qos = pub.Qos
		c.trySendPacket(puback)
	}
	return err
}

func (c *Client) handleSubscribe(sub *packets.SubscribePacket) error {
	topic := sub.Topics[0]
	exchanger := c.getExchanger(topic)
	msgs, err := exchanger.Subscribe(topic)
	if err != nil {
		return err
	}
	go func() {
		for d := range msgs {
			pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			pub.Payload = d.Payload
			pub.TopicName = d.Topic
			c.trySendPacket(pub)
		}
	}()
	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = sub.MessageID
	suback.Qos = sub.Qos
	c.trySendPacket(suback)
	return nil
}

func (c *Client) handleUnsubscribe(unsub *packets.UnsubscribePacket) error {
	topic := unsub.Topics[0] // only suport one topic a time now

	exchanger := c.getExchanger(topic)
	err := exchanger.Unsubscribe(topic)
	if err != nil {
		return err
	}

	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = unsub.MessageID
	unsuback.Qos = unsub.Qos
	return c.trySendPacket(unsuback)
}

func (c *Client) Serve() {
	defer func() {
		if c.Channel != nil {
			c.Channel.Close()
			c.Channel = nil
		}
		if c.Conn != nil {
			c.Conn.Close()
			c.Conn = nil
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
			err = c.trySendPacket(ca)
		case *packets.DisconnectPacket:
			log.Println("disconnecting client...")
			needDisconnect = true
		case *packets.PingreqPacket:
			pres := packets.NewControlPacket(packets.Pingresp)
			log.Println("ping back to client...")
			err = c.trySendPacket(pres)
		case *packets.PublishPacket:
			pub := packet.(*packets.PublishPacket)
			err = c.handlePublish(pub)
		case *packets.SubscribePacket:
			sub := packet.(*packets.SubscribePacket)
			err = c.handleSubscribe(sub)
		case *packets.UnsubscribePacket:
			unsub := packet.(*packets.UnsubscribePacket)
			err = c.handleUnsubscribe(unsub)
		default:
			log.Println("unknown packet: ", packet)
		}
		if err != nil {
			log.Println("handle packat error: ", err)
		}
	}
}
