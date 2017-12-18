package magina

import (
	"log"
	"net"
	"strings"
	"time"

	"github.com/ruizeng/magina/packets"
	"github.com/streadway/amqp"
)

type Client struct {
	Conn              net.Conn
	Broker            *Broker
	Identifier        string
	LastHeartbeat     time.Time
	KeepAliveInterval int
	needDisconnect    bool
	Channel           *amqp.Channel
	Exchangers        map[string]Exchanger
	SubscribeTopics   map[string]string
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

		if c.SubscribeTopics == nil {
			c.SubscribeTopics = make(map[string]string)
		}
		for topic := range c.SubscribeTopics {
			err := c.subscribe(topic)
			if err != nil {
				return err
			}
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
	}
	return c.Exchangers[""]
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

func (c *Client) subscribe(topic string) error {
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
	return nil
}

func (c *Client) handleSubscribe(sub *packets.SubscribePacket) error {
	topic := sub.Topics[0]
	c.SubscribeTopics[topic] = topic
	err := c.subscribe(topic)
	if err != nil {
		return err
	}
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

func (c *Client) checkHeartbeat() {
	for {
		if c.needDisconnect {
			break
		}
		checkInterval := c.KeepAliveInterval
		if checkInterval == 0 {
			checkInterval = 60
		}
		log.Printf("timer, check interval: %v", checkInterval)
		time.Sleep(time.Second * time.Duration(checkInterval))
		// no ping for 2 times
		if time.Since(c.LastHeartbeat) > time.Second*time.Duration(2*checkInterval) {
			log.Printf("client lost: %v", c.Identifier)
			if c.Broker.OnClientOffline != nil {
				c.Broker.OnClientOffline(c)
			}
			c.needDisconnect = true
		}
	}
}

// Serve serves a mqtt client.
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
	for {
		if c.needDisconnect {
			break
		}
		packet, err := packets.ReadPacket(c.Conn)
		if err != nil {
			// log.Printf("reading packets from connection error: %v", err)
			c.needDisconnect = true
			break
		}
		log.Printf("packet received =========\n%v\n===============\n", packet)
		switch packet.(type) {
		case *packets.ConnectPacket:
			conn := packet.(*packets.ConnectPacket)
			ca := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
			ca.ReturnCode = conn.Validate()
			c.Identifier = conn.ClientIdentifier
			c.KeepAliveInterval = int(conn.KeepaliveTimer)
			if c.Broker.Authenticate != nil {
				if !c.Broker.Authenticate(c, string(conn.Username), string(conn.Password)) {
					ca.ReturnCode = packets.ErrRefusedBadUsernameOrPassword
				}
			}
			err = c.initRabbit()
			if err != nil {
				log.Printf("init rabbitmq for client failed: %v\n", err)
				ca.ReturnCode = packets.ErrRefusedServerUnavailable
			}
			if ca.ReturnCode != packets.Accepted {
				c.needDisconnect = true
			} else {
				if c.Broker.OnClientOnline != nil {
					go func() { c.Broker.OnClientOnline(c) }()
				}
				go c.checkHeartbeat()
			}
			err = c.trySendPacket(ca)
		case *packets.DisconnectPacket:
			log.Println("disconnecting client...")
			c.needDisconnect = true
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
			if err == amqp.ErrClosed {
				c.Channel = nil
				err := c.initRabbit()
				if err != nil {
					log.Println("wanning: reinit rabbit error: ", err)
					continue
				}
			}
		}
		c.LastHeartbeat = time.Now()
		if c.Broker.OnClientHeartbeat != nil {
			go func() { c.Broker.OnClientHeartbeat(c) }()
		}
	}
}
