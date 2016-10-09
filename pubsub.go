package magina

import (
	"fmt"
	"strings"

	"github.com/streadway/amqp"
)

type PubSubExchanger struct {
	TopicQueue map[string]string
	TopicChan  map[string]chan ExchangeMessage
	MessageIds MessageIds
	Channel    *amqp.Channel
}

func NewPubSubExchanger(channel *amqp.Channel) *PubSubExchanger {
	return &PubSubExchanger{
		Channel: channel,
	}
}

func (pubsub *PubSubExchanger) Init() error {
	if pubsub.TopicQueue == nil {
		pubsub.TopicQueue = make(map[string]string)
	}

	if pubsub.TopicChan == nil {
		pubsub.TopicChan = make(map[string]chan ExchangeMessage)
	}

	return pubsub.Channel.ExchangeDeclare(
		defaultPubsubExchange, // name
		"topic",               // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
}

func (pubsub *PubSubExchanger) convMQTTTopic2AMQP(topic string) string {
	return strings.Replace(strings.Replace(topic, "/", ".", -1), "+", "*", -1)
}

func (pubsub *PubSubExchanger) convAMQPopic2MQTT(topic string) string {
	return strings.Replace(strings.Replace(topic, ".", "/", -1), "*", "+", -1)
}

func (pubsub *PubSubExchanger) Publish(msg ExchangeMessage) error {
	if pubsub.Channel == nil {
		return fmt.Errorf("client channel not ready")
	}
	err := pubsub.Channel.Publish(defaultPubsubExchange,
		pubsub.convMQTTTopic2AMQP(msg.Topic), false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg.Payload,
		})
	return err
}

func (pubsub *PubSubExchanger) Subscribe(topic string) (chan ExchangeMessage, error) {

	if pubsub.Channel == nil {
		return nil, fmt.Errorf("client channel not ready")
	}
	q, err := pubsub.Channel.QueueDeclare(
		"",    // name
		true,  // durable
		true,  // delete when usused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	err = pubsub.Channel.QueueBind(
		q.Name, // queue name
		pubsub.convMQTTTopic2AMQP(topic), // routing key
		defaultPubsubExchange,            // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	pubsub.TopicQueue[topic] = q.Name

	msgs, err := pubsub.Channel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}

	msgChan := make(chan ExchangeMessage)
	pubsub.TopicChan[topic] = msgChan
	go func() {
		for d := range msgs {
			msgChan <- ExchangeMessage{pubsub.convAMQPopic2MQTT(d.RoutingKey), d.Body}
		}
		close(msgChan)
	}()
	return pubsub.TopicChan[topic], nil
}

func (pubsub *PubSubExchanger) Unsubscribe(topic string) error {

	if pubsub.Channel == nil {
		return fmt.Errorf("client channel not ready")
	}

	if queueName, exist := pubsub.TopicQueue[topic]; exist {
		err := pubsub.Channel.QueueUnbind(queueName, pubsub.convMQTTTopic2AMQP(topic), defaultPubsubExchange, nil)
		delete(pubsub.TopicQueue, topic)

		close(pubsub.TopicChan[topic])
		delete(pubsub.TopicChan, topic)

		if err != nil {
			return err
		}
	}

	return nil
}
