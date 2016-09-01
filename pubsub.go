package magina

import (
	"fmt"
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

func (pubsub *PubSubExchanger) Publish(msg ExchangeMessage) error {
	if pubsub.Channel == nil {
		return fmt.Errorf("client channel not ready")
	}
	err := pubsub.Channel.Publish(defaultPubsubExchange,
		msg.Topic, false, false,
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
		topic,  // routing key
		defaultPubsubExchange, // exchange
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
			msgChan <- ExchangeMessage{d.RoutingKey, d.Body}
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
		err := pubsub.Channel.QueueUnbind(queueName, topic, defaultPubsubExchange, nil)
		delete(pubsub.TopicQueue, topic)

		close(pubsub.TopicChan[topic])
		delete(pubsub.TopicChan, topic)

		if err != nil {
			return err
		}
	}

	return nil
}
