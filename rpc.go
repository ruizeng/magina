package magina

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/streadway/amqp"
)

// RPCExchanger is the exchange in RabbitMQ for rpcs.
type RPCExchanger struct {
	Channel          *amqp.Channel
	RPCQueues        map[string]string
	RPCCorrelationID map[string]string
	MsgChan          map[string]chan ExchangeMessage
}

// NewPRCExchanger creates a new rpc exchanger.
func NewPRCExchanger(channel *amqp.Channel) *RPCExchanger {
	return &RPCExchanger{
		Channel: channel,
	}
}

// Init exchange
func (rpc *RPCExchanger) Init() error {
	rpc.RPCQueues = make(map[string]string)
	rpc.RPCCorrelationID = make(map[string]string)
	rpc.MsgChan = make(map[string]chan ExchangeMessage)
	return rpc.Channel.ExchangeDeclare(
		defaultRPCExchange, // name
		"direct",           // type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
}

func (rpc *RPCExchanger) getRPCMethod(topic string) (reqOrResp string, methodName string) {
	prefixRequest := "rpc://request/"
	prefixResponse := "rpc://response/"
	if strings.HasPrefix(topic, prefixRequest) {
		return "request", strings.TrimPrefix(topic, prefixRequest)
	} else if strings.HasPrefix(topic, prefixResponse) {
		return "response", strings.TrimPrefix(topic, prefixResponse)
	} else {
		return "", topic
	}
}

func (rpc *RPCExchanger) randomString(l int) string {
	min, max := 65, 90
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(min + rand.Intn(max-min))
	}
	return string(bytes)
}

// Publish send a rpc request or response
func (rpc *RPCExchanger) Publish(msg ExchangeMessage) error {
	if rpc.Channel == nil {
		return fmt.Errorf("client channel not ready")
	}

	reqOrResp, method := rpc.getRPCMethod(msg.Topic)

	if reqOrResp == "response" {
		return nil
	} else if reqOrResp == "request" {
		// send rpc request as rpc client.
		fmt.Println("send rpc request as rpc client:", method)
		rpc.RPCCorrelationID[method] = rpc.randomString(32)
		err := rpc.Channel.Publish(
			defaultRPCExchange, // exchange
			method,             // routing key
			false,              // mandatory
			false,              // immediate
			amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: rpc.RPCCorrelationID[method],
				ReplyTo:       rpc.RPCQueues[method],
				Body:          msg.Payload,
			})
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unknown request or response type : %v", method)
	}
	return nil
}

// Subscribe waits for prc request or response.
func (rpc *RPCExchanger) Subscribe(topic string) (chan ExchangeMessage, error) {
	if rpc.Channel == nil {
		return nil, fmt.Errorf("client channel not ready")
	}

	reqOrResp, method := rpc.getRPCMethod(topic)
	if reqOrResp == "response" {
		// receive rpc response as rpc client.
		q, err := rpc.Channel.QueueDeclare(
			"",    // name
			true,  // durable
			true,  // delete when unused
			true,  // exclusive
			false, // noWait
			nil,   // arguments
		)
		if err != nil {
			return nil, err
		}

		err = rpc.Channel.QueueBind(
			q.Name,             // queue name
			q.Name,             // routing key
			defaultRPCExchange, // exchange
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}

		rpc.RPCQueues[method] = q.Name

		msgs, err := rpc.Channel.Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		msgChan := make(chan ExchangeMessage)
		rpc.MsgChan[method] = msgChan
		go func() {
			for d := range msgs {
				fmt.Println("received rpc response: ", method, d)
				if rpc.RPCCorrelationID[method] == d.CorrelationId {
					msgChan <- ExchangeMessage{topic, d.Body}
				}
			}
			close(msgChan)
		}()

		return rpc.MsgChan[method], nil
	} else if reqOrResp == "request" {
		return nil, nil
	} else {
		return nil, fmt.Errorf("unknown request or response type : %v", method)
	}
}

// Unsubscribe rpc topic.
func (rpc *RPCExchanger) Unsubscribe(topic string) error {
	if rpc.Channel == nil {
		return fmt.Errorf("client channel not ready")
	}

	_, method := rpc.getRPCMethod(topic)

	if queueName, exist := rpc.RPCQueues[method]; exist {
		err := rpc.Channel.QueueUnbind(queueName, topic, defaultPubsubExchange, nil)
		delete(rpc.RPCQueues, method)
		delete(rpc.RPCCorrelationID, method)

		close(rpc.MsgChan[method])
		delete(rpc.MsgChan, method)

		if err != nil {
			return err
		}
	}
	return nil
}
