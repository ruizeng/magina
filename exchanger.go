package magina

const (
	defaultPubsubExchange = "pubsub"
	defaultRPCExchange    = "rpc"
)

// ExchangeMessage is a message in exchange
type ExchangeMessage struct {
	Topic   string
	Payload []byte
}

// Exchanger is an interface for handling pub/sub messages
type Exchanger interface {
	// init the exchanger
	Init() error
	// publish a message.
	Publish(ExchangeMessage) error
	// subsctibe a topic and return a channel to receive message.
	Subscribe(topic string) (chan ExchangeMessage, error)
	// unsubstring a topic
	Unsubscribe(topic string) error
}
