package queue

import (
	"errors"
	"github.com/djukela17/trading-tool-2/internal/logger"
	"github.com/streadway/amqp"
	"time"
)

type Queue struct {
	addr string
	name string
	key  string

	connection    *amqp.Connection
	channel       *amqp.Channel
	queue         amqp.Queue
	notifyClose   chan *amqp.Error
	notifyConfirm chan amqp.Confirmation

	done        chan struct{}
	isConnected bool
}

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When resending messages the server didn't confirm
	//resendDelay = 5 * time.Second
)

var (
	errNotConnected = errors.New("not connected to the queue")
	//errNotConfirmed  = errors.New("message not confirmed")
	errAlreadyClosed = errors.New("already closed: not connected to the queue")
)

func NewQueue(addr, name, key string) *Queue {
	queue := Queue{
		addr: addr,
		name: name,
		key:  key,
		done: make(chan struct{}),
	}
	//go queue.handleReconnect(addr)
	return &queue
}

func (q *Queue) Start() {
	go q.handleReconnect(q.addr)
}

func (q *Queue) handleReconnect(addr string) {
	for {
		q.isConnected = false
		logger.Debug("RabbitMQ - attempting to connect")

		for !q.connect(addr) {
			logger.Notice("RabbitMQ - failed to connect. Retrying ...")
			time.Sleep(reconnectDelay)
		}
		select {
		case <-q.done:
			return
		case <-q.notifyClose:
		}
	}
}

func (q *Queue) connect(addr string) bool {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return false
	}
	ch, err := conn.Channel()
	if err != nil {
		return false
	}

	err = ch.ExchangeDeclare(
		q.name,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return false
	}

	queue, err := ch.QueueDeclare(
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return false
	}

	err = ch.QueueBind(
		"",
		q.key,
		q.name,
		false,
		nil,
	)
	if err != nil {
		return false
	}

	q.changeConnection(conn, ch, queue)
	q.isConnected = true
	logger.Debug("RabbitMQ - connection success!")

	return true
}

func (q *Queue) changeConnection(connection *amqp.Connection, channel *amqp.Channel, queue amqp.Queue) {
	q.connection = connection
	q.channel = channel
	q.queue = queue
	q.notifyClose = make(chan *amqp.Error)
	q.notifyConfirm = make(chan amqp.Confirmation)
	q.channel.NotifyClose(q.notifyClose)
	q.channel.NotifyPublish(q.notifyConfirm)
}

func (q *Queue) UnsafePush(data []byte) error {
	if !q.isConnected {
		return errNotConnected
	}

	return q.channel.Publish(
		q.name,
		q.key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		},
	)
}

func (q *Queue) Consume() <-chan []byte {
	logger.Debug("rabbit mq consuming now")

	dataCh := make(chan []byte, 16)

	go func(chan<- []byte) {
		for {
			deliveryCh, err := q.Stream()
			if err != nil {
				logger.Warning("error while opening a stream, retry in 2")
				logger.Warning(err)
				time.Sleep(2 * time.Second)
				continue
			}
			logger.Debug("RabbitMQ - stream opened successfully!")

			for msg := range deliveryCh {
				logger.Debug("received a message: ", string(msg.Body))
				dataCh <- msg.Body
			}
		}
	}(dataCh)

	return dataCh
}

func (q *Queue) Stream() (<-chan amqp.Delivery, error) {
	if !q.isConnected {
		return nil, errNotConnected
	}
	return q.channel.Consume(
		q.queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

func (q *Queue) Close() error {
	if !q.isConnected {
		return errAlreadyClosed
	}
	err := q.channel.Close()
	if err != nil {
		return err
	}
	err = q.connection.Close()
	if err != nil {
		return err
	}
	close(q.done)
	q.isConnected = false
	return nil

}
