package pubsub

import (
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType struct {
	Durable   bool
	Transient bool
}

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queuename,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	args := amqp.Table{"x-dead-letter-exchange": routing.ExchangeDeadLetter}

	queue, err := channel.QueueDeclare(queuename, queueType.Durable, queueType.Transient, queueType.Transient, false, args)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = channel.QueueBind(queuename, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return channel, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queuename,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	channel, _, err := DeclareAndBind(conn, exchange, queuename, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := channel.Consume(queuename, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		var ackType AckType
		for msg := range deliveryChan {
			var message T
			err = json.Unmarshal(msg.Body, &message)
			if err != nil {
				fmt.Println("Error: ", err)
			} else {
				ackType = handler(message)
			}
			switch ackType {
			case Ack:
				if e := msg.Ack(false); e != nil {
					fmt.Println("Error: ", e)
				}
				fmt.Println("Ack occurred!")
			case NackRequeue:
				if e := msg.Nack(false, true); e != nil {
					fmt.Println("Error: ", e)
				}
				fmt.Println("NackRequeue occurred!")
			case NackDiscard:
				if e := msg.Nack(false, false); e != nil {
					fmt.Println("Error :", e)
				}
				fmt.Println("NackDiscard Occurred!")
			default:
				fmt.Println("Invalid AckType!")
			}

		}

	}()

	return nil
}
