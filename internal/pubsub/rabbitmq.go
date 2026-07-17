package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {

	dat, err := json.Marshal(val)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        dat,
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
	if err != nil {
		return err
	}

	return nil
}

type SimpleQueueType struct {
	Durable   bool
	Transient bool
}

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

	queue, err := channel.QueueDeclare(queuename, queueType.Durable, queueType.Transient, queueType.Transient, false, nil)
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
	handler func(T),
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
		for msg := range deliveryChan {
			var message T
			err = json.Unmarshal(msg.Body, &message)
			if err != nil {
				fmt.Println("Error: ", err)
			} else {
				handler(message)
			}
			if e := msg.Ack(false); e != nil {
				fmt.Println("Error: ", e)
			}

		}

	}()

	return nil
}
