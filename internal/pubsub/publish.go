package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}
	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}
	ctx := context.Background()
	return ch.PublishWithContext(ctx, exchange, key, false, false, msg)
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	channel, _, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	consumeCh, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for message := range consumeCh {
			var data T
			err := json.Unmarshal(message.Body, &data)
			if err != nil {
				continue
			}
			switch handler(data) {
			case Ack:
				message.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				message.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				message.Nack(false, true)
				fmt.Println("NackRequeue")
			}

		}
	}()
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := channel.QueueDeclare(
		queueName,
		queueType == Durable,
		queueType != Durable,
		queueType != Durable,
		false,
		amqp.Table{"x-dead-letter-exchange": "peril_dlx"},
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	err = channel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return channel, queue, nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var body bytes.Buffer
	encoder := gob.NewEncoder(&body)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}
	msg := amqp.Publishing{
		ContentType: "application/gob",
		Body:        body.Bytes(),
	}
	ctx := context.Background()
	return ch.PublishWithContext(ctx, exchange, key, false, false, msg)
}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	channel, _, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}
	err = channel.Qos(10, 0, false)
	if err != nil {
		return err
	}
	consumeCh, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for message := range consumeCh {
			var data T
			dec := gob.NewDecoder(bytes.NewBuffer(message.Body))
			err := dec.Decode(&data)
			if err != nil {
				continue
			}
			switch handler(data) {
			case Ack:
				message.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				message.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				message.Nack(false, true)
				fmt.Println("NackRequeue")
			}

		}
	}()
	return nil
}
