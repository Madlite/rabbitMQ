package pubsub

import "context"
import "encoding/json"
import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}
	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
	}
	ctx := context.Background()
	return ch.PublishWithContext(ctx, exchange, key, false, false, msg)
}