package main

import "fmt"
import "os"
import "os/signal"
import "github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
import "github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
import (
    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	url := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(url)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	fmt.Println("Connection established successfully!")

	channel, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer channel.Close()
	fmt.Println("Channel created successfully!")

	err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		panic(err)
	}
	fmt.Println("Message published successfully!")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down Peril server...")
}
