package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	conString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(conString)
	if err != nil {
		log.Fatal("Error: ", err)
	}
	defer connection.Close()
	fmt.Println("The connection was successful...")

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal("Error: ", err)
	}

	err = pubsub.PublishJSON(
		channel,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{IsPaused: true},
	)
	if err != nil {
		log.Fatal("Error: ", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	s := <-signalChan
	fmt.Println("Got signal:", s, "shutting down program")

}
