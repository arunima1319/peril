package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	conString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(conString)
	if err != nil {
		log.Fatal("Error: ", err)
	}
	defer connection.Close()
	fmt.Println("The connection was successful...")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error: ", err)
	}

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	_, _, err = pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.SimpleQueueType{Durable: false, Transient: true},
	)
	if err != nil {
		fmt.Println("Error: ", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	s := <-signalChan
	fmt.Println("Got signal:", s, "shutting down program")

}
