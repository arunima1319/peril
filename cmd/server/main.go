package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	gamelogic.PrintServerHelp()

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

	/*
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt)
		s := <-signalChan
		fmt.Println("Got signal:", s, "shutting down program")
	*/

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		} else if words[0] == "pause" {
			fmt.Println("Sending a pause message...")

			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)
			if err != nil {
				log.Fatal("Error: ", err)
			}

			continue
		} else if words[0] == "resume" {
			fmt.Println("Sending a resume message...")

			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)
			if err != nil {
				log.Fatal("Error: ", err)
			}
			continue
		} else if words[0] == "quit" {
			fmt.Println("Exiting the program")
			break
		} else {
			fmt.Println("Could not understand command")
			continue
		}

	}

}
