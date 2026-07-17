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

	gamestate := gamelogic.NewGameState(username)

	for {
		commands := gamelogic.GetInput()
		if len(commands) == 0 {
			continue
		} else if commands[0] == "spawn" {
			err = gamestate.CommandSpawn(commands)
			if err != nil {
				fmt.Println("Error: ", err)
			}
			continue
		} else if commands[0] == "move" {
			_, err := gamestate.CommandMove(commands)
			if err != nil {
				fmt.Println("Error: ", err)
			}
			continue
		} else if commands[0] == "status" {
			gamestate.CommandStatus()
			continue
		} else if commands[0] == "help" {
			gamelogic.PrintClientHelp()
			continue
		} else if commands[0] == "spam" {
			fmt.Println("Spamming not allowed yet!")
			continue
		} else if commands[0] == "quit" {
			gamelogic.PrintQuit()
			break
		} else {
			fmt.Println("Error: command not recognized")
			continue
		}
	}

}
