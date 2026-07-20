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

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal("Error: ", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error: ", err)
	}

	gamestate := gamelogic.NewGameState(username)

	/*
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
	*/

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.SimpleQueueType{Durable: false, Transient: true},
		handlerPause(gamestate),
	)
	if err != nil {
		fmt.Println("Error: ", err)
	}

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+username,
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueType{Durable: false, Transient: true},
		handlerMove(gamestate, channel),
	)

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueType{Durable: true, Transient: false},
		handlerWar(gamestate, channel),
	)

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
			armyMove, err := gamestate.CommandMove(commands)
			if err != nil {
				fmt.Println("Error: ", err)
			} else {
				err = pubsub.PublishJSON(
					channel,
					routing.ExchangePerilTopic,
					routing.ArmyMovesPrefix+"."+username,
					armyMove,
				)
				fmt.Println("Move was published sucessfully!")
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
