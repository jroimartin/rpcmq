package main

import (
	"log"
	"strings"
	"time"

	"github.com/jroimartin/rpcmq"
)

func main() {
	s := rpcmq.NewServer("amqp://localhost:5672", "rcp-queue")
	if err := s.Init(); err != nil {
		log.Fatalf("Init: %v", err)
	}
	defer s.Shutdown()

	if err := s.Register("toUpper", toUpper); err != nil {
		log.Fatalf("Register: %v", err)
	}

	<-time.After(10 * time.Second)
}

func toUpper(data []byte) ([]byte, error) {
	return []byte(strings.ToUpper(string(data))), nil
}
