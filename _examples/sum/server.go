// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"os"

	"github.com/jroimartin/rpcmq"
)

func main() {
	rpcmq.Log = log.New(os.Stderr, "server ", log.LstdFlags)

	s := rpcmq.NewServer("amqp://amqp_broker:5672",
		"rpc-queue", "rpc-exchange", "direct")
	if err := s.Register("echo", echo); err != nil {
		log.Fatalf("Register: %v", err)
	}
	if err := s.Init(); err != nil {
		log.Fatalf("Init: %v", err)
	}
	defer s.Shutdown()

	select {}
}

func echo(id string, data []byte) ([]byte, error) {
	log.Printf("Received (%v): echo(%v)\n", id, string(data))
	return data, nil
}
