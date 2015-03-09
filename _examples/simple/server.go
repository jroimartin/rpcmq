// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"strings"
	"time"

	"github.com/jroimartin/rpcmq"
)

func main() {
	s := rpcmq.NewServer("amqp://amqp_broker:5672", "rcp-queue",
		"rpc-exchange", "fanout")
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
	log.Printf("Received: toUpper(%v)\n", string(data))
	return []byte(strings.ToUpper(string(data))), nil
}
