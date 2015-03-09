// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"time"

	"github.com/jroimartin/rpcmq"
)

func main() {
	c := rpcmq.NewClient("amqp://amqp_broker:5672", "rcp-queue",
		"rpc-exchange", "fanout")
	if err := c.Init(); err != nil {
		log.Fatalf("Init: %v", err)
	}
	defer c.Shutdown()

	go func() {
		for {
			data := []byte("Hello gophers!")
			uuid, err := c.Call("toUpper", data, 0)
			if err != nil {
				log.Println("Call:", err)
			}
			log.Printf("Sent: toUpper(%v) (%v)\n", string(data), uuid)
			<-time.After(500 * time.Millisecond)
		}
	}()

	go func() {
		for r := range c.Results() {
			if r.Err != "" {
				log.Printf("Received error: %v (%v)", r.Err, r.UUID)
				continue
			}
			log.Printf("Received: %v (%v)\n", string(r.Data), r.UUID)
		}
	}()

	<-time.After(10 * time.Second)
}
