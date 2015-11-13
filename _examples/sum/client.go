// Copyright 2015 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jroimartin/rpcmq"
)

const iter = 500

func main() {
	rpcmq.Log = log.New(os.Stderr, "client ", log.LstdFlags)

	c := rpcmq.NewClient("amqp://amqp_broker:5672",
		"rpc-queue", "rpc-client", "rpc-exchange", "direct")
	if err := c.Init(); err != nil {
		log.Fatalf("Init: %v", err)
	}
	defer c.Shutdown()

	rets := map[int]int{}
	for i := 0; i < iter; i++ {
		rets[i] = 0
	}
	total := 0
	for i := 0; i < iter; i++ {
		total += i
	}

	go func() {
		for i := 0; i < iter; i++ {
			var uuid string
			var err error
			data := []byte(strconv.Itoa(i))
			for {
				uuid, err = c.Call("echo", data, 0)
				if err == nil {
					break
				}
				log.Println("Call:", err)
				time.Sleep(100 * time.Millisecond)
			}
			log.Printf("Sent: echo(%v) (%v)\n", string(data), uuid)
			time.Sleep(100 * time.Millisecond)
		}
	}()

	go func() {
		ctr := 0
		for r := range c.Results() {
			if r.Err != "" {
				log.Printf("Received error: %v (%v)", r.Err, r.UUID)
				continue
			}
			n, err := strconv.Atoi(string(r.Data))
			if err != nil {
				log.Fatalf("Atoi: %v", err)
			}
			log.Printf("Received: %v (%v)\n", string(r.Data), r.UUID)

			rets[n] = rets[n] + 1
			ctr += n
			log.Println(rets)
			log.Printf("%v/%v", ctr, total)
		}
	}()

	select {}
}
