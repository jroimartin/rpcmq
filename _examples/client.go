// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jroimartin/rpcmq"
)

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: client uri")
		os.Exit(2)
	}
	uri := flag.Arg(0)

	c := rpcmq.NewClient(uri, "rcp-queue")
	if err := c.Init(); err != nil {
		log.Fatalf("Init: %v", err)
	}
	defer c.Shutdown()

	go func() {
		for {
			data := []byte("Hello gophers!")
			uuid, err := c.Call("reverse", data)
			if err != nil {
				log.Println("Call:", err)
			}
			log.Printf("Sent: reverse(%v) (%v)\n", string(data), uuid)
			uuid, err = c.Call("toUpper", data)
			if err != nil {
				log.Println("Call:", err)
			}
			log.Printf("Sent: toUpper(%v) (%v)\n", string(data), uuid)
			time.Sleep(200 * time.Millisecond)
		}
	}()

	go func() {
		for {
			r := <-c.Results()
			if r.Err != "" {
				log.Printf("Received error: %v (%v)", r.Err, r.UUID)
				continue
			}
			log.Printf("Received: %v (%v)\n", string(r.Data), r.UUID)
		}
	}()

	select {}
}
