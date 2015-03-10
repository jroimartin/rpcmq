# rpcmq [![GoDoc](https://godoc.org/github.com/jroimartin/rpcmq?status.svg)](https://godoc.org/github.com/jroimartin/rpcmq)

## Introduction

The package rpcmq implements an RPC protocol over AMQP.

## Installation

`go get github.com/jroimartin/rpcmq`

## Documentation

`godoc github.com/jroimartin/rpcmq`

## Examples

The following snippets show how easy is to implement both an RPC client and
server using rpcmq. In this example the server registers a new method called
"toUpper" that takes an string and convert it to uppercase. On the other hand,
the RPC client will invoke this method remotely. Then, after 10 seconds, the
client and the server will shutdown.

**server.go**

```go
package main

import (
	"log"
	"strings"
	"time"

	"github.com/jroimartin/rpcmq"
)

func main() {
	s := rpcmq.NewServer("amqp://amqp_broker:5672", "rcp-queue",
		"rpc-exchange", "direct")
	if err := s.Register("toUpper", toUpper); err != nil {
		log.Fatalf("Register: %v", err)
	}
	if err := s.Init(); err != nil {
		log.Fatalf("Init: %v", err)
	}
	defer s.Shutdown()

	<-time.After(10 * time.Second)
}

func toUpper(data []byte) ([]byte, error) {
	log.Printf("Received: toUpper(%v)\n", string(data))
	return []byte(strings.ToUpper(string(data))), nil
}
```

**client.go**

```go
package main

import (
	"log"
	"time"

	"github.com/jroimartin/rpcmq"
)

func main() {
	c := rpcmq.NewClient("amqp://amqp_broker:5672", "rcp-queue",
		"rpc-exchange", "direct")
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
```

This code will generate the following output:

![screen shot 2015-03-03 at 11 56 33](https://cloud.githubusercontent.com/assets/1223476/6461024/73a3ea1e-c19c-11e4-9e4e-a385e9246178.png)
