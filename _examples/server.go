// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/jroimartin/rpcmq"
)

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: server uri")
		os.Exit(2)
	}
	uri := flag.Arg(0)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill)

	s := rpcmq.NewServer(uri, "rcp-queue")
	if err := s.Init(); err != nil {
		log.Fatalf("Init: %v", err)
	}
	defer s.Shutdown()

	if err := s.Register("reverse", reverse); err != nil {
		log.Fatalf("Register: %v", err)
	}
	if err := s.Register("toUpper", toUpper); err != nil {
		log.Fatalf("Register: %v", err)
	}

	<-sig
	log.Println("shutting down")
}

func toUpper(data []byte) ([]byte, error) {
	log.Println("executing toUpper")
	return []byte(strings.ToUpper(string(data))), nil
}

func reverse(data []byte) ([]byte, error) {
	log.Println("executing reverse")
	a := make([]byte, len(data))
	copy(a, data)
	for i := 0; i < len(a)/2; i++ {
		a[i], a[len(a)-1-i] = a[len(a)-1-i], a[i]
	}
	return a, nil
}
