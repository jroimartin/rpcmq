// Copyright 2014 The rpcmq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/jroimartin/rpcmq"
)

func main() {
	certFile := os.Getenv("RPCMQ_CERT")
	keyFile := os.Getenv("RPCMQ_KEY")
	caFile := os.Getenv("RPCMQ_CA")

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("LoadX509KeyPair: %v", err)
	}
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		log.Fatalf("ReadFile: %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	c := rpcmq.NewClient("amqps://amqp_broker:5671", "rcp-queue",
		"rpc-exchange", "direct")
	c.TLSConfig = tlsConfig
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
